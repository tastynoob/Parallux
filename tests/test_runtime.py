from __future__ import annotations

import io
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from contextlib import contextmanager, redirect_stderr
from pathlib import Path
from unittest import mock

from parallux import (
    CommandFailure,
    ParalluxError,
    Runtime,
    SSHTransportFailure,
)
from parallux._core import Goal, RuntimeOptions


ROOT = Path(__file__).resolve().parents[1]


@contextmanager
def runtime_goal(config_path: Path):
    options = RuntimeOptions(config_path=config_path)
    goal = Goal(mode="runtime", options=options)
    runtime = Runtime(goal, options)
    goal.bind_runtime(runtime)
    try:
        yield goal
    finally:
        runtime.shutdown(cancel=True)


def install_fake_ssh(directory: Path) -> None:
    script = directory / "ssh"
    script.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -euo pipefail

            args=("$@")
            target=""
            i=0
            while (( i < ${#args[@]} )); do
                case "${args[$i]}" in
                    -p|-o)
                        i=$((i + 2))
                        ;;
                    -*)
                        i=$((i + 1))
                        ;;
                    *)
                        target="${args[$i]}"
                        break
                        ;;
                esac
            done

            if [[ "$target" == "bad-transport" ]]; then
                echo "ssh: simulated transport failure" >&2
                exit 255
            fi

            cmd_index=$((i + 1))
            shell="${args[$cmd_index]}"
            flag="${args[$((cmd_index + 1))]}"
            remote="${args[$((cmd_index + 2))]}"

            if [[ "$target" == "flaky" && "$remote" != "true" ]]; then
                echo "ssh: simulated command transport failure" >&2
                exit 255
            fi

            if [[ "$target" == "drop-stdin" ]]; then
                eval "$shell $flag $remote" </dev/null
            else
                eval "$shell $flag $remote"
            fi
            """
        ),
        encoding="utf-8",
    )
    script.chmod(0o755)


@contextmanager
def fake_ssh_env():
    with tempfile.TemporaryDirectory() as tempdir:
        fakebin = Path(tempdir)
        install_fake_ssh(fakebin)
        path = f"{fakebin}{os.pathsep}{os.environ.get('PATH', '')}"
        with mock.patch.dict(os.environ, {"PATH": path}):
            yield


class RuntimeTests(unittest.TestCase):
    def test_local_success_and_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            with runtime_goal(Path(tempdir) / "config.py") as goal:
                goal.local("local", workspace=str(workspace), max_jobs=2)
                goal.setRunner("local")

                result = goal.run("echo ok", work_relpath="ok").sync()
                self.assertEqual(result.returncode, 0)
                self.assertEqual(result.work_dir, str(workspace / "ok"))
                self.assertEqual((workspace / "ok" / "stdout.txt").read_text().strip(), "ok")

                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    with self.assertRaises(CommandFailure) as raised:
                        goal.run("echo bad >&2; exit 5", work_relpath="bad").sync()
                self.assertEqual(raised.exception.returncode, 5)
                self.assertEqual(raised.exception.work_dir, str(workspace / "bad"))
                self.assertEqual(stderr.getvalue(), f"command failed: {workspace / 'bad'}\n")
                self.assertEqual((workspace / "bad" / "stderr.txt").read_text().strip(), "bad")

    def test_cli_dry_run_writes_local_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            config = Path(tempdir) / "config.py"
            config.write_text(
                textwrap.dedent(
                    f"""\
                    from parallux import goal

                    goal.local("local", workspace={str(workspace)!r})
                    goal.setRunner("local")
                    goal.schd("echo cli", work_relpath="cli")
                    goal.issue().sync()
                    """
                ),
                encoding="utf-8",
            )
            env = os.environ.copy()
            env["HOME"] = str(Path(tempdir) / "home")
            env["PYTHONPATH"] = f"{ROOT}{os.pathsep}{env.get('PYTHONPATH', '')}"

            result = subprocess.run(
                [sys.executable, "-m", "parallux", "--dry-run", str(config)],
                cwd=ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stderr, "")
            stdout = (workspace / "cli" / "stdout.txt").read_text()
            self.assertIn("[dry-run] echo cli", stdout)

    def test_scheduler_receives_available_runners(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            left_workspace = Path(tempdir) / "left"
            right_workspace = Path(tempdir) / "right"
            seen: list[tuple[list[str], int]] = []

            with runtime_goal(Path(tempdir) / "config.py") as goal:
                goal.local(
                    "left",
                    workspace=str(left_workspace),
                    core_pool=range(0, 1),
                )
                goal.local(
                    "right",
                    workspace=str(right_workspace),
                    core_pool=range(0, 4),
                )
                goal.setRunner(["left", "right"])

                def allocate_runner(runners, need_threads: int):
                    seen.append(([runner.name for runner in runners], need_threads))
                    return runners[0]

                goal.setScheduler(allocate_runner)

                result = goal.run(
                    "echo runner=$PARALLUX_RUNNER",
                    threads=2,
                    work_relpath="picked",
                ).sync()

            self.assertEqual(seen, [(["right"], 2)])
            self.assertEqual(result.runner, "right")
            self.assertEqual(result.cores, (0, 1))
            self.assertEqual(result.work_dir, str(right_workspace / "picked"))
            self.assertEqual(
                (right_workspace / "picked" / "stdout.txt").read_text().strip(),
                "runner=right",
            )

    def test_scheduler_cannot_return_runner_outside_available_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            left_workspace = Path(tempdir) / "left"
            right_workspace = Path(tempdir) / "right"

            with runtime_goal(Path(tempdir) / "config.py") as goal:
                left = goal.local(
                    "left",
                    workspace=str(left_workspace),
                    core_pool=range(0, 1),
                )
                goal.local(
                    "right",
                    workspace=str(right_workspace),
                    core_pool=range(0, 4),
                )
                goal.setRunner(["left", "right"])
                goal.setScheduler(lambda runners, need_threads: left)

                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    with self.assertRaises(ParalluxError) as raised:
                        goal.run("echo never", threads=2, work_relpath="invalid").sync()

            self.assertIn(
                "scheduler must return one of the available runners",
                str(raised.exception),
            )

    def test_parallel_above_available_cores_waits_for_core_release(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"

            with runtime_goal(Path(tempdir) / "config.py") as goal:
                goal.setParallel(4)
                goal.local("local", workspace=str(workspace), core_pool=range(0, 1))
                goal.setRunner("local")

                first = goal.run(
                    "sleep 0.1; echo first",
                    threads=1,
                    work_relpath="first",
                )
                second = goal.run("echo second", threads=1, work_relpath="second")

                first_result = first.sync(timeout=2)
                second_result = second.sync(timeout=2)

            self.assertEqual(first_result.cores, (0,))
            self.assertEqual(second_result.cores, (0,))
            self.assertEqual(
                (workspace / "second" / "stdout.txt").read_text().strip(),
                "second",
            )

    def test_unschedulable_thread_request_fails_instead_of_waiting(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            workspace = Path(tempdir) / "workspace"
            with runtime_goal(Path(tempdir) / "config.py") as goal:
                goal.local("local", workspace=str(workspace), core_pool=range(0, 1))
                goal.setRunner("local")

                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    with self.assertRaises(ParalluxError) as raised:
                        goal.run("echo never", threads=2, work_relpath="too-big").sync(
                            timeout=1
                        )

            self.assertIn("no runner can satisfy", str(raised.exception))

    def test_fake_ssh_remote_success_and_command_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir, fake_ssh_env():
            workspace = Path(tempdir) / "remote"
            with runtime_goal(Path(tempdir) / "config.py") as goal:
                runner = goal.ssh("fake", host="fake", workspace=str(workspace))
                goal.setRunner(runner)

                result = goal.run("echo ok", work_relpath="ok").sync()
                self.assertEqual(result.returncode, 0)
                self.assertEqual((workspace / "ok" / "stdout.txt").read_text().strip(), "ok")

                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    with self.assertRaises(CommandFailure) as raised:
                        goal.run("echo user-error >&2; exit 7", work_relpath="fail").sync()
                self.assertEqual(raised.exception.returncode, 7)
                self.assertEqual(stderr.getvalue(), f"command failed: {workspace / 'fail'}\n")
                self.assertEqual(
                    (workspace / "fail" / "stderr.txt").read_text().strip(),
                    "user-error",
                )

                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    with self.assertRaises(CommandFailure) as raised:
                        goal.run("exit 255", work_relpath="exit255").sync()
                self.assertEqual(raised.exception.returncode, 255)
                self.assertEqual(
                    stderr.getvalue(),
                    f"command failed: {workspace / 'exit255'}\n",
                )

    def test_ssh_probe_failure_reports_diagnostic(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir, fake_ssh_env():
            with runtime_goal(Path(tempdir) / "config.py") as goal:
                with self.assertRaises(ParalluxError) as raised:
                    goal.ssh(
                        "bad-transport",
                        host="bad-transport",
                        workspace=str(Path(tempdir) / "remote"),
                    )

        message = str(raised.exception)
        self.assertIn("ssh runner unavailable", message)
        self.assertIn("returncode=255", message)
        self.assertIn("ssh: simulated transport failure", message)

    def test_ssh_transport_failure_is_separate_from_command_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir, fake_ssh_env():
            workspace = Path(tempdir) / "remote"
            with runtime_goal(Path(tempdir) / "config.py") as goal:
                runner = goal.ssh("flaky", host="flaky", workspace=str(workspace))
                goal.setRunner(runner)

                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    with self.assertRaises(SSHTransportFailure) as raised:
                        goal.run("echo never", work_relpath="transport").sync()

        self.assertEqual(raised.exception.returncode, 255)
        self.assertEqual(raised.exception.work_dir, str(workspace / "transport"))
        self.assertIn("ssh transport failed: runner=flaky; target=flaky", stderr.getvalue())
        self.assertIn("ssh: simulated command transport failure", stderr.getvalue())

    def test_remote_watchdog_kills_task_when_ssh_stdin_closes(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir, fake_ssh_env():
            workspace = Path(tempdir) / "remote"
            done = Path(tempdir) / "watchdog-done"
            with runtime_goal(Path(tempdir) / "config.py") as goal:
                runner = goal.ssh("drop-stdin", host="drop-stdin", workspace=str(workspace))
                goal.setRunner(runner)

                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    with self.assertRaises(CommandFailure):
                        goal.run(f"sleep 2; touch {done}", work_relpath="watchdog").sync()

            self.assertFalse(done.exists())
            self.assertEqual(stderr.getvalue(), f"command failed: {workspace / 'watchdog'}\n")


if __name__ == "__main__":
    unittest.main()
