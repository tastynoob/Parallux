from __future__ import annotations

import os
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Mapping, Sequence

from ._core import CommandSpec, ParalluxError, RunnerSpec, RuntimeOptions, _shell_join


DEFAULT_WORKSPACE = "~/parallux"


@dataclass(frozen=True)
class CommandResult:
    runner: str
    command: str
    returncode: int
    work_relpath: str
    work_dir: str
    command_path: str
    stdout_path: str
    stderr_path: str
    cores: tuple[int, ...]
    numa_node: int | None


class CommandFailure(ParalluxError):
    def __init__(
        self,
        *,
        returncode: int,
        work_dir: str,
        stderr_path: str,
    ) -> None:
        self.returncode = returncode
        self.work_dir = work_dir
        self.stderr_path = stderr_path
        super().__init__("command failed")


class SSHTransportFailure(ParalluxError):
    def __init__(
        self,
        *,
        returncode: int,
        work_dir: str,
        diagnostic: str,
    ) -> None:
        self.returncode = returncode
        self.work_dir = work_dir
        self.diagnostic = diagnostic
        super().__init__("ssh transport failed")


class TaskDispatchFailure(ParalluxError):
    def __init__(
        self,
        *,
        error: BaseException,
        work_dir: str | None = None,
        stderr_path: str | None = None,
    ) -> None:
        self.error = error
        self.work_dir = work_dir
        self.stderr_path = stderr_path
        if work_dir is None:
            message = f"task dispatch failed: {type(error).__name__}: {error}"
        else:
            message = "task dispatch failed before command execution"
        super().__init__(message)


@dataclass(frozen=True)
class CommandPaths:
    work_relpath: str
    work_dir: str
    command_path: str
    stdout_path: str
    stderr_path: str


@dataclass
class ProcessPlan:
    spec: CommandSpec
    runner: RunnerSpec
    paths: CommandPaths
    argv: list[str]
    env: dict[str, str] | None
    cwd: str | None
    full_command: str
    local_artifacts: bool
    cores: tuple[int, ...]
    numa_node: int | None


@dataclass
class ActiveProcess:
    plan: ProcessPlan
    process: subprocess.Popen[bytes]
    stdout: BinaryIO | None
    stderr: BinaryIO | None
    ssh_stderr: BinaryIO | None
    started_at: float


@dataclass(frozen=True)
class ProcessCompletion:
    result: CommandResult | None = None
    error: BaseException | None = None


class ProcessExecutor:
    def __init__(self, goal: Any, options: RuntimeOptions) -> None:
        self.goal = goal
        self.options = options

    def paths(
        self,
        spec: CommandSpec,
        runner: RunnerSpec,
        *,
        default_work_relpath: str,
    ) -> CommandPaths:
        relpath = spec.work_relpath or default_work_relpath
        workspace = runner.workspace or DEFAULT_WORKSPACE
        if runner.kind == "local":
            work_path = Path(workspace).expanduser()
            if relpath:
                work_path = work_path / relpath
            work_dir = str(work_path.resolve())
        else:
            work_dir = self._remote_join(workspace, relpath)
        command_path = self._join_path(work_dir, "command.txt", runner=runner)
        stdout_path = self._join_path(work_dir, "stdout.txt", runner=runner)
        stderr_path = self._join_path(work_dir, "stderr.txt", runner=runner)
        return CommandPaths(
            work_relpath=relpath,
            work_dir=work_dir,
            command_path=command_path,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )

    def plan(
        self,
        spec: CommandSpec,
        runner: RunnerSpec,
        *,
        paths: CommandPaths,
        full_command: str,
        cores: Sequence[int],
        numa_node: int | None,
    ) -> ProcessPlan:
        env = self._command_env(spec, runner, paths.work_dir, paths.work_relpath)
        if runner.kind == "local":
            Path(paths.work_dir).mkdir(parents=True, exist_ok=True)
            process_env = os.environ.copy()
            process_env.update(env)
            return ProcessPlan(
                spec=spec,
                runner=runner,
                paths=paths,
                argv=[runner.shell, "-lc", full_command],
                env=process_env,
                cwd=spec.cwd or paths.work_dir,
                full_command=full_command,
                local_artifacts=True,
                cores=tuple(cores),
                numa_node=numa_node,
            )

        remote_cwd = spec.cwd or paths.work_dir
        remote_body = " && ".join(
            [
                f"mkdir -p {self._quote_remote_path(paths.work_dir)}",
                (
                    f"printf '%s\\n' {shlex.quote(spec.command)} "
                    f"> {self._quote_remote_path(paths.command_path)}"
                ),
                f"cd {self._quote_remote_path(remote_cwd)}",
                self._remote_watchdog_body(
                    runner,
                    env,
                    full_command,
                    stdout_path=paths.stdout_path,
                    stderr_path=paths.stderr_path,
                ),
            ]
        )
        argv = ["ssh", *runner.ssh_options, "-p", str(runner.port), runner.target]
        argv.extend([runner.shell, "-lc", shlex.quote(remote_body)])
        return ProcessPlan(
            spec=spec,
            runner=runner,
            paths=paths,
            argv=argv,
            env=None,
            cwd=None,
            full_command=full_command,
            local_artifacts=False,
            cores=tuple(cores),
            numa_node=numa_node,
        )

    def start(self, plan: ProcessPlan) -> ActiveProcess | ProcessCompletion:
        stdout: BinaryIO | None = None
        stderr: BinaryIO | None = None
        ssh_stderr: BinaryIO | None = None
        if plan.local_artifacts:
            Path(plan.paths.work_dir).mkdir(parents=True, exist_ok=True)
            with Path(plan.paths.command_path).open("w", encoding="utf-8") as fs:
                fs.write(plan.spec.command)
                fs.write("\n")
                fs.write(f"executor: {_shell_join(plan.argv)}\n")
            stdout = Path(plan.paths.stdout_path).open("wb")
            stderr = Path(plan.paths.stderr_path).open("wb")
        elif plan.runner.kind == "ssh":
            ssh_stderr = tempfile.TemporaryFile("w+b")
        if self.options.dry_run:
            try:
                if stdout is not None:
                    stdout.write(f"[dry-run] {plan.full_command}\n".encode("utf-8"))
            finally:
                self._close_streams(stdout, stderr, ssh_stderr)
            return ProcessCompletion(result=self._result(plan, returncode=0))

        try:
            process = subprocess.Popen(
                plan.argv,
                stdout=stdout if stdout is not None else subprocess.DEVNULL,
                stderr=(
                    stderr
                    if stderr is not None
                    else ssh_stderr
                    if ssh_stderr is not None
                    else subprocess.DEVNULL
                ),
                stdin=subprocess.PIPE if plan.runner.kind == "ssh" else None,
                env=plan.env,
                cwd=plan.cwd,
                start_new_session=True,
            )
        except BaseException:
            self._close_streams(stdout, stderr, ssh_stderr)
            raise
        return ActiveProcess(
            plan=plan,
            process=process,
            stdout=stdout,
            stderr=stderr,
            ssh_stderr=ssh_stderr,
            started_at=time.time(),
        )

    def poll(self, active: ActiveProcess) -> ProcessCompletion | None:
        plan = active.plan
        now = time.time()
        timed_out = (
            plan.spec.timeout is not None
            and now - active.started_at >= plan.spec.timeout
        )
        if timed_out and active.process.poll() is None:
            self._close_process_stdin(active.process)
            self._signal_process(active.process, signal.SIGKILL)
            if active.stderr is not None:
                active.stderr.write(
                    f"parallux: command timed out after {plan.spec.timeout}s\n".encode(
                        "utf-8"
                    )
                )
        returncode = active.process.poll()
        if returncode is None:
            return None

        self._close_process_stdin(active.process)
        ssh_stderr = self._read_and_close(active.ssh_stderr)
        self._close_streams(active.stdout, active.stderr)
        result = self._result(plan, returncode=returncode)
        if plan.runner.kind == "ssh" and self._is_ssh_transport_failure(
            returncode,
            ssh_stderr,
        ):
            self._report_ssh_transport_failure(
                runner=plan.runner,
                returncode=returncode,
                work_dir=plan.paths.work_dir,
                diagnostic=ssh_stderr,
            )
            return ProcessCompletion(
                error=SSHTransportFailure(
                    work_dir=plan.paths.work_dir,
                    returncode=returncode,
                    diagnostic=ssh_stderr,
                )
            )
        if plan.spec.check and returncode != 0:
            self._report_command_failure(
                returncode=returncode,
                work_dir=plan.paths.work_dir,
                stderr_path=plan.paths.stderr_path,
            )
            return ProcessCompletion(
                error=CommandFailure(
                    work_dir=plan.paths.work_dir,
                    stderr_path=plan.paths.stderr_path,
                    returncode=returncode,
                )
            )
        return ProcessCompletion(result=result)

    def terminate(self, active_processes: Sequence[ActiveProcess]) -> None:
        for active in active_processes:
            if active.stderr is not None:
                active.stderr.write(b"parallux: interrupted\n")
                active.stderr.flush()
            self._close_process_stdin(active.process)
            self._signal_process(active.process, signal.SIGTERM)

        deadline = time.time() + 2.0
        still_running: list[ActiveProcess] = []
        for active in active_processes:
            timeout = max(0.0, deadline - time.time())
            try:
                active.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                still_running.append(active)

        for active in still_running:
            self._signal_process(active.process, signal.SIGKILL)
        for active in still_running:
            try:
                active.process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                pass

        for active in active_processes:
            self._close_streams(active.stdout, active.stderr, active.ssh_stderr)

    def check_ssh_runner(self, runner: RunnerSpec) -> None:
        runner.validate()
        if runner.kind != "ssh":
            return
        argv = [
            "ssh",
            *runner.ssh_options,
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-p",
            str(runner.port),
            runner.target,
            runner.shell,
            "-lc",
            "true",
        ]
        try:
            result = subprocess.run(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                timeout=12.0,
                check=False,
                start_new_session=True,
            )
        except subprocess.TimeoutExpired as err:
            raise ParalluxError(
                f"ssh runner unavailable: runner={runner.name}; "
                f"target={runner.target}; error=timeout after {err.timeout}s"
            ) from err
        except OSError as err:
            raise ParalluxError(
                f"ssh runner unavailable: runner={runner.name}; "
                f"target={runner.target}; error={type(err).__name__}: {err}"
            ) from err
        if result.returncode != 0:
            diagnostic = self._format_process_output(result.stderr, result.stdout)
            raise ParalluxError(
                f"ssh runner unavailable: runner={runner.name}; "
                f"target={runner.target}; returncode={result.returncode}; "
                f"error={diagnostic}"
            )

    @staticmethod
    def report_task_dispatch_failure(
        *,
        error: BaseException,
        work_dir: str | None = None,
        stderr_path: str | None = None,
    ) -> None:
        details = []
        if work_dir is not None:
            details.append(work_dir)
        else:
            details.append(f"error={type(error).__name__}: {error}")
        print(
            "parallux: task dispatch failed: "
            + "; ".join(details),
            file=sys.stderr,
            flush=True,
        )

    def _result(self, plan: ProcessPlan, *, returncode: int) -> CommandResult:
        return CommandResult(
            runner=plan.runner.name,
            command=plan.spec.command,
            returncode=returncode,
            work_relpath=plan.paths.work_relpath,
            work_dir=plan.paths.work_dir,
            command_path=plan.paths.command_path,
            stdout_path=plan.paths.stdout_path,
            stderr_path=plan.paths.stderr_path,
            cores=plan.cores,
            numa_node=plan.numa_node,
        )

    def _command_env(
        self,
        spec: CommandSpec,
        runner: RunnerSpec,
        work_dir: str,
        work_relpath: str,
    ) -> dict[str, str]:
        env = dict(self.goal.env)
        env.update(runner.env)
        env.update(spec.env)
        env.update(
            {
                "PARALLUX_RUNNER": runner.name,
                "PARALLUX_WORK_RELPATH": work_relpath,
                "PARALLUX_WORK_DIR": work_dir,
            }
        )
        return {str(k): str(v) for k, v in env.items()}

    @staticmethod
    def _join_path(work_dir: str, filename: str, *, runner: RunnerSpec) -> str:
        if runner.kind == "local":
            return str(Path(work_dir) / filename)
        return f"{work_dir.rstrip('/')}/{filename}"

    @staticmethod
    def _remote_join(workspace: str, relpath: str) -> str:
        base = workspace.rstrip("/")
        if not relpath:
            return base
        return f"{base}/{relpath}"

    @staticmethod
    def _quote_remote_path(path: str) -> str:
        if path == "~":
            return "~"
        if path.startswith("~/"):
            rest = path[2:]
            if not rest:
                return "~"
            return "~/" + "/".join(shlex.quote(part) for part in rest.split("/"))
        return shlex.quote(path)

    def _remote_shell_body(
        self,
        runner: RunnerSpec,
        env: Mapping[str, str],
        command: str,
    ) -> str:
        invalid = [key for key in env if not self._valid_env_key(key)]
        if invalid:
            raise ParalluxError(f"invalid environment variable name for ssh runner: {invalid[0]!r}")
        exports = " ".join(
            f"export {key}={shlex.quote(value)};" for key, value in sorted(env.items())
        )
        return f"{exports} exec {shlex.quote(runner.shell)} -lc {shlex.quote(command)}"

    def _remote_watchdog_body(
        self,
        runner: RunnerSpec,
        env: Mapping[str, str],
        command: str,
        *,
        stdout_path: str,
        stderr_path: str,
    ) -> str:
        command_body = self._remote_shell_body(runner, env, command)
        stdout = self._quote_remote_path(stdout_path)
        stderr = self._quote_remote_path(stderr_path)
        return (
            "{ "
            "exec 3<&0; "
            f"setsid {shlex.quote(runner.shell)} -lc {shlex.quote(command_body)} "
            f"< /dev/null > {stdout} 2> {stderr} & "
            "task_pid=$!; "
            "( while IFS= read -r _; do :; done <&3; "
            "kill -TERM -\"$task_pid\" 2>/dev/null || "
            "kill -TERM \"$task_pid\" 2>/dev/null; "
            "sleep 2; "
            "kill -KILL -\"$task_pid\" 2>/dev/null || "
            "kill -KILL \"$task_pid\" 2>/dev/null; "
            ") & watchdog_pid=$!; "
            "wait \"$task_pid\"; rc=$?; "
            "kill \"$watchdog_pid\" 2>/dev/null; "
            "wait \"$watchdog_pid\" 2>/dev/null; "
            "exec 3<&-; "
            "exit \"$rc\"; "
            "}"
        )

    @staticmethod
    def _valid_env_key(key: str) -> bool:
        return re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key) is not None

    @staticmethod
    def _read_and_close(stream: BinaryIO | None) -> str:
        if stream is None:
            return ""
        try:
            stream.seek(0)
            return stream.read().decode("utf-8", errors="replace").strip()
        finally:
            stream.close()

    @staticmethod
    def _close_streams(*streams: BinaryIO | None) -> None:
        for stream in streams:
            if stream is not None:
                stream.close()

    @staticmethod
    def _close_process_stdin(process: subprocess.Popen[bytes]) -> None:
        if process.stdin is None or process.stdin.closed:
            return
        try:
            process.stdin.close()
        except OSError:
            return

    @staticmethod
    def _is_ssh_transport_failure(returncode: int, diagnostic: str) -> bool:
        return returncode < 0 or (returncode == 255 and bool(diagnostic))

    @classmethod
    def _format_process_output(cls, stderr: bytes, stdout: bytes = b"") -> str:
        text = stderr.decode("utf-8", errors="replace").strip()
        if not text:
            text = stdout.decode("utf-8", errors="replace").strip()
        if not text:
            return "no ssh diagnostic output"
        return cls._one_line(text)

    @staticmethod
    def _signal_process(process: subprocess.Popen[bytes], sig: int) -> None:
        if process.poll() is not None:
            return
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            return
        except OSError:
            try:
                process.send_signal(sig)
            except ProcessLookupError:
                return

    @staticmethod
    def _report_command_failure(
        *,
        work_dir: str,
        stderr_path: str,
        returncode: int,
    ) -> None:
        print(
            f"command failed: {work_dir}",
            file=sys.stderr,
            flush=True,
        )

    @staticmethod
    def _report_ssh_transport_failure(
        *,
        runner: RunnerSpec,
        work_dir: str,
        returncode: int,
        diagnostic: str,
    ) -> None:
        one_line = ProcessExecutor._one_line(diagnostic) or "ssh process ended without diagnostic"
        print(
            (
                "ssh transport failed: "
                f"runner={runner.name}; target={runner.target}; "
                f"returncode={returncode}; work_dir={work_dir}; "
                f"error={one_line}"
            ),
            file=sys.stderr,
            flush=True,
        )

    @staticmethod
    def _one_line(text: str) -> str:
        one_line = " | ".join(line.strip() for line in text.splitlines() if line.strip())
        if len(one_line) > 500:
            one_line = one_line[:497] + "..."
        return one_line
