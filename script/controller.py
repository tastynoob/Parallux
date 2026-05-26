from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Sequence

from _core import (
    ControllerOptions,
    Goal,
    ParallaxError,
    RunnerSpec,
    TaskSpec,
    _sanitize,
    _shell_join,
    _strip_remainder_marker,
    execute_config,
)


class Scheduler:
    def __init__(self, goal: Goal, options: ControllerOptions, root_path: str) -> None:
        self.goal = goal
        self.options = options
        self.root_path = Path(root_path)
        self.run_id = options.run_id or self._make_run_id()
        self.run_dir = self.root_path / f"parallax-{self.run_id}"
        self.script_dir = Path(__file__).resolve().parent
        self.semaphores = {
            runner.name: threading.Semaphore(runner.max_jobs or goal.parallel)
            for runner in self.goal.runners
        }

    def run(self) -> None:
        if not self.goal.tasks:
            raise ParallaxError("no task registered; use goal.addTask() first")
        self.run_dir.mkdir(parents=True, exist_ok=True)
        failures: list[tuple[TaskSpec, RunnerSpec, int]] = []

        runner_index = 0
        futures = set()
        task_iter = iter(self.goal.tasks)
        with ThreadPoolExecutor(max_workers=self.goal.parallel) as executor:
            def choose_runner() -> RunnerSpec:
                nonlocal runner_index
                runner = self.goal.runners[runner_index % len(self.goal.runners)]
                runner_index += 1
                return runner

            def submit_next() -> bool:
                try:
                    task = next(task_iter)
                except StopIteration:
                    return False
                runner = choose_runner()
                futures.add(executor.submit(self._run_one, task, runner))
                return True

            for _ in range(self.goal.parallel):
                if not submit_next():
                    break

            while futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                for future in done:
                    task, runner, returncode = future.result()
                    if returncode != 0:
                        failures.append((task, runner, returncode))
                while len(futures) < self.goal.parallel:
                    if not submit_next():
                        break

        if failures:
            failure_path = self.run_dir / "failures.txt"
            with failure_path.open("w", encoding="utf-8") as fs:
                for task, runner, returncode in failures:
                    fs.write(f"{task.id} runner={runner.name} returncode={returncode}\n")
            raise ParallaxError(f"{len(failures)} task(s) failed; see {failure_path}")
        print(f"all tasks finished; root: {self.root_path}; run: {self.run_dir}")

    def _run_one(self, task: TaskSpec, runner: RunnerSpec) -> tuple[TaskSpec, RunnerSpec, int]:
        work_relpath = self._task_work_relpath(task, runner)
        task_dir = self.root_path / work_relpath
        task_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = task_dir / "stdout.log"
        stderr_path = task_dir / "stderr.log"
        argv = self._runner_command(task, runner, work_relpath)
        with (task_dir / "command.txt").open("w", encoding="utf-8") as fs:
            fs.write(_shell_join(argv))
            fs.write("\n")
        with self.semaphores[runner.name]:
            with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open(
                "w", encoding="utf-8"
            ) as stderr:
                if self.options.dry_run:
                    stdout.write("[dry-run]\n")
                    stdout.write(_shell_join(argv))
                    stdout.write("\n")
                    return task, runner, 0
                completed = subprocess.run(
                    argv,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    text=True,
                    check=False,
                )
                return task, runner, completed.returncode

    def _runner_command(self, task: TaskSpec, runner: RunnerSpec, work_relpath: Path) -> list[str]:
        runner_script = self.script_dir / "runner.py"
        common = [
            "--runner",
            runner.name,
            "--root-path",
            str(self.root_path),
            "--run-id",
            self.run_id,
            "--work-relpath",
            work_relpath.as_posix(),
        ]
        tail = ["--", task.id]
        if runner.kind == "local":
            config_path = str(self.options.config_path)
            return [sys.executable, str(runner_script), *common, config_path, *tail]

        remote_config = self.options.config_arg
        remote_cmd = _shell_join([runner.python, "script/runner.py", *common, remote_config, *tail])
        if runner.workdir:
            remote_cmd = f"cd {shlex.quote(runner.workdir)} && {remote_cmd}"
        argv = ["ssh", *runner.ssh_options, "-p", str(runner.port), runner.target]
        argv.extend([runner.shell, "-lc", shlex.quote(remote_cmd)])
        return argv

    def _task_work_relpath(self, task: TaskSpec, runner: RunnerSpec) -> Path:
        if task.work_relpath is not None:
            return Path(task.work_relpath)
        return Path(f"parallax-{self.run_id}") / _sanitize(f"{task.id}-{runner.name}")

    @staticmethod
    def _make_run_id() -> str:
        now = time.time()
        stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(now))
        return f"{stamp}-{int((now % 1) * 1000):03d}-{os.getpid()}"


class ControllerRunner:
    name = "controller"

    def run(self, *_args: object, **_kwargs: object) -> None:
        raise ParallaxError("runner.run() is only available inside a task executed by runner.py")


def run_controller(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description="run a Parallax Python config")
    parser.add_argument("config", nargs="?", help="Python config file")
    parser.add_argument("-f", "--file", help="Python config file")
    parser.add_argument("--dry-run", action="store_true", help="render runner commands only")
    parser.add_argument("--list-tasks", action="store_true", help="list registered task ids and exit")
    parser.add_argument("--run-id", help="override generated run id")
    parser.add_argument("extra", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)
    args = parser.parse_args(list(argv))

    config_arg = args.file or args.config
    if not config_arg:
        parser.error("missing config file")
    config_path = Path(config_arg).resolve()
    if config_path.suffix != ".py":
        raise ParallaxError("config file must be a .py file")
    if not config_path.exists():
        raise ParallaxError(f"config file does not exist: {config_path}")

    extra = _strip_remainder_marker(args.extra)
    if extra:
        raise ParallaxError(
            "task function arguments are not supported; use a task builder to create zero-arg tasks"
        )
    mode = "collect" if args.list_tasks else "controller"
    options = ControllerOptions(
        config_path=config_path,
        config_arg=config_arg,
        dry_run=args.dry_run,
        run_id=args.run_id,
    )
    goal = Goal(mode=mode, options=options, scheduler_factory=Scheduler)
    execute_config(config_path, goal=goal, runner=ControllerRunner())

    if args.list_tasks:
        for task in goal.tasks:
            print(task.id)
        return 0

    if not goal.issued:
        goal.issue()
    return 0


def main() -> int:
    try:
        return run_controller(sys.argv[1:])
    except ParallaxError as err:
        print(f"parallax: {err}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("parallax: interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
