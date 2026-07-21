from __future__ import annotations

import argparse
import glob as _glob
import re as _re
import sys
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence


from ._core import (
    CommandSpec as _CommandSpec,
    Goal as _RuntimeGoal,
    ParalluxError,
    RunnerSpec as _RuntimeRunnerSpec,
    RunnerStatus,
    RuntimeOptions as _RuntimeOptions,
    execute_config,
)
from ._process import (
    DEFAULT_WORKSPACE,
    ActiveProcess,
    CommandFailure,
    CommandPaths,
    CommandResult,
    ProcessCompletion,
    ProcessExecutor,
    ProcessPlan,
    SSHTransportFailure,
    TaskDispatchFailure,
)
from ._scheduler import DefaultScheduler, SchedulerAssignment


__version__ = "0.1.3"
_NAME_RE = _re.compile(r"[^A-Za-z0-9_.-]+")


def _sanitize_name(value: str) -> str:
    return _NAME_RE.sub("_", value).strip("_") or "workload"


def _normalize_relpath(value: str, *, label: str) -> str:
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{label} must be a relative path")
    return path.as_posix()


def _has_glob_magic(value: str) -> bool:
    return any(char in value for char in "*?[")


def _tail_relpath(path: str, levels: int, *, strip_suffix: bool) -> str | None:
    if levels <= 0:
        raise ValueError("levels must be > 0")
    source = Path(path)
    parts = list(source.parts)
    if source.anchor and parts and parts[0] == source.anchor:
        parts = parts[1:]
    if levels > len(parts):
        return None
    selected = parts[-levels:]
    if strip_suffix and selected:
        selected[-1] = Path(selected[-1]).with_suffix("").name
    return Path(*selected).as_posix()


@dataclass(frozen=True)
class Workload:
    input_path: str
    relpath: str
    work_relpath: str
    name: str


def workloads(
    patterns: str | Iterable[str],
    *,
    levels: int,
    work_prefix: str = "",
    strip_suffix: bool = False,
    recursive: bool = False,
    sort: bool = True,
) -> list[Workload]:
    pattern_list = [patterns] if isinstance(patterns, str) else list(patterns)
    paths: list[str] = []
    for pattern in pattern_list:
        matches = _glob.glob(pattern, recursive=recursive)
        if matches or _has_glob_magic(pattern):
            paths.extend(matches)
        else:
            paths.append(pattern)
    if sort:
        paths = sorted(dict.fromkeys(paths))

    prefix = _normalize_relpath(work_prefix, label="work_prefix") if work_prefix else ""
    result: list[Workload] = []
    for path in paths:
        relpath = _tail_relpath(path, levels, strip_suffix=strip_suffix)
        if relpath is None:
            continue
        work_relpath = f"{prefix}/{relpath}" if prefix else relpath
        result.append(
            Workload(
                input_path=path,
                relpath=relpath,
                work_relpath=work_relpath,
                name=_sanitize_name(relpath),
            )
        )
    return result


class Runner(Protocol):
    """Runner handle returned by goal.local() and goal.ssh()."""

    name: str

    def status(self, *, numa_node: int | None = None) -> RunnerStatus: ...

    def active_jobs(self) -> int: ...

    def available_jobs(self) -> int:
        """Return currently unused job slots for this Runner."""
        ...

    def logical_core_count(self, *, numa_node: int | None = None) -> int:
        """Return total logical threads known for this Runner or NUMA node."""
        ...

    def configured_cores(self, *, numa_node: int | None = None) -> list[int]:
        """Return core ids managed by Parallux for binding."""
        ...

    def configured_core_count(self, *, numa_node: int | None = None) -> int:
        """Return the number of cores managed by Parallux for binding."""
        ...

    def available_cores(self, *, numa_node: int | None = None) -> list[int]:
        """Return currently unleased configured core ids."""
        ...

    def available_core_count(self, *, numa_node: int | None = None) -> int:
        """Return the number of currently unleased configured cores."""
        ...

    def run(
        self,
        command: str,
        *,
        name: str | None = None,
        threads: int = 0,
        thread: int | None = None,
        numa_node: int | None = None,
        cores: Sequence[int] | None = None,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
        work_relpath: str | None = None,
        check: bool = True,
        timeout: float | None = None,
    ) -> Any: ...


class Handle(Protocol):
    command: str
    runner: Runner | None
    runner_name: str | None
    work_relpath: str | None
    work_dir: str | None
    command_path: str | None
    stdout_path: str | None
    stderr_path: str | None
    cores: tuple[int, ...]
    numa_node: int | None

    def sync(self, timeout: float | None = None) -> Any: ...

    def done(self) -> bool: ...

    def assigned(self) -> bool: ...


class GoalShell(Protocol):
    mode: str
    argv: list[str]
    args: dict[str, str]

    def local(
        self,
        name: str = "local",
        *,
        workspace: str | None = None,
        shell: str = "bash",
        max_jobs: int | None = None,
        env: Mapping[str, str] | None = None,
        core_pool: Iterable[int] | None = None,
        numa_nodes: Mapping[int, Iterable[int]] | None = None,
    ) -> Runner: ...

    def ssh(
        self,
        name: str,
        *,
        host: str | None = None,
        user: str | None = None,
        port: int = 22,
        workspace: str | None = None,
        shell: str = "bash",
        max_jobs: int | None = None,
        env: Mapping[str, str] | None = None,
        core_pool: Iterable[int] | None = None,
        numa_nodes: Mapping[int, Iterable[int]] | None = None,
        ssh_options: Sequence[str] | None = None,
    ) -> Runner: ...

    def setRunner(self, runners: str | Runner | Sequence[str | Runner]) -> None: ...

    def setParallel(self, parallel: int) -> None: ...

    def setEnv(self, key: str, value: Any) -> None: ...

    def schd(
        self,
        command: str,
        *,
        name: str | None = None,
        threads: int = 0,
        thread: int | None = None,
        numa_node: int | None = None,
        cores: Sequence[int] | None = None,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
        work_relpath: str | None = None,
        check: bool = True,
        timeout: float | None = None,
    ) -> Any: ...

    def run(
        self,
        command: str,
        *,
        name: str | None = None,
        threads: int = 0,
        thread: int | None = None,
        numa_node: int | None = None,
        cores: Sequence[int] | None = None,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
        work_relpath: str | None = None,
        check: bool = True,
        timeout: float | None = None,
    ) -> Handle: ...

    def issue(self) -> Handle: ...

    def runner_status(
        self,
        runner: Runner | None = None,
        *,
        numa_node: int | None = None,
    ) -> RunnerStatus | list[RunnerStatus]: ...


class _Missing:
    def __init__(self, name: str) -> None:
        self._proxy_name = name
        self._target: Any | None = None

    def __getattr__(self, attr: str) -> Any:
        if self._target is not None:
            return getattr(self._target, attr)
        raise RuntimeError(
            f"parallux.{self._proxy_name} is not bound. Run the config through "
            "parallux or python3 -m parallux to bind the real object."
        )

    def _bind(self, target: Any) -> None:
        self._target = target


goal: GoalShell = _Missing("goal")  # type: ignore[assignment]


def _bind(real_goal: Any) -> None:
    goal._bind(real_goal)  # type: ignore[attr-defined]


class Handle:
    def __init__(self, future: Future[CommandResult], spec: _CommandSpec) -> None:
        self._future = future
        self.command = spec.command
        self.runner: _RuntimeRunnerSpec | None = None
        self.runner_name: str | None = None
        self.work_relpath: str | None = None
        self.work_dir: str | None = None
        self.command_path: str | None = None
        self.stdout_path: str | None = None
        self.stderr_path: str | None = None
        self.cores: tuple[int, ...] = ()
        self.numa_node = spec.numa_node
        self._lock = threading.Lock()

    def sync(self, timeout: float | None = None) -> CommandResult:
        return self._future.result(timeout=timeout)

    def done(self) -> bool:
        return self._future.done()

    def assigned(self) -> bool:
        return self.runner is not None

    def _assign(
        self,
        *,
        runner: _RuntimeRunnerSpec,
        work_relpath: str,
        work_dir: str,
        command_path: str,
        stdout_path: str,
        stderr_path: str,
        cores: Sequence[int],
        numa_node: int | None,
    ) -> None:
        with self._lock:
            self.runner = runner
            self.runner_name = runner.name
            self.work_relpath = work_relpath
            self.work_dir = work_dir
            self.command_path = command_path
            self.stdout_path = stdout_path
            self.stderr_path = stderr_path
            self.cores = tuple(int(core) for core in cores)
            self.numa_node = numa_node


class GroupHandle:
    def __init__(self, handles: Sequence[Handle]) -> None:
        self.handles = list(handles)

    def sync(self, timeout: float | None = None) -> list[CommandResult]:
        results: list[CommandResult] = []
        errors: list[BaseException] = []
        for handle in self.handles:
            try:
                results.append(handle.sync(timeout=timeout))
            except Exception as err:
                errors.append(err)
        if errors:
            messages: list[str] = []
            command_errors = [err for err in errors if isinstance(err, CommandFailure)]
            ssh_errors = [err for err in errors if isinstance(err, SSHTransportFailure)]
            dispatch_errors = [
                err for err in errors if isinstance(err, TaskDispatchFailure)
            ]
            other_errors = [
                err
                for err in errors
                if not isinstance(
                    err,
                    (CommandFailure, SSHTransportFailure, TaskDispatchFailure),
                )
            ]
            if command_errors:
                messages.append(f"{len(command_errors)} command(s) failed")
            if ssh_errors:
                messages.append(f"{len(ssh_errors)} ssh transport failure(s)")
            if dispatch_errors:
                messages.append(
                    f"{len(dispatch_errors)} task(s) failed before command execution"
                )
            if other_errors:
                messages.append(
                    f"{len(other_errors)} task(s) failed with scheduler error; "
                    f"first error: {other_errors[0]}"
                )
            raise ParalluxError("\n".join(messages)) from errors[0]
        return results

    def done(self) -> bool:
        return all(handle.done() for handle in self.handles)


@dataclass
class PendingCommand:
    spec: _CommandSpec
    future: Future[CommandResult]
    handle: Handle


@dataclass
class ScheduledProcess:
    assignment: SchedulerAssignment
    plan: ProcessPlan


@dataclass
class RunningCommand:
    future: Future[CommandResult]
    handle: Handle
    assignment: SchedulerAssignment
    active: ActiveProcess


class Runtime:
    def __init__(self, goal: _RuntimeGoal, options: _RuntimeOptions) -> None:
        self.goal = goal
        self.options = options
        self.condition = threading.Condition()
        self.pending: list[PendingCommand] = []
        self.running: list[RunningCommand] = []
        self.handles: list[Handle] = []
        self.scheduler = DefaultScheduler(goal)
        self.executor = ProcessExecutor(goal, options)
        self._next_task_id = 1
        self.stopping = False
        self.thread = threading.Thread(target=self._run_loop, name="parallux-scheduler", daemon=True)
        self.thread.start()

    def submit(self, spec: _CommandSpec) -> Handle:
        future: Future[CommandResult] = Future()
        with self.condition:
            self._assign_task_id_locked(spec)
            handle = Handle(future, spec)
            self.pending.append(PendingCommand(spec=spec, future=future, handle=handle))
            self.handles.append(handle)
            self.condition.notify_all()
        return handle

    def submit_many(self, specs: Sequence[_CommandSpec]) -> GroupHandle:
        return GroupHandle([self.submit(spec) for spec in specs])

    def _assign_task_id_locked(self, spec: _CommandSpec) -> None:
        if spec._task_id is not None:
            raise ParalluxError(f"task has already been submitted: {spec._task_id}")
        spec._task_id = self._next_task_id
        self._next_task_id += 1

    def finalize(self) -> None:
        if self.goal.has_scheduled():
            self.goal.issue()
        GroupHandle(self.handles).sync()
        self.shutdown()

    def shutdown(self, *, cancel: bool = False) -> None:
        running_to_stop: list[RunningCommand] = []
        with self.condition:
            if cancel:
                for pending in self.pending:
                    pending.future.set_exception(ParalluxError("task cancelled"))
                self.pending.clear()
                running_to_stop = list(self.running)
                self.running.clear()
                for running in running_to_stop:
                    self.scheduler.release_started(running.assignment)
                    if not running.future.done():
                        running.future.set_exception(ParalluxError("task cancelled"))
            self.stopping = True
            self.condition.notify_all()
        if running_to_stop:
            self.executor.terminate([running.active for running in running_to_stop])
        if self.thread.is_alive() and threading.current_thread() is not self.thread:
            self.thread.join()

    def _run_loop(self) -> None:
        while True:
            with self.condition:
                if self.stopping and not self.pending and not self.running:
                    return
                self._start_ready_locked()
                wait_time = self._poll_running_locked()
                if self.pending or self.running:
                    self.condition.wait(timeout=wait_time)
                else:
                    self.condition.wait()

    def _start_ready_locked(self) -> None:
        started = True
        while started:
            started = False
            if len(self.running) >= self.goal.parallel:
                return
            for pending in list(self.pending):
                try:
                    scheduled = self._prepare_process_locked(pending.spec)
                except BaseException as err:
                    self.pending.remove(pending)
                    pending.future.set_exception(self._task_dispatch_failure(err))
                    started = True
                    break
                if scheduled is None:
                    continue
                self._assign_handle(pending.handle, scheduled)
                self.pending.remove(pending)
                try:
                    started_process = self.executor.start(scheduled.plan)
                except BaseException as err:
                    self.scheduler.release_unstarted(scheduled.assignment)
                    pending.future.set_exception(
                        self._task_dispatch_failure(err, paths=scheduled.plan.paths)
                    )
                    started = True
                    break
                if isinstance(started_process, ActiveProcess):
                    self.scheduler.mark_started(scheduled.assignment)
                    self.running.append(
                        RunningCommand(
                            future=pending.future,
                            handle=pending.handle,
                            assignment=scheduled.assignment,
                            active=started_process,
                        )
                    )
                else:
                    self.scheduler.release_unstarted(scheduled.assignment)
                    self._complete_future(pending.future, started_process)
                started = True
                break

    def _prepare_process_locked(
        self,
        spec: _CommandSpec,
    ) -> ScheduledProcess | None:
        assignment = self.scheduler.try_assign(spec, running_count=len(self.running))
        if assignment is None:
            return None
        paths: CommandPaths | None = None
        try:
            paths = self.executor.paths(
                spec,
                assignment.runner,
                default_work_relpath=self._default_work_relpath(spec),
            )
            full_command = assignment.lease.wrap(spec.command, assignment.runner.shell)
            plan = self.executor.plan(
                spec,
                assignment.runner,
                paths=paths,
                full_command=full_command,
                cores=assignment.lease.cores,
                numa_node=assignment.lease.mem_node,
            )
        except BaseException as err:
            self.scheduler.release_unstarted(assignment)
            raise self._task_dispatch_failure(err, paths=paths) from err
        return ScheduledProcess(assignment=assignment, plan=plan)

    @staticmethod
    def _assign_handle(handle: Handle, scheduled: ScheduledProcess) -> None:
        paths = scheduled.plan.paths
        lease = scheduled.assignment.lease
        handle._assign(
            runner=scheduled.assignment.runner,
            work_relpath=paths.work_relpath,
            work_dir=paths.work_dir,
            command_path=paths.command_path,
            stdout_path=paths.stdout_path,
            stderr_path=paths.stderr_path,
            cores=lease.cores,
            numa_node=lease.mem_node,
        )

    def _poll_running_locked(self) -> float:
        for running in list(self.running):
            completion = self.executor.poll(running.active)
            if completion is None:
                continue
            self.running.remove(running)
            self.scheduler.release_started(running.assignment)
            self._complete_future(running.future, completion)
            self.condition.notify_all()
        return 0.05

    def _complete_future(
        self,
        future: Future[CommandResult],
        completion: ProcessCompletion,
    ) -> None:
        if completion.error is not None:
            future.set_exception(completion.error)
        elif completion.result is not None:
            future.set_result(completion.result)
        else:
            future.set_exception(ParalluxError("process completed without result"))

    def _task_dispatch_failure(
        self,
        err: BaseException,
        *,
        paths: CommandPaths | None = None,
    ) -> TaskDispatchFailure:
        if isinstance(err, TaskDispatchFailure):
            return err
        if paths is None:
            self.executor.report_task_dispatch_failure(error=err)
            return TaskDispatchFailure(error=err)
        self.executor.report_task_dispatch_failure(
            work_dir=paths.work_dir,
            stderr_path=paths.stderr_path,
            error=err,
        )
        return TaskDispatchFailure(
            work_dir=paths.work_dir,
            stderr_path=paths.stderr_path,
            error=err,
        )

    def check_ssh_runner(self, runner: _RuntimeRunnerSpec) -> None:
        runner.bind(self.goal).validate()
        self.executor.check_ssh_runner(runner)

    def runner_status(
        self,
        runner: _RuntimeRunnerSpec,
        *,
        numa_node: int | None = None,
    ) -> RunnerStatus:
        with self.condition:
            return self.scheduler.runner_status(runner, numa_node=numa_node)

    def _default_work_relpath(self, spec: _CommandSpec) -> str:
        return self._task_id_text(spec)

    @staticmethod
    def _task_id(spec: _CommandSpec) -> int:
        if spec._task_id is None:
            raise ParalluxError("task has not been submitted")
        return spec._task_id

    def _task_id_text(self, spec: _CommandSpec) -> str:
        return str(self._task_id(spec))


def _split_config_args(values: Sequence[str]) -> tuple[list[str], dict[str, str]]:
    argv = list(values)
    if argv and argv[0] == "--":
        argv = argv[1:]
    args: dict[str, str] = {}
    for item in argv:
        if "=" in item:
            key, value = item.split("=", 1)
            if key:
                args[key] = value
    return argv, args


def run_runtime(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description="run a Parallux config in a controlled runtime")
    parser.add_argument("config", help="Python config file")
    parser.add_argument("--dry-run", action="store_true", help="write commands without executing them")
    raw_argv = list(argv)
    if "--" in raw_argv:
        marker = raw_argv.index("--")
        parallux_argv = raw_argv[:marker]
        raw_config_args = raw_argv[marker + 1 :]
    else:
        parallux_argv = raw_argv
        raw_config_args = []
    args = parser.parse_args(parallux_argv)

    config_path = Path(args.config).resolve()
    if config_path.suffix != ".py":
        raise ParalluxError("config file must be a .py file")
    if not config_path.exists():
        raise ParalluxError(f"config file does not exist: {config_path}")

    config_argv, config_args = _split_config_args(raw_config_args)
    options = _RuntimeOptions(
        config_path=config_path,
        dry_run=args.dry_run,
        argv=config_argv,
        args=config_args,
    )
    real_goal = _RuntimeGoal(mode="runtime", options=options)
    runtime = Runtime(real_goal, options)
    real_goal.bind_runtime(runtime)
    try:
        execute_config(config_path, goal=real_goal)
        runtime.finalize()
    except BaseException:
        runtime.shutdown(cancel=True)
        raise
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    sys.modules.setdefault("parallux", sys.modules[__name__])

    try:
        return run_runtime(sys.argv[1:] if argv is None else argv)
    except ParalluxError as err:
        print(f"parallux: {err}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("parallux: interrupted", file=sys.stderr)
        return 130


__all__ = [
    "GoalShell",
    "Handle",
    "Runner",
    "RunnerStatus",
    "Workload",
    "goal",
    "workloads",
]


if __name__ == "__main__":
    raise SystemExit(main())
