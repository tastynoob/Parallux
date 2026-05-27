from __future__ import annotations

import argparse
import glob as _glob
import os
import re as _re
import re
import shlex
import subprocess
import sys
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Mapping, Protocol, Sequence


from ._core import (
    CommandSpec as _CommandSpec,
    Goal as _RuntimeGoal,
    ParalluxError,
    RunnerSpec as _RuntimeRunnerSpec,
    RunnerStatus,
    RuntimeOptions as _RuntimeOptions,
    _shell_join,
    execute_config,
)


__version__ = "0.1.0"
DEFAULT_WORKSPACE = "~/parallux"
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


def _tail_relpath(path: str, levels: int, *, strip_suffix: bool) -> str:
    if levels <= 0:
        raise ValueError("levels must be > 0")
    source = Path(path)
    parts = list(source.parts)
    if source.anchor and parts and parts[0] == source.anchor:
        parts = parts[1:]
    if levels > len(parts):
        raise ValueError(f"levels={levels} is deeper than path: {path}")
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


class RunnerSpec(Protocol):
    name: str
    kind: str
    host: str | None
    user: str | None
    port: int
    workspace: str | None
    shell: str
    max_jobs: int | None
    env: dict[str, str]
    core_pool: list[int]
    numa_nodes: dict[int, list[int]]
    ssh_options: list[str]

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
    runner: RunnerSpec | None
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
    ) -> RunnerSpec: ...

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
    ) -> RunnerSpec: ...

    def setRunner(self, runners: str | RunnerSpec | Sequence[str | RunnerSpec]) -> None: ...

    def setParallel(self, parallel: int) -> None: ...

    def setEnv(self, key: str, value: Any) -> None: ...

    def schd(
        self,
        command: str,
        *,
        runner: RunnerSpec | None = None,
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
        runner: RunnerSpec | None = None,
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
        runner: RunnerSpec | None = None,
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
            except BaseException as err:
                errors.append(err)
        if errors:
            first = errors[0]
            raise ParalluxError(f"{len(errors)} command(s) failed; first error: {first}") from first
        return results

    def done(self) -> bool:
        return all(handle.done() for handle in self.handles)


def _format_cores(cores: Sequence[int]) -> str:
    values = sorted(set(int(core) for core in cores))
    if not values:
        return ""
    ranges: list[str] = []
    start = prev = values[0]
    for core in values[1:]:
        if core == prev + 1:
            prev = core
            continue
        ranges.append(str(start) if start == prev else f"{start}-{prev}")
        start = prev = core
    ranges.append(str(start) if start == prev else f"{start}-{prev}")
    return ",".join(ranges)


class CoreLease:
    def __init__(self, allocator: "CoreAllocator", cores: list[int], mem_node: int | None) -> None:
        self.allocator = allocator
        self.cores = cores
        self.mem_node = mem_node

    def release(self) -> None:
        self.allocator.release(self.cores)

    def wrap(self, command: str, shell: str) -> str:
        if not self.cores:
            return command
        args = ["numactl"]
        if self.mem_node is not None:
            args.extend(["-m", str(self.mem_node)])
        args.extend(["-C", _format_cores(self.cores), shell, "-lc", command])
        return _shell_join(args)


class CoreAllocator:
    def __init__(self, spec: _RuntimeRunnerSpec) -> None:
        self.spec = spec
        self.pool = self._configured_pool()
        self.available = set(self.pool)

    def try_acquire(
        self,
        *,
        threads: int = 0,
        numa_node: int | None = None,
        cores: Sequence[int] | None = None,
    ) -> CoreLease | None:
        if cores:
            requested = [int(core) for core in cores]
            self._validate_exact(requested)
            if not set(requested).issubset(self.available):
                return None
        elif threads > 0 and self.pool:
            requested = self._try_allocate_count(threads, numa_node)
            if requested is None:
                return None
        else:
            if numa_node is not None and not self.pool:
                raise ParalluxError(
                    f"runner {self.spec.name!r} needs core_pool/numa_nodes for NUMA binding"
                )
            requested = []
        self.available.difference_update(requested)
        mem_node = numa_node if numa_node is not None else self._infer_node(requested)
        return CoreLease(self, requested, mem_node)

    def release(self, cores: Sequence[int]) -> None:
        if not cores or not self.pool:
            return
        self.available.update(int(core) for core in cores if int(core) in set(self.pool))

    def logical_core_count(self, numa_node: int | None = None) -> int:
        pool = self._candidate_pool(numa_node)
        if pool:
            return len(set(pool))
        if self.spec.kind == "local":
            return os.cpu_count() or 0
        return 0

    def configured_cores(self, numa_node: int | None = None) -> list[int]:
        return sorted(set(self._candidate_pool(numa_node)))

    def configured_core_count(self, numa_node: int | None = None) -> int:
        return len(self.configured_cores(numa_node))

    def available_cores(self, numa_node: int | None = None) -> list[int]:
        return [core for core in self.configured_cores(numa_node) if core in self.available]

    def available_core_count(self, numa_node: int | None = None) -> int:
        return len(self.available_cores(numa_node))

    def scheduling_core_score(self, numa_node: int | None = None) -> int:
        if self.pool:
            return self.available_core_count(numa_node)
        return self.logical_core_count(numa_node)

    def _validate_exact(self, cores: Sequence[int]) -> None:
        if not self.pool:
            raise ParalluxError(f"runner {self.spec.name!r} needs core_pool for explicit cores")
        unknown = set(cores) - set(self.pool)
        if unknown:
            raise ParalluxError(
                f"runner {self.spec.name!r} requested cores outside core_pool: "
                f"{sorted(unknown)}"
            )

    def _try_allocate_count(self, count: int, numa_node: int | None) -> list[int] | None:
        pool = self._candidate_pool(numa_node)
        if not pool:
            raise ParalluxError(
                f"runner {self.spec.name!r} needs core_pool/numa_nodes for NUMA allocation"
            )
        available = [core for core in pool if core in self.available]
        if len(available) < count:
            return None
        return available[:count]

    def _candidate_pool(self, numa_node: int | None) -> list[int]:
        if numa_node is not None and self.spec.numa_nodes:
            return list(self.spec.numa_nodes.get(numa_node, []))
        return list(self.spec.core_pool)

    def _infer_node(self, cores: Sequence[int]) -> int | None:
        if not cores or not self.spec.numa_nodes:
            return None
        core_set = set(cores)
        for node, node_cores in self.spec.numa_nodes.items():
            if core_set.issubset(set(node_cores)):
                return node
        return None

    def _configured_pool(self) -> list[int]:
        cores = sorted(set(self.spec.core_pool))
        if not cores and self.spec.numa_nodes:
            merged: set[int] = set()
            for node_cores in self.spec.numa_nodes.values():
                merged.update(node_cores)
            cores = sorted(merged)
        return cores


@dataclass
class PendingCommand:
    spec: _CommandSpec
    future: Future[CommandResult]
    handle: Handle


@dataclass
class Launch:
    runner: _RuntimeRunnerSpec
    lease: CoreLease
    argv: list[str]
    env: dict[str, str] | None
    cwd: str | None
    work_relpath: str
    work_dir: str
    command_path: str
    stdout_path: str
    stderr_path: str
    full_command: str
    local_artifacts: bool


@dataclass
class RunningCommand:
    spec: _CommandSpec
    runner: _RuntimeRunnerSpec
    process: subprocess.Popen[bytes]
    future: Future[CommandResult]
    handle: Handle
    stdout: BinaryIO | None
    stderr: BinaryIO | None
    lease: CoreLease
    started_at: float
    work_relpath: str
    work_dir: str
    command_path: str
    stdout_path: str
    stderr_path: str


class Runtime:
    def __init__(self, goal: _RuntimeGoal, options: _RuntimeOptions) -> None:
        self.goal = goal
        self.options = options
        self.condition = threading.Condition()
        self.pending: list[PendingCommand] = []
        self.running: list[RunningCommand] = []
        self.handles: list[Handle] = []
        self.runner_active: dict[str, int] = {}
        self.allocators: dict[str, CoreAllocator] = {}
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
        with self.condition:
            self.stopping = True
            self.condition.notify_all()
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
                    launch = self._prepare_launch_locked(pending.spec)
                except BaseException as err:
                    self.pending.remove(pending)
                    pending.future.set_exception(err)
                    started = True
                    break
                if launch is None:
                    continue
                pending.handle._assign(
                    runner=launch.runner,
                    work_relpath=launch.work_relpath,
                    work_dir=launch.work_dir,
                    command_path=launch.command_path,
                    stdout_path=launch.stdout_path,
                    stderr_path=launch.stderr_path,
                    cores=launch.lease.cores,
                    numa_node=launch.lease.mem_node,
                )
                self.pending.remove(pending)
                try:
                    running = self._start_process(pending, launch)
                except BaseException as err:
                    launch.lease.release()
                    pending.future.set_exception(err)
                    started = True
                    break
                if running is not None:
                    self.running.append(running)
                    self.runner_active[launch.runner.name] = (
                        self.runner_active.get(launch.runner.name, 0) + 1
                    )
                started = True
                break

    def _prepare_launch_locked(
        self,
        spec: _CommandSpec,
    ) -> Launch | None:
        for runner in self._candidate_runners(spec):
            if self.runner_active.get(runner.name, 0) >= (runner.max_jobs or self.goal.parallel):
                continue
            allocator = self._allocator(runner)
            lease = allocator.try_acquire(
                threads=spec.threads,
                numa_node=spec.numa_node,
                cores=spec.cores,
            )
            if lease is None:
                continue
            work_relpath, work_dir, command_path, stdout_path, stderr_path = self._paths(spec, runner)
            full_command = lease.wrap(spec.command, runner.shell)
            argv, env, cwd, local_artifacts = self._command_launch(
                spec,
                runner,
                full_command,
                work_relpath,
                work_dir,
                command_path,
                stdout_path,
                stderr_path,
            )
            return Launch(
                runner=runner,
                lease=lease,
                argv=argv,
                env=env,
                cwd=cwd,
                work_relpath=work_relpath,
                work_dir=work_dir,
                command_path=command_path,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                full_command=full_command,
                local_artifacts=local_artifacts,
            )
        return None

    def _start_process(
        self,
        pending: PendingCommand,
        launch: Launch,
    ) -> RunningCommand | None:
        stdout: BinaryIO | None = None
        stderr: BinaryIO | None = None
        if launch.local_artifacts:
            Path(launch.work_dir).mkdir(parents=True, exist_ok=True)
            with Path(launch.command_path).open("w", encoding="utf-8") as fs:
                fs.write(pending.spec.command)
                fs.write("\n")
                fs.write(f"executor: {_shell_join(launch.argv)}\n")
            stdout = Path(launch.stdout_path).open("wb")
            stderr = Path(launch.stderr_path).open("wb")
        if self.options.dry_run:
            if stdout is not None:
                stdout.write(f"[dry-run] {launch.full_command}\n".encode("utf-8"))
                stdout.close()
            if stderr is not None:
                stderr.close()
            launch.lease.release()
            result = CommandResult(
                runner=launch.runner.name,
                command=pending.spec.command,
                returncode=0,
                work_relpath=launch.work_relpath,
                work_dir=launch.work_dir,
                command_path=launch.command_path,
                stdout_path=launch.stdout_path,
                stderr_path=launch.stderr_path,
                cores=tuple(launch.lease.cores),
                numa_node=launch.lease.mem_node,
            )
            pending.future.set_result(result)
            return None
        process = subprocess.Popen(
            launch.argv,
            stdout=stdout if stdout is not None else subprocess.DEVNULL,
            stderr=stderr if stderr is not None else subprocess.DEVNULL,
            env=launch.env,
            cwd=launch.cwd,
        )
        return RunningCommand(
            spec=pending.spec,
            runner=launch.runner,
            process=process,
            future=pending.future,
            handle=pending.handle,
            stdout=stdout,
            stderr=stderr,
            lease=launch.lease,
            started_at=time.time(),
            work_relpath=launch.work_relpath,
            work_dir=launch.work_dir,
            command_path=launch.command_path,
            stdout_path=launch.stdout_path,
            stderr_path=launch.stderr_path,
        )

    def _poll_running_locked(self) -> float:
        now = time.time()
        for running in list(self.running):
            timed_out = (
                running.spec.timeout is not None
                and now - running.started_at >= running.spec.timeout
            )
            if timed_out and running.process.poll() is None:
                running.process.kill()
                if running.stderr is not None:
                    running.stderr.write(
                        f"parallux: command timed out after {running.spec.timeout}s\n".encode(
                            "utf-8"
                        )
                    )
            returncode = running.process.poll()
            if returncode is None:
                continue
            self.running.remove(running)
            self.runner_active[running.runner.name] -= 1
            running.lease.release()
            if running.stdout is not None:
                running.stdout.close()
            if running.stderr is not None:
                running.stderr.close()
            result = CommandResult(
                runner=running.runner.name,
                command=running.spec.command,
                returncode=returncode,
                work_relpath=running.work_relpath,
                work_dir=running.work_dir,
                command_path=running.command_path,
                stdout_path=running.stdout_path,
                stderr_path=running.stderr_path,
                cores=tuple(running.lease.cores),
                numa_node=running.lease.mem_node,
            )
            if running.spec.check and returncode != 0:
                running.future.set_exception(
                    ParalluxError(
                        f"command failed with exit code {returncode}: {running.spec.command}"
                    )
                )
            else:
                running.future.set_result(result)
            self.condition.notify_all()
        return 0.05

    def _candidate_runners(self, spec: _CommandSpec) -> list[_RuntimeRunnerSpec]:
        if spec.runner is not None:
            spec.runner.bind(self.goal).validate()
            self._ensure_runner(spec.runner)
            return [spec.runner]
        if not self.goal.runners:
            raise ParalluxError("no runner configured")
        for runner in self.goal.runners:
            self._ensure_runner(runner)
        return sorted(
            self.goal.runners,
            key=lambda item: self._runner_sort_key(item, spec),
        )

    def _runner_sort_key(self, runner: _RuntimeRunnerSpec, spec: _CommandSpec) -> tuple[float, int, int, str]:
        active = self.runner_active.get(runner.name, 0)
        max_jobs = runner.max_jobs or self.goal.parallel
        core_score = self._allocator(runner).scheduling_core_score(spec.numa_node)
        return active / float(max_jobs), active, -core_score, runner.name

    def _ensure_runner(self, runner: _RuntimeRunnerSpec) -> None:
        runner.bind(self.goal).validate()
        self.runner_active.setdefault(runner.name, 0)
        self.allocators.setdefault(runner.name, CoreAllocator(runner))

    def _allocator(self, runner: _RuntimeRunnerSpec) -> CoreAllocator:
        self._ensure_runner(runner)
        return self.allocators[runner.name]

    def runner_status(
        self,
        runner: _RuntimeRunnerSpec,
        *,
        numa_node: int | None = None,
    ) -> RunnerStatus:
        with self.condition:
            self._ensure_runner(runner)
            allocator = self._allocator(runner)
            active = self.runner_active.get(runner.name, 0)
            max_jobs = runner.max_jobs or self.goal.parallel
            configured_cores = allocator.configured_cores(numa_node)
            available_cores = allocator.available_cores(numa_node)
            return RunnerStatus(
                name=runner.name,
                kind=runner.kind,
                active_jobs=active,
                max_jobs=max_jobs,
                available_jobs=max(0, max_jobs - active),
                logical_core_count=allocator.logical_core_count(numa_node),
                configured_cores=configured_cores,
                configured_core_count=len(configured_cores),
                available_cores=available_cores,
                available_core_count=len(available_cores),
                numa_node=numa_node,
            )

    def _paths(self, spec: _CommandSpec, runner: _RuntimeRunnerSpec) -> tuple[str, str, str, str, str]:
        relpath = spec.work_relpath or self._default_work_relpath(spec)
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
        return relpath, work_dir, command_path, stdout_path, stderr_path

    def _command_launch(
        self,
        spec: _CommandSpec,
        runner: _RuntimeRunnerSpec,
        full_command: str,
        work_relpath: str,
        work_dir: str,
        command_path: str,
        stdout_path: str,
        stderr_path: str,
    ) -> tuple[list[str], dict[str, str] | None, str | None, bool]:
        env = self._command_env(spec, runner, work_dir, work_relpath)
        if runner.kind == "local":
            Path(work_dir).mkdir(parents=True, exist_ok=True)
            process_env = os.environ.copy()
            process_env.update(env)
            return [runner.shell, "-lc", full_command], process_env, spec.cwd or work_dir, True

        remote_cwd = spec.cwd or work_dir
        remote_body = " && ".join(
            [
                f"mkdir -p {self._quote_remote_path(work_dir)}",
                (
                    f"printf '%s\\n' {shlex.quote(spec.command)} "
                    f"> {self._quote_remote_path(command_path)}"
                ),
                f"cd {self._quote_remote_path(remote_cwd)}",
                (
                    f"{{ {self._remote_shell_body(runner, env, full_command)}; }} "
                    f"> {self._quote_remote_path(stdout_path)} "
                    f"2> {self._quote_remote_path(stderr_path)}"
                ),
            ]
        )
        argv = ["ssh", *runner.ssh_options, "-p", str(runner.port), runner.target]
        argv.extend([runner.shell, "-lc", shlex.quote(remote_body)])
        return argv, None, None, False

    def _command_env(
        self,
        spec: _CommandSpec,
        runner: _RuntimeRunnerSpec,
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
    def _join_path(work_dir: str, filename: str, *, runner: _RuntimeRunnerSpec) -> str:
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
        runner: _RuntimeRunnerSpec,
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

    @staticmethod
    def _valid_env_key(key: str) -> bool:
        return re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key) is not None

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
    finally:
        with runtime.condition:
            runtime.stopping = True
            runtime.condition.notify_all()
        runtime.thread.join()
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
    "RunnerSpec",
    "RunnerStatus",
    "Workload",
    "goal",
    "workloads",
]


if __name__ == "__main__":
    raise SystemExit(main())
