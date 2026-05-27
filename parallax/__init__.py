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
    AutoRunner as _AutoRunner,
    CommandSpec as _CommandSpec,
    Goal as _RuntimeGoal,
    ParallaxError,
    RunnerSpec as _RuntimeRunnerSpec,
    RuntimeOptions as _RuntimeOptions,
    _sanitize,
    _shell_join,
    execute_config,
)


__version__ = "0.1.0"
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
    ) -> Any: ...


class Handle(Protocol):
    def sync(self, timeout: float | None = None) -> Any: ...

    def done(self) -> bool: ...


class GoalShell(Protocol):
    mode: str
    root_path: str
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

    def setRoot(self, root_path: str) -> None: ...

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


RunnerShell = RunnerSpec


class _Missing:
    def __init__(self, name: str) -> None:
        self._proxy_name = name
        self._target: Any | None = None

    def __getattr__(self, attr: str) -> Any:
        if self._target is not None:
            return getattr(self._target, attr)
        raise RuntimeError(
            f"parallax.{self._proxy_name} is not bound. Run the config through "
            "parallax or python3 -m parallax to bind the real object."
        )

    def _bind(self, target: Any) -> None:
        self._target = target


goal: GoalShell = _Missing("goal")  # type: ignore[assignment]
runner: RunnerShell = _Missing("runner")  # type: ignore[assignment]


def _bind(real_goal: Any, real_runner: Any) -> None:
    goal._bind(real_goal)  # type: ignore[attr-defined]
    runner._bind(real_runner)  # type: ignore[attr-defined]


@dataclass(frozen=True)
class CommandResult:
    id: str
    runner: str
    command: str
    returncode: int
    log_dir: str
    work_dir: str


class Handle:
    def __init__(self, future: Future[CommandResult]) -> None:
        self._future = future

    def sync(self, timeout: float | None = None) -> CommandResult:
        return self._future.result(timeout=timeout)

    def done(self) -> bool:
        return self._future.done()


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
            raise ParallaxError(f"{len(errors)} command(s) failed; first error: {first}") from first
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
                raise ParallaxError(
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

    def _validate_exact(self, cores: Sequence[int]) -> None:
        if not self.pool:
            raise ParallaxError(f"runner {self.spec.name!r} needs core_pool for explicit cores")
        unknown = set(cores) - set(self.pool)
        if unknown:
            raise ParallaxError(
                f"runner {self.spec.name!r} requested cores outside core_pool: "
                f"{sorted(unknown)}"
            )

    def _try_allocate_count(self, count: int, numa_node: int | None) -> list[int] | None:
        pool = self._candidate_pool(numa_node)
        if not pool:
            raise ParallaxError(
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


@dataclass
class RunningCommand:
    spec: _CommandSpec
    runner: _RuntimeRunnerSpec
    process: subprocess.Popen[bytes]
    future: Future[CommandResult]
    stdout: BinaryIO
    stderr: BinaryIO
    lease: CoreLease
    started_at: float
    log_dir: Path
    work_dir: str


class Runtime:
    def __init__(self, goal: _RuntimeGoal, options: _RuntimeOptions) -> None:
        self.goal = goal
        self.options = options
        self.run_id = options.run_id or self._make_run_id()
        self.condition = threading.Condition()
        self.pending: list[PendingCommand] = []
        self.running: list[RunningCommand] = []
        self.handles: list[Handle] = []
        self.runner_active: dict[str, int] = {}
        self.allocators: dict[str, CoreAllocator] = {}
        self.stopping = False
        self.thread = threading.Thread(target=self._run_loop, name="parallax-scheduler", daemon=True)
        self.thread.start()

    def submit(self, spec: _CommandSpec) -> Handle:
        future: Future[CommandResult] = Future()
        handle = Handle(future)
        with self.condition:
            self.pending.append(PendingCommand(spec=spec, future=future))
            self.handles.append(handle)
            self.condition.notify_all()
        return handle

    def submit_many(self, specs: Sequence[_CommandSpec]) -> GroupHandle:
        return GroupHandle([self.submit(spec) for spec in specs])

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
                runner, lease, argv, env, cwd, log_dir, work_dir, full_command = launch
                self.pending.remove(pending)
                try:
                    running = self._start_process(
                        pending,
                        runner,
                        lease,
                        argv,
                        env,
                        cwd,
                        log_dir,
                        work_dir,
                        full_command,
                    )
                except BaseException as err:
                    lease.release()
                    pending.future.set_exception(err)
                    started = True
                    break
                if running is not None:
                    self.running.append(running)
                    self.runner_active[runner.name] = self.runner_active.get(runner.name, 0) + 1
                started = True
                break

    def _prepare_launch_locked(
        self,
        spec: _CommandSpec,
    ) -> tuple[
        _RuntimeRunnerSpec,
        CoreLease,
        list[str],
        dict[str, str] | None,
        str | None,
        Path,
        str,
        str,
    ] | None:
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
            log_dir, work_dir = self._paths(spec, runner)
            full_command = lease.wrap(spec.command, runner.shell)
            argv, env, cwd = self._command_launch(spec, runner, full_command, work_dir)
            return runner, lease, argv, env, cwd, log_dir, work_dir, full_command
        return None

    def _start_process(
        self,
        pending: PendingCommand,
        runner: _RuntimeRunnerSpec,
        lease: CoreLease,
        argv: list[str],
        env: dict[str, str] | None,
        cwd: str | None,
        log_dir: Path,
        work_dir: str,
        full_command: str,
    ) -> RunningCommand | None:
        log_dir.mkdir(parents=True, exist_ok=True)
        with (log_dir / "command.txt").open("w", encoding="utf-8") as fs:
            fs.write(pending.spec.command)
            fs.write("\n")
            fs.write(f"executor: {_shell_join(argv)}\n")
        stdout = (log_dir / "stdout.log").open("wb")
        stderr = (log_dir / "stderr.log").open("wb")
        if self.options.dry_run:
            stdout.write(f"[dry-run] {full_command}\n".encode("utf-8"))
            stdout.close()
            stderr.close()
            lease.release()
            result = CommandResult(
                id=pending.spec.id,
                runner=runner.name,
                command=pending.spec.command,
                returncode=0,
                log_dir=str(log_dir),
                work_dir=work_dir,
            )
            pending.future.set_result(result)
            return None
        process = subprocess.Popen(
            argv,
            stdout=stdout,
            stderr=stderr,
            env=env,
            cwd=cwd,
        )
        return RunningCommand(
            spec=pending.spec,
            runner=runner,
            process=process,
            future=pending.future,
            stdout=stdout,
            stderr=stderr,
            lease=lease,
            started_at=time.time(),
            log_dir=log_dir,
            work_dir=work_dir,
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
                running.stderr.write(
                    f"parallax: command timed out after {running.spec.timeout}s\n".encode("utf-8")
                )
            returncode = running.process.poll()
            if returncode is None:
                continue
            self.running.remove(running)
            self.runner_active[running.runner.name] -= 1
            running.lease.release()
            running.stdout.close()
            running.stderr.close()
            result = CommandResult(
                id=running.spec.id,
                runner=running.runner.name,
                command=running.spec.command,
                returncode=returncode,
                log_dir=str(running.log_dir),
                work_dir=running.work_dir,
            )
            if running.spec.check and returncode != 0:
                running.future.set_exception(
                    ParallaxError(
                        f"command {running.spec.id!r} failed with exit code "
                        f"{returncode}: {running.spec.command}"
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
            raise ParallaxError("no runner configured")
        for runner in self.goal.runners:
            self._ensure_runner(runner)
        return sorted(
            self.goal.runners,
            key=lambda item: (
                self.runner_active.get(item.name, 0) / float(item.max_jobs or self.goal.parallel),
                self.runner_active.get(item.name, 0),
                item.name,
            ),
        )

    def _ensure_runner(self, runner: _RuntimeRunnerSpec) -> None:
        runner.bind(self.goal).validate()
        self.runner_active.setdefault(runner.name, 0)
        self.allocators.setdefault(runner.name, CoreAllocator(runner))

    def _allocator(self, runner: _RuntimeRunnerSpec) -> CoreAllocator:
        self._ensure_runner(runner)
        return self.allocators[runner.name]

    def _paths(self, spec: _CommandSpec, runner: _RuntimeRunnerSpec) -> tuple[Path, str]:
        relpath = spec.work_relpath or f"parallax-{self.run_id}/{_sanitize(spec.id)}-{runner.name}"
        log_dir = Path(self.goal.root_path) / relpath
        workspace = runner.workspace or self.goal.root_path
        work_path = Path(workspace) / relpath
        if runner.kind == "local":
            work_dir = str(work_path.resolve())
        else:
            work_dir = work_path.as_posix()
        return log_dir, work_dir

    def _command_launch(
        self,
        spec: _CommandSpec,
        runner: _RuntimeRunnerSpec,
        full_command: str,
        work_dir: str,
    ) -> tuple[list[str], dict[str, str] | None, str | None]:
        env = self._command_env(spec, runner, work_dir)
        if runner.kind == "local":
            Path(work_dir).mkdir(parents=True, exist_ok=True)
            process_env = os.environ.copy()
            process_env.update(env)
            return [runner.shell, "-lc", full_command], process_env, spec.cwd or work_dir

        remote_cwd = spec.cwd or work_dir
        remote_body = " && ".join(
            [
                f"mkdir -p {shlex.quote(work_dir)}",
                f"cd {shlex.quote(remote_cwd)}",
                f"{{ {self._remote_shell_body(runner, env, full_command)}; }}",
            ]
        )
        argv = ["ssh", *runner.ssh_options, "-p", str(runner.port), runner.target]
        argv.extend([runner.shell, "-lc", shlex.quote(remote_body)])
        return argv, None, None

    def _command_env(
        self,
        spec: _CommandSpec,
        runner: _RuntimeRunnerSpec,
        work_dir: str,
    ) -> dict[str, str]:
        env = dict(self.goal.env)
        env.update(runner.env)
        env.update(spec.env)
        relpath = spec.work_relpath or f"parallax-{self.run_id}/{_sanitize(spec.id)}-{runner.name}"
        log_dir = Path(self.goal.root_path) / relpath
        env.update(
            {
                "PARALLAX_RUN_ID": self.run_id,
                "PARALLAX_RUNNER": runner.name,
                "PARALLAX_WORK_RELPATH": relpath,
                "PARALLAX_WORK_DIR": work_dir,
                "PARALLAX_LOG_DIR": str(log_dir),
            }
        )
        return {str(k): str(v) for k, v in env.items()}

    def _remote_shell_body(
        self,
        runner: _RuntimeRunnerSpec,
        env: Mapping[str, str],
        command: str,
    ) -> str:
        invalid = [key for key in env if not self._valid_env_key(key)]
        if invalid:
            raise ParallaxError(f"invalid environment variable name for ssh runner: {invalid[0]!r}")
        exports = " ".join(
            f"export {key}={shlex.quote(value)};" for key, value in sorted(env.items())
        )
        return f"{exports} exec {shlex.quote(runner.shell)} -lc {shlex.quote(command)}"

    @staticmethod
    def _valid_env_key(key: str) -> bool:
        return re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key) is not None

    @staticmethod
    def _make_run_id() -> str:
        now = time.time()
        stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(now))
        return f"{stamp}-{int((now % 1) * 1000):03d}-{os.getpid()}"


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
    parser = argparse.ArgumentParser(description="run a Parallax config in a controlled runtime")
    parser.add_argument("config", help="Python config file")
    parser.add_argument("--dry-run", action="store_true", help="write commands without executing them")
    parser.add_argument("--root", help="override log root")
    parser.add_argument("--run-id", help="override generated run id")
    raw_argv = list(argv)
    if "--" in raw_argv:
        marker = raw_argv.index("--")
        parallax_argv = raw_argv[:marker]
        raw_config_args = raw_argv[marker + 1 :]
    else:
        parallax_argv = raw_argv
        raw_config_args = []
    args = parser.parse_args(parallax_argv)

    config_path = Path(args.config).resolve()
    if config_path.suffix != ".py":
        raise ParallaxError("config file must be a .py file")
    if not config_path.exists():
        raise ParallaxError(f"config file does not exist: {config_path}")

    config_argv, config_args = _split_config_args(raw_config_args)
    options = _RuntimeOptions(
        config_path=config_path,
        dry_run=args.dry_run,
        run_id=args.run_id,
        argv=config_argv,
        args=config_args,
    )
    real_goal = _RuntimeGoal(
        mode="runtime",
        options=options,
        root_path=args.root or "workspace/log_root",
        root_locked=args.root is not None,
    )
    runtime = Runtime(real_goal, options)
    real_goal.bind_runtime(runtime)
    try:
        execute_config(config_path, goal=real_goal, runner=_AutoRunner(real_goal))
        runtime.finalize()
    finally:
        with runtime.condition:
            runtime.stopping = True
            runtime.condition.notify_all()
        runtime.thread.join()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    sys.modules.setdefault("parallax", sys.modules[__name__])

    try:
        return run_runtime(sys.argv[1:] if argv is None else argv)
    except ParallaxError as err:
        print(f"parallax: {err}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("parallax: interrupted", file=sys.stderr)
        return 130


__all__ = [
    "GoalShell",
    "Handle",
    "RunnerShell",
    "RunnerSpec",
    "Workload",
    "goal",
    "runner",
    "workloads",
]


if __name__ == "__main__":
    raise SystemExit(main())
