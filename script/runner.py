from __future__ import annotations

import argparse
import fcntl
import inspect
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from _core import (
    Goal,
    ParallaxError,
    RunnerSpec,
    TaskFunc,
    TaskSpec,
    _sanitize,
    _shell_join,
    _strip_remainder_marker,
    _validate_relative_path,
    execute_config,
)


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

    def __enter__(self) -> "CoreLease":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
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
    def __init__(self, spec: RunnerSpec, root_path: str, runner_name: str) -> None:
        self.spec = spec
        lock_dir = Path(root_path) / ".parallax-locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        self.lock_path = lock_dir / f"{_sanitize(runner_name)}.lock"
        self.state_path = lock_dir / f"{_sanitize(runner_name)}.json"

    def acquire(
        self,
        *,
        num_cores: int = 0,
        numa_node: int | None = None,
        cores: Sequence[int] | None = None,
    ) -> CoreLease:
        if cores:
            requested = [int(core) for core in cores]
        elif num_cores > 0:
            requested = self._allocate_count(num_cores, numa_node)
        else:
            requested = []
        mem_node = numa_node if numa_node is not None else self._infer_node(requested)
        return CoreLease(self, requested, mem_node)

    def release(self, cores: Sequence[int]) -> None:
        if not cores:
            return
        with self._locked_state() as state:
            available = set(state["available"])
            available.update(int(core) for core in cores)
            state["available"] = sorted(available & set(state["pool"]))

    def _allocate_count(self, count: int, numa_node: int | None) -> list[int]:
        if count <= 0:
            return []
        pool = self._candidate_pool(numa_node)
        if not pool:
            raise ParallaxError(
                f"runner {self.spec.name!r} needs core_pool/numa_nodes for NUMA allocation"
            )
        while True:
            with self._locked_state() as state:
                available = [core for core in pool if core in set(state["available"])]
                if len(available) >= count:
                    allocated = available[:count]
                    state["available"] = [
                        core for core in state["available"] if core not in set(allocated)
                    ]
                    return allocated
            time.sleep(1)

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

    def _initial_state(self) -> dict[str, list[int]]:
        cores = self._configured_pool()
        return {"pool": cores, "available": cores}

    def _normalize_state(self, state: dict[str, list[int]]) -> dict[str, list[int]]:
        pool = self._configured_pool()
        if state.get("pool") != pool:
            return {"pool": pool, "available": pool}
        available = sorted(set(state.get("available", [])) & set(pool))
        return {"pool": pool, "available": available}

    def _locked_state(self) -> Any:
        allocator = self

        class LockedState:
            def __enter__(self) -> dict[str, list[int]]:
                self.lock_file = allocator.lock_path.open("w", encoding="utf-8")
                fcntl.flock(self.lock_file, fcntl.LOCK_EX)
                if allocator.state_path.exists():
                    with allocator.state_path.open("r", encoding="utf-8") as fs:
                        self.state = allocator._normalize_state(json.load(fs))
                else:
                    self.state = allocator._initial_state()
                return self.state

            def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                with allocator.state_path.open("w", encoding="utf-8") as fs:
                    json.dump(self.state, fs)
                fcntl.flock(self.lock_file, fcntl.LOCK_UN)
                self.lock_file.close()

        return LockedState()


class RuntimeRunner:
    def __init__(
        self,
        *,
        goal: Goal,
        runner_name: str,
        root_path: str,
        run_id: str,
        work_relpath: str,
        dry_run: bool = False,
    ) -> None:
        self.goal = goal
        self.name = runner_name
        self.root_path = root_path
        self.run_id = run_id
        self.work_relpath = ""
        self._work_dir = ""
        self.set_work_relpath(work_relpath)
        self.dry_run = dry_run
        self.current_task: TaskSpec | None = None
        self.spec = goal.local("local")
        self.allocator = CoreAllocator(self.spec, root_path, runner_name)

    @property
    def work_dir(self) -> str:
        return self._work_dir

    def set_work_relpath(self, work_relpath: str) -> None:
        self.work_relpath = _validate_relative_path(work_relpath, label="runner work_relpath")
        self._work_dir = str((Path(self.root_path) / self.work_relpath).resolve())

    @property
    def env(self) -> dict[str, str]:
        merged = dict(self.goal.env)
        merged.update(self.spec.env)
        if self.current_task:
            merged.update(self.current_task.env)
        return merged

    def run(
        self,
        command: str,
        *,
        num_cores: int | None = None,
        numa_node: int | None = None,
        cores: Sequence[int] | None = None,
        check: bool = True,
        timeout: float | None = None,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
    ) -> int:
        if self.current_task is None:
            raise ParallaxError("runner.run() can only be used while a task is executing")
        use_cores = self.current_task.num_cores if num_cores is None else num_cores
        use_node = self.current_task.numa_node if numa_node is None else numa_node
        command_env = os.environ.copy()
        command_env.update(self.env)
        command_env.update({str(k): str(v) for k, v in dict(env or {}).items()})
        with self.allocator.acquire(num_cores=use_cores or 0, numa_node=use_node, cores=cores) as lease:
            full_command = lease.wrap(command, self.spec.shell)
            if self.dry_run:
                print("[dry-run]", full_command)
                return 0
            completed = subprocess.run(
                [self.spec.shell, "-lc", full_command],
                cwd=cwd or self.work_dir,
                env=command_env,
                text=True,
                check=False,
                timeout=timeout,
            )
            if check and completed.returncode != 0:
                raise ParallaxError(f"command failed with exit code {completed.returncode}: {command}")
            return completed.returncode

    def _find_runner_spec(self, runner_name: str) -> RunnerSpec:
        for spec in self.goal.runners:
            if spec.name == runner_name:
                return spec
        if runner_name == "local":
            return self.goal.local("local")
        raise ParallaxError(f"runner {runner_name!r} is not configured")


def run_worker(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description="execute one Parallax task")
    parser.add_argument("--runner", default="local", help="runner name")
    parser.add_argument("--root-path", default="workspace/log_root", help="root path for locks")
    parser.add_argument("--run-id", default="manual", help="run id")
    parser.add_argument("--work-relpath", help="task working path relative to root path")
    parser.add_argument("--dry-run", action="store_true", help="render commands only")
    parser.add_argument("config", help="Python config file")
    parser.add_argument("task_tail", nargs=argparse.REMAINDER, help="-- task_id key=value...")
    args = parser.parse_args(list(argv))

    tail = _strip_remainder_marker(args.task_tail)
    if not tail:
        raise ParallaxError("missing task id")
    task_id = tail[0]
    if tail[1:]:
        raise ParallaxError(
            "task function arguments are not supported; use a task builder to create zero-arg tasks"
        )
    config_path = Path(args.config).resolve()

    goal = Goal(mode="worker")
    runtime_runner = RuntimeRunner(
        goal=goal,
        runner_name=args.runner,
        root_path=args.root_path,
        run_id=args.run_id,
        work_relpath=args.work_relpath
        or str(Path(f"parallax-{args.run_id}") / _sanitize(f"{task_id}-{args.runner}")),
        dry_run=args.dry_run,
    )
    execute_config(config_path, goal=goal, runner=runtime_runner)
    runtime_runner.spec = runtime_runner._find_runner_spec(args.runner)
    runtime_runner.allocator = CoreAllocator(runtime_runner.spec, args.root_path, args.runner)

    task = goal.find_task(task_id)
    if args.work_relpath is None and task.work_relpath is not None:
        runtime_runner.set_work_relpath(task.work_relpath)
    runtime_runner.current_task = task
    Path(runtime_runner.work_dir).mkdir(parents=True, exist_ok=True)
    os.environ.update(goal.env)
    os.environ.update(runtime_runner.spec.env)
    os.environ.update(task.env)
    os.environ.update(
        {
            "PARALLAX_ROOT_PATH": args.root_path,
            "PARALLAX_RUN_ID": args.run_id,
            "PARALLAX_WORK_RELPATH": runtime_runner.work_relpath,
            "PARALLAX_WORK_DIR": runtime_runner.work_dir,
        }
    )
    os.chdir(runtime_runner.work_dir)
    invoke_task(task.func)
    return 0


def invoke_task(func: TaskFunc) -> Any:
    signature = inspect.signature(func)
    if signature.parameters:
        raise ParallaxError(
            f"task {func.__name__!r} must not declare parameters; "
            "use a task builder/closure to capture values"
        )
    return func()


def main() -> int:
    try:
        return run_worker(sys.argv[1:])
    except ParallaxError as err:
        print(f"runner: {err}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("runner: interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
