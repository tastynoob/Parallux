from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Sequence

from ._core import CommandSpec, ParalluxError, RunnerSpec, RunnerStatus, _shell_join


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
    def __init__(self, spec: RunnerSpec) -> None:
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
class SchedulerAssignment:
    runner: RunnerSpec
    lease: CoreLease


class DefaultScheduler:
    def __init__(self, goal: Any) -> None:
        self.goal = goal
        self.runner_active: dict[str, int] = {}
        self.allocators: dict[str, CoreAllocator] = {}

    def try_assign(
        self,
        spec: CommandSpec,
        *,
        running_count: int,
    ) -> SchedulerAssignment | None:
        if running_count >= self.goal.parallel:
            return None
        for runner in self._candidate_runners(spec):
            if self.runner_active.get(runner.name, 0) >= self._max_jobs(runner):
                continue
            lease = self._allocator(runner).try_acquire(
                threads=spec.threads,
                numa_node=spec.numa_node,
                cores=spec.cores,
            )
            if lease is None:
                continue
            return SchedulerAssignment(runner=runner, lease=lease)
        return None

    def mark_started(self, assignment: SchedulerAssignment) -> None:
        name = assignment.runner.name
        self.runner_active[name] = self.runner_active.get(name, 0) + 1

    def release_started(self, assignment: SchedulerAssignment) -> None:
        name = assignment.runner.name
        self.runner_active[name] = self.runner_active.get(name, 0) - 1
        assignment.lease.release()

    def release_unstarted(self, assignment: SchedulerAssignment) -> None:
        assignment.lease.release()

    def runner_status(
        self,
        runner: RunnerSpec,
        *,
        numa_node: int | None = None,
    ) -> RunnerStatus:
        self._ensure_runner(runner)
        allocator = self._allocator(runner)
        active = self.runner_active.get(runner.name, 0)
        max_jobs = self._max_jobs(runner)
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

    def _candidate_runners(self, spec: CommandSpec) -> list[RunnerSpec]:
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

    def _runner_sort_key(self, runner: RunnerSpec, spec: CommandSpec) -> tuple[float, int, int, str]:
        active = self.runner_active.get(runner.name, 0)
        max_jobs = self._max_jobs(runner)
        core_score = self._allocator(runner).scheduling_core_score(spec.numa_node)
        return active / float(max_jobs), active, -core_score, runner.name

    def _ensure_runner(self, runner: RunnerSpec) -> None:
        runner.bind(self.goal).validate()
        self.runner_active.setdefault(runner.name, 0)
        self.allocators.setdefault(runner.name, CoreAllocator(runner))

    def _allocator(self, runner: RunnerSpec) -> CoreAllocator:
        self._ensure_runner(runner)
        return self.allocators[runner.name]

    def _max_jobs(self, runner: RunnerSpec) -> int:
        return runner.max_jobs or self.goal.parallel
