from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Sequence

from ._core import (
    CommandSpec,
    ParalluxError,
    RunnerSpec,
    RunnerStatus,
    SchedulerSelector,
    _shell_join,
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

    def can_satisfy(
        self,
        *,
        threads: int = 0,
        numa_node: int | None = None,
        cores: Sequence[int] | None = None,
    ) -> bool:
        if cores:
            requested = [int(core) for core in cores]
            return bool(self.pool) and set(requested).issubset(set(self.pool))
        if threads > 0 and self.pool:
            return len(set(self._candidate_pool(numa_node))) >= threads
        if numa_node is not None and not self.pool:
            return False
        return True

    def can_acquire(
        self,
        *,
        threads: int = 0,
        numa_node: int | None = None,
        cores: Sequence[int] | None = None,
    ) -> bool:
        if not self.can_satisfy(threads=threads, numa_node=numa_node, cores=cores):
            return False
        if cores:
            requested = [int(core) for core in cores]
            return set(requested).issubset(self.available)
        if threads > 0 and self.pool:
            pool = self._candidate_pool(numa_node)
            available = [core for core in pool if core in self.available]
            return len(available) >= threads
        return True

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
class TaskAllocation:
    runner: RunnerSpec
    lease: CoreLease


def default_allocate_runner(runners: list[RunnerSpec], need_threads: int) -> RunnerSpec | None:
    del need_threads
    if not runners:
        return None
    return runners[0]


class TaskAllocator:
    def __init__(self, goal: Any) -> None:
        self.goal = goal
        self.runners = list(goal.runners)
        self.runner_active: dict[str, int] = {}
        self.allocators: dict[str, CoreAllocator] = {}

    def set_runners(self, runners: Sequence[RunnerSpec]) -> None:
        self.runners = list(runners)

    def try_allocate(
        self,
        spec: CommandSpec,
        *,
        scheduler: SchedulerSelector,
        running_count: int,
    ) -> TaskAllocation | None:
        if running_count >= self.goal.parallel:
            return None
        if spec.runner is not None:
            runner = self._validate_runner(spec.runner)
            if self._runner_can_allocate_now(runner, spec):
                return self._allocate_on_runner(runner, spec)
            if running_count == 0 and not self._runner_can_ever_allocate(runner, spec):
                raise self._unschedulable_error(spec, runners=[runner])
            return None

        candidates = self._candidate_runners(spec)
        if not candidates:
            if running_count == 0:
                raise self._unschedulable_error(spec, runners=self.runners)
            return None

        runner = scheduler(list(candidates), self._needed_threads(spec))
        if runner is None:
            return None
        runner = self._validate_scheduler_result(runner, candidates)
        return self._allocate_on_runner(runner, spec)

    def mark_started(self, allocation: TaskAllocation) -> None:
        name = allocation.runner.name
        self.runner_active[name] = self.runner_active.get(name, 0) + 1

    def release_started(self, allocation: TaskAllocation) -> None:
        name = allocation.runner.name
        self.runner_active[name] = self.runner_active.get(name, 0) - 1
        allocation.lease.release()

    def release_unstarted(self, allocation: TaskAllocation) -> None:
        allocation.lease.release()

    def runner_status(
        self,
        runner: RunnerSpec,
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

    def _ensure_runner(self, runner: RunnerSpec) -> None:
        runner.bind(self.goal).validate()
        self.runner_active.setdefault(runner.name, 0)
        self.allocators.setdefault(runner.name, CoreAllocator(runner))

    def _allocator(self, runner: RunnerSpec) -> CoreAllocator:
        self._ensure_runner(runner)
        return self.allocators[runner.name]

    def _max_jobs(self, runner: RunnerSpec) -> int:
        return runner.max_jobs or self.goal.parallel

    def _candidate_runners(self, spec: CommandSpec) -> list[RunnerSpec]:
        if not self.runners:
            raise ParalluxError("no runner configured")
        candidates = [
            runner
            for runner in (self._validate_runner(item) for item in self.runners)
            if self._runner_can_allocate_now(runner, spec)
        ]
        return sorted(
            candidates,
            key=lambda runner: self._runner_sort_key(
                self.runner_status(runner, spec.numa_node)
            ),
        )

    def _runner_can_allocate_now(self, runner: RunnerSpec, spec: CommandSpec) -> bool:
        self._ensure_runner(runner)
        if self.runner_active.get(runner.name, 0) >= self._max_jobs(runner):
            return False
        return self._allocator(runner).can_acquire(
            threads=spec.threads,
            numa_node=spec.numa_node,
            cores=spec.cores,
        )

    def _runner_can_ever_allocate(self, runner: RunnerSpec, spec: CommandSpec) -> bool:
        self._ensure_runner(runner)
        return self._allocator(runner).can_satisfy(
            threads=spec.threads,
            numa_node=spec.numa_node,
            cores=spec.cores,
        )

    def _allocate_on_runner(
        self,
        runner: RunnerSpec,
        spec: CommandSpec,
    ) -> TaskAllocation | None:
        lease = self._allocator(runner).try_acquire(
            threads=spec.threads,
            numa_node=spec.numa_node,
            cores=spec.cores,
        )
        if lease is None:
            return None
        return TaskAllocation(runner=runner, lease=lease)

    def _validate_runner(self, runner: RunnerSpec) -> RunnerSpec:
        if not isinstance(runner, RunnerSpec):
            raise ParalluxError(
                "scheduler must return a Runner created by goal.local() or goal.ssh()"
            )
        runner.bind(self.goal).validate()
        return runner

    def _validate_scheduler_result(
        self,
        runner: RunnerSpec,
        candidates: Sequence[RunnerSpec],
    ) -> RunnerSpec:
        runner = self._validate_runner(runner)
        if not any(runner is candidate for candidate in candidates):
            raise ParalluxError("scheduler must return one of the available runners")
        return runner

    def _unschedulable_error(
        self,
        spec: CommandSpec,
        *,
        runners: Sequence[RunnerSpec],
    ) -> ParalluxError:
        runner_names = ", ".join(runner.name for runner in runners) or "<none>"
        if spec.cores is not None:
            request = (
                f"cores={list(spec.cores)}; "
                f"need_threads={self._needed_threads(spec)}"
            )
        else:
            request = f"threads={spec.threads}"
        if spec.numa_node is not None:
            request = f"{request}; numa_node={spec.numa_node}"
        return ParalluxError(
            f"no runner can satisfy task resource request: {request}; "
            f"runners={runner_names}"
        )

    @staticmethod
    def _runner_sort_key(status: RunnerStatus) -> tuple[float, int, int, str]:
        core_score = status.available_core_count
        if status.configured_core_count == 0:
            core_score = status.logical_core_count
        return (
            status.active_jobs / float(status.max_jobs),
            status.active_jobs,
            -core_score,
            status.name,
        )

    @staticmethod
    def _needed_threads(spec: CommandSpec) -> int:
        if spec.cores is not None:
            return len(spec.cores)
        return spec.threads
