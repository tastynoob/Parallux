from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence, TypeVar, overload


TaskFunc = Callable[[], Any]
F = TypeVar("F", bound=TaskFunc)


class RunnerSpec(Protocol):
    name: str
    kind: str
    host: str | None
    user: str | None
    port: int
    workdir: str | None
    python: str
    shell: str
    max_jobs: int | None
    env: dict[str, str]
    core_pool: list[int]
    numa_nodes: dict[int, list[int]]
    ssh_options: list[str]


class GoalShell(Protocol):
    def local(
        self,
        name: str = "local",
        *,
        workdir: str | None = None,
        python: str = "python3",
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
        workdir: str | None = None,
        python: str = "python3",
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

    def addTask(
        self,
        task: TaskFunc | Sequence[TaskFunc],
        *,
        name: str | None = None,
        num_cores: int = 0,
        numa_node: int | None = None,
        env: Mapping[str, str] | None = None,
        work_relpath: str | None = None,
    ) -> None: ...

    @overload
    def task(self, func: F) -> F: ...

    @overload
    def task(
        self,
        *,
        name: str | None = None,
        num_cores: int = 0,
        numa_node: int | None = None,
        env: Mapping[str, str] | None = None,
        work_relpath: str | None = None,
    ) -> Callable[[F], F]: ...

    def task(self, func: F | None = None, **kwargs: Any) -> F | Callable[[F], F]: ...

    def issue(self, *, root_path: str = "workspace/log_root") -> None: ...


class RunnerShell(Protocol):
    name: str
    work_relpath: str
    work_dir: str
    env: dict[str, str]

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
    ) -> int: ...


class _Missing:
    def __init__(self, name: str) -> None:
        self.name = name
        self._target: Any | None = None

    def __getattr__(self, attr: str) -> Any:
        if self._target is not None:
            return getattr(self._target, attr)
        raise RuntimeError(
            f"parallax.{self.name} is not bound. Run the config through "
            "script/controller.py or script/runner.py to bind the real object."
        )

    def _bind(self, target: Any) -> None:
        self._target = target


goal: GoalShell = _Missing("goal")  # type: ignore[assignment]
runner: RunnerShell = _Missing("runner")  # type: ignore[assignment]


def _bind(real_goal: Any, real_runner: Any) -> None:
    goal._bind(real_goal)  # type: ignore[attr-defined]
    runner._bind(real_runner)  # type: ignore[attr-defined]


__all__ = ["GoalShell", "RunnerShell", "RunnerSpec", "goal", "runner"]
