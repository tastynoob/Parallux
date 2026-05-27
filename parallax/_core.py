from __future__ import annotations

import re
import shlex
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")


class ParallaxError(RuntimeError):
    pass


def _sanitize(value: str) -> str:
    cleaned = _NAME_RE.sub("_", value).strip("._")
    if not cleaned or cleaned in (".", ".."):
        return "task"
    return cleaned


def _shell_join(argv: Sequence[Any]) -> str:
    return " ".join(shlex.quote(str(arg)) for arg in argv)


def _validate_relative_path(value: str, *, label: str) -> str:
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise ParallaxError(f"{label} must be a relative path")
    return path.as_posix()


def _normalize_threads(threads: int, thread: int | None) -> int:
    if thread is not None:
        threads = thread
    if threads < 0:
        raise ParallaxError("threads must be >= 0")
    return int(threads)


@dataclass
class RuntimeOptions:
    config_path: Path
    dry_run: bool = False
    argv: list[str] = field(default_factory=list)
    args: dict[str, str] = field(default_factory=dict)


@dataclass
class CommandSpec:
    command: str
    index: int
    _task_id: int | None = field(default=None, repr=False)
    runner: RunnerSpec | None = None
    name: str | None = None
    threads: int = 0
    numa_node: int | None = None
    cores: tuple[int, ...] | None = None
    env: dict[str, str] = field(default_factory=dict)
    cwd: str | None = None
    work_relpath: str | None = None
    check: bool = True
    timeout: float | None = None


@dataclass(frozen=True)
class RunnerStatus:
    """Snapshot of scheduler-visible Runner capacity.

    Core availability only covers cores declared through core_pool or
    numa_nodes. It is not an operating-system CPU utilization metric.
    """

    name: str
    kind: str
    active_jobs: int
    max_jobs: int
    available_jobs: int
    logical_core_count: int
    configured_cores: list[int]
    configured_core_count: int
    available_cores: list[int]
    available_core_count: int
    numa_node: int | None = None


@dataclass
class RunnerSpec:
    name: str
    kind: str = "local"
    host: str | None = None
    user: str | None = None
    port: int = 22
    workspace: str | None = None
    shell: str = "bash"
    max_jobs: int | None = None
    env: dict[str, str] = field(default_factory=dict)
    core_pool: list[int] = field(default_factory=list)
    numa_nodes: dict[int, list[int]] = field(default_factory=dict)
    ssh_options: list[str] = field(default_factory=list)
    _goal: Any | None = field(default=None, repr=False, compare=False)

    @property
    def target(self) -> str:
        if self.kind == "local":
            return "local"
        host = self.host or self.name
        if self.user:
            return f"{self.user}@{host}"
        return host

    def validate(self) -> None:
        if not self.name:
            raise ParallaxError("runner name cannot be empty")
        if self.kind not in ("local", "ssh"):
            raise ParallaxError(f"runner {self.name!r} kind must be 'local' or 'ssh'")
        if self.port <= 0:
            raise ParallaxError(f"runner {self.name!r} port must be > 0")
        if self.max_jobs is not None and self.max_jobs <= 0:
            raise ParallaxError(f"runner {self.name!r} max_jobs must be > 0")
        for core in self.core_pool:
            if not isinstance(core, int) or core < 0:
                raise ParallaxError(f"runner {self.name!r} core_pool must contain non-negative ints")
        for node, cores in self.numa_nodes.items():
            if not isinstance(node, int) or node < 0:
                raise ParallaxError(f"runner {self.name!r} numa node must be a non-negative int")
            for core in cores:
                if not isinstance(core, int) or core < 0:
                    raise ParallaxError(
                        f"runner {self.name!r} numa node cores must contain non-negative ints"
                    )

    def bind(self, goal: Any) -> RunnerSpec:
        self._goal = goal
        return self

    def status(self, *, numa_node: int | None = None) -> RunnerStatus:
        return self._bound_goal().runner_status(self, numa_node=numa_node)

    def active_jobs(self) -> int:
        return self.status().active_jobs

    def available_jobs(self) -> int:
        """Return currently unused job slots for this Runner."""
        return self.status().available_jobs

    def logical_core_count(self, *, numa_node: int | None = None) -> int:
        """Return total logical threads known for this Runner or NUMA node."""
        return self.status(numa_node=numa_node).logical_core_count

    def configured_cores(self, *, numa_node: int | None = None) -> list[int]:
        """Return core ids managed by Parallax for binding."""
        return self.status(numa_node=numa_node).configured_cores

    def configured_core_count(self, *, numa_node: int | None = None) -> int:
        """Return the number of cores managed by Parallax for binding."""
        return self.status(numa_node=numa_node).configured_core_count

    def available_cores(self, *, numa_node: int | None = None) -> list[int]:
        """Return currently unleased configured core ids."""
        return self.status(numa_node=numa_node).available_cores

    def available_core_count(self, *, numa_node: int | None = None) -> int:
        """Return the number of currently unleased configured cores."""
        return self.status(numa_node=numa_node).available_core_count

    def run(
        self,
        command: str,
        *,
        name: str | None = None,
        threads: int = 0,
        thread: int | None = None,
        numa_node: int | None = None,
        cores: Sequence[int] | None = None,
        env: Mapping[str, Any] | None = None,
        cwd: str | None = None,
        work_relpath: str | None = None,
        check: bool = True,
        timeout: float | None = None,
    ) -> Any:
        return self._bound_goal().run(
            command,
            runner=self,
            name=name,
            threads=threads,
            thread=thread,
            numa_node=numa_node,
            cores=cores,
            env=env,
            cwd=cwd,
            work_relpath=work_relpath,
            check=check,
            timeout=timeout,
        )

    def _bound_goal(self) -> Any:
        if self._goal is None:
            raise ParallaxError(f"runner {self.name!r} is not bound to a goal")
        return self._goal


class Goal:
    def __init__(
        self,
        *,
        mode: str,
        options: RuntimeOptions | None = None,
    ) -> None:
        self.mode = mode
        self.options = options
        self.runners: list[RunnerSpec] = [self.local("local")]
        self.env: dict[str, str] = {}
        self.parallel = 1
        self.argv = list(options.argv if options else [])
        self.args = dict(options.args if options else {})
        self._runtime: Any | None = None
        self._scheduled: list[CommandSpec] = []
        self._command_index = 0

    def bind_runtime(self, runtime: Any) -> None:
        self._runtime = runtime

    def local(
        self,
        name: str = "local",
        *,
        workspace: str | None = None,
        shell: str = "bash",
        max_jobs: int | None = None,
        env: Mapping[str, Any] | None = None,
        core_pool: Iterable[int] | None = None,
        numa_nodes: Mapping[int, Iterable[int]] | None = None,
    ) -> RunnerSpec:
        return RunnerSpec(
            name=name,
            kind="local",
            workspace=workspace,
            shell=shell,
            max_jobs=max_jobs,
            env={str(k): str(v) for k, v in dict(env or {}).items()},
            core_pool=list(core_pool or []),
            numa_nodes={int(k): list(v) for k, v in dict(numa_nodes or {}).items()},
        ).bind(self)

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
        env: Mapping[str, Any] | None = None,
        core_pool: Iterable[int] | None = None,
        numa_nodes: Mapping[int, Iterable[int]] | None = None,
        ssh_options: Sequence[str] | None = None,
    ) -> RunnerSpec:
        return RunnerSpec(
            name=name,
            kind="ssh",
            host=host or name,
            user=user,
            port=port,
            workspace=workspace,
            shell=shell,
            max_jobs=max_jobs,
            env={str(k): str(v) for k, v in dict(env or {}).items()},
            core_pool=list(core_pool or []),
            numa_nodes={int(k): list(v) for k, v in dict(numa_nodes or {}).items()},
            ssh_options=list(ssh_options or []),
        ).bind(self)

    def setRunner(self, runners: str | RunnerSpec | Sequence[str | RunnerSpec]) -> None:
        values = [runners] if isinstance(runners, (str, RunnerSpec)) else list(runners)
        if not values:
            raise ParallaxError("goal.setRunner() needs at least one runner")
        parsed: list[RunnerSpec] = []
        for item in values:
            if isinstance(item, RunnerSpec):
                spec = item.bind(self)
            elif item == "local":
                spec = self.local("local")
            elif isinstance(item, str):
                spec = self.ssh(item, host=item)
            else:
                raise ParallaxError(f"unsupported runner value: {item!r}")
            spec.validate()
            parsed.append(spec)
        self.runners = parsed

    def setParallel(self, parallel: int) -> None:
        if parallel <= 0:
            raise ParallaxError("goal.setParallel() must be > 0")
        self.parallel = int(parallel)

    def setEnv(self, key: str, value: Any) -> None:
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
            raise ParallaxError(f"invalid environment variable name: {key!r}")
        self.env[key] = str(value)

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
        env: Mapping[str, Any] | None = None,
        cwd: str | None = None,
        work_relpath: str | None = None,
        check: bool = True,
        timeout: float | None = None,
    ) -> CommandSpec:
        spec = self._make_command(
            command,
            runner=runner,
            name=name,
            threads=threads,
            thread=thread,
            numa_node=numa_node,
            cores=cores,
            env=env,
            cwd=cwd,
            work_relpath=work_relpath,
            check=check,
            timeout=timeout,
        )
        self._scheduled.append(spec)
        return spec

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
        env: Mapping[str, Any] | None = None,
        cwd: str | None = None,
        work_relpath: str | None = None,
        check: bool = True,
        timeout: float | None = None,
    ) -> Any:
        spec = self._make_command(
            command,
            runner=runner,
            name=name,
            threads=threads,
            thread=thread,
            numa_node=numa_node,
            cores=cores,
            env=env,
            cwd=cwd,
            work_relpath=work_relpath,
            check=check,
            timeout=timeout,
        )
        return self._runtime_required().submit(spec)

    def issue(self) -> Any:
        specs = self._scheduled
        self._scheduled = []
        return self._runtime_required().submit_many(specs)

    def has_scheduled(self) -> bool:
        return bool(self._scheduled)

    def runner_status(
        self,
        runner: RunnerSpec | None = None,
        *,
        numa_node: int | None = None,
    ) -> RunnerStatus | list[RunnerStatus]:
        runtime = self._runtime_required()
        if runner is None:
            return [runtime.runner_status(item, numa_node=numa_node) for item in self.runners]
        return runtime.runner_status(runner, numa_node=numa_node)

    def _make_command(
        self,
        command: str,
        *,
        runner: RunnerSpec | None,
        name: str | None,
        threads: int,
        thread: int | None,
        numa_node: int | None,
        cores: Sequence[int] | None,
        env: Mapping[str, Any] | None,
        cwd: str | None,
        work_relpath: str | None,
        check: bool,
        timeout: float | None,
    ) -> CommandSpec:
        if not isinstance(command, str) or not command:
            raise ParallaxError("command must be a non-empty string")
        if runner is not None:
            runner.bind(self).validate()
        command_index = self._command_index
        self._command_index += 1
        normalized_work_relpath = (
            _validate_relative_path(work_relpath, label="work_relpath")
            if work_relpath is not None
            else None
        )
        return CommandSpec(
            command=command,
            index=command_index,
            name=_sanitize(name) if name is not None else None,
            runner=runner,
            threads=_normalize_threads(threads, thread),
            numa_node=numa_node,
            cores=tuple(int(core) for core in cores) if cores is not None else None,
            env={str(k): str(v) for k, v in dict(env or {}).items()},
            cwd=cwd,
            work_relpath=normalized_work_relpath,
            check=check,
            timeout=timeout,
        )

    def _runtime_required(self) -> Any:
        if self._runtime is None:
            raise ParallaxError("Parallax runtime is not bound; run with parallax <config.py>")
        return self._runtime


def execute_config(path: Path, *, goal: Goal) -> None:
    project_root = str(Path(__file__).resolve().parent.parent)
    config_dir = str(path.resolve().parent)
    for path_item in (config_dir, project_root):
        try:
            sys.path.remove(path_item)
        except ValueError:
            pass
        sys.path.insert(0, path_item)
    import parallax as parallax_shell

    if not hasattr(parallax_shell, "_bind"):
        raise ParallaxError("import parallax resolved to a module without runtime binding support")
    parallax_shell._bind(goal)
    globals_dict = {
        "__file__": str(path),
        "__name__": "__parallax_config__",
        "goal": goal,
    }
    code = compile(path.read_text(encoding="utf-8"), str(path), "exec")
    exec(code, globals_dict)
