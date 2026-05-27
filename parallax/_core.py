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
    return _NAME_RE.sub("_", value).strip("_") or "command"


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
    run_id: str | None = None
    argv: list[str] = field(default_factory=list)
    args: dict[str, str] = field(default_factory=dict)


@dataclass
class CommandSpec:
    id: str
    command: str
    index: int
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

    def schd(
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
    ) -> CommandSpec:
        return self._bound_goal().schd(
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
        root_path: str = "workspace/log_root",
        root_locked: bool = False,
    ) -> None:
        self.mode = mode
        self.options = options
        self.root_path = root_path
        self.root_locked = root_locked
        self.runners: list[RunnerSpec] = [self.local("local")]
        self.env: dict[str, str] = {}
        self.parallel = 1
        self.argv = list(options.argv if options else [])
        self.args = dict(options.args if options else {})
        self._runtime: Any | None = None
        self._scheduled: list[CommandSpec] = []
        self._name_counts: dict[str, int] = {}

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

    def setRoot(self, root_path: str) -> None:
        if self.root_locked:
            return
        if not root_path:
            raise ParallaxError("goal.setRoot() needs a non-empty path")
        self.root_path = str(root_path)

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
        base_name = _sanitize(name or "cmd")
        command_id = self._next_command_id(base_name)
        normalized_work_relpath = (
            _validate_relative_path(work_relpath, label="work_relpath")
            if work_relpath is not None
            else None
        )
        return CommandSpec(
            id=command_id,
            name=base_name,
            command=command,
            index=sum(self._name_counts.values()),
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

    def _next_command_id(self, base_name: str) -> str:
        count = self._name_counts.get(base_name, 0) + 1
        self._name_counts[base_name] = count
        if count == 1:
            return base_name
        return f"{base_name}.{count}"

    def _runtime_required(self) -> Any:
        if self._runtime is None:
            raise ParallaxError("Parallax runtime is not bound; run with parallax <config.py>")
        return self._runtime


class AutoRunner:
    name = "auto"

    def __init__(self, goal: Goal) -> None:
        self.goal = goal

    def schd(self, command: str, **kwargs: Any) -> CommandSpec:
        return self.goal.schd(command, **kwargs)

    def run(self, command: str, **kwargs: Any) -> Any:
        return self.goal.run(command, **kwargs)


def execute_config(path: Path, *, goal: Goal, runner: Any) -> None:
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
    parallax_shell._bind(goal, runner)
    globals_dict = {
        "__file__": str(path),
        "__name__": "__parallax_config__",
        "goal": goal,
        "runner": runner,
    }
    code = compile(path.read_text(encoding="utf-8"), str(path), "exec")
    exec(code, globals_dict)
