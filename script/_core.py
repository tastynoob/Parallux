from __future__ import annotations

import re
import shlex
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence


TaskFunc = Callable[..., Any]
_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")


class ParallaxError(RuntimeError):
    pass


def _sanitize(value: str) -> str:
    return _NAME_RE.sub("_", value).strip("_") or "task"


def _strip_remainder_marker(values: Sequence[str]) -> list[str]:
    items = list(values)
    if items and items[0] == "--":
        return items[1:]
    return items


def _shell_join(argv: Sequence[Any]) -> str:
    return " ".join(shlex.quote(str(arg)) for arg in argv)


def _validate_relative_path(value: str, *, label: str) -> str:
    path = Path(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise ParallaxError(f"{label} must be a relative path under root_path")
    return path.as_posix()


@dataclass
class RunnerSpec:
    name: str
    kind: str = "local"
    host: str | None = None
    user: str | None = None
    port: int = 22
    workdir: str | None = None
    python: str = "python3"
    shell: str = "bash"
    max_jobs: int | None = None
    env: dict[str, str] = field(default_factory=dict)
    core_pool: list[int] = field(default_factory=list)
    numa_nodes: dict[int, list[int]] = field(default_factory=dict)
    ssh_options: list[str] = field(default_factory=list)

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


@dataclass
class TaskSpec:
    id: str
    name: str
    func: TaskFunc
    index: int
    num_cores: int = 0
    numa_node: int | None = None
    env: dict[str, str] = field(default_factory=dict)
    work_relpath: str | None = None


@dataclass
class ControllerOptions:
    config_path: Path
    config_arg: str
    dry_run: bool = False
    run_id: str | None = None


class Goal:
    def __init__(
        self,
        *,
        mode: str,
        options: ControllerOptions | None = None,
        scheduler_factory: Callable[[Any, ControllerOptions, str], Any] | None = None,
    ) -> None:
        self.mode = mode
        self.options = options
        self.scheduler_factory = scheduler_factory
        self.tasks: list[TaskSpec] = []
        self.runners: list[RunnerSpec] = [self.local("local")]
        self.env: dict[str, str] = {}
        self.parallel = 1
        self.issued = False
        self._name_counts: dict[str, int] = {}

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
    ) -> RunnerSpec:
        return RunnerSpec(
            name=name,
            kind="local",
            workdir=workdir,
            python=python,
            shell=shell,
            max_jobs=max_jobs,
            env=dict(env or {}),
            core_pool=list(core_pool or []),
            numa_nodes={int(k): list(v) for k, v in dict(numa_nodes or {}).items()},
        )

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
    ) -> RunnerSpec:
        return RunnerSpec(
            name=name,
            kind="ssh",
            host=host or name,
            user=user,
            port=port,
            workdir=workdir,
            python=python,
            shell=shell,
            max_jobs=max_jobs,
            env=dict(env or {}),
            core_pool=list(core_pool or []),
            numa_nodes={int(k): list(v) for k, v in dict(numa_nodes or {}).items()},
            ssh_options=list(ssh_options or []),
        )

    def setRunner(self, runners: str | RunnerSpec | Sequence[str | RunnerSpec]) -> None:
        values = [runners] if isinstance(runners, (str, RunnerSpec)) else list(runners)
        if not values:
            raise ParallaxError("goal.setRunner() needs at least one runner")
        parsed: list[RunnerSpec] = []
        for item in values:
            if isinstance(item, RunnerSpec):
                spec = item
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

    def addTask(
        self,
        task: TaskFunc | Sequence[TaskFunc],
        *,
        name: str | None = None,
        num_cores: int = 0,
        numa_node: int | None = None,
        env: Mapping[str, str] | None = None,
        work_relpath: str | None = None,
    ) -> None:
        if isinstance(task, Sequence) and not callable(task):
            for item in task:
                self.addTask(
                    item,
                    name=name,
                    num_cores=num_cores,
                    numa_node=numa_node,
                    env=env,
                    work_relpath=work_relpath,
                )
            return
        if not callable(task):
            raise ParallaxError(f"goal.addTask() expects a callable task, got {task!r}")
        if num_cores < 0:
            raise ParallaxError("task num_cores must be >= 0")
        normalized_work_relpath = (
            _validate_relative_path(work_relpath, label="task work_relpath")
            if work_relpath is not None
            else None
        )
        base_name = _sanitize(name or getattr(task, "__name__", "task"))
        task_id = self._next_task_id(base_name)
        self.tasks.append(
            TaskSpec(
                id=task_id,
                name=base_name,
                func=task,
                index=len(self.tasks),
                num_cores=int(num_cores),
                numa_node=numa_node,
                env={str(k): str(v) for k, v in dict(env or {}).items()},
                work_relpath=normalized_work_relpath,
            )
        )

    def task(self, func: TaskFunc | None = None, **kwargs: Any) -> Any:
        def decorate(real_func: TaskFunc) -> TaskFunc:
            self.addTask(real_func, **kwargs)
            return real_func

        if func is None:
            return decorate
        return decorate(func)

    def issue(self, *, root_path: str = "workspace/log_root") -> None:
        if self.mode != "controller":
            return
        if self.issued:
            raise ParallaxError("goal.issue() can only be called once")
        if self.options is None:
            raise ParallaxError("controller options are missing")
        if self.scheduler_factory is None:
            raise ParallaxError("controller scheduler is missing")

        self.issued = True
        self.scheduler_factory(self, self.options, root_path).run()

    def _next_task_id(self, base_name: str) -> str:
        count = self._name_counts.get(base_name, 0) + 1
        self._name_counts[base_name] = count
        if count == 1:
            return base_name
        return f"{base_name}.{count}"

    def find_task(self, task_id_or_name: str) -> TaskSpec:
        by_id = [task for task in self.tasks if task.id == task_id_or_name]
        if len(by_id) == 1:
            return by_id[0]
        by_name = [task for task in self.tasks if task.name == task_id_or_name]
        if len(by_name) == 1:
            return by_name[0]
        if len(by_name) > 1:
            choices = ", ".join(task.id for task in by_name[:8])
            raise ParallaxError(
                f"task name {task_id_or_name!r} is ambiguous; use one of: {choices}"
            )
        raise ParallaxError(f"unknown task: {task_id_or_name!r}")


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
