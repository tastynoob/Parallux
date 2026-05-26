from __future__ import annotations

import argparse
import fcntl
import inspect
import json
import os
import re
import shlex
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
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


@dataclass
class ControllerOptions:
    config_path: Path
    config_arg: str
    dry_run: bool = False
    run_id: str | None = None


class Goal:
    def __init__(self, *, mode: str, options: ControllerOptions | None = None) -> None:
        self.mode = mode
        self.options = options
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
    ) -> None:
        if isinstance(task, Sequence) and not callable(task):
            for item in task:
                self.addTask(
                    item,
                    name=name,
                    num_cores=num_cores,
                    numa_node=numa_node,
                    env=env,
                )
            return
        if not callable(task):
            raise ParallaxError(f"goal.addTask() expects a callable task, got {task!r}")
        if num_cores < 0:
            raise ParallaxError("task num_cores must be >= 0")
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
        self.issued = True
        Scheduler(self, self.options, root_path).run()

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


class Scheduler:
    def __init__(self, goal: Goal, options: ControllerOptions, root_path: str) -> None:
        self.goal = goal
        self.options = options
        self.root_path = Path(root_path)
        self.run_id = options.run_id or self._make_run_id()
        self.run_dir = self.root_path / f"parallax-{self.run_id}"
        self.script_dir = Path(__file__).resolve().parent
        self.semaphores = {
            runner.name: threading.Semaphore(runner.max_jobs or goal.parallel)
            for runner in self.goal.runners
        }

    def run(self) -> None:
        if not self.goal.tasks:
            raise ParallaxError("no task registered; use goal.addTask() first")
        self.run_dir.mkdir(parents=True, exist_ok=True)
        failures: list[tuple[TaskSpec, RunnerSpec, int]] = []

        runner_index = 0
        futures = set()
        task_iter = iter(self.goal.tasks)
        with ThreadPoolExecutor(max_workers=self.goal.parallel) as executor:
            def choose_runner() -> RunnerSpec:
                nonlocal runner_index
                runner = self.goal.runners[runner_index % len(self.goal.runners)]
                runner_index += 1
                return runner

            def submit_next() -> bool:
                try:
                    task = next(task_iter)
                except StopIteration:
                    return False
                runner = choose_runner()
                futures.add(executor.submit(self._run_one, task, runner))
                return True

            for _ in range(self.goal.parallel):
                if not submit_next():
                    break

            while futures:
                done, futures = wait(futures, return_when=FIRST_COMPLETED)
                for future in done:
                    task, runner, returncode = future.result()
                    if returncode != 0:
                        failures.append((task, runner, returncode))
                while len(futures) < self.goal.parallel:
                    if not submit_next():
                        break

        if failures:
            failure_path = self.run_dir / "failures.txt"
            with failure_path.open("w", encoding="utf-8") as fs:
                for task, runner, returncode in failures:
                    fs.write(f"{task.id} runner={runner.name} returncode={returncode}\n")
            raise ParallaxError(f"{len(failures)} task(s) failed; see {failure_path}")
        print(f"all tasks finished; logs: {self.run_dir}")

    def _run_one(self, task: TaskSpec, runner: RunnerSpec) -> tuple[TaskSpec, RunnerSpec, int]:
        task_dir = self.run_dir / _sanitize(f"{task.id}-{runner.name}")
        task_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = task_dir / "stdout.log"
        stderr_path = task_dir / "stderr.log"
        argv = self._runner_command(task, runner)
        with (task_dir / "command.txt").open("w", encoding="utf-8") as fs:
            fs.write(_shell_join(argv))
            fs.write("\n")
        with self.semaphores[runner.name]:
            with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open(
                "w", encoding="utf-8"
            ) as stderr:
                if self.options.dry_run:
                    stdout.write("[dry-run]\n")
                    stdout.write(_shell_join(argv))
                    stdout.write("\n")
                    return task, runner, 0
                completed = subprocess.run(
                    argv,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    text=True,
                    check=False,
                )
                return task, runner, completed.returncode

    def _runner_command(self, task: TaskSpec, runner: RunnerSpec) -> list[str]:
        runner_script = self.script_dir / "runner.py"
        common = [
            "--runner",
            runner.name,
            "--root-path",
            str(self.root_path),
            "--run-id",
            self.run_id,
        ]
        tail = ["--", task.id]
        if runner.kind == "local":
            config_path = str(self.options.config_path)
            return [sys.executable, str(runner_script), *common, config_path, *tail]

        remote_config = self.options.config_arg
        remote_cmd = _shell_join([runner.python, "script/runner.py", *common, remote_config, *tail])
        if runner.workdir:
            remote_cmd = f"cd {shlex.quote(runner.workdir)} && {remote_cmd}"
        argv = ["ssh", *runner.ssh_options, "-p", str(runner.port), runner.target]
        argv.extend([runner.shell, "-lc", shlex.quote(remote_cmd)])
        return argv

    @staticmethod
    def _make_run_id() -> str:
        now = time.time()
        stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(now))
        return f"{stamp}-{int((now % 1) * 1000):03d}-{os.getpid()}"


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
        dry_run: bool = False,
    ) -> None:
        self.goal = goal
        self.name = runner_name
        self.root_path = root_path
        self.dry_run = dry_run
        self.current_task: TaskSpec | None = None
        self.spec = goal.local("local")
        self.allocator = CoreAllocator(self.spec, root_path, runner_name)

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
                cwd=self.spec.workdir,
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


class ControllerRunner:
    name = "controller"

    def run(self, *_args: Any, **_kwargs: Any) -> None:
        raise ParallaxError("runner.run() is only available inside a task executed by runner.py")


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


def run_controller(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description="run a Parallax Python config")
    parser.add_argument("config", nargs="?", help="Python config file")
    parser.add_argument("-f", "--file", help="Python config file")
    parser.add_argument("--dry-run", action="store_true", help="render runner commands only")
    parser.add_argument("--list-tasks", action="store_true", help="list registered task ids and exit")
    parser.add_argument("--run-id", help="override generated run id")
    parser.add_argument("extra", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)
    args = parser.parse_args(list(argv))

    config_arg = args.file or args.config
    if not config_arg:
        parser.error("missing config file")
    config_path = Path(config_arg).resolve()
    if config_path.suffix != ".py":
        raise ParallaxError("config file must be a .py file")
    if not config_path.exists():
        raise ParallaxError(f"config file does not exist: {config_path}")

    extra = _strip_remainder_marker(args.extra)
    if extra:
        raise ParallaxError(
            "task function arguments are not supported; use a task builder to create zero-arg tasks"
        )
    mode = "collect" if args.list_tasks else "controller"
    options = ControllerOptions(
        config_path=config_path,
        config_arg=config_arg,
        dry_run=args.dry_run,
        run_id=args.run_id,
    )
    goal = Goal(mode=mode, options=options)
    execute_config(config_path, goal=goal, runner=ControllerRunner())

    if args.list_tasks:
        for task in goal.tasks:
            print(task.id)
        return 0

    if not goal.issued:
        goal.issue()
    return 0


def run_worker(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description="execute one Parallax task")
    parser.add_argument("--runner", default="local", help="runner name")
    parser.add_argument("--root-path", default="workspace/log_root", help="root path for locks")
    parser.add_argument("--run-id", default="manual", help="run id")
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
        dry_run=args.dry_run,
    )
    execute_config(config_path, goal=goal, runner=runtime_runner)
    runtime_runner.spec = runtime_runner._find_runner_spec(args.runner)
    runtime_runner.allocator = CoreAllocator(runtime_runner.spec, args.root_path, args.runner)

    task = goal.find_task(task_id)
    runtime_runner.current_task = task
    os.environ.update(goal.env)
    os.environ.update(runtime_runner.spec.env)
    os.environ.update(task.env)
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
