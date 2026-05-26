# Parallax

Parallax is a Python-configured parallel execution tool for running tasks across local and remote runners.

## Overview

Parallax uses Python files as configuration files. A configuration file declares runners, registers zero-argument tasks, and starts scheduling with `goal.issue()`.

The public configuration API is exposed through:

```python
from parallax import goal, runner
```

`parallax.py` is a typed shell for editor completion. Runtime objects are bound by `script/controller.py` and `script/runner.py` when a configuration is executed.

## Commands

Run a configuration:

```bash
python3 script/controller.py -f examples/basic.py
```

List registered tasks:

```bash
python3 script/controller.py -f examples/basic.py --list-tasks
```

Render scheduled runner commands without executing them:

```bash
python3 script/controller.py -f examples/basic.py --dry-run
```

Execute one task directly:

```bash
python3 script/runner.py --runner local examples/basic.py -- task_hello
```

## Configuration

Minimal configuration:

```python
from parallax import goal, runner

goal.setRunner("local")
goal.setParallel(2)
goal.setEnv("ENV_VAR", "value")

def task_hello():
    print("Hello")
    runner.run("echo $ENV_VAR")

goal.addTask(task_hello)
goal.issue(root_path="workspace/log_root")
```

## Tasks

Tasks must be zero-argument Python functions.

Use task builders to generate parameterized tasks:

```python
from parallax import goal, runner

def build_case(case_name):
    def task():
        runner.run(f"./run-case {case_name}")

    task.__name__ = f"case_{case_name}"
    return task

for case in ["a", "b", "c"]:
    goal.addTask(build_case(case))
```

Each scheduled task is executed through `script/runner.py`. The runner reloads the same configuration file, registers tasks, resolves the requested task id, and executes that task. In runner mode, `goal.issue()` is a no-op and does not recursively schedule work.

## Runners

Local runner:

```python
local = goal.local(name="local", max_jobs=2)
goal.setRunner(local)
```

SSH runner:

```python
server = goal.ssh(
    name="server-a",
    host="10.0.0.11",
    user="ci",
    workdir="/path/to/project",
    max_jobs=4,
)

goal.setRunner(["local", server])
```

SSH runners assume that the project and configuration path are already available on the remote host. Automatic file synchronization is not implemented.

## NUMA

Runners can declare a core pool and NUMA nodes:

```python
local = goal.local(
    name="local",
    core_pool=range(0, 8),
    numa_nodes={0: range(0, 4), 1: range(4, 8)},
)
goal.setRunner(local)

def task_with_numa():
    runner.run("echo $USER", num_cores=2, numa_node=0)

goal.addTask(task_with_numa)
```

`runner.run()` allocates cores from the runner pool and wraps the command with `numactl -m <node> -C <cores>`.

## Imports

Configuration files can import modules from their own directory:

```python
from parallax import goal
from task_helpers import build_print_task

goal.addTask(build_print_task("hello", "from helper"))
```

## Logs

Logs are written under `root_path/parallax-<run-id>/`:

```text
parallax-<run-id>/
  task_id-runner/
    command.txt
    stdout.log
    stderr.log
  failures.txt
```

## Files

- `parallax.py`: typed configuration shell for `from parallax import goal, runner`
- `script/controller.py`: controller entrypoint
- `script/runner.py`: single-task runner entrypoint
- `script/_core.py`: internal implementation
