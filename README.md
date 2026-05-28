# Parallux

Parallux 是一个基于 Python 配置文件的命令提交与调度运行时，支持本地执行、SSH 远端执行、全局并行度控制、Runner 级并行度控制以及 NUMA 核心绑定。

## 概述

Parallux 配置文件需要在受控运行时中执行。配置文件通过 `from parallux import goal` 获取运行时对象，并使用 Python 语法描述 Runner、环境变量、命令注册和调度流程。

```bash
parallux config.py -- key=value
```

配置文件可以作为普通 Python 文件编辑，但不应直接通过 `python3 config.py` 执行。Parallux 会在运行前绑定 `goal`，从而确保配置文件始终在受控环境中运行。

```python
from parallux import goal


local_a = goal.local(name="local-a", env={"RUNNER_TAG": "A"}, max_jobs=1)
local_b = goal.local(name="local-b", env={"RUNNER_TAG": "B"}, max_jobs=1)

goal.setRunner([local_a, local_b])
goal.setParallel(2)

for index in range(10):
    goal.schd(
        "echo runner=$PARALLUX_RUNNER tag=$RUNNER_TAG",
        name=f"run_{index}",
    )

goal.issue().sync()
```

## 安装

开发模式安装：

```bash
python3 -m pip install -e .
```

安装完成后会提供 `parallux` 命令：

```bash
parallux --help
```

未安装时，可以在源码目录中使用模块入口：

```bash
python3 -m parallux examples/basic.py
```

## 运行配置文件

通过命令行运行配置文件：

```bash
parallux examples/basic.py
```

向配置文件传递参数：

```bash
parallux examples/basic.py -- case=hello
```

配置文件内可通过以下对象读取参数：

```python
goal.argv
goal.args
```

仅生成执行计划而不实际运行命令：

```bash
parallux examples/basic.py --dry-run
```

配置文件可以声明为可执行文件：

```python
#!/usr/bin/env parallux
```

添加执行权限后，可以直接运行：

```bash
./examples/basic.py
```

## Runner

本地 Runner：

```python
local = goal.local(
    name="local",
    workspace="workspace/runners/local",
    max_jobs=2,
)
goal.setRunner(local)
```

SSH Runner：

```python
server = goal.ssh(
    name="server-a",
    host="runner.example.com",
    user="user",
    workspace="workspace/runners/server-a",
    max_jobs=4,
)

goal.setRunner([local, server])
```

`workspace` 表示 Runner 侧的输出根目录。未显式指定 `workspace` 时，默认使用：

```text
~/parallux
```

指定 `work_relpath` 时，每个命令的工作目录为：

```text
<runner.workspace>/<work-relpath>/
```

未指定 `work_relpath` 时，Parallux 会使用默认路径：

```text
<runner.workspace>/<auto-work-relpath>/
```

默认工作路径在任务提交时按递增编号生成，并保证同一次运行内唯一。该编号属于内部调度细节，配置文件需要读取输出位置时应使用句柄或执行结果中的 `work_relpath`、`work_dir`、`stdout_path` 和 `stderr_path`。全局并行度由 `goal.setParallel(n)` 控制，单个 Runner 的并行度由 `max_jobs` 控制。

## 调度接口

`goal.schd()` 用于注册命令。注册操作不会立即启动命令：

```python
goal.schd("make test", threads=1)
```

`goal.issue()` 用于提交当前已经注册的命令，并返回任务组句柄：

```python
handle = goal.issue()
handle.sync()
```

`goal.run()` 用于立即提交一个非阻塞命令，并返回命令句柄：

```python
handle = goal.run("echo immediate")
handle.sync()
```

`goal.schd()` 和 `goal.run()` 始终由全局调度器选择 Runner。需要固定 Runner 时，应直接使用 Runner 实例的 `run()`：

```python
local = goal.local(name="local", workspace="workspace/local")
goal.setRunner(local)

local.run("echo immediate on local").sync()
```

配置文件退出时，如果仍存在已经注册但尚未提交的命令，Parallux 会在结束前自动提交并等待这些命令完成。

## Handle

`goal.run()` 和 Runner 实例的 `run()` 返回单命令句柄，`goal.issue()` 返回任务组句柄。单命令句柄在命令被调度后会记录分配结果：

```python
handle = goal.run("echo runner=$PARALLUX_RUNNER")

result = handle.sync()
print(handle.runner_name)
print(handle.work_dir)
print(result.stdout_path)
```

单命令句柄和执行结果包含以下信息：

```text
command
runner / runner_name
work_relpath
work_dir
command_path
stdout_path
stderr_path
cores
numa_node
```

## Runner 状态

Runner 提供运行时状态查询接口，可用于配置文件内部的调度决策：

```python
status = local.status()

print(status.active_jobs)
print(status.available_jobs)
print(status.logical_core_count)
print(status.configured_cores)
print(status.available_cores)
print(status.available_core_count)
```

常用快捷接口：

```python
local.active_jobs()
local.available_jobs()
local.logical_core_count()
local.configured_cores()
local.configured_core_count()
local.available_cores()
local.available_core_count()
```

也可以通过 `goal.runner_status()` 查询所有 Runner 或指定 Runner 的状态：

```python
all_status = goal.runner_status()
one_status = goal.runner_status(local)
```

`logical_core_count` 表示 Runner 已知的总逻辑线程数。`configured_cores` 表示由 Parallux 管理、可用于核心绑定的核心集合。`available_cores` 表示当前尚未被 Parallux 任务租用的 `configured_cores` 子集，不表示操作系统层面的 CPU 空闲率。未声明 `core_pool` 或 `numa_nodes` 时，`configured_cores` 和 `available_cores` 为空列表。

## 命令选项

`goal.schd()`、`goal.run()` 和 Runner 实例的 `run()` 支持相同的命令选项：

```python
goal.schd(
    "echo hello",
    name="hello",
    threads=1,
    numa_node=0,
    cores=[0],
    env={"KEY": "value"},
    cwd=None,
    work_relpath="suite/case",
    check=True,
    timeout=60,
)
```

命令运行时会注入以下环境变量：

```text
PARALLUX_RUNNER
PARALLUX_WORK_RELPATH
PARALLUX_WORK_DIR
```

## 输出文件

每个命令的输出文件写入该命令的工作目录：

```text
<runner.workspace>/<work-relpath>/
  command.txt
  stdout.txt
  stderr.txt
```

未显式指定 `work_relpath` 时，输出文件写入默认工作目录：

```text
<runner.workspace>/<auto-work-relpath>/
  command.txt
  stdout.txt
  stderr.txt
```

## NUMA

Runner 可以声明核心池和 NUMA 节点：

```python
local = goal.local(
    name="local",
    core_pool=range(0, 8),
    numa_nodes={0: range(0, 4), 1: range(4, 8)},
)
goal.setRunner(local)

for index in range(2):
    goal.schd(
        "echo runner=$PARALLUX_RUNNER",
        threads=1,
        numa_node=0,
        work_relpath=f"numa/{index}",
    )

goal.issue().sync()
```

当 Runner 配置了核心池时，`threads` 会从核心池中申请核心，并使用 `numactl` 包装命令。

## Workload 工具

`workloads()` 用于根据输入路径生成稳定的工作路径，适用于批量 workload 场景。

```python
from parallux import goal, workloads


for workload in workloads(
    "inputs/spec/*/*/*.gz",
    levels=3,
    work_prefix="smt-perf-test",
    strip_suffix=True,
):
    goal.schd(
        f"./run-one {workload.input_path}",
        name=f"smt_perf_{workload.name}",
        work_relpath=workload.work_relpath,
    )

goal.issue().sync()
```

例如输入路径为 `a/b/c/d` 且 `levels=3` 时，`workload.relpath` 为 `b/c/d`。

## 文件结构

- `parallux/__init__.py`：公开 API、受控运行时入口、调度器、本地/SSH 执行逻辑以及 NUMA 分配逻辑
- `parallux/__main__.py`：`python3 -m parallux` 入口
- `parallux/_core.py`：核心配置模型和配置加载逻辑
- `pyproject.toml`：包元数据和 `parallux` 命令入口
