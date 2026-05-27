# Parallax

Parallax 是一个基于 Python 配置文件的命令提交与调度运行时，支持本地执行、SSH 远端执行、全局并行度控制、Runner 级并行度控制以及 NUMA 核心绑定。

## 概述

Parallax 配置文件需要在受控运行时中执行。配置文件通过 `from parallax import goal, runner` 获取运行时对象，并使用 Python 语法描述 Runner、环境变量、命令注册和调度流程。

```bash
parallax config.py -- key=value
```

配置文件可以作为普通 Python 文件编辑，但不应直接通过 `python3 config.py` 执行。Parallax 会在运行前绑定 `goal` 和 `runner`，从而确保配置文件始终在受控环境中运行。

```python
from parallax import goal


local_a = goal.local(name="local-a", env={"RUNNER_TAG": "A"}, max_jobs=1)
local_b = goal.local(name="local-b", env={"RUNNER_TAG": "B"}, max_jobs=1)

goal.setRunner([local_a, local_b])
goal.setParallel(2)

for _ in range(10):
    goal.schd("echo runner=$PARALLAX_RUNNER tag=$RUNNER_TAG")

goal.issue().sync()
```

## 安装

开发模式安装：

```bash
python3 -m pip install -e .
```

安装完成后会提供 `parallax` 命令：

```bash
parallax --help
```

未安装时，可以在源码目录中使用模块入口：

```bash
python3 -m parallax examples/basic.py
```

## 运行配置文件

通过命令行运行配置文件：

```bash
parallax examples/basic.py
```

向配置文件传递参数：

```bash
parallax examples/basic.py -- case=hello
```

配置文件内可通过以下对象读取参数：

```python
goal.argv
goal.args
```

仅生成执行计划而不实际运行命令：

```bash
parallax examples/basic.py --dry-run
```

指定日志根目录：

```bash
parallax examples/basic.py --root workspace/log_root
```

配置文件可以声明为可执行文件：

```python
#!/usr/bin/env parallax
```

添加执行权限后，可以直接运行：

```bash
./examples/basic.py
```

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

也可以直接通过指定 Runner 提交命令：

```python
local = goal.local(name="local", workspace="workspace/local")
goal.setRunner(local)

local.schd("echo scheduled on local")
local.run("echo immediate on local").sync()
```

配置文件退出时，如果仍存在已经注册但尚未提交的命令，Parallax 会在结束前自动提交并等待这些命令完成。

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

`workspace` 表示 Runner 侧的命令工作目录根路径。日志始终写入调度侧的 `goal.root_path`。全局并行度由 `goal.setParallel(n)` 控制，单个 Runner 的并行度由 `max_jobs` 控制。

## 命令选项

`goal.schd()`、`goal.run()`、`runner.schd()` 和 `runner.run()` 支持相同的命令选项：

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
PARALLAX_RUN_ID
PARALLAX_RUNNER
PARALLAX_WORK_RELPATH
PARALLAX_WORK_DIR
PARALLAX_LOG_DIR
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

for _ in range(2):
    local.schd("echo runner=$PARALLAX_RUNNER", threads=1, numa_node=0)

goal.issue().sync()
```

当 Runner 配置了核心池时，`threads` 会从核心池中申请核心，并使用 `numactl` 包装命令。

## Workload 工具

`workloads()` 用于根据输入路径生成稳定的工作路径，适用于批量 workload 场景。

```python
from parallax import goal, workloads


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

## 日志

日志写入 `goal.root_path` 指定的根目录：

```text
<root-path>/
  <work-relpath>/
    command.txt
    stdout.log
    stderr.log
```

默认日志根目录为 `workspace/log_root`。配置文件可以设置默认日志根目录：

```python
goal.setRoot("workspace/log_root")
```

命令行参数 `--root` 的优先级高于 `goal.setRoot()`。

## 文件结构

- `parallax/__init__.py`：公开 API、受控运行时入口、调度器、本地/SSH 执行逻辑以及 NUMA 分配逻辑
- `parallax/__main__.py`：`python3 -m parallax` 入口
- `parallax/_core.py`：核心配置模型和配置加载逻辑
- `pyproject.toml`：包元数据和 `parallax` 命令入口
