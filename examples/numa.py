from parallax import goal, runner


local = goal.local(
    name="local",
    core_pool=range(0, 4),
    numa_nodes={0: range(0, 2), 1: range(2, 4)},
)

goal.setRunner(local)
goal.setParallel(2)


def task_with_numa():
    runner.run("echo runner=$USER", num_cores=1, numa_node=0)


goal.addTask([task_with_numa] * 2)
goal.issue(root_path="workspace/log_root")
