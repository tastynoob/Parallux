#!/usr/bin/env parallax

from parallax import goal


local = goal.local(
    name="local",
    core_pool=range(0, 4),
    numa_nodes={0: range(0, 2), 1: range(2, 4)},
)

goal.setRunner(local)
goal.setParallel(2)


for _ in range(2):
    local.schd("echo runner=$USER", threads=1, numa_node=0)

goal.issue().sync()
