#!/usr/bin/env parallax

from parallax import goal


local_a = goal.local(
    name="local-a",
    env={"REMOTE_TAG": "A"},
    max_jobs=1,
    workspace="workspace/runners/local-a",
)
local_b = goal.local(name="local-b", env={"REMOTE_TAG": "B"}, max_jobs=1)

goal.setRunner([local_a, local_b])
goal.setParallel(2)

for _ in range(4):
    goal.schd("echo runner=$PARALLAX_RUNNER tag=$REMOTE_TAG")

goal.issue().sync()
