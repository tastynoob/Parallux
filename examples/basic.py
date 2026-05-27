#!/usr/bin/env parallax

from parallax import goal


goal.setRunner("local")
goal.setParallel(2)
goal.setEnv("ENV_VAR", "value")


goal.schd("echo Hello, World!", name="hello")
goal.schd("echo ENV_VAR=$ENV_VAR", name="env")
goal.schd("echo runner=$USER", name="runner")

for case in ["a", "b", "c", "d"]:
    goal.schd(f"echo case={case} runner=$PARALLAX_RUNNER", name=f"case_{case}")

goal.issue().sync()
