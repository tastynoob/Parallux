#!/usr/bin/env parallax

from parallax import goal


goal.setRunner("local")
goal.setParallel(2)
goal.setEnv("ENV_VAR", "value")


goal.schd("echo Hello, World!", name="hello", work_relpath="hello")
goal.schd("echo ENV_VAR=$ENV_VAR", name="env", work_relpath="env")
goal.schd("echo user=$USER", name="user", work_relpath="user")

for case in ["a", "b", "c", "d"]:
    goal.schd(
        f"echo case={case} runner=$PARALLAX_RUNNER",
        name=f"case_{case}",
        work_relpath=f"case/{case}",
    )

goal.issue().sync()
