#!/usr/bin/env parallux

from parallux import goal
from task_helpers import build_print_command


goal.setRunner("local")
goal.setParallel(1)
goal.schd(build_print_command("imported_task", "loaded from examples/task_helpers.py"))
goal.issue().sync()
