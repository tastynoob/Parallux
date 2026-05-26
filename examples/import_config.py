from parallax import goal
from task_helpers import build_print_task


goal.setRunner("local")
goal.setParallel(1)
goal.addTask(build_print_task("imported_task", "loaded from examples/task_helpers.py"))
goal.issue(root_path="workspace/log_root")
