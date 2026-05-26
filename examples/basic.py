from parallax import goal, runner


goal.setRunner("local")
goal.setParallel(2)
goal.setEnv("ENV_VAR", "value")


def task_hello():
    print("Hello, World!")
    print("ENV_VAR =", runner.env["ENV_VAR"])


def task_echo():
    runner.run("echo runner=$USER")


def build_case(case_name):
    def task():
        print(f"case={case_name}, runner={runner.name}")

    task.__name__ = f"case_{case_name}"
    return task


goal.addTask(task_hello)
goal.addTask(task_echo)

for case in ["a", "b", "c", "d"]:
    goal.addTask(build_case(case))

goal.issue(root_path="workspace/log_root")
