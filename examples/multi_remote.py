from parallax import goal, runner


local_a = goal.local(name="local-a", env={"REMOTE_TAG": "A"}, max_jobs=1)
local_b = goal.local(name="local-b", env={"REMOTE_TAG": "B"}, max_jobs=1)

goal.setRunner([local_a, local_b])
goal.setParallel(2)

def task_show_remote():
    print(f"python runner={runner.name}")
    runner.run("echo shell tag=$REMOTE_TAG")


goal.addTask([task_show_remote] * 4)
goal.issue(root_path="workspace/log_root")
