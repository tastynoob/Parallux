def build_print_task(name, message):
    def task():
        print(f"{name}: {message}")

    task.__name__ = name
    return task
