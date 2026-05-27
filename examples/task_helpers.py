import shlex


def build_print_command(name, message):
    return "printf '%s\\n' " + shlex.quote(f"{name}: {message}")
