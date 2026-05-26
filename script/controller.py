from __future__ import annotations

import sys

from _core import ParallaxError, run_controller


def main() -> int:
    try:
        return run_controller(sys.argv[1:])
    except ParallaxError as err:
        print(f"parallax: {err}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("parallax: interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
