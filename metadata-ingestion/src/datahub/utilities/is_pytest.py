import sys


def is_pytest_running() -> bool:
    return "pytest" in sys.modules
