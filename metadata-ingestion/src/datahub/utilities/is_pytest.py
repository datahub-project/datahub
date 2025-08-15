import os
import sys


def is_pytest_running() -> bool:
    return "pytest" in sys.modules and os.environ.get("DATAHUB_TEST_MODE") == "1"
