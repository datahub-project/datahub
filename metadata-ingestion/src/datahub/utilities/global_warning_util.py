from typing import List

global_warnings: List = []


def add_global_warning(warn: str) -> None:
    global_warnings.append(warn)


def get_global_warnings() -> List:
    return global_warnings


def clear_global_warnings() -> None:
    global_warnings.clear()
