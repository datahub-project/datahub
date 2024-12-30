from typing import Optional, TypeVar

_T = TypeVar("_T")


def assert_not_null(value: Optional[_T]) -> _T:
    assert value is not None, "value is unexpectedly None"
    return value
