from typing import Optional, TypeVar

# The current PytestConfig solution is somewhat ugly and not ideal.
# However, it is currently the best solution available, as the type itself is not
# exported: https://docs.pytest.org/en/stable/reference.html#config.
# As pytest's type support improves, this will likely change.
# TODO: revisit pytestconfig as https://github.com/pytest-dev/pytest/issues/7469 progresses.
from _pytest.config import Config as PytestConfig  # noqa: F401

_T = TypeVar("_T")


def assert_not_null(value: Optional[_T]) -> _T:
    assert value is not None, "value is unexpectedly None"
    return value
