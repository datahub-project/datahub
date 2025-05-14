import doctest
from types import ModuleType


def assert_doctest(module: ModuleType) -> None:
    result = doctest.testmod(
        module,
        raise_on_error=True,
        verbose=True,
    )
    if result.attempted == 0:
        raise ValueError(f"No doctests found in {module.__name__}")
