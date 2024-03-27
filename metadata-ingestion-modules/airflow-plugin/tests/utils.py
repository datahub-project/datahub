try:
    from pytest import Config as PytestConfig  # type: ignore[attr-defined]
except ImportError:
    # Support for pytest 6.x.
    from _pytest.config import Config as PytestConfig  # type: ignore

__all__ = ["PytestConfig"]
