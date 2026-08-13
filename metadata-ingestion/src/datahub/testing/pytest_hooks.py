import dataclasses
import os
import time
from typing import Optional

import pytest

__all__ = [
    "load_golden_flags",
    "get_golden_settings",
    "pytest_configure",
    "pytest_addoption",
    "GoldenFileSettings",
]


@dataclasses.dataclass
class GoldenFileSettings:
    update_golden: bool
    copy_output: bool


_registered: bool = False
_settings: Optional[GoldenFileSettings] = None


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-golden-files",
        action="store_true",
        default=False,
    )

    # TODO: Deprecate and remove this flag.
    parser.addoption("--copy-output-files", action="store_true", default=False)

    global _registered
    _registered = True


@pytest.fixture(scope="session", autouse=True)
def load_golden_flags(pytestconfig: pytest.Config) -> None:
    global _settings
    _settings = GoldenFileSettings(
        update_golden=pytestconfig.getoption("--update-golden-files"),
        copy_output=pytestconfig.getoption("--copy-output-files"),
    )


def pytest_configure(config: pytest.Config) -> None:
    """Run golden comparisons in UTC regardless of the developer's clock.

    Golden files hold absolute UTC epochs. Outside UTC the harness resolves those
    same instants against the local offset, so tests fail on a clean checkout for
    a reason unrelated to the change being made. Worse, the failure message
    recommends `--update-golden-files`, and following that advice rewrites every
    timestamp in the fixture to the local offset while reporting success.

    This must be a hook rather than a fixture. Suites freeze the clock with a naive
    literal -- `@time_machine.travel("2022-02-03 07:00:00")` -- which is resolved
    against the local zone when the decorator is constructed, i.e. at module import.
    Collection imports test modules before any fixture runs, so a session-scoped
    autouse fixture is already too late. pytest_configure runs before collection.

    tzset() is POSIX-only, and the assignment is guarded by it rather than run
    unconditionally. Setting TZ without applying it would leave Windows with a
    half-applied pin: inert for this process, but still inherited by every
    subprocess the suite spawns. Windows is skipped entirely instead.
    """
    if hasattr(time, "tzset"):
        os.environ["TZ"] = "UTC"
        time.tzset()


def get_golden_settings() -> GoldenFileSettings:
    if not _registered:
        raise ValueError(
            "Golden files aren't set up properly. Call register_golden_flags from a conftest pytest_addoptions method."
        )
    if not _settings:
        raise ValueError(
            "Golden files aren't set up properly. Ensure load_golden_flags is imported in your conftest."
        )
    return _settings
