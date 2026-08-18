import dataclasses
import os
import time
from typing import Iterator, Optional

import pytest

__all__ = [
    "load_golden_flags",
    "get_golden_settings",
    "local_timezone",
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
    """Pin the test process to UTC, regardless of the developer's clock.

    Some suites freeze the clock with a naive literal -- a module-level
    `FROZEN_TIME = "2022-02-03 07:00:00"` fed to `@time_machine.travel(...)` --
    which resolves against the local zone when the decorator is constructed, at
    module import. The goldens they compare against hold absolute UTC epochs, so
    outside UTC the comparison fails for a reason unrelated to the change being
    made. Worse, the failure message recommends `--update-golden-files`, and
    following that advice rewrites the offending timestamps to the local offset
    and reports success.

    Resolution at import is also why this is a hook: collection imports test
    modules before any fixture runs, so a session-scoped autouse fixture is
    already too late. pytest_configure runs before collection.

    tzset() is POSIX-only, and the assignment is guarded by it rather than run
    unconditionally. Setting TZ without applying it would leave Windows with a
    half-applied pin: inert for this process, but still inherited by every
    subprocess the suite spawns. Windows is skipped entirely instead.

    This also registers the `timezone` marker that `local_timezone` reads, so the
    two must be imported into a conftest together or the marker is unregistered.
    """
    config.addinivalue_line(
        "markers",
        "timezone(tz): run this test with TZ set to the given zone, e.g. 'XXX-5:30'",
    )
    if hasattr(time, "tzset"):
        os.environ["TZ"] = "UTC"
        time.tzset()


@pytest.fixture(autouse=True)
def local_timezone(request: pytest.FixtureRequest) -> Iterator[None]:
    """Restore TZ after any test that moves it, and apply the `timezone` marker.

    The pin is process-wide, so a test that moves TZ and then raises leaves every
    later test on the wrong clock. Restoration is therefore unconditional; the
    marker only decides whether a zone is applied on the way in.

    A marked test exercises product code that reads the clock at runtime -- a
    naive `datetime.now()`, say. The marker cannot reach a module-level
    `@time_machine.travel(...)` literal, which is resolved at import, long before
    any fixture runs; make that literal tz-aware instead.
    """
    marker = request.node.get_closest_marker("timezone")
    tzset = getattr(time, "tzset", None)

    if marker is not None:
        if not marker.args:
            pytest.fail(
                "@pytest.mark.timezone requires a zone, "
                "e.g. @pytest.mark.timezone('XXX-5:30')"
            )
        if tzset is None:
            # Running it anyway would assert against the local zone while
            # claiming to be in another one.
            pytest.skip("the timezone marker requires tzset(), which is POSIX-only")

    saved_tz = os.environ.get("TZ")
    if marker is not None and tzset is not None:
        os.environ["TZ"] = marker.args[0]
        tzset()
    try:
        yield
    finally:
        if saved_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = saved_tz
        if tzset is not None:
            tzset()


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
