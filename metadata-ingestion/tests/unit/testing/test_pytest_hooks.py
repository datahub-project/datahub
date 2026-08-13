import os
import time
from typing import Iterator

import pytest

from datahub.testing.pytest_hooks import pytest_configure


@pytest.fixture
def restore_timezone() -> Iterator[None]:
    """Put the process clock back, even if the test body raises.

    These tests move TZ inside a suite whose golden comparisons depend on it
    being UTC, and CI runs in random order. monkeypatch alone is not enough:
    it restores the environment variable but nothing re-reads it, so libc stays
    on whatever zone the test last applied. tzset is captured here rather than
    looked up at teardown, so it survives a test that deletes the attribute.
    """
    saved_tz = os.environ.get("TZ")
    tzset = getattr(time, "tzset", None)
    try:
        yield
    finally:
        if saved_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = saved_tz
        if tzset is not None:
            tzset()


def test_pin_timezone_sets_utc(
    restore_timezone: None, pytestconfig: pytest.Config
) -> None:
    os.environ["TZ"] = "Asia/Kolkata"
    time.tzset()
    assert time.tzname[0] != "UTC"

    pytest_configure(pytestconfig)

    assert os.environ["TZ"] == "UTC"
    assert time.tzname == ("UTC", "UTC")


def test_pin_timezone_is_inert_without_tzset(
    restore_timezone: None, monkeypatch: pytest.MonkeyPatch, pytestconfig: pytest.Config
) -> None:
    """Platforms without tzset must be left alone entirely.

    Setting TZ there would not affect this process -- nothing reads it -- but it
    is still inherited by every subprocess the suite spawns, and by libraries
    that consult the variable directly. A half-applied pin is worse than none,
    so the assignment is guarded rather than unconditional.
    """
    monkeypatch.delattr(time, "tzset", raising=False)
    os.environ.pop("TZ", None)

    pytest_configure(pytestconfig)

    assert "TZ" not in os.environ
