import os
import time

import pytest

from datahub.testing.pytest_hooks import pytest_configure


def test_pin_timezone_sets_utc(pytestconfig: pytest.Config) -> None:
    # POSIX TZ form: a fixed +5:30 offset, so the test does not depend on host
    # tzdata. A zone name silently resolves to UTC where tzdata is absent, which
    # would make this precondition fail and the assertions below vacuous.
    os.environ["TZ"] = "XXX-5:30"
    time.tzset()
    assert time.tzname[0] != "UTC"

    pytest_configure(pytestconfig)

    assert os.environ["TZ"] == "UTC"
    assert time.tzname == ("UTC", "UTC")


def test_pin_timezone_is_inert_without_tzset(
    monkeypatch: pytest.MonkeyPatch, pytestconfig: pytest.Config
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


@pytest.mark.timezone("XXX-5:30")
def test_timezone_marker_applies_the_requested_zone() -> None:
    assert time.tzname[0] == "XXX"
    assert -time.timezone / 3600 == 5.5


def test_pin_holds_for_unmarked_tests() -> None:
    """An unmarked test runs under the session pin, not whatever ran before it.

    tests/unit runs with --random-order, so this cannot be relied on to land after
    the marked test above; when it does, it also catches a failure to restore.
    """
    assert time.tzname == ("UTC", "UTC")
    assert os.environ["TZ"] == "UTC"
