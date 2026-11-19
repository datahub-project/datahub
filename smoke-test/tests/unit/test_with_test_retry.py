import pytest

from tests.e2e import utils
from utilities.domains import Domain

pytestmark = [pytest.mark.domain(Domain.PLATFORM)]


@pytest.fixture
def clock(monkeypatch):
    """Record tenacity's sleeps instead of blocking."""
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(utils.tenacity.nap.time, "sleep", sleep)
    monkeypatch.setenv("DATAHUB_TEST_SLEEP_BETWEEN", "20")
    monkeypatch.setenv("DATAHUB_TEST_SLEEP_TIMES", "3")
    return sleeps


def test_a_late_success_is_caught_after_short_backoffs(clock):
    calls = []

    @utils.with_test_retry()
    def check() -> None:
        calls.append(1)
        if len(calls) < 3:
            raise AssertionError("not yet")

    check()

    assert clock == [1, 2]


def test_a_persistent_failure_keeps_the_old_patience(clock):
    @utils.with_test_retry()
    def check() -> None:
        raise AssertionError("never")

    with pytest.raises(AssertionError):
        check()

    # The old fixed-sleep config allowed 2 x 20s of sleeping before giving up.
    assert sum(clock) >= 40
