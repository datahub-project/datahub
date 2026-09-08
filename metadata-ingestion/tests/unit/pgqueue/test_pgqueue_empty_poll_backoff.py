import pytest

from datahub.pgqueue.empty_poll_backoff import EmptyPollBackoff


def test_doubles_until_max_then_stays_capped() -> None:
    backoff = EmptyPollBackoff(1000, 5000)
    assert backoff.next_sleep_millis() == 1000
    assert backoff.next_sleep_millis() == 2000
    assert backoff.next_sleep_millis() == 4000
    assert backoff.next_sleep_millis() == 5000
    assert backoff.next_sleep_millis() == 5000


def test_reset_returns_to_min() -> None:
    backoff = EmptyPollBackoff(1000, 5000)
    backoff.next_sleep_millis()
    backoff.next_sleep_millis()
    backoff.reset()
    assert backoff.next_sleep_millis() == 1000


def test_min_above_max_clamps_to_max() -> None:
    backoff = EmptyPollBackoff(8000, 5000)
    assert backoff.next_sleep_millis() == 5000
    assert backoff.next_sleep_millis() == 5000


def test_rejects_non_positive_bounds() -> None:
    with pytest.raises(ValueError):
        EmptyPollBackoff(0, 1000)
    with pytest.raises(ValueError):
        EmptyPollBackoff(1000, 0)
