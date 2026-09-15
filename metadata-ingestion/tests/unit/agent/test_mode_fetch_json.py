"""fetch_json's retry behaviour, which nothing exercised.

The existing Mode tests mock _get_request_json outright or cover 404
handling, so the retry loop this function exists for -- 429 with
retry-after, 504, backoff, exhaustion, and the report callbacks that count
them -- had no coverage at all.
"""

import pytest
import tenacity
from requests.exceptions import ConnectionError as RequestsConnectionError, HTTPError

from datahub.ingestion.source.mode import fetch_json
from datahub.utilities.ratelimiter import RateLimiter


class _Response:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"ok": True}
        self.headers = headers or {}
        # The real code reads .text when building the error message for a
        # status it will not retry.
        self.text = f"body for {status_code}"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise HTTPError(f"{self.status_code}", response=self)

    def json(self):
        return self._payload


class _Session:
    """Serves a scripted sequence, so a test says how many attempts it wants."""

    def __init__(self, *responses):
        self._responses = list(responses)
        self.calls = 0

    def get(self, url, timeout=None):
        self.calls += 1
        item = self._responses.pop(0) if self._responses else _Response()
        if isinstance(item, Exception):
            raise item
        return item


def _fetch(session, **over):
    kwargs = dict(
        timeout=5,
        rate_limiter=RateLimiter(max_calls=100, period=1),
        # No real waiting: the backoff is tenacity's and is not what these
        # tests are about, and a multiplier of 0 keeps them instant.
        retry_backoff_multiplier=0,
        max_retry_interval=0,
        max_attempts=3,
    )
    kwargs.update(over)
    return fetch_json(session, "https://mode.example/api/x", **kwargs)  # type: ignore[arg-type]


def test_a_successful_response_is_returned_without_retrying():
    session = _Session(_Response(payload={"spaces": []}))
    assert _fetch(session) == {"spaces": []}
    assert session.calls == 1


def test_204_is_no_content_rather_than_a_json_parse_error():
    session = _Session(_Response(status_code=204))
    assert _fetch(session) == {}
    assert session.calls == 1


def test_429_is_retried_and_counted():
    """The callback is how a caller's report counts rate limiting -- every
    current caller passes one, and nothing asserted it fired."""
    counted = []
    session = _Session(_Response(status_code=429), _Response(payload={"ok": 1}))
    result = _fetch(session, on_rate_limited=lambda: counted.append(1))
    assert result == {"ok": 1}
    assert session.calls == 2
    assert len(counted) == 1


def test_429_honours_retry_after(monkeypatch):
    """Mode sends `retry-after`; ignoring it is how you get rate-limited
    again immediately."""
    slept: list = []
    monkeypatch.setattr("datahub.ingestion.source.mode.time.sleep", slept.append)
    session = _Session(
        _Response(status_code=429, headers={"retry-after": "7"}), _Response()
    )
    _fetch(session)
    # tenacity's own backoff sleeps too, and patching time.sleep catches
    # both; what matters is that the header's value was honoured first.
    assert slept[0] == 7.0


def test_504_is_retried_and_counted_separately():
    counted = []
    session = _Session(_Response(status_code=504), _Response(payload={"ok": 2}))
    assert _fetch(session, on_retried_after_timeout=lambda: counted.append(1)) == {
        "ok": 2
    }
    assert session.calls == 2
    assert len(counted) == 1


def test_a_connection_error_is_retried():
    session = _Session(RequestsConnectionError("reset"), _Response(payload={"ok": 3}))
    assert _fetch(session) == {"ok": 3}
    assert session.calls == 2


def test_retries_are_exhausted_rather_than_looping_forever():
    """max_attempts bounds it. Without this the probe would hang on a source
    that is rate-limiting persistently."""
    session = _Session(*[_Response(status_code=429) for _ in range(5)])
    # tenacity is configured without reraise, so exhaustion surfaces as its
    # own RetryError rather than the last HTTPError. Worth pinning: a caller
    # branching on HTTPError would not catch this.
    with pytest.raises(tenacity.RetryError):
        _fetch(session, max_attempts=3)
    assert session.calls == 3


@pytest.mark.parametrize("status", [400, 401, 403, 404, 500])
def test_a_status_that_will_not_improve_is_not_retried(status):
    """Only 429, 504 and connection errors are worth trying again. Retrying
    a 403 three times turns one refusal into three."""
    session = _Session(*[_Response(status_code=status) for _ in range(4)])
    with pytest.raises(HTTPError):
        _fetch(session)
    assert session.calls == 1
