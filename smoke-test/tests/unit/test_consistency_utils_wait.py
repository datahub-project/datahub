from unittest.mock import MagicMock

import pytest
import requests

from utilities.consistency_utils import wait_for_writes_to_sync
from utilities.domains import Domain

pytestmark = [pytest.mark.no_cypress_suite1, pytest.mark.domain(Domain.PLATFORM)]


class _Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def time(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds


def _json_response(status_code: int, payload: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload if payload is not None else {}
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status_code}")
    else:
        resp.raise_for_status.return_value = None
    return resp


@pytest.fixture
def lag_env(monkeypatch):
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "test-token")
    monkeypatch.delenv("USE_STATIC_SLEEP", raising=False)
    monkeypatch.delenv("DATAHUB_TEST_FORCE_LEGACY_WAIT", raising=False)
    monkeypatch.setenv("DATAHUB_TEST_LAG_AUTH_TIMEOUT_SECONDS", "2")
    monkeypatch.setattr(
        "utilities.consistency_utils.ELASTICSEARCH_REFRESH_INTERVAL_SECONDS", 0
    )
    clock = _Clock()
    monkeypatch.setattr("utilities.consistency_utils.time", clock)
    return clock


def test_missing_token_is_fatal(monkeypatch):
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
    monkeypatch.delenv("USE_STATIC_SLEEP", raising=False)
    with pytest.raises(RuntimeError, match="DATAHUB_GMS_TOKEN"):
        wait_for_writes_to_sync()


def test_persistent_403_raises_with_privilege_hint(lag_env, monkeypatch):
    monkeypatch.setattr(
        "utilities.consistency_utils.requests.get",
        lambda *args, **kwargs: _json_response(403),
    )
    with pytest.raises(RuntimeError, match="VIEW_SYSTEM_STATUS") as exc:
        wait_for_writes_to_sync(legacy_wait=True, max_timeout_in_sec=30)
    assert "MANAGE_SYSTEM_OPERATIONS" in str(exc.value)
    assert lag_env.t >= 2


def test_5xx_retries_until_timeout(lag_env, monkeypatch):
    monkeypatch.setattr(
        "utilities.consistency_utils.requests.get",
        lambda *args, **kwargs: _json_response(500),
    )
    wait_for_writes_to_sync(legacy_wait=True, max_timeout_in_sec=30)
    assert lag_env.t >= 30


def test_lag_zero_returns(lag_env, monkeypatch):
    empty_lag: dict = {"consumerGroups": {}}
    monkeypatch.setattr(
        "utilities.consistency_utils.requests.get",
        lambda *args, **kwargs: _json_response(200, empty_lag),
    )
    wait_for_writes_to_sync(legacy_wait=True, max_timeout_in_sec=30)
    assert lag_env.t < 2


def test_auth_retry_window_resets_after_success(lag_env, monkeypatch):
    lagging: dict = {"consumerGroups": {"g": {"topic": {"metrics": {"totalLag": 5}}}}}

    def fake_get(*args, **kwargs):
        if 2.0 <= lag_env.t < 3.0:
            return _json_response(200, lagging)
        return _json_response(403)

    monkeypatch.setattr("utilities.consistency_utils.requests.get", fake_get)
    with pytest.raises(RuntimeError, match="VIEW_SYSTEM_STATUS"):
        wait_for_writes_to_sync(legacy_wait=True, max_timeout_in_sec=30)
    assert lag_env.t >= 5


def _lag_envelope(partitions):
    return {"consumerGroups": {"group": {"topic": {"partitions": partitions}}}}


def test_offset_wait_skips_polling_when_consumers_have_caught_up(monkeypatch):
    from utilities import consistency_utils

    fetches: list[str] = []

    def fetch(gms_url, endpoint):
        fetches.append(endpoint)
        return _lag_envelope({"0": {"offset": 7, "lag": 0}})

    sleeps: list[float] = []
    monkeypatch.setattr(consistency_utils, "_fetch_detailed_lag_envelope", fetch)
    monkeypatch.setattr(consistency_utils, "_get_gms_url", lambda: "http://gms")
    monkeypatch.setattr(consistency_utils.time, "sleep", sleeps.append)

    assert consistency_utils.wait_for_offsets_to_be_consumed(["mcp", "mcl"])

    # One checkpoint fetch per consumer and nothing to poll afterwards.
    assert len(fetches) == 2
    assert sleeps == []


def test_offset_wait_polls_until_the_checkpoint_is_passed(monkeypatch):
    from utilities import consistency_utils

    responses = iter(
        [
            _lag_envelope({"0": {"offset": 5, "lag": 2}}),  # checkpoint: 7
            _lag_envelope({"0": {"offset": 6, "lag": 1}}),
            _lag_envelope({"0": {"offset": 7, "lag": 0}}),
        ]
    )
    sleeps: list[float] = []
    monkeypatch.setattr(
        consistency_utils,
        "_fetch_detailed_lag_envelope",
        lambda gms_url, endpoint: next(responses),
    )
    monkeypatch.setattr(consistency_utils, "_get_gms_url", lambda: "http://gms")
    monkeypatch.setattr(consistency_utils.time, "sleep", sleeps.append)

    assert consistency_utils.wait_for_offsets_to_be_consumed(["mcl"])

    assert len(sleeps) == 1
