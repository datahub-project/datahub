from unittest.mock import MagicMock

import pytest
import requests

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utilities.domains import Domain

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
        "tests.consistency_utils.ELASTICSEARCH_REFRESH_INTERVAL_SECONDS", 0
    )
    clock = _Clock()
    monkeypatch.setattr("tests.consistency_utils.time", clock)
    return clock


def test_missing_token_is_fatal(monkeypatch):
    monkeypatch.delenv("DATAHUB_GMS_TOKEN", raising=False)
    monkeypatch.delenv("USE_STATIC_SLEEP", raising=False)
    with pytest.raises(RuntimeError, match="DATAHUB_GMS_TOKEN"):
        wait_for_writes_to_sync()


def test_persistent_403_raises_with_privilege_hint(lag_env, monkeypatch):
    monkeypatch.setattr(
        "tests.consistency_utils.requests.get",
        lambda *args, **kwargs: _json_response(403),
    )
    with pytest.raises(RuntimeError, match="VIEW_SYSTEM_STATUS") as exc:
        wait_for_writes_to_sync(legacy_wait=True, max_timeout_in_sec=30)
    assert "MANAGE_SYSTEM_OPERATIONS" in str(exc.value)
    assert lag_env.t >= 2


def test_5xx_retries_until_timeout(lag_env, monkeypatch):
    monkeypatch.setattr(
        "tests.consistency_utils.requests.get",
        lambda *args, **kwargs: _json_response(500),
    )
    wait_for_writes_to_sync(legacy_wait=True, max_timeout_in_sec=30)
    assert lag_env.t >= 30


def test_lag_zero_returns(lag_env, monkeypatch):
    empty_lag: dict = {"consumerGroups": {}}
    monkeypatch.setattr(
        "tests.consistency_utils.requests.get",
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

    monkeypatch.setattr("tests.consistency_utils.requests.get", fake_get)
    with pytest.raises(RuntimeError, match="VIEW_SYSTEM_STATUS"):
        wait_for_writes_to_sync(legacy_wait=True, max_timeout_in_sec=30)
    assert lag_env.t >= 5
