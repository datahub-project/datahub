from unittest.mock import MagicMock

import pytest
import requests

from lib.graphql import (
    GraphqlResult,
    RequestHealthTracker,
    effective_status_code,
    execute_graphql,
    meets_expectation,
)


def _mock_response(
    *, status_code: int = 200, json_data: dict | None = None, text: str = ""
):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.text = text
    return resp


def test_execute_graphql_success() -> None:
    session = MagicMock()
    session.post.return_value = _mock_response(
        json_data={"data": {"me": {"corpUser": {"urn": "urn:li:corpuser:x"}}}}
    )
    session.post.return_value.content = b'{"data":{"me":{}}}'
    result = execute_graphql(session, "http://localhost:9002", "query {}", "getMe")
    assert result.ok is True
    assert effective_status_code(result) == 200
    assert meets_expectation(result, 200)


def test_graphql_errors_map_to_403() -> None:
    result = GraphqlResult(
        ok=False,
        status_code=200,
        elapsed_ms=1.0,
        data={"errors": [{"message": "denied"}]},
        operation_name="getDomain",
        error_kind="graphql",
    )
    assert effective_status_code(result) == 403
    assert meets_expectation(result, 403)
    assert not meets_expectation(result, 200)


def test_execute_graphql_http_error() -> None:
    session = MagicMock()
    session.post.return_value = _mock_response(status_code=401, text="Unauthorized")
    result = execute_graphql(session, "http://localhost:9002", "query {}", "getMe")
    assert effective_status_code(result) == 401
    assert not meets_expectation(result, 200)


def test_request_health_tracker_expected_403() -> None:
    tracker = RequestHealthTracker()
    tracker.record(
        GraphqlResult(
            ok=False,
            status_code=200,
            elapsed_ms=3.0,
            data={"errors": [{"message": "denied"}]},
            operation_name="getDomain",
            error_kind="graphql",
            error_message="denied",
            response_bytes=100,
        ),
        expected_status_code=403,
    )
    summary = tracker.summary()
    assert summary["failure_count"] == 0
    tracker.ensure_all_success("persona-zero-authz/getDomain")


def test_request_health_tracker_status_mismatch() -> None:
    tracker = RequestHealthTracker()
    tracker.record(
        GraphqlResult(
            ok=False,
            status_code=200,
            elapsed_ms=3.0,
            data={"errors": [{"message": "denied"}]},
            operation_name="getDomain",
            error_kind="graphql",
        ),
        expected_status_code=200,
    )
    assert tracker.summary()["failure_count"] == 1
    with pytest.raises(RuntimeError, match="mismatched expected status"):
        tracker.ensure_all_success("persona-domain-owner/getDomain")


def test_execute_graphql_timeout() -> None:
    session = MagicMock()
    session.post.side_effect = requests.Timeout("timed out")
    result = execute_graphql(session, "http://localhost:9002", "query {}", "getMe")
    assert not meets_expectation(result, 200)
    assert not meets_expectation(result, 403)


def test_response_size_summary_kb() -> None:
    results = [
        GraphqlResult(
            ok=True,
            status_code=200,
            elapsed_ms=1.0,
            data={},
            operation_name="getMe",
            response_bytes=1024,
        ),
        GraphqlResult(
            ok=True,
            status_code=200,
            elapsed_ms=2.0,
            data={},
            operation_name="getMe",
            response_bytes=3072,
        ),
    ]
    summary = RequestHealthTracker.response_size_summary(results)
    assert summary == {
        "avg_kb": 2.0,
        "min_kb": 1.0,
        "max_kb": 3.0,
        "sample_count": 2,
    }
