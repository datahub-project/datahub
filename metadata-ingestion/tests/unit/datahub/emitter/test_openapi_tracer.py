from datetime import timedelta
from unittest.mock import Mock, patch

import pytest

from datahub.configuration.common import OperationalError
from datahub.emitter.openapi_tracer import OpenAPITrace, logger
from datahub.emitter.response_helper import TraceData


# Create a concrete test class that implements the protocol
class TestTracer(OpenAPITrace):
    _gms_server: str

    def __init__(self) -> None:
        self._gms_server = "http://test-server"
        self._emit_generic_payload = Mock()


@pytest.fixture
def api_trace() -> TestTracer:
    return TestTracer()


def test_empty_trace_data(api_trace):
    api_trace.await_status([], timedelta(seconds=10))
    assert not api_trace._emit_generic_payload.called


def test_successful_completion(api_trace):
    trace = TraceData(
        trace_id="test-trace-id", data={"urn:li:dataset:test": ["status"]}
    )

    mock_response = Mock(
        json=lambda: {
            "urn:li:dataset:test": {
                "status": {
                    "success": True,
                    "primaryStorage": {"writeStatus": "ACTIVE_STATE"},
                    "searchStorage": {"writeStatus": "TRACE_NOT_IMPLEMENTED"},
                }
            }
        }
    )
    api_trace._emit_generic_payload.return_value = mock_response

    api_trace.await_status([trace], timedelta(seconds=10))
    assert not trace.data  # Should be empty after successful completion


def test_timeout(api_trace):
    trace = TraceData(
        trace_id="test-trace-id", data={"urn:li:dataset:test": ["status"]}
    )

    mock_response = Mock()
    mock_response.json.return_value = {
        "urn:li:dataset:test": {
            "status": {
                "success": True,
                "primaryStorage": {"writeStatus": "PENDING"},
                "searchStorage": {"writeStatus": "PENDING"},
            }
        }
    }
    api_trace._emit_generic_payload.return_value = mock_response

    with pytest.raises(OperationalError) as exc_info:
        api_trace.await_status([trace], timedelta(seconds=0.1))
    assert "Timeout waiting for async write completion" in str(exc_info.value)


def test_persistence_failure(api_trace):
    trace = TraceData(
        trace_id="test-trace-id", data={"urn:li:dataset:test": ["status"]}
    )

    mock_response = Mock()
    mock_response.json.return_value = {
        "urn:li:dataset:test": {
            "status": {
                "success": False,
                "primaryStorage": {"writeStatus": "ERROR"},
                "searchStorage": {"writeStatus": "PENDING"},
            }
        }
    }
    api_trace._emit_generic_payload.return_value = mock_response

    with pytest.raises(OperationalError) as exc_info:
        api_trace.await_status([trace], timedelta(seconds=10))
    assert "Persistence failure" in str(exc_info.value)


def test_multiple_aspects(api_trace):
    trace = TraceData(
        trace_id="test-trace-id", data={"urn:li:dataset:test": ["status", "schema"]}
    )

    mock_response = Mock()
    mock_response.json.return_value = {
        "urn:li:dataset:test": {
            "status": {
                "success": True,
                "primaryStorage": {"writeStatus": "ACTIVE_STATE"},
                "searchStorage": {"writeStatus": "ACTIVE_STATE"},
            },
            "schema": {
                "success": True,
                "primaryStorage": {"writeStatus": "HISTORIC_STATE"},
                "searchStorage": {"writeStatus": "NO_OP"},
            },
        }
    }
    api_trace._emit_generic_payload.return_value = mock_response

    api_trace.await_status([trace], timedelta(seconds=10))
    assert not trace.data


def test_logging(api_trace):
    with patch.object(logger, "debug") as mock_debug, patch.object(
        logger, "error"
    ) as mock_error:
        # Test empty trace data logging
        api_trace.await_status([], timedelta(seconds=10))
        mock_debug.assert_called_once_with("No trace data to verify")

        # Test error logging
        trace = TraceData(trace_id="test-id", data={"urn:test": ["status"]})
        api_trace._emit_generic_payload.side_effect = Exception("Test error")
        with pytest.raises(Exception):
            api_trace.await_status([trace], timedelta(seconds=10))
        mock_error.assert_called_once()
