import json
import os
import time
from datetime import timedelta
from typing import Any, Dict
from unittest.mock import ANY, MagicMock, Mock, patch

import pytest
from requests import Response, Session

from datahub.configuration.common import (
    ConfigurationError,
    TraceTimeoutError,
    TraceValidationError,
)
from datahub.emitter import rest_emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.response_helper import TraceData
from datahub.emitter.rest_emitter import (
    BATCH_INGEST_MAX_PAYLOAD_LENGTH,
    INGEST_MAX_PAYLOAD_BYTES,
    DataHubRestEmitter,
    DatahubRestEmitter,
    EmitMode,
    RequestsSessionConfig,
    RestSinkEndpoint,
    logger,
)
from datahub.errors import APITracingWarning
from datahub.ingestion.graph.config import ClientMode
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    Status,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetProfile,
    DatasetProperties,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
)
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.server_config_util import RestServiceConfig

MOCK_GMS_ENDPOINT = "http://fakegmshost:8080"


@pytest.fixture
def sample_config() -> Dict[str, Any]:
    return {
        "models": {},
        "patchCapable": True,
        "versions": {
            "acryldata/datahub": {
                "version": "v1.0.1",
                "commit": "dc127c5f031d579732899ccd81a53a3514dc4a6d",
            }
        },
        "managedIngestion": {"defaultCliVersion": "1.0.0.2", "enabled": True},
        "statefulIngestionCapable": True,
        "supportsImpactAnalysis": True,
        "timeZone": "GMT",
        "telemetry": {"enabledCli": True, "enabledIngestion": False},
        "datasetUrnNameCasing": False,
        "retention": "true",
        "datahub": {"serverType": "dev"},
        "noCode": "true",
    }


@pytest.fixture
def mock_session():
    session = MagicMock(spec=Session)
    return session


@pytest.fixture
def mock_response(sample_config):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = sample_config
    response.text = str(sample_config)
    return response


class TestDataHubRestEmitter:
    @pytest.fixture
    def openapi_emitter(self) -> DataHubRestEmitter:
        openapi_emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=True)

        # Set the underlying private attribute directly
        openapi_emitter._server_config = RestServiceConfig(
            raw_config={
                "versions": {
                    "acryldata/datahub": {
                        "version": "v1.0.1rc0"  # Supports OpenApi & Tracing
                    }
                }
            }
        )

        return openapi_emitter

    def test_datahub_rest_emitter_missing_gms_server(self):
        """Test that emitter raises ConfigurationError when gms_server is not provided."""
        # Case 1: Empty string
        with pytest.raises(ConfigurationError) as excinfo:
            DataHubRestEmitter("")
        assert "gms server is required" in str(excinfo.value)

    def test_connection_error_reraising(self):
        """Test that test_connection() properly re-raises ConfigurationError."""
        # Create a basic emitter
        emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT)

        # Mock the session's get method to raise ConfigurationError
        with patch.object(emitter._session, "get") as mock_get:
            # Configure the mock to raise ConfigurationError
            mock_error = ConfigurationError("Connection failed")
            mock_get.side_effect = mock_error

            # Verify that the exception is re-raised
            with pytest.raises(ConfigurationError) as excinfo:
                emitter.test_connection()

            # Verify it's the same exception object (not a new one)
            assert excinfo.value is mock_error

    def test_datahub_rest_emitter_construction(self) -> None:
        emitter = DatahubRestEmitter(MOCK_GMS_ENDPOINT)
        assert emitter._session_config.timeout == rest_emitter._DEFAULT_TIMEOUT_SEC
        assert (
            emitter._session_config.retry_status_codes
            == rest_emitter._DEFAULT_RETRY_STATUS_CODES
        )
        assert (
            emitter._session_config.retry_max_times
            == rest_emitter._DEFAULT_RETRY_MAX_TIMES
        )

    def test_datahub_rest_emitter_timeout_construction(self) -> None:
        emitter = DatahubRestEmitter(
            MOCK_GMS_ENDPOINT, connect_timeout_sec=2, read_timeout_sec=4
        )
        assert emitter._session_config.timeout == (2, 4)

    def test_datahub_rest_emitter_general_timeout_construction(self) -> None:
        emitter = DatahubRestEmitter(
            MOCK_GMS_ENDPOINT, timeout_sec=2, read_timeout_sec=4
        )
        assert emitter._session_config.timeout == (2, 4)

    def test_datahub_rest_emitter_retry_construction(self) -> None:
        emitter = DatahubRestEmitter(
            MOCK_GMS_ENDPOINT,
            retry_status_codes=[418],
            retry_max_times=42,
        )
        assert emitter._session_config.retry_status_codes == [418]
        assert emitter._session_config.retry_max_times == 42

    def test_datahub_rest_emitter_extra_params(self) -> None:
        emitter = DatahubRestEmitter(
            MOCK_GMS_ENDPOINT, extra_headers={"key1": "value1", "key2": "value2"}
        )
        assert emitter._session.headers.get("key1") == "value1"
        assert emitter._session.headers.get("key2") == "value2"

    def test_openapi_emitter_emit(self, openapi_emitter):
        item = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
            aspect=DatasetProfile(
                rowCount=2000,
                columnCount=15,
                timestampMillis=1626995099686,
            ),
        )

        with patch.object(openapi_emitter, "_emit_generic") as mock_method:
            openapi_emitter.emit_mcp(item)

            mock_method.assert_called_once_with(
                f"{MOCK_GMS_ENDPOINT}/openapi/v3/entity/dataset?async=false",
                payload=[
                    {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
                        "datasetProfile": {
                            "value": {
                                "rowCount": 2000,
                                "columnCount": 15,
                                "timestampMillis": 1626995099686,
                                "partitionSpec": {
                                    "partition": "FULL_TABLE_SNAPSHOT",
                                    "type": "FULL_TABLE",
                                },
                            },
                            "systemMetadata": {
                                "lastObserved": ANY,
                                "runId": "no-run-id-provided",
                                "lastRunId": "no-run-id-provided",
                                "properties": {
                                    "clientId": "acryl-datahub",
                                    "clientVersion": "1!0.0.0.dev0",
                                },
                            },
                            "headers": {},
                        },
                    }
                ],
                method="post",
            )

    def test_openapi_emitter_emit_mcps(self, openapi_emitter):
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
            items = [
                MetadataChangeProposalWrapper(
                    entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount{i},PROD)",
                    aspect=DatasetProfile(
                        rowCount=2000 + i,
                        columnCount=15,
                        timestampMillis=1626995099686,
                    ),
                )
                for i in range(3)
            ]

            result = openapi_emitter.emit_mcps(items)

            assert result == 1

            # Single chunk test - all items should be in one batch
            mock_emit.assert_called_once()
            call_args = mock_emit.call_args
            assert (
                call_args[0][0]
                == f"{MOCK_GMS_ENDPOINT}/openapi/v3/entity/dataset?async=false"
            )
            assert isinstance(call_args[1]["payload"], str)  # Should be JSON string

    def test_openapi_emitter_emit_mcps_max_bytes(self, openapi_emitter):
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
            # Create a large payload that will force chunking
            large_payload = "x" * (
                INGEST_MAX_PAYLOAD_BYTES // 2
            )  # Each item will be about half max size
            items = [
                MetadataChangeProposalWrapper(
                    entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:mysql,LargePayload{i},PROD)",
                    aspect=DatasetProperties(name=large_payload),
                )
                for i in range(
                    3
                )  # This should create at least 2 chunks given the payload size
            ]

            openapi_emitter.emit_mcps(items)

            # Verify multiple chunks were created
            assert mock_emit.call_count > 1

            # Verify each chunk's payload is within size limits
            for call in mock_emit.call_args_list:
                args = call[1]
                assert "payload" in args
                payload = args["payload"]
                assert isinstance(payload, str)  # Should be JSON string
                assert len(payload.encode()) <= INGEST_MAX_PAYLOAD_BYTES

                # Verify the payload structure
                payload_data = json.loads(payload)
                assert payload_data[0]["urn"].startswith("urn:li:dataset")
                assert "datasetProperties" in payload_data[0]

    def test_openapi_emitter_emit_mcps_max_items(self, openapi_emitter):
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
            # Create more items than BATCH_INGEST_MAX_PAYLOAD_LENGTH
            items = [
                MetadataChangeProposalWrapper(
                    entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:mysql,Item{i},PROD)",
                    aspect=DatasetProfile(
                        rowCount=i,
                        columnCount=15,
                        timestampMillis=1626995099686,
                    ),
                )
                for i in range(
                    BATCH_INGEST_MAX_PAYLOAD_LENGTH + 2
                )  # Create 2 more than max
            ]

            openapi_emitter.emit_mcps(items)

            # Verify multiple chunks were created
            assert mock_emit.call_count == 2

            # Check first chunk
            first_call = mock_emit.call_args_list[0]
            first_payload = json.loads(first_call[1]["payload"])
            assert len(first_payload) == BATCH_INGEST_MAX_PAYLOAD_LENGTH

            # Check second chunk
            second_call = mock_emit.call_args_list[1]
            second_payload = json.loads(second_call[1]["payload"])
            assert len(second_payload) == 2  # Should have the remaining 2 items

    def test_openapi_emitter_emit_mcps_multiple_entity_types(self, openapi_emitter):
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
            # Create items for two different entity types
            dataset_items = [
                MetadataChangeProposalWrapper(
                    entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:mysql,Dataset{i},PROD)",
                    aspect=DatasetProfile(
                        rowCount=i,
                        columnCount=15,
                        timestampMillis=1626995099686,
                    ),
                )
                for i in range(2)
            ]

            dashboard_items = [
                MetadataChangeProposalWrapper(
                    entityUrn=f"urn:li:dashboard:(looker,dashboards.{i})",
                    aspect=Status(removed=False),
                )
                for i in range(2)
            ]

            items = dataset_items + dashboard_items
            result = openapi_emitter.emit_mcps(items)

            # Should return number of unique entity URLs
            assert result == 2
            assert mock_emit.call_count == 2

            # Check that calls were made with different URLs but correct payloads
            calls = {
                call[0][0]: json.loads(call[1]["payload"])
                for call in mock_emit.call_args_list
            }

            # Verify each URL got the right aspects
            for url, payload in calls.items():
                if "datasetProfile" in payload[0]:
                    assert url.endswith("dataset?async=false")
                    assert len(payload) == 2
                    assert all("datasetProfile" in item for item in payload)
                else:
                    assert url.endswith("dashboard?async=false")
                    assert len(payload) == 2
                    assert all("status" in item for item in payload)

    def test_openapi_emitter_emit_mcp_with_tracing(self, openapi_emitter):
        """Test emitting a single MCP with tracing enabled"""
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
            # Mock the response for the initial emit
            mock_response = Mock(spec=Response)
            mock_response.status_code = 200
            mock_response.headers = {
                "traceparent": "00-00063609cb934b9d0d4e6a7d6d5e1234-1234567890abcdef-01"
            }
            mock_response.json.return_value = [
                {
                    "urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
                    "datasetProfile": {},
                }
            ]
            mock_emit.return_value = mock_response

            # Create test item
            item = MetadataChangeProposalWrapper(
                entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
                aspect=DatasetProfile(
                    rowCount=2000,
                    columnCount=15,
                    timestampMillis=1626995099686,
                ),
            )

            # Set up mock for trace verification responses
            trace_responses = [
                # First check - pending
                {
                    "urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)": {
                        "datasetProfile": {
                            "success": True,
                            "primaryStorage": {"writeStatus": "PENDING"},
                            "searchStorage": {"writeStatus": "PENDING"},
                        }
                    }
                },
                # Second check - completed
                {
                    "urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)": {
                        "datasetProfile": {
                            "success": True,
                            "primaryStorage": {"writeStatus": "ACTIVE_STATE"},
                            "searchStorage": {"writeStatus": "ACTIVE_STATE"},
                        }
                    }
                },
            ]

            def side_effect(*args, **kwargs):
                if "trace/write" in args[0]:
                    mock_trace_response = Mock(spec=Response)
                    mock_trace_response.json.return_value = trace_responses.pop(0)
                    return mock_trace_response
                return mock_response

            mock_emit.side_effect = side_effect

            # Emit with tracing enabled
            openapi_emitter.emit_mcp(
                item,
                emit_mode=EmitMode.ASYNC_WAIT,
                wait_timeout=timedelta(seconds=10),
            )

            # Verify initial emit call
            assert (
                mock_emit.call_args_list[0][0][0]
                == f"{MOCK_GMS_ENDPOINT}/openapi/v3/entity/dataset?async=true"
            )

            # Verify trace verification calls
            trace_calls = [
                call for call in mock_emit.call_args_list if "trace/write" in call[0][0]
            ]
            assert len(trace_calls) == 2
            assert "00063609cb934b9d0d4e6a7d6d5e1234" in trace_calls[0][0][0]

    def test_openapi_emitter_emit_mcps_with_tracing(self, openapi_emitter):
        """Test emitting multiple MCPs with tracing enabled"""
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
            # Create test items
            items = [
                MetadataChangeProposalWrapper(
                    entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount{i},PROD)",
                    aspect=Status(removed=False),
                )
                for i in range(2)
            ]

            # Mock responses for initial emit
            emit_responses = []
            mock_resp = Mock(spec=Response)
            mock_resp.status_code = 200
            mock_resp.headers = {"traceparent": "test-trace"}
            mock_resp.json.return_value = [
                {
                    "urn": f"urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount{i},PROD)",
                    "status": {"removed": False},
                }
                for i in range(2)
            ]
            emit_responses.append(mock_resp)

            # Mock trace verification responses
            trace_responses = [
                # First check - all pending
                {
                    f"urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount{i},PROD)": {
                        "status": {
                            "success": True,
                            "primaryStorage": {"writeStatus": "PENDING"},
                            "searchStorage": {"writeStatus": "PENDING"},
                        }
                    }
                    for i in range(2)
                },
                # Second check - all completed
                {
                    f"urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount{i},PROD)": {
                        "status": {
                            "success": True,
                            "primaryStorage": {"writeStatus": "ACTIVE_STATE"},
                            "searchStorage": {"writeStatus": "ACTIVE_STATE"},
                        }
                    }
                    for i in range(2)
                },
            ]

            response_iter = iter(emit_responses)
            trace_response_iter = iter(trace_responses)

            def side_effect(*args, **kwargs):
                if "trace/write" in args[0]:
                    mock_trace_response = Mock(spec=Response)
                    mock_trace_response.json.return_value = next(trace_response_iter)
                    return mock_trace_response
                return next(response_iter)

            mock_emit.side_effect = side_effect

            # Emit with tracing enabled
            result = openapi_emitter.emit_mcps(
                items,
                emit_mode=EmitMode.ASYNC_WAIT,
                wait_timeout=timedelta(seconds=10),
            )

            assert result == 1  # Should return number of unique entity URLs

            # Verify initial emit calls
            emit_calls = [
                call
                for call in mock_emit.call_args_list
                if "entity/dataset" in call[0][0]
            ]
            assert len(emit_calls) == 1
            assert (
                emit_calls[0][0][0]
                == f"{MOCK_GMS_ENDPOINT}/openapi/v3/entity/dataset?async=true"
            )

            # Verify trace verification calls
            trace_calls = [
                call for call in mock_emit.call_args_list if "trace/write" in call[0][0]
            ]
            assert len(trace_calls) == 2

    def test_openapi_emitter_trace_timeout(self, openapi_emitter):
        """Test that tracing properly handles timeouts"""
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
            # Mock initial emit response
            mock_response = Mock(spec=Response)
            mock_response.status_code = 200
            mock_response.headers = {"traceparent": "test-trace-123"}
            mock_response.json.return_value = [
                {
                    "urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
                    "datasetProfile": {},
                }
            ]

            # Mock trace verification to always return pending
            mock_trace_response = Mock(spec=Response)
            mock_trace_response.json.return_value = {
                "urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)": {
                    "datasetProfile": {
                        "success": True,
                        "primaryStorage": {"writeStatus": "PENDING"},
                        "searchStorage": {"writeStatus": "PENDING"},
                    }
                }
            }

            def side_effect(*args, **kwargs):
                return (
                    mock_trace_response if "trace/write" in args[0] else mock_response
                )

            mock_emit.side_effect = side_effect

            item = MetadataChangeProposalWrapper(
                entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
                aspect=DatasetProfile(
                    rowCount=2000,
                    columnCount=15,
                    timestampMillis=1626995099686,
                ),
            )

            # Emit with very short timeout
            with pytest.raises(TraceTimeoutError) as exc_info:
                openapi_emitter.emit_mcp(
                    item,
                    emit_mode=EmitMode.ASYNC_WAIT,
                    wait_timeout=timedelta(milliseconds=1),
                )

            assert "Timeout waiting for async write completion" in str(exc_info.value)

    def test_openapi_emitter_missing_trace_header(self, openapi_emitter):
        """Test behavior when trace header is missing"""
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
            # Mock response without trace header
            mock_response = Mock(spec=Response)
            mock_response.status_code = 200
            mock_response.headers = {}  # No traceparent header
            mock_response.json.return_value = [
                {
                    "urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,MissingTraceHeader,PROD)",
                    "status": {"removed": False},
                }
            ]
            mock_emit.return_value = mock_response

            item = MetadataChangeProposalWrapper(
                entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,MissingTraceHeader,PROD)",
                aspect=Status(removed=False),
            )

            # Should not raise exception but log a warning.
            with pytest.warns(APITracingWarning):
                openapi_emitter.emit_mcp(
                    item,
                    emit_mode=EmitMode.ASYNC_WAIT,
                    wait_timeout=timedelta(seconds=10),
                )

    def test_openapi_emitter_invalid_status_code(self, openapi_emitter):
        """Test behavior when response has non-200 status code"""
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
            # Mock response with error status code
            mock_response = Mock(spec=Response)
            mock_response.status_code = 500
            mock_response.headers = {"traceparent": "test-trace-123"}
            mock_response.json.return_value = [
                {
                    "urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,InvalidStatusCode,PROD)",
                    "datasetProfile": {},
                }
            ]
            mock_emit.return_value = mock_response

            item = MetadataChangeProposalWrapper(
                entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,InvalidStatusCode,PROD)",
                aspect=DatasetProfile(
                    rowCount=2000,
                    columnCount=15,
                    timestampMillis=1626995099686,
                ),
            )

            # Should not raise exception but log warning
            openapi_emitter.emit_mcp(
                item,
                emit_mode=EmitMode.ASYNC_WAIT,
                wait_timeout=timedelta(seconds=10),
            )

    def test_openapi_emitter_trace_failure(self, openapi_emitter):
        """Test handling of trace verification failures"""
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
            test_urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,TraceFailure,PROD)"

            # Create initial emit response
            emit_response = Mock(spec=Response)
            emit_response.status_code = 200
            emit_response.headers = {
                "traceparent": "00-00063609cb934b9d0d4e6a7d6d5e1234-1234567890abcdef-01"
            }
            emit_response.json.return_value = [{"urn": test_urn, "datasetProfile": {}}]

            # Create trace verification response
            trace_response = Mock(spec=Response)
            trace_response.status_code = 200
            trace_response.headers = {}
            trace_response.json.return_value = {
                test_urn: {
                    "datasetProfile": {
                        "success": False,
                        "error": "Failed to write to storage",
                        "primaryStorage": {"writeStatus": "ERROR"},
                        "searchStorage": {"writeStatus": "ERROR"},
                    }
                }
            }

            def side_effect(*args, **kwargs):
                if "trace/write" in args[0]:
                    return trace_response
                return emit_response

            mock_emit.side_effect = side_effect

            item = MetadataChangeProposalWrapper(
                entityUrn=test_urn,
                aspect=DatasetProfile(
                    rowCount=2000,
                    columnCount=15,
                    timestampMillis=1626995099686,
                ),
            )

            with pytest.raises(TraceValidationError) as exc_info:
                openapi_emitter.emit_mcp(
                    item,
                    emit_mode=EmitMode.ASYNC_WAIT,
                    wait_timeout=timedelta(seconds=10),
                )

            error_message = str(exc_info.value)

            # Check for key error message components
            assert "Unable to validate async write" in error_message
            assert "to DataHub GMS" in error_message
            assert "Failed to write to storage" in error_message
            assert "primaryStorage" in error_message
            assert "writeStatus" in error_message
            assert "'ERROR'" in error_message

            # Verify trace was actually called
            trace_calls = [
                call for call in mock_emit.call_args_list if "trace/write" in call[0][0]
            ]
            assert len(trace_calls) > 0
            assert "00063609cb934b9d0d4e6a7d6d5e1234" in trace_calls[0][0][0]

    def test_await_status_empty_trace_data(self, openapi_emitter):
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
            openapi_emitter._await_status([], timedelta(seconds=10))
            assert not mock_emit._emit_generic.called

    def test_await_status_successful_completion(self, openapi_emitter):
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
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
            mock_emit.return_value = mock_response

            openapi_emitter._await_status([trace], timedelta(seconds=10))
            assert not trace.data  # Should be empty after successful completion

    def test_await_status_timeout(self, openapi_emitter):
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
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
            mock_emit.return_value = mock_response

            with pytest.raises(TraceTimeoutError) as exc_info:
                openapi_emitter._await_status([trace], timedelta(seconds=0.1))
            assert "Timeout waiting for async write completion" in str(exc_info.value)

    def test_await_status_persistence_failure(self, openapi_emitter):
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
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
            mock_emit.return_value = mock_response

            with pytest.raises(TraceValidationError) as exc_info:
                openapi_emitter._await_status([trace], timedelta(seconds=10))
            assert "Persistence failure" in str(exc_info.value)

    def test_await_status_multiple_aspects(self, openapi_emitter):
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
            trace = TraceData(
                trace_id="test-trace-id",
                data={"urn:li:dataset:test": ["status", "schema"]},
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
            mock_emit.return_value = mock_response

            openapi_emitter._await_status([trace], timedelta(seconds=10))
            assert not trace.data

    def test_await_status_logging(self, openapi_emitter):
        with (
            patch.object(logger, "debug") as mock_debug,
            patch.object(logger, "error") as mock_error,
            patch(
                "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
            ) as mock_emit,
        ):
            # Test empty trace data logging
            openapi_emitter._await_status([], timedelta(seconds=10))
            mock_debug.assert_called_once_with("No trace data to verify")

            # Test error logging
            trace = TraceData(trace_id="test-id", data={"urn:test": ["status"]})
            mock_emit.side_effect = TraceValidationError("Test error")
            with pytest.raises(TraceValidationError):
                openapi_emitter._await_status([trace], timedelta(seconds=10))
            mock_error.assert_called_once()

    def test_openapi_emitter_same_url_different_methods(self, openapi_emitter):
        """Test handling of requests with same URL but different HTTP methods"""
        with patch(
            "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
        ) as mock_emit:
            items = [
                # POST requests for updating
                MetadataChangeProposalWrapper(
                    entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:mysql,UpdateMe{i},PROD)",
                    entityType="dataset",
                    aspectName="datasetProperties",
                    changeType=ChangeTypeClass.UPSERT,
                    aspect=DatasetProperties(name=f"Updated Dataset {i}"),
                )
                for i in range(2)
            ] + [
                # PATCH requests for fetching
                next(
                    iter(
                        DatasetPatchBuilder(
                            f"urn:li:dataset:(urn:li:dataPlatform:mysql,PatchMe{i},PROD)"
                        )
                        .set_qualified_name(f"PatchMe{i}")
                        .build()
                    )
                )
                for i in range(2)
            ]

            # Run the test
            result = openapi_emitter.emit_mcps(items)

            # Verify that we made 2 calls (one for each HTTP method)
            assert result == 2
            assert mock_emit.call_count == 2

            # Check that calls were made with different methods but the same URL
            calls = {}
            for call in mock_emit.call_args_list:
                method = call[1]["method"]
                url = call[0][0]
                calls[(method, url)] = call

            assert (
                "post",
                f"{MOCK_GMS_ENDPOINT}/openapi/v3/entity/dataset?async=false",
            ) in calls
            assert (
                "patch",
                f"{MOCK_GMS_ENDPOINT}/openapi/v3/entity/dataset?async=false",
            ) in calls

    def test_openapi_emitter_mixed_method_chunking(self, openapi_emitter):
        """Test that chunking works correctly across different HTTP methods"""
        with (
            patch(
                "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
            ) as mock_emit,
            patch("datahub.emitter.rest_emitter.BATCH_INGEST_MAX_PAYLOAD_LENGTH", 2),
        ):
            # Create more items than the chunk size for each method
            items = [
                # POST items (4 items, should create 2 chunks)
                MetadataChangeProposalWrapper(
                    entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:mysql,Dataset{i},PROD)",
                    entityType="dataset",
                    aspectName="datasetProfile",
                    changeType=ChangeTypeClass.UPSERT,
                    aspect=DatasetProfile(
                        rowCount=i, columnCount=15, timestampMillis=0
                    ),
                )
                for i in range(4)
            ] + [
                # PATCH items (3 items, should create 2 chunks)
                next(
                    iter(
                        DatasetPatchBuilder(
                            f"urn:li:dataset:(urn:li:dataPlatform:mysql,PatchMe{i},PROD)"
                        )
                        .set_qualified_name(f"PatchMe{i}")
                        .build()
                    )
                )
                for i in range(3)
            ]

            # Run the test with a smaller chunk size to force multiple chunks
            result = openapi_emitter.emit_mcps(items)

            # Should have 4 chunks total:
            # - 2 chunks for POST (4 items with max 2 per chunk)
            # - 2 chunks for PATCH (3 items with max 2 per chunk)
            assert result == 4
            assert mock_emit.call_count == 4

            # Count the calls by method and verify chunking
            post_calls = [
                call for call in mock_emit.call_args_list if call[1]["method"] == "post"
            ]
            patch_calls = [
                call
                for call in mock_emit.call_args_list
                if call[1]["method"] == "patch"
            ]

            assert len(post_calls) == 2  # 2 chunks for POST
            assert len(patch_calls) == 2  # 2 chunks for PATCH

            # Verify first chunks have max size and last chunks have remainders
            post_payloads = [json.loads(call[1]["payload"]) for call in post_calls]
            patch_payloads = [json.loads(call[1]["payload"]) for call in patch_calls]

            assert len(post_payloads[0]) == 2
            assert len(post_payloads[1]) == 2
            assert len(patch_payloads[0]) == 2
            assert len(patch_payloads[1]) == 1

            # Verify all post calls are to the dataset endpoint
            for call in post_calls:
                assert (
                    call[0][0]
                    == f"{MOCK_GMS_ENDPOINT}/openapi/v3/entity/dataset?async=false"
                )

            # Verify all patch calls are to the dataset endpoint
            for call in patch_calls:
                assert (
                    call[0][0]
                    == f"{MOCK_GMS_ENDPOINT}/openapi/v3/entity/dataset?async=false"
                )

    def test_openapi_sync_full_emit_mode(self, openapi_emitter):
        """Test that SYNC_WAIT emit mode correctly sets async=false URL parameter and sync header"""

        # Create a test MCP
        test_mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(test,sync_full,PROD)",
            aspect=DatasetProfile(
                rowCount=500,
                columnCount=10,
                timestampMillis=1626995099686,
            ),
        )

        # Test with SYNC_WAIT emit mode
        with patch.object(openapi_emitter, "_emit_generic") as mock_emit:
            # Configure mock to return a simple response
            mock_response = Mock(spec=Response)
            mock_response.status_code = 200
            mock_response.headers = {}
            mock_emit.return_value = mock_response

            # Call emit_mcp with SYNC_WAIT mode
            openapi_emitter.emit_mcp(test_mcp, emit_mode=EmitMode.SYNC_WAIT)

            # Verify _emit_generic was called
            mock_emit.assert_called_once()

            # Check URL contains async=false
            url = mock_emit.call_args[0][0]
            assert "async=false" in url

            # Check payload contains the synchronization header
            payload = mock_emit.call_args[1]["payload"]
            # Convert payload to dict if it's a JSON string
            if isinstance(payload, str):
                payload = json.loads(payload)

            # Verify the headers in the payload
            assert isinstance(payload, list)
            assert len(payload) > 0
            assert "headers" in payload[0]["datasetProfile"]
            assert (
                payload[0]["datasetProfile"]["headers"].get(
                    "X-DataHub-Sync-Index-Update"
                )
                == "true"
            )

    def test_config_cache_ttl(self, mock_session, mock_response):
        """Test that config is cached and respects TTL"""
        mock_session.get.return_value = mock_response

        # Create emitter with 2 second TTL
        emitter = DataHubRestEmitter(
            MOCK_GMS_ENDPOINT, server_config_refresh_interval=2
        )
        emitter._session = mock_session

        # First call should fetch from server
        config1 = emitter.fetch_server_config()
        assert mock_session.get.call_count == 1

        # Second call within TTL should use cache
        config2 = emitter.fetch_server_config()
        assert mock_session.get.call_count == 1  # Still 1, not 2
        assert config1 is config2  # Same object reference

        # Wait for TTL to expire
        time.sleep(2.1)

        # Next call should fetch from server again
        emitter.fetch_server_config()
        assert mock_session.get.call_count == 2

    def test_config_cache_default_ttl(self, mock_session, mock_response):
        """Test that default TTL is 60 seconds"""
        emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT)
        assert emitter._server_config_refresh_interval is None

    def test_config_cache_manual_invalidation(self, mock_session, mock_response):
        """Test manual cache invalidation"""
        mock_session.get.return_value = mock_response

        emitter = DataHubRestEmitter(
            MOCK_GMS_ENDPOINT, server_config_refresh_interval=60
        )
        emitter._session = mock_session

        # First call fetches from server
        emitter.fetch_server_config()
        assert mock_session.get.call_count == 1

        # Second call uses cache
        emitter.fetch_server_config()
        assert mock_session.get.call_count == 1

        # Invalidate cache
        emitter.invalidate_config_cache()

        # Next call should fetch from server
        emitter.fetch_server_config()
        assert mock_session.get.call_count == 2

    def test_config_cache_with_pre_set_config(self):
        """Test that pre-set config doesn't trigger fetch"""
        emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT)

        # Manually set config without fetch time
        test_config = RestServiceConfig(raw_config={"noCode": "true", "test": "value"})
        emitter._server_config = test_config

        # Mock session to ensure it's not called
        with patch.object(emitter._session, "get") as mock_get:
            config = emitter.fetch_server_config()

            # Should not have made any HTTP calls
            mock_get.assert_not_called()

            # Should return the pre-set config
            assert config is test_config

    def test_config_cache_with_connection_error(self, mock_session):
        """Test that failed fetches don't affect cache"""
        emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT)
        emitter._session = mock_session

        # First call fails
        mock_session.get.side_effect = ConfigurationError("Connection failed")

        with pytest.raises(ConfigurationError):
            emitter.fetch_server_config()
        assert mock_session.get.call_count == 1

        # Cache should still be empty
        assert not hasattr(emitter, "_server_config") or emitter._server_config is None
        assert emitter._config_fetch_time is None

        # Fix the error and set up successful response
        mock_session.get.side_effect = None
        mock_response = Mock(
            status_code=200, json=lambda: {"noCode": "true", "test": "value"}
        )
        mock_session.get.return_value = mock_response

        # Second call should still hit server (no cache from failed attempt)
        config = emitter.fetch_server_config()
        assert mock_session.get.call_count == 2
        assert config is not None

    def test_config_property_uses_fetch_method(self, mock_session, mock_response):
        """Test that server_config property uses fetch_server_config method"""
        mock_session.get.return_value = mock_response

        emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT)
        emitter._session = mock_session

        # Access via property
        with patch.object(
            emitter, "fetch_server_config", wraps=emitter.fetch_server_config
        ) as mock_fetch:
            config = emitter.server_config
            mock_fetch.assert_called_once()
            assert config is not None

    def test_config_cache_multiple_emitters(self, mock_session, mock_response):
        """Test that each emitter has its own cache"""
        mock_session.get.return_value = mock_response

        emitter1 = DataHubRestEmitter(MOCK_GMS_ENDPOINT)
        emitter1._session = mock_session

        emitter2 = DataHubRestEmitter(MOCK_GMS_ENDPOINT)
        # Create a separate mock for emitter2
        mock_session2 = Mock()
        mock_session2.get.return_value = mock_response
        emitter2._session = mock_session2

        # Each emitter should make its own call
        config1 = emitter1.fetch_server_config()
        config2 = emitter2.fetch_server_config()

        assert mock_session.get.call_count == 1
        assert mock_session2.get.call_count == 1

        # Configs should be different objects
        assert config1 is not config2

    def test_config_cache_custom_ttl(self, mock_session, mock_response):
        """Test creating emitter with custom TTL"""
        mock_session.get.return_value = mock_response

        # Create with 5 minute TTL
        emitter = DataHubRestEmitter(
            MOCK_GMS_ENDPOINT, server_config_refresh_interval=300
        )
        emitter._session = mock_session

        assert emitter._server_config_refresh_interval == 300

        # Fetch config
        emitter.fetch_server_config()
        assert mock_session.get.call_count == 1

        # After 1 minute, should still use cache
        # Simulate time passing by manipulating _config_fetch_time
        emitter._config_fetch_time = time.time() - 60  # 1 minute ago

        emitter.fetch_server_config()
        assert mock_session.get.call_count == 1  # Still cached

        # After 6 minutes, should fetch again
        emitter._config_fetch_time = time.time() - 360  # 6 minutes ago

        emitter.fetch_server_config()
        assert mock_session.get.call_count == 2  # Fetched again

    def test_unicode_character_emission(self):
        """Test that unicode characters are properly escaped"""
        emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=False)

        # Create MCP with unicode characters in dataset properties
        test_mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,UnicodeTest,PROD)",
            aspect=DatasetProperties(
                name="Test Dataset with émojis 🚀 and spëcial chars ñ",
                description="Dataset with unicode: café, naïve, résumé, 中文, العربية",
            ),
        )

        with patch.object(emitter, "_emit_generic") as mock_emit:
            emitter.emit_mcp(test_mcp)

            # Verify _emit_generic was called
            mock_emit.assert_called_once()

            # Get the payload and verify unicode escaping
            payload = mock_emit.call_args[0][1]  # Second positional argument

            # unicode escapes are double-escaped within the JSON structure
            assert "\\\\u00e9" in payload  # é (double-escaped)
            assert (
                "\\\\ud83d\\\\ude80" in payload
            )  # 🚀 rocket emoji as UTF-16 surrogate pair (double-escaped)
            assert "\\\\u00eb" in payload  # ë (double-escaped)
            assert "\\\\u00f1" in payload  # ñ (double-escaped)
            assert "\\\\u4e2d\\\\u6587" in payload  # 中文 (double-escaped)

            # ASCII characters should remain unescaped
            assert "Test Dataset" in payload
            assert "Dataset with unicode" in payload

    def test_preserve_unicode_escapes_function_directly(self):
        """Test the preserve_unicode_escapes function with various unicode scenarios"""
        from datahub.emitter.rest_emitter import preserve_unicode_escapes

        # Test simple unicode characters
        test_dict = {
            "name": "Café",
            "description": "naïve résumé",
            "emoji": "Hello 👋 World 🌍",
            "chinese": "你好世界",
            "arabic": "مرحبا بالعالم",
            "nested": {
                "field": "spëciàl chàrs",
                "list": ["item1", "itëm2 🚀", "中文项目"],
            },
        }

        result = preserve_unicode_escapes(test_dict)

        # Verify unicode escaping - check the actual dictionary values
        assert result["name"] == "Caf\\u00e9"  # é
        assert result["description"] == "na\\u00efve r\\u00e9sum\\u00e9"  # ï, é
        assert result["emoji"] == "Hello \\u1f44b World \\u1f30d"  # 👋 🌍
        assert result["chinese"] == "\\u4f60\\u597d\\u4e16\\u754c"  # 你好世界
        assert result["nested"]["field"] == "sp\\u00ebci\\u00e0l ch\\u00e0rs"  # ë, à, à
        assert result["nested"]["list"][1] == "it\\u00ebm2 \\u1f680"  # ë, 🚀
        assert result["nested"]["list"][2] == "\\u4e2d\\u6587\\u9879\\u76ee"  # 中文项目

        # ASCII should remain unchanged
        assert result["nested"]["list"][0] == "item1"  # Pure ASCII


class TestOpenApiModeSelection:
    def test_sdk_client_mode_no_env_var(self, mock_session, mock_response):
        """Test that SDK client mode defaults to OpenAPI when no env var is present"""
        mock_session.get.return_value = mock_response

        # Ensure no env vars
        with patch.dict(os.environ, {}, clear=True):
            emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, client_mode=ClientMode.SDK)
            emitter._session = mock_session
            emitter.test_connection()
            assert emitter._openapi_ingestion is True

    def test_non_sdk_client_mode_no_env_var(self, mock_session, mock_response):
        """Test that non-SDK client modes default to RestLi when no env var is present"""
        mock_session.get.return_value = mock_response

        # Ensure no env vars
        with patch.dict(os.environ, {}, clear=True):
            # Test INGESTION mode
            emitter = DataHubRestEmitter(
                MOCK_GMS_ENDPOINT, client_mode=ClientMode.INGESTION
            )
            emitter._session = mock_session
            emitter.test_connection()
            assert emitter._openapi_ingestion is False

            # Test CLI mode
            emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, client_mode=ClientMode.CLI)
            emitter._session = mock_session
            emitter.test_connection()
            assert emitter._openapi_ingestion is False

    def test_env_var_restli_overrides_sdk_mode(self, mock_session, mock_response):
        """Test that env var set to RESTLI overrides SDK client mode default"""
        mock_session.get.return_value = mock_response

        with (
            patch.dict(
                os.environ,
                {"DATAHUB_REST_EMITTER_DEFAULT_ENDPOINT": "RESTLI"},
                clear=True,
            ),
            patch(
                "datahub.emitter.rest_emitter.DEFAULT_REST_EMITTER_ENDPOINT",
                RestSinkEndpoint.RESTLI,
            ),
        ):
            emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, client_mode=ClientMode.SDK)
            emitter._session = mock_session
            emitter.test_connection()
            assert emitter._openapi_ingestion is False

    def test_env_var_openapi_any_client_mode(self, mock_session, mock_response):
        """Test that env var set to OPENAPI enables OpenAPI for any client mode"""
        mock_session.get.return_value = mock_response

        with (
            patch.dict(
                os.environ,
                {"DATAHUB_REST_EMITTER_DEFAULT_ENDPOINT": "OPENAPI"},
                clear=True,
            ),
            patch(
                "datahub.emitter.rest_emitter.DEFAULT_REST_EMITTER_ENDPOINT",
                RestSinkEndpoint.OPENAPI,
            ),
        ):
            # Test INGESTION mode
            emitter = DataHubRestEmitter(
                MOCK_GMS_ENDPOINT, client_mode=ClientMode.INGESTION
            )
            emitter._session = mock_session
            emitter.test_connection()
            assert emitter._openapi_ingestion is True

            # Test CLI mode
            emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, client_mode=ClientMode.CLI)
            emitter._session = mock_session
            emitter.test_connection()
            assert emitter._openapi_ingestion is True

            # Test SDK mode
            emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, client_mode=ClientMode.SDK)
            emitter._session = mock_session
            emitter.test_connection()
            assert emitter._openapi_ingestion is True

    def test_constructor_param_true_overrides_all(self):
        """Test that explicit constructor parameter True overrides all other settings"""
        with (
            patch.dict(
                os.environ,
                {"DATAHUB_REST_EMITTER_DEFAULT_ENDPOINT": "RESTLI"},
                clear=True,
            ),
            patch(
                "datahub.emitter.rest_emitter.DEFAULT_REST_EMITTER_ENDPOINT",
                RestSinkEndpoint.RESTLI,
            ),
        ):
            # Even with env var and non-SDK mode, constructor param should win
            emitter = DataHubRestEmitter(
                MOCK_GMS_ENDPOINT,
                client_mode=ClientMode.INGESTION,
                openapi_ingestion=True,
            )
            assert emitter._openapi_ingestion is True

    def test_constructor_param_false_overrides_all(self):
        """Test that explicit constructor parameter False overrides all other settings"""
        with (
            patch.dict(
                os.environ,
                {"DATAHUB_REST_EMITTER_DEFAULT_ENDPOINT": "OPENAPI"},
                clear=True,
            ),
            patch(
                "datahub.emitter.rest_emitter.DEFAULT_REST_EMITTER_ENDPOINT",
                RestSinkEndpoint.OPENAPI,
            ),
        ):
            # Even with env var and SDK mode, constructor param should win
            emitter = DataHubRestEmitter(
                MOCK_GMS_ENDPOINT, client_mode=ClientMode.SDK, openapi_ingestion=False
            )
            assert emitter._openapi_ingestion is False

    def test_debug_logging(self, mock_session, mock_response):
        """Test that debug logging is called with correct protocol information"""
        mock_session.get.return_value = mock_response

        with patch("datahub.emitter.rest_emitter.logger") as mock_logger:
            # Test OpenAPI logging
            emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=True)
            emitter._session = mock_session
            emitter.test_connection()
            mock_logger.debug.assert_any_call("Using OpenAPI for ingestion.")

            # Test RestLi logging
            mock_logger.reset_mock()
            emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=False)
            emitter._session = mock_session
            emitter.test_connection()
            mock_logger.debug.assert_any_call("Using Restli for ingestion.")


class TestOpenApiIntegration:
    def test_sdk_mode_uses_openapi_by_default(self, mock_session, mock_response):
        """Test that SDK mode uses OpenAPI by default for emit_mcp"""
        mock_session.get.return_value = mock_response

        with patch.dict("os.environ", {}, clear=True):
            emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, client_mode=ClientMode.SDK)
            emitter._session = mock_session
            emitter.test_connection()

            # Verify _openapi_ingestion was set correctly
            assert emitter._openapi_ingestion is True

            # Create test MCP
            test_mcp = MetadataChangeProposalWrapper(
                entityUrn="urn:li:dataset:(test,sdk,PROD)",
                aspect=Status(removed=False),
            )

            # Mock _emit_generic to inspect what URL is used
            with patch.object(emitter, "_emit_generic") as mock_emit:
                emitter.emit_mcp(test_mcp)

                # Check that OpenAPI URL format was used
                mock_emit.assert_called_once()
                url = mock_emit.call_args[0][0]
                assert "openapi" in url
                assert url.startswith(f"{MOCK_GMS_ENDPOINT}/openapi")

    def test_ingestion_mode_uses_restli_by_default(self, mock_session, mock_response):
        """Test that INGESTION mode uses RestLi by default for emit_mcp"""
        mock_session.get.return_value = mock_response

        with patch.dict("os.environ", {}, clear=True):
            emitter = DataHubRestEmitter(
                MOCK_GMS_ENDPOINT, client_mode=ClientMode.INGESTION
            )
            emitter._session = mock_session
            emitter.test_connection()

            # Verify _openapi_ingestion was set correctly
            assert emitter._openapi_ingestion is False

            # Create test MCP
            test_mcp = MetadataChangeProposalWrapper(
                entityUrn="urn:li:dataset:(test,ingestion,PROD)",
                aspect=Status(removed=False),
            )

            # Mock _emit_generic to inspect what URL is used
            with patch.object(emitter, "_emit_generic") as mock_emit:
                emitter.emit_mcp(test_mcp)

                # Check that RestLi URL format was used (not OpenAPI)
                mock_emit.assert_called_once()
                url = mock_emit.call_args[0][0]
                assert "openapi" not in url
                assert "aspects?action=ingestProposal" in url

    def test_explicit_openapi_parameter_uses_openapi_api(self):
        """Test that explicit openapi_ingestion=True uses OpenAPI regardless of mode"""
        emitter = DataHubRestEmitter(
            MOCK_GMS_ENDPOINT,
            client_mode=ClientMode.INGESTION,  # Would normally use RestLi
            openapi_ingestion=True,  # Override to use OpenAPI
        )

        # Verify _openapi_ingestion was set correctly
        assert emitter._openapi_ingestion is True

        # Create test MCP
        test_mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(test,explicit,PROD)",
            aspect=DatasetProfile(rowCount=100, columnCount=10, timestampMillis=0),
        )

        # Mock _emit_generic to inspect what URL is used
        with patch.object(emitter, "_emit_generic") as mock_emit:
            emitter.emit_mcp(test_mcp)

            # Check that OpenAPI URL format was used
            mock_emit.assert_called_once()
            url = mock_emit.call_args[0][0]
            assert "openapi" in url

            # Verify the payload is formatted for OpenAPI
            payload = mock_emit.call_args[1]["payload"]
            assert isinstance(payload, list) or (
                isinstance(payload, str) and payload.startswith("[")
            )

    def test_openapi_batch_endpoint_selection(self):
        """Test that OpenAPI batch operations use correct endpoints based on entity type"""
        emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=True)

        # Create MCPs with different entity types
        dataset_mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dataset:(test,batch1,PROD)",
            entityType="dataset",
            aspect=Status(removed=False),
        )

        dashboard_mcp = MetadataChangeProposalWrapper(
            entityUrn="urn:li:dashboard:(test,batch2)",
            entityType="dashboard",
            aspect=Status(removed=False),
        )

        # Mock _emit_generic to inspect URLs used
        with patch.object(emitter, "_emit_generic") as mock_emit:
            # Configure mock to return appropriate responses
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {}
            mock_response.json.return_value = []
            mock_emit.return_value = mock_response

            # Emit batch of different entity types
            emitter.emit_mcps([dataset_mcp, dashboard_mcp])

            # Verify we made two calls to different endpoints
            assert mock_emit.call_count == 2

            # Get the URLs from the calls
            calls = mock_emit.call_args_list
            urls = [call[0][0] for call in calls]

            # Verify we called both entity endpoints
            assert any("entity/dataset" in url for url in urls)
            assert any("entity/dashboard" in url for url in urls)


class TestRequestsSessionConfig:
    def test_get_client_mode_from_session(self):
        """Test extracting ClientMode from session headers with various inputs."""
        # Case 1: Session with valid ClientMode in headers (string)
        session = Session()
        session.headers.update({"X-DataHub-Client-Mode": "SDK"})
        mode = RequestsSessionConfig.get_client_mode_from_session(session)
        assert mode == ClientMode.SDK

        # Case 2: Session with valid ClientMode in headers (bytes)
        session = Session()
        session.headers.update({"X-DataHub-Client-Mode": b"INGESTION"})
        mode = RequestsSessionConfig.get_client_mode_from_session(session)
        assert mode == ClientMode.INGESTION

        # Case 3: Session with no ClientMode header
        session = Session()
        mode = RequestsSessionConfig.get_client_mode_from_session(session)
        assert mode is None

        # Case 4: Session with invalid ClientMode value
        session = Session()
        session.headers.update({"X-DataHub-Client-Mode": "INVALID_MODE"})
        mode = RequestsSessionConfig.get_client_mode_from_session(session)
        assert mode is None

        # Case 5: Session with empty ClientMode value
        session = Session()
        session.headers.update({"X-DataHub-Client-Mode": ""})
        mode = RequestsSessionConfig.get_client_mode_from_session(session)
        assert mode is None

        # Case 6: Test with exception during processing
        mock_session = Mock()
        mock_session.headers = {
            "X-DataHub-Client-Mode": object()
        }  # Will cause error when decoded
        mode = RequestsSessionConfig.get_client_mode_from_session(mock_session)
        assert mode is None

        # Case 7: Different capitalization should not match
        session = Session()
        session.headers.update({"X-DataHub-Client-Mode": "sdk"})  # lowercase
        mode = RequestsSessionConfig.get_client_mode_from_session(session)
        assert mode is None

        # Case 8: Test with all available ClientMode values
        for client_mode in ClientMode:
            session = Session()
            session.headers.update({"X-DataHub-Client-Mode": client_mode.name})
            mode = RequestsSessionConfig.get_client_mode_from_session(session)
            assert mode == client_mode
