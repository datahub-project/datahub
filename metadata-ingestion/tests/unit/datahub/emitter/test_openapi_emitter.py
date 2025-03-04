import json
from datetime import timedelta
from unittest.mock import ANY, Mock, patch

import pytest
from requests import Response

from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.openapi_emitter import (
    DataHubOpenApiEmitter,
)
from datahub.emitter.rest_emitter import (
    BATCH_INGEST_MAX_PAYLOAD_LENGTH,
    INGEST_MAX_PAYLOAD_BYTES,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    Status,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetProfile,
    DatasetProperties,
)


def test_openapi_emitter_emit():
    openapi_emitter = DataHubOpenApiEmitter("http://localhost:8080")
    item = MetadataChangeProposalWrapper(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:mysql,User.UserAccount,PROD)",
        aspect=DatasetProfile(
            rowCount=2000,
            columnCount=15,
            timestampMillis=1626995099686,
        ),
    )

    with patch.object(openapi_emitter, "_emit_generic_payload") as mock_method:
        openapi_emitter.emit_mcp(item)

        mock_method.assert_called_once_with(
            "http://localhost:8080/openapi/v3/entity/dataset?async=false",
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
                    },
                }
            ],
        )


def test_openapi_emitter_emit_mcps():
    with patch(
        "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
    ) as mock_emit:
        openapi_emitter = DataHubOpenApiEmitter("http://localhost:8080")

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
            == "http://localhost:8080/openapi/v3/entity/dataset?async=true"
        )
        assert isinstance(call_args[1]["payload"], str)  # Should be JSON string


def test_openapi_emitter_emit_mcps_max_bytes():
    with patch(
        "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
    ) as mock_emit:
        openapi_emitter = DataHubOpenApiEmitter("http://localhost:8080")

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


def test_openapi_emitter_emit_mcps_max_items():
    with patch(
        "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
    ) as mock_emit:
        openapi_emitter = DataHubOpenApiEmitter("http://localhost:8080")

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


def test_openapi_emitter_emit_mcps_multiple_entity_types():
    with patch(
        "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
    ) as mock_emit:
        openapi_emitter = DataHubOpenApiEmitter("http://localhost:8080")

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
                assert url.endswith("dataset?async=true")
                assert len(payload) == 2
                assert all("datasetProfile" in item for item in payload)
            else:
                assert url.endswith("dashboard?async=true")
                assert len(payload) == 2
                assert all("status" in item for item in payload)


def test_openapi_emitter_emit_mcp_with_tracing():
    """Test emitting a single MCP with tracing enabled"""
    with patch(
        "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
    ) as mock_emit:
        openapi_emitter = DataHubOpenApiEmitter("http://localhost:8080")

        # Mock the response for the initial emit
        mock_response = Mock(spec=Response)
        mock_response.status_code = 200
        mock_response.headers = {"traceparent": "test-trace-123"}
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
            item, async_flag=True, trace_flag=True, trace_timeout=timedelta(seconds=10)
        )

        # Verify initial emit call
        assert (
            mock_emit.call_args_list[0][0][0]
            == "http://localhost:8080/openapi/v3/entity/dataset?async=true"
        )

        # Verify trace verification calls
        trace_calls = [
            call for call in mock_emit.call_args_list if "trace/write" in call[0][0]
        ]
        assert len(trace_calls) == 2
        assert "test-trace-123" in trace_calls[0][0][0]


def test_openapi_emitter_emit_mcps_with_tracing():
    """Test emitting multiple MCPs with tracing enabled"""
    with patch(
        "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
    ) as mock_emit:
        openapi_emitter = DataHubOpenApiEmitter("http://localhost:8080")

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
            items, async_flag=True, trace_flag=True, trace_timeout=timedelta(seconds=10)
        )

        assert result == 1  # Should return number of unique entity URLs

        # Verify initial emit calls
        emit_calls = [
            call for call in mock_emit.call_args_list if "entity/dataset" in call[0][0]
        ]
        assert len(emit_calls) == 1
        assert (
            emit_calls[0][0][0]
            == "http://localhost:8080/openapi/v3/entity/dataset?async=true"
        )

        # Verify trace verification calls
        trace_calls = [
            call for call in mock_emit.call_args_list if "trace/write" in call[0][0]
        ]
        assert len(trace_calls) == 2


def test_openapi_emitter_trace_timeout():
    """Test that tracing properly handles timeouts"""
    with patch(
        "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic_payload"
    ) as mock_emit:
        openapi_emitter = DataHubOpenApiEmitter("http://localhost:8080")

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
            return mock_trace_response if "trace/write" in args[0] else mock_response

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
        with pytest.raises(OperationalError) as exc_info:
            openapi_emitter.emit_mcp(
                item,
                async_flag=True,
                trace_flag=True,
                trace_timeout=timedelta(milliseconds=1),
            )

        assert "Timeout waiting for async write completion" in str(exc_info.value)


def test_openapi_emitter_missing_trace_header():
    """Test behavior when trace header is missing"""
    with patch(
        "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic_payload"
    ) as mock_emit:
        openapi_emitter = DataHubOpenApiEmitter("http://localhost:8080")

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

        # Should not raise exception but log warning
        openapi_emitter.emit_mcp(
            item, async_flag=True, trace_flag=True, trace_timeout=timedelta(seconds=10)
        )


def test_openapi_emitter_invalid_status_code():
    """Test behavior when response has non-200 status code"""
    with patch(
        "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic_payload"
    ) as mock_emit:
        openapi_emitter = DataHubOpenApiEmitter("http://localhost:8080")

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
            item, async_flag=True, trace_flag=True, trace_timeout=timedelta(seconds=10)
        )


def test_openapi_emitter_trace_failure():
    """Test handling of trace verification failures"""
    with patch(
        "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic_payload"
    ) as mock_emit:
        openapi_emitter = DataHubOpenApiEmitter("http://localhost:8080")

        test_urn = "urn:li:dataset:(urn:li:dataPlatform:mysql,TraceFailure,PROD)"

        # Create initial emit response
        emit_response = Mock(spec=Response)
        emit_response.status_code = 200
        emit_response.headers = {"traceparent": "test-trace-123"}
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

        with pytest.raises(OperationalError) as exc_info:
            openapi_emitter.emit_mcp(
                item,
                async_flag=True,
                trace_flag=True,
                trace_timeout=timedelta(seconds=10),
            )

        assert "Unable to validate async write to DataHub GMS" in str(exc_info.value)

        # Verify the error details are included
        assert "Failed to write to storage" in str(exc_info.value)

        # Verify trace was actually called
        trace_calls = [
            call for call in mock_emit.call_args_list if "trace/write" in call[0][0]
        ]
        assert len(trace_calls) > 0
        assert "test-trace-123" in trace_calls[0][0][0]
