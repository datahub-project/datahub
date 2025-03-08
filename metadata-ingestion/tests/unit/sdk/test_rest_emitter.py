import json
from unittest.mock import ANY, patch

from datahub.emitter import rest_emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import (
    BATCH_INGEST_MAX_PAYLOAD_LENGTH,
    INGEST_MAX_PAYLOAD_BYTES,
    DataHubRestEmitter,
    DatahubRestEmitter,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    Status,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetProfile,
    DatasetProperties,
)

MOCK_GMS_ENDPOINT = "http://fakegmshost:8080"


def test_datahub_rest_emitter_construction() -> None:
    emitter = DatahubRestEmitter(MOCK_GMS_ENDPOINT)
    assert emitter._session_config.timeout == rest_emitter._DEFAULT_TIMEOUT_SEC
    assert (
        emitter._session_config.retry_status_codes
        == rest_emitter._DEFAULT_RETRY_STATUS_CODES
    )
    assert (
        emitter._session_config.retry_max_times == rest_emitter._DEFAULT_RETRY_MAX_TIMES
    )


def test_datahub_rest_emitter_timeout_construction() -> None:
    emitter = DatahubRestEmitter(
        MOCK_GMS_ENDPOINT, connect_timeout_sec=2, read_timeout_sec=4
    )
    assert emitter._session_config.timeout == (2, 4)


def test_datahub_rest_emitter_general_timeout_construction() -> None:
    emitter = DatahubRestEmitter(MOCK_GMS_ENDPOINT, timeout_sec=2, read_timeout_sec=4)
    assert emitter._session_config.timeout == (2, 4)


def test_datahub_rest_emitter_retry_construction() -> None:
    emitter = DatahubRestEmitter(
        MOCK_GMS_ENDPOINT,
        retry_status_codes=[418],
        retry_max_times=42,
    )
    assert emitter._session_config.retry_status_codes == [418]
    assert emitter._session_config.retry_max_times == 42


def test_datahub_rest_emitter_extra_params() -> None:
    emitter = DatahubRestEmitter(
        MOCK_GMS_ENDPOINT, extra_headers={"key1": "value1", "key2": "value2"}
    )
    assert emitter._session.headers.get("key1") == "value1"
    assert emitter._session.headers.get("key2") == "value2"


def test_openapi_emitter_emit():
    openapi_emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=True)
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
                    },
                }
            ],
        )


def test_openapi_emitter_emit_mcps():
    with patch(
        "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
    ) as mock_emit:
        openapi_emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=True)

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
            == f"{MOCK_GMS_ENDPOINT}/openapi/v3/entity/dataset?async=true"
        )
        assert isinstance(call_args[1]["payload"], str)  # Should be JSON string


def test_openapi_emitter_emit_mcps_max_bytes():
    with patch(
        "datahub.emitter.rest_emitter.DataHubRestEmitter._emit_generic"
    ) as mock_emit:
        openapi_emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=True)

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
        openapi_emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=True)

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
        openapi_emitter = DataHubRestEmitter(MOCK_GMS_ENDPOINT, openapi_ingestion=True)

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
