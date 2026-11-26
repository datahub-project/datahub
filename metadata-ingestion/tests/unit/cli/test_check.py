import json
from unittest.mock import MagicMock, patch

import pytest

from tests.test_helpers.click_helpers import run_datahub_cmd

# /openapi/operations/kafka/*/consumer/offsets has two response formats in the wild.
# Object format: {"consumerGroupId": ..., "topics": {topic: ...}} per consumer type.
_KAFKA_CONSUMER_OFFSETS_RESPONSE = {
    "mcp": {
        "consumerGroupId": "generic-mce-consumer-job-client",
        "topics": {
            "MetadataChangeProposal_v1": {
                "partitions": {
                    "0": {"offset": 3, "lag": 1},
                    "1": {"offset": 2, "lag": 0},
                },
                "metrics": {
                    "maxLag": 1,
                    "medianLag": 0,
                    "totalLag": 1,
                    "avgLag": 0.5,
                },
            }
        },
    },
    "mcl": {
        "consumerGroupId": "generic-mae-consumer-job-client",
        "topics": {
            "MetadataChangeLog_Versioned_v1": {
                "partitions": {
                    "0": {"offset": 24532, "lag": 0},
                },
                "metrics": {
                    "maxLag": 0,
                    "medianLag": 0,
                    "totalLag": 0,
                    "avgLag": 0,
                },
            }
        },
    },
}

# Map format: {consumer group: {topic: ...}} per consumer type
# (KafkaOffsetResponse extends LinkedHashMap in KafkaController).
_KAFKA_CONSUMER_OFFSETS_LEGACY_RESPONSE = {
    consumer_type: {payload["consumerGroupId"]: payload["topics"]}
    for consumer_type, payload in _KAFKA_CONSUMER_OFFSETS_RESPONSE.items()
}

# Envelope format from /openapi/operations/messaging/*/consumer/lag:
# {"transport": ..., "consumerGroups": {group: {topic: ...}}} per consumer type.
_MESSAGING_CONSUMER_LAG_RESPONSE = {
    consumer_type: {"transport": "kafka", "consumerGroups": groups}
    for consumer_type, groups in _KAFKA_CONSUMER_OFFSETS_LEGACY_RESPONSE.items()
}

_EXPECTED_OUTPUT_VALUES = [
    "MetadataChangeProposal_v1",
    "generic-mce-consumer-job-client",
    "MetadataChangeLog_Versioned_v1",
    "generic-mae-consumer-job-client",
    "24532",
]


def test_cli_help():
    result = run_datahub_cmd(["--help"])
    assert result.output


def test_cli_version():
    result = run_datahub_cmd(["--debug", "version"])
    assert result.output


def test_check_local_docker():
    # This just verifies that it runs without error.
    # We don't actually know what environment this will be run in, so
    # we can't depend on the output. Eventually, we should mock the docker SDK.
    result = run_datahub_cmd(["check", "local-docker"], check_result=False)
    assert result.output


@pytest.mark.parametrize(
    "response",
    [
        pytest.param(_MESSAGING_CONSUMER_LAG_RESPONSE, id="messaging-envelope"),
        pytest.param(_KAFKA_CONSUMER_OFFSETS_RESPONSE, id="kafka-object"),
        pytest.param(_KAFKA_CONSUMER_OFFSETS_LEGACY_RESPONSE, id="kafka-map"),
    ],
)
@patch("datahub.cli.check_cli.get_default_graph")
def test_get_kafka_consumer_offsets(mock_get_default_graph, response):
    mock_graph = MagicMock()
    mock_graph.get_kafka_consumer_offsets.return_value = response
    mock_get_default_graph.return_value = mock_graph

    result = run_datahub_cmd(["check", "get-kafka-consumer-offsets"])

    for value in _EXPECTED_OUTPUT_VALUES:
        assert value in result.output


@patch("datahub.cli.check_cli.get_default_graph")
def test_get_kafka_consumer_offsets_json(mock_get_default_graph):
    mock_graph = MagicMock()
    mock_graph.get_kafka_consumer_offsets.return_value = (
        _MESSAGING_CONSUMER_LAG_RESPONSE
    )
    mock_get_default_graph.return_value = mock_graph

    result = run_datahub_cmd(["check", "get-kafka-consumer-offsets", "-o", "json"])

    rows = json.loads(result.output)
    assert {
        "consumerType": "mcp",
        "consumerGroup": "generic-mce-consumer-job-client",
        "topic": "MetadataChangeProposal_v1",
        "partition": "0",
        "offset": 3,
        "lag": 1,
        "avgLag": 0.5,
        "maxLag": 1,
        "totalLag": 1,
    } in rows
    assert len(rows) == 3


@patch("datahub.cli.check_cli.get_default_graph")
def test_get_kafka_consumer_offsets_malformed_payloads(mock_get_default_graph):
    response = {
        "mcp": "error string payload",
        "mcl": {"errors": ["boom"]},
        "mcl-timeseries": {
            "my-group": {
                "bad-topic": "not a dict",
                "my-topic": {
                    "partitions": {"0": {"offset": 7, "lag": 2}},
                    "metrics": None,
                },
            }
        },
    }
    mock_graph = MagicMock()
    mock_graph.get_kafka_consumer_offsets.return_value = response
    mock_get_default_graph.return_value = mock_graph

    result = run_datahub_cmd(["check", "get-kafka-consumer-offsets", "-o", "json"])

    rows = json.loads(result.output)
    assert rows == [
        {
            "consumerType": "mcl-timeseries",
            "consumerGroup": "my-group",
            "topic": "my-topic",
            "partition": "0",
            "offset": 7,
            "lag": 2,
            "avgLag": None,
            "maxLag": None,
            "totalLag": None,
        }
    ]


@patch("datahub.cli.check_cli.get_default_graph")
def test_get_kafka_consumer_offsets_empty(mock_get_default_graph):
    mock_graph = MagicMock()
    mock_graph.get_kafka_consumer_offsets.return_value = {}
    mock_get_default_graph.return_value = mock_graph

    result = run_datahub_cmd(["check", "get-kafka-consumer-offsets"])

    assert "No Kafka consumer offset data found." in result.output
