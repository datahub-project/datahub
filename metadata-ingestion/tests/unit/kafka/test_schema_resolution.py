import json
from typing import Dict, List, Optional
from unittest.mock import MagicMock, Mock, create_autospec

import pytest
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
)

from datahub.ingestion.source.kafka.kafka_config import KafkaSourceConfig
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.kafka_schema_inference import (
    KafkaSchemaInference,
)
from datahub.ingestion.source.kafka.schema_resolution import KafkaSchemaResolver
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from tests.unit.kafka.conftest import BOOTSTRAP


def _source_config(
    topic_subject_map: Optional[Dict[str, str]] = None,
) -> KafkaSourceConfig:
    """A real config with inference off, so only registry strategies run."""
    return KafkaSourceConfig.model_validate(
        {
            "connection": {"bootstrap": BOOTSTRAP},
            "schema_resolution": {"enabled": False},
            "topic_subject_map": topic_subject_map or {},
        }
    )


def _inference() -> MagicMock:
    return create_autospec(KafkaSchemaInference, instance=True)


class TestKafkaSchemaResolver:
    @pytest.fixture
    def mock_source_config(self):
        return _source_config(
            {
                "test-topic-value": "com.example.TestRecord-value",
                "mapped-topic-key": "com.example.KeyRecord-key",
            }
        )

    @pytest.fixture
    def mock_schema_registry_client(self):
        return MagicMock()

    @pytest.fixture
    def known_subjects(self):
        return [
            "test-topic-value",
            "test-topic-key",
            "user-topic-value",
            "com.example.TestRecord-value",
            "com.example.User-value",
            "order-topic-com.example.Order-value",
        ]

    @pytest.fixture
    def schema_resolver(
        self, mock_source_config, mock_schema_registry_client, known_subjects
    ):
        return KafkaSchemaResolver(
            source_config=mock_source_config,
            schema_registry_client=mock_schema_registry_client,
            known_subjects=known_subjects,
            max_workers=2,
        )

    def test_topic_name_strategy_success(
        self, schema_resolver, mock_schema_registry_client
    ):
        mock_registered_schema = RegisteredSchema(
            schema_id=1,
            guid=None,
            schema=Schema(
                schema_str='{"type": "record", "name": "Test"}', schema_type="AVRO"
            ),
            subject="test-topic-value",
            version=1,
        )
        mock_schema_registry_client.get_latest_version.return_value = (
            mock_registered_schema
        )

        result = schema_resolver._try_topic_name_strategy(
            "test-topic", is_key_schema=False
        )

        assert result.schema is not None
        assert result.resolution_method == "topic_name_strategy"
        assert result.subject_name == "test-topic-value"
        mock_schema_registry_client.get_latest_version.assert_called_once_with(
            "test-topic-value"
        )

    def test_topic_name_strategy_not_found(
        self, schema_resolver, mock_schema_registry_client
    ):
        result = schema_resolver._try_topic_name_strategy(
            "unknown-topic", is_key_schema=False
        )

        assert result.schema is None
        assert result.resolution_method == "topic_name_failed"
        mock_schema_registry_client.get_latest_version.assert_not_called()

    def test_extract_record_name_from_schema_avro(self, schema_resolver):
        schema_str = json.dumps(
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        result = schema_resolver._extract_record_name_from_schema(schema_str)

        assert result.record_name == "User"
        assert result.namespace == "com.example"
        assert result.full_name == "com.example.User"

    def test_extract_record_name_from_schema_no_namespace(self, schema_resolver):
        schema_str = json.dumps(
            {
                "type": "record",
                "name": "SimpleRecord",
                "fields": [{"name": "value", "type": "string"}],
            }
        )
        result = schema_resolver._extract_record_name_from_schema(schema_str)

        assert result.record_name == "SimpleRecord"
        assert result.namespace is None
        assert result.full_name == "SimpleRecord"

    def test_extract_record_name_from_schema_invalid_json(self, schema_resolver):
        result = schema_resolver._extract_record_name_from_schema("invalid json")
        assert result.record_name is None

    def test_record_name_strategies_success(
        self, schema_resolver, mock_schema_registry_client
    ):
        mock_inference = _inference()
        schema_resolver.schema_inference = mock_inference

        mock_inference.sample_topic_messages.return_value = [
            b"\x00\x00\x00\x00\x01" + b"dummy_data"
        ]

        mock_schema = Schema(
            schema_str='{"type": "record", "name": "User", "namespace": "com.example"}',
            schema_type="AVRO",
        )
        mock_schema_registry_client.get_schema.return_value = mock_schema
        mock_schema_registry_client.get_latest_version.return_value = RegisteredSchema(
            schema_id=3,
            guid=None,
            schema=mock_schema,
            subject="com.example.User-value",
            version=1,
        )

        result = schema_resolver._try_record_name_strategies(
            "user-topic", is_key_schema=False
        )

        assert result.schema is not None
        assert result.resolution_method == "record_name_strategy"
        assert result.subject_name == "com.example.User-value"
        assert result.record_name == "com.example.User"

    def test_topic_record_name_strategy_success(
        self, schema_resolver, mock_schema_registry_client
    ):
        mock_inference = _inference()
        schema_resolver.schema_inference = mock_inference

        mock_inference.sample_topic_messages.return_value = [
            b"\x00\x00\x00\x00\x01" + b"dummy_data"
        ]

        mock_schema = Schema(
            schema_str='{"type": "record", "name": "Order", "namespace": "com.example"}',
            schema_type="AVRO",
        )
        mock_schema_registry_client.get_schema.return_value = mock_schema
        mock_schema_registry_client.get_latest_version.return_value = RegisteredSchema(
            schema_id=4,
            guid=None,
            schema=mock_schema,
            subject="order-topic-com.example.Order-value",
            version=1,
        )

        # The plain RecordNameStrategy subject (`com.example.Order-value`) is not a
        # known subject, so resolution must fall through to TopicRecordNameStrategy
        # (`order-topic-com.example.Order-value`), which is known.
        result = schema_resolver._try_record_name_strategies(
            "order-topic", is_key_schema=False
        )

        assert result.schema is not None
        assert result.resolution_method == "topic_record_name_strategy"
        assert result.subject_name == "order-topic-com.example.Order-value"

    def test_record_name_strategies_no_inference(self, schema_resolver):
        schema_resolver.schema_inference = None

        result = schema_resolver._try_record_name_strategies(
            "test-topic", is_key_schema=False
        )

        assert result.schema is None
        assert result.resolution_method == "no_inference_available"

    def test_resolve_schemas_batch(self, schema_resolver, mock_schema_registry_client):
        mock_inference = _inference()
        schema_resolver.schema_inference = mock_inference

        mock_schema_registry_client.get_latest_version.return_value = RegisteredSchema(
            schema_id=1,
            guid=None,
            schema=Schema(
                schema_str='{"type": "record", "name": "Test"}', schema_type="AVRO"
            ),
            subject="test-topic-value",
            version=1,
        )
        mock_inference.infer_schemas_batch.return_value = {
            "unknown-topic": [Mock(spec=SchemaField)]
        }

        results = schema_resolver.resolve_schemas_batch(
            ["test-topic", "unknown-topic"], is_key_schema=False
        )

        assert len(results) == 2
        assert results["test-topic"].resolution_method == "topic_name_strategy"
        assert results["unknown-topic"].resolution_method == "schema_inference"

    def test_resolve_schemas_batch_empty_topics(self, schema_resolver):
        results = schema_resolver.resolve_schemas_batch([], is_key_schema=False)
        assert results == {}

    def test_extract_record_name_from_message_schema_registry_format(
        self, schema_resolver, mock_schema_registry_client
    ):
        mock_schema_registry_client.get_schema.return_value = Schema(
            schema_str='{"type": "record", "name": "Order", "namespace": "com.example"}',
            schema_type="AVRO",
        )

        result = schema_resolver._extract_record_name_from_message(
            b"\x00\x00\x00\x00\x01" + b"dummy_avro_data"
        )

        assert result.record_name == "Order"
        assert result.namespace == "com.example"
        assert result.full_name == "com.example.Order"
        mock_schema_registry_client.get_schema.assert_called_once_with(1)

    def test_extract_record_name_from_message_not_schema_registry(
        self, schema_resolver
    ):
        result = schema_resolver._extract_record_name_from_message(b"plain_json_data")
        assert result.record_name is None

    def test_extract_record_name_from_message_too_short(self, schema_resolver):
        result = schema_resolver._extract_record_name_from_message(b"123")
        assert result.record_name is None


def _resolver(
    client: MagicMock,
    known_subjects: List[str],
    report: Optional[KafkaSourceReport] = None,
    topic_subject_map: Optional[Dict[str, str]] = None,
) -> KafkaSchemaResolver:
    return KafkaSchemaResolver(
        source_config=_source_config(topic_subject_map),
        schema_registry_client=client,
        known_subjects=known_subjects,
        max_workers=1,
        report=report,
    )


def _registered(subject: str) -> RegisteredSchema:
    return RegisteredSchema(
        schema_id=1,
        guid=None,
        schema=Schema('{"type": "record", "name": "R"}', "AVRO"),
        subject=subject,
        version=1,
    )


def test_subject_map_is_used_when_topic_name_subject_is_missing():
    """A topic whose "<topic>-value" subject is not registered resolves through
    topic_subject_map; with no inference configured an unmapped topic is still
    present in the batch, just unresolved, and costs no registry call."""
    client = MagicMock()
    client.get_latest_version.return_value = _registered("com.example.Order-value")
    resolver = _resolver(
        client,
        ["com.example.Order-value"],
        topic_subject_map={"orders-value": "com.example.Order-value"},
    )

    results = resolver.resolve_schemas_batch(["orders", "unmapped"])

    assert results["orders"].is_resolved
    assert results["orders"].resolution_method == "topic_subject_map"
    assert results["orders"].subject_name == "com.example.Order-value"
    assert not results["unmapped"].is_resolved
    assert results["unmapped"].resolution_method == "none"
    client.get_latest_version.assert_called_once_with("com.example.Order-value")


def test_unreachable_registry_is_reported_but_missing_subject_is_not():
    """A missing subject is routine while probing; an unreachable registry is
    counted so the operator can tell the two apart."""
    report = KafkaSourceReport()
    client = MagicMock()
    client.get_latest_version.side_effect = [
        OSError("connection refused"),
        KeyError("gone"),
    ]
    resolver = _resolver(client, ["a-value", "b-value"], report=report)

    results = resolver.resolve_schemas_batch(["a", "b"])

    assert not results["a"].is_resolved
    assert not results["b"].is_resolved
    assert report.schema_registry_connectivity_failures == 1


def test_record_name_strategies_are_skipped_for_key_schemas():
    """Record names are only embedded in values; key schemas never trigger the
    registry lookups that the record-name strategies would make."""
    client = MagicMock()
    resolver = _resolver(client, [])
    resolver.schema_inference = _inference()
    resolver.schema_inference.sample_topic_messages.return_value = [
        None,
        b"\x00\x00\x00\x00\x01payload",
    ]

    result = resolver._try_record_name_strategies("orders", is_key_schema=True)

    assert result.resolution_method == "no_record_names_found"
    client.get_schema.assert_not_called()


def test_sampling_failure_during_record_name_extraction_is_tolerated():
    client = MagicMock()
    resolver = _resolver(client, [])
    resolver.schema_inference = _inference()
    resolver.schema_inference.sample_topic_messages.side_effect = OSError("down")

    result = resolver._try_record_name_strategies("orders", is_key_schema=False)

    assert result.resolution_method == "no_record_names_found"


def test_unreachable_registry_during_schema_id_lookup_is_reported():
    report = KafkaSourceReport()
    client = MagicMock()
    client.get_schema.side_effect = OSError("registry down")
    resolver = _resolver(client, [], report=report)

    result = resolver._extract_record_name_from_message(b"\x00\x00\x00\x00\x07payload")

    assert result.record_name is None
    assert report.schema_registry_connectivity_failures == 1
