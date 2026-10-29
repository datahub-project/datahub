import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
)

from datahub.ingestion.source.confluent_schema_registry import ConfluentSchemaRegistry
from datahub.ingestion.source.kafka.kafka_config import (
    KafkaSourceConfig,
    SchemaResolutionFallback,
)
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.schema_resolution import KafkaSchemaResolver


class TestKafkaSchemaResolver:
    @pytest.fixture
    def mock_source_config(self):
        config = MagicMock()
        config.schema_resolution = SchemaResolutionFallback(enabled=False)
        config.topic_subject_map = {
            "test-topic-value": "com.example.TestRecord-value",
            "mapped-topic-key": "com.example.KeyRecord-key",
        }
        config.connection = MagicMock()
        config.connection.bootstrap = "localhost:9092"
        config.connection.consumer_config = {}
        return config

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

    def test_topic_subject_map_success(
        self, schema_resolver, mock_schema_registry_client
    ):
        mock_registered_schema = RegisteredSchema(
            schema_id=2,
            guid=None,
            schema=Schema(
                schema_str='{"type": "record", "name": "TestRecord"}',
                schema_type="AVRO",
            ),
            subject="com.example.TestRecord-value",
            version=1,
        )
        mock_schema_registry_client.get_latest_version.return_value = (
            mock_registered_schema
        )

        result = schema_resolver._try_topic_subject_map(
            "test-topic", is_key_schema=False
        )

        assert result.schema is not None
        assert result.resolution_method == "topic_subject_map"
        assert result.subject_name == "com.example.TestRecord-value"
        mock_schema_registry_client.get_latest_version.assert_called_once_with(
            "com.example.TestRecord-value"
        )

    def test_topic_subject_map_not_configured(
        self, schema_resolver, mock_schema_registry_client
    ):
        result = schema_resolver._try_topic_subject_map(
            "unmapped-topic", is_key_schema=False
        )

        assert result.schema is None
        assert result.resolution_method == "subject_map_failed"
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

    @patch("datahub.ingestion.source.kafka.schema_resolution.KafkaSchemaInference")
    def test_record_name_strategies_success(
        self, mock_inference_class, schema_resolver, mock_schema_registry_client
    ):
        mock_inference = MagicMock()
        mock_inference_class.return_value = mock_inference
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

    @patch("datahub.ingestion.source.kafka.schema_resolution.KafkaSchemaInference")
    def test_topic_record_name_strategy_success(
        self, mock_inference_class, schema_resolver, mock_schema_registry_client
    ):
        mock_inference = MagicMock()
        mock_inference_class.return_value = mock_inference
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

    @patch("datahub.ingestion.source.kafka.schema_resolution.KafkaSchemaInference")
    def test_resolve_schemas_batch(
        self, mock_inference_class, schema_resolver, mock_schema_registry_client
    ):
        mock_inference = MagicMock()
        mock_inference_class.return_value = mock_inference
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
            "unknown-topic": [MagicMock()]
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

    def test_key_and_value_inference_produce_distinct_fields(self) -> None:
        config = KafkaSourceConfig(
            schema_resolution=SchemaResolutionFallback(
                enabled=True,
                max_messages_per_topic=1,
                offset_reset_strategy="earliest",
            )
        )
        message = MagicMock()
        message.error.return_value = None
        message.key.return_value = b'{"id": "order-1", "nested": {"id": "nested-1"}}'
        message.value.return_value = b'{"id": 42}'
        consumer = MagicMock()
        consumer.poll.return_value = message
        registry_client = MagicMock()
        registry_client.get_subjects.return_value = []
        resolver = KafkaSchemaResolver(
            config, registry_client, known_subjects=[], max_workers=1
        )

        with patch(
            "datahub.ingestion.source.kafka.kafka_schema_inference.Consumer",
            return_value=consumer,
        ):
            value = resolver.resolve_schemas_batch(["orders"], is_key_schema=False)[
                "orders"
            ]
            key = resolver.resolve_schemas_batch(["orders"], is_key_schema=True)[
                "orders"
            ]

        with patch(
            "datahub.ingestion.source.confluent_schema_registry.get_kafka_schema_registry_client",
            return_value=registry_client,
        ):
            registry = ConfluentSchemaRegistry.create(config, KafkaSourceReport())
        metadata = registry.build_schema_metadata_with_key(
            topic="orders",
            platform_urn="urn:li:dataPlatform:kafka",
            value_schema=value.schema,
            value_fields=value.fields,
            key_schema=key.schema,
            key_fields=key.fields,
        )

        assert metadata is not None
        assert [field.fieldPath for field in metadata.fields] == [
            "[version=2.0].[key=True].[type=string].id",
            "[version=2.0].[key=True].[type=record].nested.[type=string].id",
            "id",
        ]
        assert [field.isPartOfKey for field in metadata.fields] == [True, True, False]

        assert value.is_resolved
        assert value.resolution_method == "schema_inference"
        assert [field.fieldPath for field in value.fields] == ["id"]

        assert key.is_resolved
        assert key.resolution_method == "schema_inference"
        assert len(key.fields) == 2

    @pytest.mark.parametrize("subject", ["orders-key", "com.example.OrderKey"])
    def test_registered_key_schema_with_inference_enabled(self, subject: str) -> None:
        config = KafkaSourceConfig(
            schema_resolution=SchemaResolutionFallback(enabled=True),
            topic_subject_map={"orders-key": subject},
        )
        registered_schema = RegisteredSchema(
            schema_id=1,
            guid=None,
            schema=Schema(
                schema_str=json.dumps(
                    {
                        "type": "record",
                        "name": "OrderKey",
                        "fields": [{"name": "order_id", "type": "string"}],
                    }
                ),
                schema_type="AVRO",
            ),
            subject=subject,
            version=1,
        )
        registry_client = MagicMock()
        registry_client.get_latest_version.return_value = registered_schema
        resolver = KafkaSchemaResolver(
            config, registry_client, known_subjects=[subject], max_workers=1
        )

        result = resolver.resolve_schemas_batch(["orders"], is_key_schema=True)[
            "orders"
        ]

        assert result.is_resolved
        assert result.schema == registered_schema.schema
        assert result.subject_name == subject
        registry_client.get_latest_version.assert_called_once_with(subject)

    @pytest.mark.parametrize("is_key_schema", [False, True])
    @pytest.mark.parametrize("subject_prefix", ["", "orders-"])
    def test_record_name_lookup_uses_raw_key_or_value(
        self, is_key_schema: bool, subject_prefix: str
    ) -> None:
        config = KafkaSourceConfig(
            schema_resolution=SchemaResolutionFallback(
                enabled=True, max_messages_per_topic=1, offset_reset_strategy="earliest"
            )
        )
        key_schema = Schema(
            schema_str='{"type":"record","name":"OrderKey","fields":[]}',
            schema_type="AVRO",
        )
        value_schema = Schema(
            schema_str='{"type":"record","name":"OrderValue","fields":[]}',
            schema_type="AVRO",
        )
        schema_id = 1 if is_key_schema else 2
        schema = key_schema if is_key_schema else value_schema
        subject = subject_prefix + (
            "OrderKey-key" if is_key_schema else "OrderValue-value"
        )
        registry_client = MagicMock()
        registry_client.get_schema.side_effect = {
            1: key_schema,
            2: value_schema,
        }.__getitem__
        registry_client.get_latest_version.return_value = RegisteredSchema(
            schema_id=schema_id, guid=None, schema=schema, subject=subject, version=1
        )
        message = MagicMock()
        message.error.return_value = None
        message.key.return_value = b"\x00\x00\x00\x00\x01key-payload"
        message.value.return_value = b"\x00\x00\x00\x00\x02value-payload"
        consumer = MagicMock()
        consumer.poll.return_value = message
        resolver = KafkaSchemaResolver(
            config, registry_client, known_subjects=[subject], max_workers=1
        )

        with patch(
            "datahub.ingestion.source.kafka.kafka_schema_inference.Consumer",
            return_value=consumer,
        ):
            result = resolver.resolve_schemas_batch(
                ["orders"], is_key_schema=is_key_schema
            )["orders"]

        assert result.is_resolved
        assert result.schema == schema
        assert result.subject_name == subject
        assert result.resolution_method == (
            "topic_record_name_strategy" if subject_prefix else "record_name_strategy"
        )
        registry_client.get_schema.assert_called_once_with(schema_id)
        registry_client.get_latest_version.assert_called_once_with(subject)

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
