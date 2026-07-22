from typing import List
from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.kafka.kafka_config import SchemaResolutionFallback
from datahub.ingestion.source.kafka.kafka_constants import OffsetResetStrategy
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.ingestion.source.kafka.kafka_schema_inference import (
    KafkaSchemaInference,
)
from datahub.ingestion.source.kafka.kafka_utils import flatten_json
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    NumberTypeClass,
    StringTypeClass,
)

_INFERENCE_MODULE = "datahub.ingestion.source.kafka.kafka_schema_inference"


class ScriptedConsumer:
    """Yields a fixed sequence of poll results (then None), and records close()."""

    def __init__(self, messages: List[object]) -> None:
        self._messages = list(messages)
        self.closed = False

    def subscribe(self, topics: List[str]) -> None:
        pass

    def poll(self, timeout: float) -> object:
        return self._messages.pop(0) if self._messages else None

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def fallback_config():
    return SchemaResolutionFallback(
        enabled=True,
        sample_timeout_seconds=1.0,
        offset_reset_strategy="hybrid",
    )


@pytest.fixture
def schema_inference(fallback_config):
    return KafkaSchemaInference(
        bootstrap_servers="localhost:9092",
        consumer_config={"group.id": "test"},
        fallback_config=fallback_config,
        max_workers=2,
    )


class TestKafkaSchemaInference:
    def test_initialization(self, schema_inference, fallback_config):
        assert schema_inference.bootstrap_servers == "localhost:9092"
        assert schema_inference.consumer_config == {"group.id": "test"}
        assert schema_inference.fallback_config == fallback_config

    def test_infer_schemas_batch_empty_topics(self, schema_inference):
        assert schema_inference.infer_schemas_batch([]) == {}

    def test_extract_fields_from_samples(self, schema_inference):
        sample_messages = [
            {"field1": "string_value", "field2": 123, "field3": True},
            {"field1": "another_string", "field2": 456, "field4": 78.9},
            {"field1": None, "field2": 789, "field3": False},
        ]
        result = schema_inference._extract_fields_from_samples(
            "test-topic", sample_messages
        )

        assert isinstance(result, list)
        assert all(isinstance(field, SchemaField) for field in result)
        field_paths = [field.fieldPath for field in result]
        assert "field1" in field_paths
        assert "field2" in field_paths
        assert "field3" in field_paths
        assert "field4" in field_paths

    def test_extract_fields_from_samples_type_detection(self, schema_inference):
        result = schema_inference._extract_fields_from_samples(
            "test-topic",
            [
                {
                    "string_field": "test",
                    "int_field": 123,
                    "bool_field": True,
                    "float_field": 45.6,
                }
            ],
        )
        field_types = {field.fieldPath: field.type.type for field in result}

        assert isinstance(field_types["string_field"], StringTypeClass)
        assert isinstance(field_types["int_field"], NumberTypeClass)
        assert isinstance(field_types["bool_field"], BooleanTypeClass)
        assert isinstance(field_types["float_field"], NumberTypeClass)

    def test_infer_schema_from_messages_success(self, schema_inference):
        with patch.object(schema_inference, "sample_topic_messages") as mock_sample:
            mock_sample.return_value = [
                {"field1": "value1", "field2": 123},
                {"field1": "value2", "field2": 456},
            ]
            result = schema_inference._infer_schema_from_messages("test-topic")

        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(field, SchemaField) for field in result)
        field_paths = [field.fieldPath for field in result]
        assert "field1" in field_paths
        assert "field2" in field_paths

    def test_infer_schema_from_messages_no_samples(self, schema_inference):
        with patch.object(schema_inference, "sample_topic_messages") as mock_sample:
            mock_sample.return_value = []
            result = schema_inference._infer_schema_from_messages("empty-topic")
        assert result == []

    def test_sampled_but_no_fields_is_reported(self, fallback_config):
        report = KafkaSourceReport()
        inference = KafkaSchemaInference(
            bootstrap_servers="localhost:9092",
            consumer_config={"group.id": "test"},
            fallback_config=fallback_config,
            max_workers=1,
            report=report,
        )
        # Non-dict samples yield no fields, so the topic would go schemaless.
        with patch.object(inference, "sample_topic_messages") as mock_sample:
            mock_sample.return_value = ["not-a-dict", 123]
            result = inference._infer_schema_from_messages("opaque-topic")

        assert result == []
        assert report.schema_inference_no_fields == 1

    def test_infer_schemas_batch_sequential(self, schema_inference):
        schema_inference.max_workers = 1

        with patch.object(
            schema_inference, "_infer_schema_from_messages"
        ) as mock_infer:
            mock_infer.side_effect = [
                [Mock(spec=SchemaField)],
                [Mock(spec=SchemaField), Mock(spec=SchemaField)],
            ]
            result = schema_inference.infer_schemas_batch(["topic1", "topic2"])

        assert len(result) == 2
        assert len(result["topic1"]) == 1
        assert len(result["topic2"]) == 2
        assert mock_infer.call_count == 2

    def test_infer_schemas_batch_parallel(self, schema_inference):
        schema_inference.max_workers = 2

        with patch.object(schema_inference, "_infer_schemas_parallel") as mock_parallel:
            mock_parallel.return_value = {
                "topic1": [Mock(spec=SchemaField)],
                "topic2": [Mock(spec=SchemaField), Mock(spec=SchemaField)],
            }
            result = schema_inference.infer_schemas_batch(["topic1", "topic2"])

        mock_parallel.assert_called_once_with(["topic1", "topic2"])
        assert len(result) == 2

    def testsample_topic_messages_hybrid_strategy(self, schema_inference):
        with patch.object(
            schema_inference, "_sample_messages_with_strategy"
        ) as mock_sample:
            mock_sample.side_effect = [[], [{"field": "value"}]]
            result = schema_inference.sample_topic_messages("test-topic")

        assert mock_sample.call_count == 2
        mock_sample.assert_any_call("test-topic", "latest")
        mock_sample.assert_any_call("test-topic", "earliest")
        assert result == [{"field": "value"}]

    def testsample_topic_messages_latest_strategy(self, schema_inference):
        schema_inference.fallback_config.offset_reset_strategy = "latest"

        with patch.object(
            schema_inference, "_sample_messages_with_strategy"
        ) as mock_sample:
            mock_sample.return_value = [{"field": "value"}]
            result = schema_inference.sample_topic_messages("test-topic")

        mock_sample.assert_called_once_with("test-topic", "latest")
        assert result == [{"field": "value"}]

    def testsample_topic_messages_earliest_strategy(self, schema_inference):
        schema_inference.fallback_config.offset_reset_strategy = "earliest"

        with patch.object(
            schema_inference, "_sample_messages_with_strategy"
        ) as mock_sample:
            mock_sample.return_value = [{"field": "value"}]
            result = schema_inference.sample_topic_messages("test-topic")

        mock_sample.assert_called_once_with("test-topic", "earliest")
        assert result == [{"field": "value"}]

    def test_consumer_creation_failure_is_reported(self, fallback_config):
        report = KafkaSourceReport()
        inference = KafkaSchemaInference(
            bootstrap_servers="localhost:9092",
            consumer_config={"group.id": "test"},
            fallback_config=fallback_config,
            max_workers=1,
            report=report,
        )
        with patch(
            "datahub.ingestion.source.kafka.kafka_schema_inference.Consumer",
            side_effect=OSError("broker down"),
        ):
            result = inference._sample_messages_with_strategy(
                "test-topic", OffsetResetStrategy.LATEST
            )

        assert result == []
        assert report.schema_inference_sampling_failures == 1

    def test_extraction_failure_is_reported(self, fallback_config):
        report = KafkaSourceReport()
        inference = KafkaSchemaInference(
            bootstrap_servers="localhost:9092",
            consumer_config={"group.id": "test"},
            fallback_config=fallback_config,
            max_workers=1,
            report=report,
        )
        with (
            patch.object(inference, "sample_topic_messages", return_value=[{"a": 1}]),
            patch.object(
                inference,
                "_extract_fields_from_samples",
                side_effect=ValueError("boom"),
            ),
        ):
            result = inference._infer_schema_from_messages("test-topic")

        assert result == []
        assert report.schema_inference_sampling_failures == 1

    def test_poll_loop_skips_errors_and_decode_failures(self, fallback_config):
        report = KafkaSourceReport()
        inference = KafkaSchemaInference(
            bootstrap_servers="localhost:9092",
            consumer_config={"group.id": "test"},
            fallback_config=fallback_config,
            max_workers=1,
            report=report,
        )

        errored = Mock()
        errored.error.return_value = "broker error"
        undecodable = Mock()
        undecodable.error.return_value = None
        undecodable.value.return_value = b"not-json"
        valid = Mock()
        valid.error.return_value = None
        valid.value.return_value = b'{"a": 1}'
        scripted = ScriptedConsumer([None, errored, undecodable, valid])

        def fake_process(value: object) -> object:
            if value == b"not-json":
                raise ValueError("undecodable")
            return {"a": 1}

        with (
            patch(f"{_INFERENCE_MODULE}.Consumer", return_value=scripted),
            patch(
                f"{_INFERENCE_MODULE}.process_kafka_message_for_sampling",
                side_effect=fake_process,
            ),
        ):
            result = inference._sample_messages_with_strategy(
                "test-topic", OffsetResetStrategy.LATEST
            )

        # None (timeout) and errored messages are skipped; the undecodable one is
        # skipped too, leaving only the valid message. The consumer is always closed.
        assert result == [{"a": 1}]
        assert scripted.closed

    def test_poll_loop_all_decode_failures_are_reported(self, fallback_config):
        report = KafkaSourceReport()
        inference = KafkaSchemaInference(
            bootstrap_servers="localhost:9092",
            consumer_config={"group.id": "test"},
            fallback_config=fallback_config,
            max_workers=1,
            report=report,
        )

        undecodable = Mock()
        undecodable.error.return_value = None
        undecodable.value.return_value = b"not-json"
        scripted = ScriptedConsumer([undecodable, undecodable])

        with (
            patch(f"{_INFERENCE_MODULE}.Consumer", return_value=scripted),
            patch(
                f"{_INFERENCE_MODULE}.process_kafka_message_for_sampling",
                side_effect=ValueError("undecodable"),
            ),
        ):
            result = inference._sample_messages_with_strategy(
                "test-topic", OffsetResetStrategy.LATEST
            )

        assert result == []
        assert report.schema_inference_message_decode_failures == 2
        assert report.warnings
        assert scripted.closed

    def test_flatten_for_schema_inference(self):
        nested_obj = {
            "field1": "string_value",
            "nested": {"field2": 123, "field3": True},
            "array": [1, 2, 3],
        }
        result = flatten_json(nested_obj, stringify_values=False)

        assert result["field1"] == "string_value"
        assert result["nested.field2"] == 123
        assert result["nested.field3"] is True
        assert result["array"] == [1, 2, 3]
        assert isinstance(result["nested.field2"], int)
        assert isinstance(result["nested.field3"], bool)
