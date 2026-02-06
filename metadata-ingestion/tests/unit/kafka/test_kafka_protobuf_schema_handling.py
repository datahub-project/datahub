"""Tests for Kafka source handling of Protobuf schemas during profiling."""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kafka.kafka import KafkaSource, KafkaSourceConfig
from datahub.ingestion.source.kafka.kafka_constants import (
    SCHEMA_TYPE_AVRO,
    SCHEMA_TYPE_JSON,
    SCHEMA_TYPE_PROTOBUF,
)
from datahub.metadata.schema_classes import KafkaSchemaClass, SchemaMetadataClass

SAMPLE_PROTOBUF_SCHEMA = """syntax = "proto3";
package flowtraders.tradestore.model.v1;

import "google/protobuf/timestamp.proto";
import "google/protobuf/wrappers.proto";

option java_package = "com.flowtraders.tradestore.model.protobuf.v1";

message TradeStoreEvent {
  TradeKey tradeKey = 1;
  Metadata metadata = 2;
}
"""

SAMPLE_AVRO_SCHEMA = """{
  "type": "record",
  "name": "TestRecord",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"}
  ]
}"""


@pytest.fixture
def kafka_source():
    with patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer"):
        ctx = PipelineContext(run_id="test")
        config = KafkaSourceConfig.model_validate(
            {
                "connection": {"bootstrap": "localhost:9092"},
                "profiling": {"enabled": True},
            }
        )
        source = KafkaSource(config, ctx)
        yield source
        source.close()


class TestProtobufSchemaHandling:
    """Test that Protobuf schemas are not incorrectly parsed as Avro during profiling."""

    def test_protobuf_schema_not_parsed_as_avro(self, kafka_source):
        protobuf_schema_metadata = SchemaMetadataClass(
            schemaName="test_topic",
            platform="urn:li:dataPlatform:kafka",
            version=0,
            hash="",
            platformSchema=KafkaSchemaClass(
                documentSchema=SAMPLE_PROTOBUF_SCHEMA,
                documentSchemaType=SCHEMA_TYPE_PROTOBUF,
            ),
            fields=[],
        )

        sample_data = b"\x00\x00\x00\x00\x01some binary protobuf data"

        result = kafka_source._process_message_part(
            data=sample_data,
            prefix="value",
            topic="test_topic",
            schema_metadata=protobuf_schema_metadata,
            is_key=False,
        )

        assert result is not None
        assert not any(
            "avro_decode_error" in str(warning)
            for warning in kafka_source.report.warnings
        )

    def test_avro_schema_still_parsed_correctly(self, kafka_source):
        avro_schema_metadata = SchemaMetadataClass(
            schemaName="test_topic",
            platform="urn:li:dataPlatform:kafka",
            version=0,
            hash="",
            platformSchema=KafkaSchemaClass(
                documentSchema=SAMPLE_AVRO_SCHEMA,
                documentSchemaType=SCHEMA_TYPE_AVRO,
            ),
            fields=[],
        )

        sample_data = b"\x00\x00\x00\x00\x01\x02\x08Alice"

        with (
            patch("avro.schema.parse") as mock_parse,
            patch("avro.io.BinaryDecoder"),
            patch("avro.io.DatumReader") as mock_reader,
        ):
            mock_schema = MagicMock()
            mock_parse.return_value = mock_schema
            mock_reader_instance = MagicMock()
            mock_reader_instance.read.return_value = {"id": 1, "name": "Alice"}
            mock_reader.return_value = mock_reader_instance

            result = kafka_source._process_message_part(
                data=sample_data,
                prefix="value",
                topic="test_topic",
                schema_metadata=avro_schema_metadata,
                is_key=False,
            )

            mock_parse.assert_called_once_with(SAMPLE_AVRO_SCHEMA)
            assert result is not None

    def test_json_schema_skips_avro_parsing(self, kafka_source):
        json_schema_metadata = SchemaMetadataClass(
            schemaName="test_topic",
            platform="urn:li:dataPlatform:kafka",
            version=0,
            hash="",
            platformSchema=KafkaSchemaClass(
                documentSchema='{"type": "object", "properties": {"id": {"type": "integer"}}}',
                documentSchemaType=SCHEMA_TYPE_JSON,
            ),
            fields=[],
        )

        sample_data = b'{"id": 123, "name": "test"}'

        result = kafka_source._process_message_part(
            data=sample_data,
            prefix="value",
            topic="test_topic",
            schema_metadata=json_schema_metadata,
            is_key=False,
        )

        assert result is not None
        assert not any(
            "avro_decode_error" in str(warning)
            for warning in kafka_source.report.warnings
        )

    def test_key_schema_type_checked_correctly(self, kafka_source):
        protobuf_key_schema_metadata = SchemaMetadataClass(
            schemaName="test_topic",
            platform="urn:li:dataPlatform:kafka",
            version=0,
            hash="",
            platformSchema=KafkaSchemaClass(
                keySchema=SAMPLE_PROTOBUF_SCHEMA,
                keySchemaType=SCHEMA_TYPE_PROTOBUF,
                documentSchema=SAMPLE_AVRO_SCHEMA,
                documentSchemaType=SCHEMA_TYPE_AVRO,
            ),
            fields=[],
        )

        sample_key_data = b"\x00\x00\x00\x00\x01some binary protobuf key data"

        result = kafka_source._process_message_part(
            data=sample_key_data,
            prefix="key",
            topic="test_topic",
            schema_metadata=protobuf_key_schema_metadata,
            is_key=True,
        )

        assert result is not None
        assert not any(
            "avro_decode_error" in str(warning)
            for warning in kafka_source.report.warnings
        )

    def test_missing_schema_type_uses_fallback(self, kafka_source):
        schema_metadata_no_type = SchemaMetadataClass(
            schemaName="test_topic",
            platform="urn:li:dataPlatform:kafka",
            version=0,
            hash="",
            platformSchema=KafkaSchemaClass(
                documentSchema=SAMPLE_AVRO_SCHEMA,
            ),
            fields=[],
        )

        sample_data = b'{"id": 123, "name": "test"}'

        result = kafka_source._process_message_part(
            data=sample_data,
            prefix="value",
            topic="test_topic",
            schema_metadata=schema_metadata_no_type,
            is_key=False,
        )

        assert result is not None
