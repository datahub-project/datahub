import unittest
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
)

from datahub.configuration.common import ConfigurationError
from datahub.emitter.mce_builder import (
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kafka import KafkaSource
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import BrowsePathsClass, DataPlatformInstanceClass


class TestKafkaSource(object):
    def test_get_schema_str_replace_confluent_ref_avro(self):

        # References external schema 'TestTopic1' in the definition of 'my_field1' field.
        schema_str_orig = """
{
  "fields": [
    {
      "name": "my_field1",
      "type": "TestTopic1"
    }
  ],
  "name": "TestTopic1Val",
  "namespace": "io.acryl",
  "type": "record"
}
"""
        schema_str_ref = """
{
  "doc": "Sample schema to help you get started.",
  "fields": [
    {
      "doc": "The int type is a 32-bit signed integer.",
      "name": "my_field1",
      "type": "int"
    }
  ],
  "name": "TestTopic1",
  "namespace": "io.acryl",
  "type": "record"
}
"""

        schema_str_final = (
            """
{
  "fields": [
    {
      "name": "my_field1",
      "type": """
            + schema_str_ref
            + """
    }
  ],
  "name": "TestTopic1Val",
  "namespace": "io.acryl",
  "type": "record"
}
"""
        )

        ctx = PipelineContext(run_id="test")
        kafka_source = KafkaSource.create(
            {
                "connection": {"bootstrap": "localhost:9092"},
            },
            ctx,
        )

        def new_get_latest_version(_):
            return RegisteredSchema(
                schema_id="schema_id_1",
                schema=Schema(schema_str=schema_str_ref, schema_type="AVRO"),
                subject="test",
                version=1,
            )

        with patch.object(
            kafka_source.schema_registry_client,
            "get_latest_version",
            new_get_latest_version,
        ):
            schema_str = kafka_source.get_schema_str_replace_confluent_ref_avro(
                # The external reference would match by name.
                schema=Schema(
                    schema_str=schema_str_orig,
                    schema_type="AVRO",
                    references=[
                        dict(name="TestTopic1", subject="schema_subject_1", version=1)
                    ],
                )
            )
            assert schema_str == KafkaSource._compact_schema(schema_str_final)

        with patch.object(
            kafka_source.schema_registry_client,
            "get_latest_version",
            new_get_latest_version,
        ):
            schema_str = kafka_source.get_schema_str_replace_confluent_ref_avro(
                # The external reference would match by subject.
                schema=Schema(
                    schema_str=schema_str_orig,
                    schema_type="AVRO",
                    references=[
                        dict(name="schema_subject_1", subject="TestTopic1", version=1)
                    ],
                )
            )
            assert schema_str == KafkaSource._compact_schema(schema_str_final)

    @patch("datahub.ingestion.source.kafka.confluent_kafka.Consumer", autospec=True)
    def test_kafka_source_configuration(self, mock_kafka):
        ctx = PipelineContext(run_id="test")
        kafka_source = KafkaSource.create(
            {"connection": {"bootstrap": "foobar:9092"}}, ctx
        )
        kafka_source.close()
        assert mock_kafka.call_count == 1

    @pytest.mark.parametrize(
        "ignore_warnings_on_schema_type",
        [
            pytest.param(
                False,
                id="ignore_warnings_on_schema_type-FALSE",
            ),
            pytest.param(
                True,
                id="ignore_warnings_on_schema_type-TRUE",
            ),
        ],
    )
    @patch("datahub.ingestion.source.kafka.confluent_kafka.Consumer", autospec=True)
    def test_kafka_unsupported_schema_type(
        self, mock_kafka, ignore_warnings_on_schema_type
    ):
        mock_kafka_instance = mock_kafka.return_value
        mock_cluster_metadata = MagicMock()
        mock_cluster_metadata.topics = ["foobar"]
        mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

        ctx = PipelineContext(run_id="test")
        kafka_source = KafkaSource.create(
            {
                "connection": {"bootstrap": "localhost:9092"},
                "ignore_warnings_on_schema_type": f"{ignore_warnings_on_schema_type}",
            },
            ctx,
        )
        with patch.object(
            kafka_source.schema_registry_client,
            "get_latest_version",
        ) as mock_get_latest_version:
            mock_get_latest_version.return_value = RegisteredSchema(
                schema_id="schema_id_1",
                schema=Schema(schema_str="{}", schema_type="JSON"),
                subject="foobar-value",
                version=1,
            )
            # we use list() to force the generation of the workunits
            list(kafka_source.get_workunits())
            if ignore_warnings_on_schema_type:
                assert not kafka_source.report.warnings
            else:
                assert kafka_source.report.warnings

    @pytest.mark.parametrize(
        "ignore_warnings_on_missing_schema,error,expect_warnings",
        [
            pytest.param(
                False,
                SchemaRegistryError(404, 40401, "Subject not found."),
                True,
                id="SUBJECT_NOT_FOUND-DO_NOT_IGNORE_WARNINGS",
            ),
            pytest.param(
                True,
                SchemaRegistryError(404, 40401, "Subject not found."),
                False,
                id="SUBJECT_NOT_FOUND-IGNORE_WARNINGS",
            ),
            pytest.param(
                True,
                SchemaRegistryError(400, -1, "Unknown error."),
                True,
                id="UNKNOWN_SR_ERROR-IGNORE_WARNINGS",
            ),
            pytest.param(
                True,
                Exception("Unknown exception."),
                True,
                id="UNKNOWN_EXCEPTION-IGNORE_WARNINGS",
            ),
        ],
    )
    @patch("datahub.ingestion.source.kafka.confluent_kafka.Consumer", autospec=True)
    def test_kafka_warnings_on_missing_schema(
        self, mock_kafka, ignore_warnings_on_missing_schema, error, expect_warnings
    ):
        mock_kafka_instance = mock_kafka.return_value
        mock_cluster_metadata = MagicMock()
        mock_cluster_metadata.topics = ["foobar"]
        mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

        ctx = PipelineContext(run_id="test")
        kafka_source = KafkaSource.create(
            {
                "connection": {"bootstrap": "localhost:9092"},
                "ignore_warnings_on_missing_schema": f"{ignore_warnings_on_missing_schema}",
            },
            ctx,
        )

        with patch.object(
            kafka_source.schema_registry_client,
            "get_latest_version",
        ) as mock_get_latest_version:
            mock_get_latest_version.side_effect = [error]
            # we use list() to force the generation of the workunits
            list(kafka_source.get_workunits())
            assert (len(kafka_source.report.warnings) > 0) == expect_warnings

    @patch("datahub.ingestion.source.kafka.confluent_kafka.Consumer", autospec=True)
    def test_kafka_source_workunits_wildcard_topic(self, mock_kafka):
        mock_kafka_instance = mock_kafka.return_value
        mock_cluster_metadata = MagicMock()
        mock_cluster_metadata.topics = ["foobar", "bazbaz"]
        mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

        ctx = PipelineContext(run_id="test")
        kafka_source = KafkaSource.create(
            {"connection": {"bootstrap": "localhost:9092"}}, ctx
        )
        workunits = list(kafka_source.get_workunits())

        first_mce = workunits[0].metadata
        assert isinstance(first_mce, MetadataChangeEvent)
        mock_kafka.assert_called_once()
        mock_kafka_instance.list_topics.assert_called_once()
        assert len(workunits) == 2

    @patch("datahub.ingestion.source.kafka.confluent_kafka.Consumer", autospec=True)
    def test_kafka_source_workunits_topic_pattern(self, mock_kafka):
        mock_kafka_instance = mock_kafka.return_value
        mock_cluster_metadata = MagicMock()
        mock_cluster_metadata.topics = ["test", "foobar", "bazbaz"]
        mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

        ctx = PipelineContext(run_id="test1")
        kafka_source = KafkaSource.create(
            {
                "topic_patterns": {"allow": ["test"]},
                "connection": {"bootstrap": "localhost:9092"},
            },
            ctx,
        )
        workunits = [w for w in kafka_source.get_workunits()]

        mock_kafka.assert_called_once()
        mock_kafka_instance.list_topics.assert_called_once()
        assert len(workunits) == 1

        mock_cluster_metadata.topics = ["test", "test2", "bazbaz"]
        ctx = PipelineContext(run_id="test2")
        kafka_source = KafkaSource.create(
            {
                "topic_patterns": {"allow": ["test.*"]},
                "connection": {"bootstrap": "localhost:9092"},
            },
            ctx,
        )
        workunits = [w for w in kafka_source.get_workunits()]
        assert len(workunits) == 2

    @patch("datahub.ingestion.source.kafka.confluent_kafka.Consumer", autospec=True)
    def test_kafka_source_workunits_with_platform_instance(self, mock_kafka):

        PLATFORM_INSTANCE = "kafka_cluster"
        PLATFORM = "kafka"
        TOPIC_NAME = "test"

        mock_kafka_instance = mock_kafka.return_value
        mock_cluster_metadata = MagicMock()
        mock_cluster_metadata.topics = [TOPIC_NAME]
        mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

        ctx = PipelineContext(run_id="test1")
        kafka_source = KafkaSource.create(
            {
                "connection": {"bootstrap": "localhost:9092"},
                "platform_instance": PLATFORM_INSTANCE,
            },
            ctx,
        )
        workunits = [w for w in kafka_source.get_workunits()]

        # We should only have 1 topic
        assert len(workunits) == 1
        proposed_snap = workunits[0].metadata.proposedSnapshot
        assert proposed_snap.urn == make_dataset_urn_with_platform_instance(
            platform=PLATFORM,
            name=TOPIC_NAME,
            platform_instance=PLATFORM_INSTANCE,
            env="PROD",
        )

        # DataPlatform aspect should be present when platform_instance is configured
        data_platform_aspects = [
            asp
            for asp in proposed_snap.aspects
            if type(asp) == DataPlatformInstanceClass
        ]
        assert len(data_platform_aspects) == 1
        assert data_platform_aspects[0].instance == make_dataplatform_instance_urn(
            PLATFORM, PLATFORM_INSTANCE
        )

        # The default browse path should include the platform_instance value
        browse_path_aspects = [
            asp for asp in proposed_snap.aspects if type(asp) == BrowsePathsClass
        ]
        assert len(browse_path_aspects) == 1
        assert (
            f"/prod/{PLATFORM}/{PLATFORM_INSTANCE}/{TOPIC_NAME}"
            in browse_path_aspects[0].paths
        )

    @patch("datahub.ingestion.source.kafka.confluent_kafka.Consumer", autospec=True)
    def test_close(self, mock_kafka):
        mock_kafka_instance = mock_kafka.return_value
        ctx = PipelineContext(run_id="test")
        kafka_source = KafkaSource.create(
            {
                "topic_patterns": {"allow": ["test.*"]},
                "connection": {"bootstrap": "localhost:9092"},
            },
            ctx,
        )
        kafka_source.close()
        assert mock_kafka_instance.close.call_count == 1

    def test_kafka_source_stateful_ingestion_requires_platform_instance(
        self,
    ):
        class StatefulProviderMock:
            def __init__(self, config, ctx):
                self.ctx = ctx
                self.config = config

            def is_stateful_ingestion_configured(self):
                return self.config.stateful_ingestion.enabled

        kafka_source_patcher = unittest.mock.patch.object(
            KafkaSource, "__bases__", (StatefulProviderMock,)
        )

        ctx = PipelineContext(run_id="test", pipeline_name="test")
        with pytest.raises(ConfigurationError):
            with kafka_source_patcher:
                # prevent delattr on __bases__ on context __exit__
                kafka_source_patcher.is_local = True
                KafkaSource.create(
                    {
                        "stateful_ingestion": {"enabled": "true"},
                        "connection": {"bootstrap": "localhost:9092"},
                    },
                    ctx,
                )
