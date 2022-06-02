import unittest
from itertools import chain
from typing import Dict, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
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
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kafka import KafkaSource, KafkaSourceConfig
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    DataPlatformInstanceClass,
    KafkaSchemaClass,
    SchemaMetadataClass,
)


class KafkaSourceTest(object):
    @patch("datahub.ingestion.source.kafka.confluent_kafka.Consumer", autospec=True)
    def test_kafka_source_configuration(self, mock_kafka):
        ctx = PipelineContext(run_id="test")
        kafka_source = KafkaSource(
            KafkaSourceConfig.parse_obj({"connection": {"bootstrap": "foobar:9092"}}),
            ctx,
        )
        kafka_source.close()
        assert mock_kafka.call_count == 1

    @patch("datahub.ingestion.source.kafka.confluent_kafka.Consumer", autospec=True)
    def test_kafka_source_workunits_wildcard_topic(self, mock_kafka):
        mock_kafka_instance = mock_kafka.return_value
        mock_cluster_metadata = MagicMock()
        mock_cluster_metadata.topics = ["foobar", "bazbaz"]
        mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

        ctx = PipelineContext(run_id="test")
        kafka_source = KafkaSource(
            KafkaSourceConfig.parse_obj(
                {"connection": {"bootstrap": "localhost:9092"}}
            ),
            ctx,
        )
        workunits = list(kafka_source.get_workunits())

        first_mce = workunits[0].metadata
        assert isinstance(first_mce, MetadataChangeEvent)
        mock_kafka.assert_called_once()
        mock_kafka_instance.list_topics.assert_called_once()
        assert len(workunits) == 4

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
        assert len(workunits) == 2

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
        assert len(workunits) == 4

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

        # We should only have 1 topic + sub-type wu.
        assert len(workunits) == 2
        assert isinstance(workunits[0], MetadataWorkUnit)
        assert isinstance(workunits[0].metadata, MetadataChangeEvent)
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

    @patch(
        "datahub.ingestion.source.kafka.confluent_kafka.schema_registry.schema_registry_client.SchemaRegistryClient",
        autospec=True,
    )
    @patch("datahub.ingestion.source.kafka.confluent_kafka.Consumer", autospec=True)
    def test_kafka_source_workunits_schema_registry_subject_name_strategies(
        self, mock_kafka_consumer, mock_schema_registry_client
    ):
        # Setup the topic to key/value schema mappings for all types of schema registry subject name strategies.
        # <key=topic_name, value=(<key_schema>,<value_schema>)
        topic_subject_schema_map: Dict[
            str, Tuple[RegisteredSchema, RegisteredSchema]
        ] = {
            # TopicNameStrategy is used for subject
            "topic1": (
                RegisteredSchema(
                    schema_id="schema_id_2",
                    schema=Schema(
                        schema_str='{"type":"record", "name":"Topic1Key", "namespace": "test.acryl", "fields": [{"name":"t1key", "type": "string"}]}',
                        schema_type="AVRO",
                    ),
                    subject="topic1-key",
                    version=1,
                ),
                RegisteredSchema(
                    schema_id="schema_id_1",
                    schema=Schema(
                        schema_str='{"type":"record", "name":"Topic1Value", "namespace": "test.acryl", "fields": [{"name":"t1value", "type": "string"}]}',
                        schema_type="AVRO",
                    ),
                    subject="topic1-value",
                    version=1,
                ),
            ),
            # RecordNameStrategy is used for subject
            "topic2": (
                RegisteredSchema(
                    schema_id="schema_id_3",
                    schema=Schema(
                        schema_str='{"type":"record", "name":"Topic2Key", "namespace": "test.acryl", "fields": [{"name":"t2key", "type": "string"}]}',
                        schema_type="AVRO",
                    ),
                    subject="test.acryl.Topic2Key",
                    version=1,
                ),
                RegisteredSchema(
                    schema_id="schema_id_4",
                    schema=Schema(
                        schema_str='{"type":"record", "name":"Topic2Value", "namespace": "test.acryl", "fields": [{"name":"t2value", "type": "string"}]}',
                        schema_type="AVRO",
                    ),
                    subject="test.acryl.Topic2Value",
                    version=1,
                ),
            ),
            # TopicRecordNameStrategy is used for subject
            "topic3": (
                RegisteredSchema(
                    schema_id="schema_id_4",
                    schema=Schema(
                        schema_str='{"type":"record", "name":"Topic3Key", "namespace": "test.acryl", "fields": [{"name":"t3key", "type": "string"}]}',
                        schema_type="AVRO",
                    ),
                    subject="topic3-test.acryl.Topic3Key-key",
                    version=1,
                ),
                RegisteredSchema(
                    schema_id="schema_id_5",
                    schema=Schema(
                        schema_str='{"type":"record", "name":"Topic3Value", "namespace": "test.acryl", "fields": [{"name":"t3value", "type": "string"}]}',
                        schema_type="AVRO",
                    ),
                    subject="topic3-test.acryl.Topic3Value-value",
                    version=1,
                ),
            ),
        }

        # Mock the kafka consumer
        mock_kafka_instance = mock_kafka_consumer.return_value
        mock_cluster_metadata = MagicMock()
        mock_cluster_metadata.topics = list(topic_subject_schema_map.keys())
        mock_cluster_metadata.topics.append("schema_less_topic")
        mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

        # Mock the schema registry client
        # - mock get_subjects: all subjects in topic_subject_schema_map
        mock_schema_registry_client.return_value.get_subjects.return_value = [
            v.subject for v in chain(*topic_subject_schema_map.values())
        ]

        # - mock get_latest_version
        def mock_get_latest_version(subject_name: str) -> Optional[RegisteredSchema]:
            for registered_schema in chain(*topic_subject_schema_map.values()):
                if registered_schema.subject == subject_name:
                    return registered_schema
            return None

        mock_schema_registry_client.return_value.get_latest_version = (
            mock_get_latest_version
        )

        # Test the kafka source
        source_config = {
            "connection": {"bootstrap": "localhost:9092"},
            # Setup the topic_subject_map for topic2 which uses RecordNameStrategy
            "topic_subject_map": {
                "topic2-key": "test.acryl.Topic2Key",
                "topic2-value": "test.acryl.Topic2Value",
            },
        }
        ctx = PipelineContext(run_id="test")
        kafka_source = KafkaSource.create(source_config, ctx)
        workunits = list(kafka_source.get_workunits())

        mock_kafka_consumer.assert_called_once()
        mock_kafka_instance.list_topics.assert_called_once()
        assert len(workunits) == 8
        i: int = -1
        for wu in workunits:
            assert isinstance(wu, MetadataWorkUnit)
            if not isinstance(wu.metadata, MetadataChangeEvent):
                continue
            mce: MetadataChangeEvent = wu.metadata
            i += 1

            if i < len(topic_subject_schema_map.keys()):
                # First 3 workunits (topics) must have schemaMetadata aspect
                assert isinstance(mce.proposedSnapshot.aspects[1], SchemaMetadataClass)
                schemaMetadataAspect: SchemaMetadataClass = (
                    mce.proposedSnapshot.aspects[1]
                )
                assert isinstance(schemaMetadataAspect.platformSchema, KafkaSchemaClass)
                # Make sure the schema name is present in topic_subject_schema_map.
                assert schemaMetadataAspect.schemaName in topic_subject_schema_map
                # Make sure the schema_str matches for the key schema.
                assert (
                    schemaMetadataAspect.platformSchema.keySchema
                    == topic_subject_schema_map[schemaMetadataAspect.schemaName][
                        0
                    ].schema.schema_str
                )
                # Make sure the schema_str matches for the value schema.
                assert (
                    schemaMetadataAspect.platformSchema.documentSchema
                    == topic_subject_schema_map[schemaMetadataAspect.schemaName][
                        1
                    ].schema.schema_str
                )
                # Make sure we have 2 fields, one from the key schema & one from the value schema.
                assert len(schemaMetadataAspect.fields) == 2
            else:
                # Last topic('schema_less_topic') has no schema defined in the registry.
                # The schemaMetadata aspect should not be present for this.
                for aspect in mce.proposedSnapshot.aspects:
                    assert not isinstance(aspect, SchemaMetadataClass)

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
    @patch(
        "datahub.ingestion.source.kafka.confluent_kafka.schema_registry.schema_registry_client.SchemaRegistryClient",
        autospec=True,
    )
    @patch("datahub.ingestion.source.kafka.confluent_kafka.Consumer", autospec=True)
    def test_kafka_ignore_warnings_on_schema_type(
        self,
        mock_kafka_consumer,
        mock_schema_registry_client,
        ignore_warnings_on_schema_type,
    ):
        # define the key and value schemas for topic1
        topic1_key_schema = RegisteredSchema(
            schema_id="schema_id_2",
            schema=Schema(
                schema_str="{}",
                schema_type="JSON",
            ),
            subject="topic1-key",
            version=1,
        )
        topic1_value_schema = RegisteredSchema(
            schema_id="schema_id_1",
            schema=Schema(
                schema_str="{}",
                schema_type="JSON",
            ),
            subject="topic1-value",
            version=1,
        )

        # Mock the kafka consumer
        mock_kafka_instance = mock_kafka_consumer.return_value
        mock_cluster_metadata = MagicMock()
        mock_cluster_metadata.topics = ["topic1"]
        mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

        # Mock the schema registry client
        mock_schema_registry_client.return_value.get_subjects.return_value = [
            "topic1-key",
            "topic1-value",
        ]

        # - mock get_latest_version
        def mock_get_latest_version(subject_name: str) -> Optional[RegisteredSchema]:
            if subject_name == "topic1-key":
                return topic1_key_schema
            elif subject_name == "topic1-value":
                return topic1_value_schema
            return None

        mock_schema_registry_client.return_value.get_latest_version = (
            mock_get_latest_version
        )

        # Test the kafka source
        source_config = {
            "connection": {"bootstrap": "localhost:9092"},
            "ignore_warnings_on_schema_type": f"{ignore_warnings_on_schema_type}",
        }
        ctx = PipelineContext(run_id="test")
        kafka_source = KafkaSource.create(source_config, ctx)
        workunits = list(kafka_source.get_workunits())

        assert len(workunits) == 1
        if ignore_warnings_on_schema_type:
            assert not kafka_source.report.warnings
        else:
            assert kafka_source.report.warnings
