import json
from itertools import chain
from typing import Dict, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
)
from freezegun import freeze_time

from datahub.configuration.common import ConfigurationError
from datahub.emitter.mce_builder import (
    OwnerType,
    make_dataplatform_instance_urn,
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_global_tag_aspect_with_tag_list,
    make_glossary_terms_aspect_from_urn_list,
    make_owner_urn,
    make_ownership_aspect_from_urn_list,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kafka.kafka import KafkaSource, KafkaSourceConfig
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    DataPlatformInstanceClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    KafkaSchemaClass,
    OwnershipClass,
    SchemaMetadataClass,
)


@pytest.fixture
def mock_admin_client():
    with patch(
        "datahub.ingestion.source.kafka.kafka.AdminClient", autospec=True
    ) as mock:
        yield mock


@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_configuration(mock_kafka):
    ctx = PipelineContext(run_id="test")
    kafka_source = KafkaSource(
        KafkaSourceConfig.parse_obj({"connection": {"bootstrap": "foobar:9092"}}),
        ctx,
    )
    kafka_source.close()
    assert mock_kafka.call_count == 1


@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_workunits_wildcard_topic(mock_kafka, mock_admin_client):
    mock_kafka_instance = mock_kafka.return_value
    mock_cluster_metadata = MagicMock()
    mock_cluster_metadata.topics = {"foobar": None, "bazbaz": None}
    mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

    ctx = PipelineContext(run_id="test")
    kafka_source = KafkaSource(
        KafkaSourceConfig.parse_obj({"connection": {"bootstrap": "localhost:9092"}}),
        ctx,
    )
    workunits = list(kafka_source.get_workunits())

    first_mce = workunits[0].metadata
    assert isinstance(first_mce, MetadataChangeEvent)
    mock_kafka.assert_called_once()
    mock_kafka_instance.list_topics.assert_called_once()
    assert len(workunits) == 4


@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_workunits_topic_pattern(mock_kafka, mock_admin_client):
    mock_kafka_instance = mock_kafka.return_value
    mock_cluster_metadata = MagicMock()
    mock_cluster_metadata.topics = {"test": None, "foobar": None, "bazbaz": None}
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

    mock_cluster_metadata.topics = {"test": None, "test2": None, "bazbaz": None}
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


@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_workunits_with_platform_instance(mock_kafka, mock_admin_client):
    PLATFORM_INSTANCE = "kafka_cluster"
    PLATFORM = "kafka"
    TOPIC_NAME = "test"

    mock_kafka_instance = mock_kafka.return_value
    mock_cluster_metadata = MagicMock()
    mock_cluster_metadata.topics = {TOPIC_NAME: None}
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
        if isinstance(asp, DataPlatformInstanceClass)
    ]
    assert len(data_platform_aspects) == 1
    assert data_platform_aspects[0].instance == make_dataplatform_instance_urn(
        PLATFORM, PLATFORM_INSTANCE
    )

    # The default browse path should include the platform_instance value
    browse_path_aspects = [
        asp for asp in proposed_snap.aspects if isinstance(asp, BrowsePathsClass)
    ]
    assert len(browse_path_aspects) == 1
    assert f"/prod/{PLATFORM}/{PLATFORM_INSTANCE}" in browse_path_aspects[0].paths


@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_workunits_no_platform_instance(mock_kafka, mock_admin_client):
    PLATFORM = "kafka"
    TOPIC_NAME = "test"

    mock_kafka_instance = mock_kafka.return_value
    mock_cluster_metadata = MagicMock()
    mock_cluster_metadata.topics = {TOPIC_NAME: None}
    mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

    ctx = PipelineContext(run_id="test1")
    kafka_source = KafkaSource.create(
        {"connection": {"bootstrap": "localhost:9092"}},
        ctx,
    )
    workunits = [w for w in kafka_source.get_workunits()]

    # We should only have 1 topic + sub-type wu.
    assert len(workunits) == 2
    assert isinstance(workunits[0], MetadataWorkUnit)
    assert isinstance(workunits[0].metadata, MetadataChangeEvent)
    proposed_snap = workunits[0].metadata.proposedSnapshot
    assert proposed_snap.urn == make_dataset_urn(
        platform=PLATFORM,
        name=TOPIC_NAME,
        env="PROD",
    )

    # DataPlatform aspect should not be present when platform_instance is not configured
    data_platform_aspects = [
        asp
        for asp in proposed_snap.aspects
        if isinstance(asp, DataPlatformInstanceClass)
    ]
    assert len(data_platform_aspects) == 0

    # The default browse path should include the platform_instance value
    browse_path_aspects = [
        asp for asp in proposed_snap.aspects if isinstance(asp, BrowsePathsClass)
    ]
    assert len(browse_path_aspects) == 1
    assert f"/prod/{PLATFORM}" in browse_path_aspects[0].paths


@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_close(mock_kafka, mock_admin_client):
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


@patch(
    "datahub.ingestion.source.confluent_schema_registry.SchemaRegistryClient",
    autospec=True,
)
@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_workunits_schema_registry_subject_name_strategies(
    mock_kafka_consumer, mock_schema_registry_client, mock_admin_client
):
    # Setup the topic to key/value schema mappings for all types of schema registry subject name strategies.
    # <key=topic_name, value=(<key_schema>,<value_schema>)
    topic_subject_schema_map: Dict[str, Tuple[RegisteredSchema, RegisteredSchema]] = {
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
    mock_cluster_metadata.topics = {k: None for k in topic_subject_schema_map.keys()}
    mock_cluster_metadata.topics["schema_less_topic"] = None
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
        "ingest_schemas_as_entities": True,
    }
    ctx = PipelineContext(run_id="test")
    kafka_source = KafkaSource.create(source_config, ctx)
    workunits = list(kafka_source.get_workunits())

    mock_kafka_consumer.assert_called_once()
    mock_kafka_instance.list_topics.assert_called_once()
    # Along with with 4 topics(3 with schema and 1 schemaless) which constitutes to 8 workunits,
    #   there will be 6 schemas (1 key and 1 value schema for 3 topics) which constitutes to 12 workunits
    assert len(workunits) == 20
    i: int = -1
    for wu in workunits:
        assert isinstance(wu, MetadataWorkUnit)
        if not isinstance(wu.metadata, MetadataChangeEvent):
            continue
        mce: MetadataChangeEvent = wu.metadata
        i += 1

        # Only topic (named schema_less_topic) does not have schema metadata but other workunits (that are created
        #   for schema) will have corresponding SchemaMetadata aspect
        if i < len(topic_subject_schema_map.keys()):
            # First 3 workunits (topics) must have schemaMetadata aspect
            assert isinstance(mce.proposedSnapshot.aspects[1], SchemaMetadataClass)
            schemaMetadataAspect: SchemaMetadataClass = mce.proposedSnapshot.aspects[1]
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
            # Make sure the schema_type matches for the key schema.
            assert (
                schemaMetadataAspect.platformSchema.keySchemaType
                == topic_subject_schema_map[schemaMetadataAspect.schemaName][
                    0
                ].schema.schema_type
            )
            # Make sure the schema_str matches for the value schema.
            assert (
                schemaMetadataAspect.platformSchema.documentSchema
                == topic_subject_schema_map[schemaMetadataAspect.schemaName][
                    1
                ].schema.schema_str
            )
            # Make sure the schema_type matches for the value schema.
            assert (
                schemaMetadataAspect.platformSchema.documentSchemaType
                == topic_subject_schema_map[schemaMetadataAspect.schemaName][
                    1
                ].schema.schema_type
            )
            # Make sure we have 2 fields, one from the key schema & one from the value schema.
            assert len(schemaMetadataAspect.fields) == 2
        elif i == len(topic_subject_schema_map.keys()):
            # Last topic('schema_less_topic') has no schema defined in the registry.
            # The schemaMetadata aspect should not be present for this.
            for aspect in mce.proposedSnapshot.aspects:
                assert not isinstance(aspect, SchemaMetadataClass)
        else:
            # Last 2 workunits (schemas) must have schemaMetadata aspect
            assert isinstance(mce.proposedSnapshot.aspects[1], SchemaMetadataClass)
            schemaMetadataAspectObj: SchemaMetadataClass = mce.proposedSnapshot.aspects[
                1
            ]
            assert isinstance(schemaMetadataAspectObj.platformSchema, KafkaSchemaClass)


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
    "datahub.ingestion.source.confluent_schema_registry.SchemaRegistryClient",
    autospec=True,
)
@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_ignore_warnings_on_schema_type(
    mock_kafka_consumer,
    mock_schema_registry_client,
    mock_admin_client,
    ignore_warnings_on_schema_type,
):
    # define the key and value schemas for topic1
    topic1_key_schema = RegisteredSchema(
        schema_id="schema_id_2",
        schema=Schema(
            schema_str="{}",
            schema_type="UNKNOWN_TYPE",
        ),
        subject="topic1-key",
        version=1,
    )
    topic1_value_schema = RegisteredSchema(
        schema_id="schema_id_1",
        schema=Schema(
            schema_str="{}",
            schema_type="UNKNOWN_TYPE",
        ),
        subject="topic1-value",
        version=1,
    )

    # Mock the kafka consumer
    mock_kafka_instance = mock_kafka_consumer.return_value
    mock_cluster_metadata = MagicMock()
    mock_cluster_metadata.topics = {"topic1": None}
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
    assert len(workunits) == 2
    if ignore_warnings_on_schema_type:
        assert not kafka_source.report.warnings
    else:
        assert kafka_source.report.warnings


@patch("datahub.ingestion.source.kafka.kafka.AdminClient", autospec=True)
@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_succeeds_with_admin_client_init_error(
    mock_kafka, mock_kafka_admin_client
):
    mock_kafka_instance = mock_kafka.return_value
    mock_cluster_metadata = MagicMock()
    mock_cluster_metadata.topics = {"test": None, "foobar": None, "bazbaz": None}
    mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

    mock_kafka_admin_client.side_effect = Exception()

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

    mock_kafka_admin_client.assert_called_once()

    assert len(workunits) == 2


@patch("datahub.ingestion.source.kafka.kafka.AdminClient", autospec=True)
@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_succeeds_with_describe_configs_error(
    mock_kafka, mock_kafka_admin_client
):
    mock_kafka_instance = mock_kafka.return_value
    mock_cluster_metadata = MagicMock()
    mock_cluster_metadata.topics = {"test": None, "foobar": None, "bazbaz": None}
    mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

    mock_admin_client_instance = mock_kafka_admin_client.return_value
    mock_admin_client_instance.describe_configs.side_effect = Exception()

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

    mock_kafka_admin_client.assert_called_once()
    mock_admin_client_instance.describe_configs.assert_called_once()

    assert len(workunits) == 2


@freeze_time("2023-09-20 10:00:00")
@patch(
    "datahub.ingestion.source.confluent_schema_registry.SchemaRegistryClient",
    autospec=True,
)
@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_topic_meta_mappings(
    mock_kafka_consumer, mock_schema_registry_client, mock_admin_client
):
    # Setup the topic to key/value schema mappings for all types of schema registry subject name strategies.
    # <key=topic_name, value=(<key_schema>,<value_schema>)
    topic_subject_schema_map: Dict[str, Tuple[RegisteredSchema, RegisteredSchema]] = {
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
                    schema_str=json.dumps(
                        {
                            "type": "record",
                            "name": "Topic1Value",
                            "namespace": "test.acryl",
                            "fields": [{"name": "t1value", "type": "string"}],
                            "owner": "@charles",
                            "business_owner": "jdoe.last@gmail.com",
                            "data_governance.team_owner": "Finance",
                            "has_pii": True,
                            "int_property": 1,
                            "double_property": 2.5,
                        }
                    ),
                    schema_type="AVRO",
                ),
                subject="topic1-value",
                version=1,
            ),
        )
    }

    # Mock the kafka consumer
    mock_kafka_instance = mock_kafka_consumer.return_value
    mock_cluster_metadata = MagicMock()
    mock_cluster_metadata.topics = {k: None for k in topic_subject_schema_map.keys()}
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

    ctx = PipelineContext(run_id="test1")
    kafka_source = KafkaSource.create(
        {
            "connection": {"bootstrap": "localhost:9092"},
            "ingest_schemas_as_entities": True,
            "meta_mapping": {
                "owner": {
                    "match": "^@(.*)",
                    "operation": "add_owner",
                    "config": {"owner_type": "user"},
                },
                "business_owner": {
                    "match": ".*",
                    "operation": "add_owner",
                    "config": {"owner_type": "user"},
                },
                "has_pii": {
                    "match": True,
                    "operation": "add_tag",
                    "config": {"tag": "has_pii_test"},
                },
                "int_property": {
                    "match": 1,
                    "operation": "add_tag",
                    "config": {"tag": "int_meta_property"},
                },
                "double_property": {
                    "match": 2.5,
                    "operation": "add_term",
                    "config": {"term": "double_meta_property"},
                },
                "data_governance.team_owner": {
                    "match": "Finance",
                    "operation": "add_term",
                    "config": {"term": "Finance_test"},
                },
            },
        },
        ctx,
    )
    # Along with with 1 topics(and 5 meta mapping) it constitutes to 6 workunits,
    #   there will be 2 schemas which constitutes to 4 workunits (1 mce and 1 mcp each)
    workunits = [w for w in kafka_source.get_workunits()]
    assert len(workunits) == 10
    mce = workunits[0].metadata
    assert isinstance(mce, MetadataChangeEvent)

    ownership_aspect = [
        asp for asp in mce.proposedSnapshot.aspects if isinstance(asp, OwnershipClass)
    ][0]
    assert ownership_aspect == make_ownership_aspect_from_urn_list(
        [
            make_owner_urn("charles", OwnerType.USER),
            make_owner_urn("jdoe.last@gmail.com", OwnerType.USER),
        ],
        "SERVICE",
    )

    tags_aspect = [
        asp for asp in mce.proposedSnapshot.aspects if isinstance(asp, GlobalTagsClass)
    ][0]
    assert tags_aspect == make_global_tag_aspect_with_tag_list(
        ["has_pii_test", "int_meta_property"]
    )

    terms_aspect = [
        asp
        for asp in mce.proposedSnapshot.aspects
        if isinstance(asp, GlossaryTermsClass)
    ][0]
    assert terms_aspect == make_glossary_terms_aspect_from_urn_list(
        [
            "urn:li:glossaryTerm:Finance_test",
            "urn:li:glossaryTerm:double_meta_property",
        ]
    )
    assert isinstance(workunits[1].metadata, MetadataChangeProposalWrapper)
    mce = workunits[2].metadata
    assert isinstance(mce, MetadataChangeEvent)
    assert isinstance(workunits[3].metadata, MetadataChangeProposalWrapper)

    mce = workunits[4].metadata
    assert isinstance(mce, MetadataChangeEvent)
    ownership_aspect = [
        asp for asp in mce.proposedSnapshot.aspects if isinstance(asp, OwnershipClass)
    ][0]
    assert ownership_aspect == make_ownership_aspect_from_urn_list(
        [
            make_owner_urn("charles", OwnerType.USER),
            make_owner_urn("jdoe.last@gmail.com", OwnerType.USER),
        ],
        "SERVICE",
    )

    tags_aspect = [
        asp for asp in mce.proposedSnapshot.aspects if isinstance(asp, GlobalTagsClass)
    ][0]
    assert tags_aspect == make_global_tag_aspect_with_tag_list(
        ["has_pii_test", "int_meta_property"]
    )

    terms_aspect = [
        asp
        for asp in mce.proposedSnapshot.aspects
        if isinstance(asp, GlossaryTermsClass)
    ][0]
    assert terms_aspect == make_glossary_terms_aspect_from_urn_list(
        [
            "urn:li:glossaryTerm:Finance_test",
            "urn:li:glossaryTerm:double_meta_property",
        ]
    )

    assert isinstance(workunits[5].metadata, MetadataChangeProposalWrapper)
    assert isinstance(workunits[6].metadata, MetadataChangeProposalWrapper)
    assert isinstance(workunits[7].metadata, MetadataChangeProposalWrapper)
    assert isinstance(workunits[8].metadata, MetadataChangeProposalWrapper)
    assert isinstance(workunits[9].metadata, MetadataChangeProposalWrapper)
    assert workunits[6].metadata.aspectName == "glossaryTermKey"
    assert workunits[7].metadata.aspectName == "glossaryTermKey"
    assert workunits[8].metadata.aspectName == "tagKey"
    assert workunits[9].metadata.aspectName == "tagKey"


def test_kafka_source_oauth_cb_configuration():
    with pytest.raises(
        ConfigurationError,
        match=(
            "oauth_cb must be a string representing python function reference "
            "in the format <python-module>:<function-name>."
        ),
    ):
        KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "foobar:9092",
                    "consumer_config": {
                        "oauth_cb": test_kafka_ignore_warnings_on_schema_type
                    },
                }
            }
        )
