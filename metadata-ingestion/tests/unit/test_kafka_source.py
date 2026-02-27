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
    make_data_platform_urn,
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
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    DataPlatformInstanceClass,
    KafkaSchemaClass,
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
        KafkaSourceConfig.model_validate({"connection": {"bootstrap": "foobar:9092"}}),
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
        KafkaSourceConfig.model_validate(
            {"connection": {"bootstrap": "localhost:9092"}}
        ),
        ctx,
    )
    workunits = list(kafka_source.get_workunits())

    mock_kafka.assert_called_once()
    mock_kafka_instance.list_topics.assert_called_once()
    assert len(workunits) == 10


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
    assert len(workunits) == 5

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
    assert len(workunits) == 10


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

    assert len(workunits) == 5

    expected_urn = make_dataset_urn_with_platform_instance(
        platform=PLATFORM,
        name=TOPIC_NAME,
        platform_instance=PLATFORM_INSTANCE,
        env="PROD",
    )

    for wu in workunits:
        assert isinstance(wu, MetadataWorkUnit)
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        assert wu.metadata.entityUrn == expected_urn

    data_platform_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "dataPlatformInstance"
    ]
    assert len(data_platform_mcps) == 1
    data_platform_aspect = data_platform_mcps[0].aspect
    assert isinstance(data_platform_aspect, DataPlatformInstanceClass)
    assert data_platform_aspect.instance == make_dataplatform_instance_urn(
        PLATFORM, PLATFORM_INSTANCE
    )

    browse_path_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "browsePathsV2"
    ]
    assert len(browse_path_mcps) == 1
    browse_paths_aspect = browse_path_mcps[0].aspect
    assert isinstance(browse_paths_aspect, BrowsePathsV2Class)
    path_ids = [entry.id for entry in browse_paths_aspect.path]
    platform_instance_urn = make_dataplatform_instance_urn(PLATFORM, PLATFORM_INSTANCE)
    assert path_ids == [platform_instance_urn, "prod", PLATFORM]


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

    assert len(workunits) == 5

    expected_urn = make_dataset_urn(
        platform=PLATFORM,
        name=TOPIC_NAME,
        env="PROD",
    )

    for wu in workunits:
        assert isinstance(wu, MetadataWorkUnit)
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        assert wu.metadata.entityUrn == expected_urn

    data_platform_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "dataPlatformInstance"
    ]
    assert len(data_platform_mcps) == 1
    data_platform_aspect = data_platform_mcps[0].aspect
    assert isinstance(data_platform_aspect, DataPlatformInstanceClass)
    assert data_platform_aspect.platform == make_data_platform_urn(PLATFORM)
    assert data_platform_aspect.instance is None

    browse_path_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "browsePathsV2"
    ]
    assert len(browse_path_mcps) == 1
    browse_paths_aspect = browse_path_mcps[0].aspect
    assert isinstance(browse_paths_aspect, BrowsePathsV2Class)
    path_ids = [entry.id for entry in browse_paths_aspect.path]
    assert path_ids == ["prod", PLATFORM]


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
                guid=None,
                schema_id="schema_id_2",
                schema=Schema(
                    schema_str='{"type":"record", "name":"Topic1Key", "namespace": "test.acryl", "fields": [{"name":"t1key", "type": "string"}]}',
                    schema_type="AVRO",
                ),
                subject="topic1-key",
                version=1,
            ),
            RegisteredSchema(
                guid=None,
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
                guid=None,
                schema_id="schema_id_3",
                schema=Schema(
                    schema_str='{"type":"record", "name":"Topic2Key", "namespace": "test.acryl", "fields": [{"name":"t2key", "type": "string"}]}',
                    schema_type="AVRO",
                ),
                subject="test.acryl.Topic2Key",
                version=1,
            ),
            RegisteredSchema(
                guid=None,
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
                guid=None,
                schema_id="schema_id_4",
                schema=Schema(
                    schema_str='{"type":"record", "name":"Topic3Key", "namespace": "test.acryl", "fields": [{"name":"t3key", "type": "string"}]}',
                    schema_type="AVRO",
                ),
                subject="topic3-test.acryl.Topic3Key-key",
                version=1,
            ),
            RegisteredSchema(
                guid=None,
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
    mock_cluster_metadata.topics = {k: None for k in topic_subject_schema_map}
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

    schema_metadata_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "schemaMetadata"
    ]

    assert len(schema_metadata_mcps) == 9

    for mcp in schema_metadata_mcps:
        schema_metadata = mcp.aspect
        assert isinstance(schema_metadata, SchemaMetadataClass)
        assert isinstance(schema_metadata.platformSchema, KafkaSchemaClass)
        if schema_metadata.schemaName in topic_subject_schema_map:
            # Make sure the schema_str matches for the key schema
            assert (
                schema_metadata.platformSchema.keySchema
                == topic_subject_schema_map[schema_metadata.schemaName][
                    0
                ].schema.schema_str
            )
            # Make sure the schema_type matches for the key schema
            assert (
                schema_metadata.platformSchema.keySchemaType
                == topic_subject_schema_map[schema_metadata.schemaName][
                    0
                ].schema.schema_type
            )
            # Make sure the schema_str matches for the value schema
            assert (
                schema_metadata.platformSchema.documentSchema
                == topic_subject_schema_map[schema_metadata.schemaName][
                    1
                ].schema.schema_str
            )
            # Make sure the schema_type matches for the value schema
            assert (
                schema_metadata.platformSchema.documentSchemaType
                == topic_subject_schema_map[schema_metadata.schemaName][
                    1
                ].schema.schema_type
            )
            # Make sure we have 2 fields, one from the key schema & one from the value schema
            assert len(schema_metadata.fields) == 2


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
        guid=None,
        schema_id="schema_id_2",
        schema=Schema(
            schema_str="{}",
            schema_type="UNKNOWN_TYPE",
        ),
        subject="topic1-key",
        version=1,
    )
    topic1_value_schema = RegisteredSchema(
        guid=None,
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
    assert len(workunits) == 6
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

    assert len(workunits) == 5


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

    assert len(workunits) == 5


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
                guid=None,
                schema_id="schema_id_2",
                schema=Schema(
                    schema_str='{"type":"record", "name":"Topic1Key", "namespace": "test.acryl", "fields": [{"name":"t1key", "type": "string"}]}',
                    schema_type="AVRO",
                ),
                subject="topic1-key",
                version=1,
            ),
            RegisteredSchema(
                guid=None,
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
    mock_cluster_metadata.topics = {k: None for k in topic_subject_schema_map}
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
    workunits = [w for w in kafka_source.get_workunits()]

    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)

    # Check ownership aspects
    ownership_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "ownership"
    ]
    # Should have 2 ownership aspects (1 topic + 1 value schema)
    # Key schema doesn't have meta mappings since the key schema doesn't have owner/tags fields
    assert len(ownership_mcps) == 2
    for ownership_mcp in ownership_mcps:
        assert ownership_mcp.aspect == make_ownership_aspect_from_urn_list(
            [
                make_owner_urn("charles", OwnerType.USER),
                make_owner_urn("jdoe.last@gmail.com", OwnerType.USER),
            ],
            "SERVICE",
        )

    # Check globalTags aspects
    tags_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "globalTags"
    ]
    # Should have 2 globalTags aspects (1 topic + 1 value schema)
    assert len(tags_mcps) == 2
    for tags_mcp in tags_mcps:
        assert tags_mcp.aspect == make_global_tag_aspect_with_tag_list(
            ["has_pii_test", "int_meta_property"]
        )

    # Check glossaryTerms aspects
    terms_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "glossaryTerms"
    ]
    # Should have 2 glossaryTerms aspects (1 topic + 1 value schema)
    assert len(terms_mcps) == 2
    for terms_mcp in terms_mcps:
        assert terms_mcp.aspect == make_glossary_terms_aspect_from_urn_list(
            [
                "urn:li:glossaryTerm:Finance_test",
                "urn:li:glossaryTerm:double_meta_property",
            ]
        )

    # Check subTypes aspects
    subtypes_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "subTypes"
    ]
    assert len(subtypes_mcps) == 3  # 1 topic + 2 schemas

    # Check browsePathsV2 aspects
    browse_paths_v2_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "browsePathsV2"
    ]
    assert len(browse_paths_v2_mcps) == 3  # 1 topic + 2 schemas

    # Check glossaryTermKey aspects (created for glossary terms referenced)
    glossary_term_key_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "glossaryTermKey"
    ]
    assert len(glossary_term_key_mcps) == 2  # 2 glossary terms

    # Check tagKey aspects (created for tags referenced)
    tag_key_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "tagKey"
    ]
    assert len(tag_key_mcps) == 2  # 2 tags


@patch(
    "datahub.ingestion.source.confluent_schema_registry.SchemaRegistryClient",
    autospec=True,
)
@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_with_hyphenated_namespace_schema(
    mock_kafka_consumer, mock_schema_registry_client, mock_admin_client
):
    """Test that schemas with hyphens in namespace (e.g., from Debezium CDC)
    are ingested successfully by default, matching Java Schema Registry client behavior."""
    # Debezium-style schemas where the topic name (with hyphens) is used as namespace
    topic_subject_schema_map: Dict[str, Tuple[RegisteredSchema, RegisteredSchema]] = {
        "my-debezium-topic": (
            RegisteredSchema(
                guid=None,
                schema_id="schema_id_key",
                schema=Schema(
                    schema_str=json.dumps(
                        {
                            "type": "record",
                            "name": "Key",
                            "namespace": "my-debezium-topic.public.users",
                            "fields": [{"name": "id", "type": "int"}],
                        }
                    ),
                    schema_type="AVRO",
                ),
                subject="my-debezium-topic-key",
                version=1,
            ),
            RegisteredSchema(
                guid=None,
                schema_id="schema_id_value",
                schema=Schema(
                    schema_str=json.dumps(
                        {
                            "type": "record",
                            "name": "Value",
                            "namespace": "my-debezium-topic.public.users",
                            "fields": [
                                {"name": "id", "type": "int"},
                                {"name": "name", "type": "string"},
                            ],
                            "doc": "Debezium CDC event for users table",
                        }
                    ),
                    schema_type="AVRO",
                ),
                subject="my-debezium-topic-value",
                version=1,
            ),
        ),
    }

    # Mock the kafka consumer
    mock_kafka_instance = mock_kafka_consumer.return_value
    mock_cluster_metadata = MagicMock()
    mock_cluster_metadata.topics = {k: None for k in topic_subject_schema_map}
    mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

    # Mock the schema registry client
    mock_schema_registry_client.return_value.get_subjects.return_value = [
        v.subject for v in chain(*topic_subject_schema_map.values())
    ]

    def mock_get_latest_version(subject_name: str) -> Optional[RegisteredSchema]:
        for registered_schema in chain(*topic_subject_schema_map.values()):
            if registered_schema.subject == subject_name:
                return registered_schema
        return None

    mock_schema_registry_client.return_value.get_latest_version = (
        mock_get_latest_version
    )

    ctx = PipelineContext(run_id="test_hyphen")
    kafka_source = KafkaSource.create(
        {
            "connection": {"bootstrap": "localhost:9092"},
        },
        ctx,
    )
    workunits = list(kafka_source.get_workunits())

    # Verify schema was parsed and fields were extracted successfully
    schema_metadata_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "schemaMetadata"
    ]
    assert len(schema_metadata_mcps) == 1
    schema_metadata = schema_metadata_mcps[0].aspect
    assert isinstance(schema_metadata, SchemaMetadataClass)
    # Key schema has 1 field, value schema has 2 fields
    assert len(schema_metadata.fields) == 3
    # Verify the schema name contains the topic name
    assert schema_metadata.schemaName == "my-debezium-topic"


def test_kafka_source_oauth_cb_configuration():
    with pytest.raises(
        ConfigurationError,
        match=(
            "oauth_cb must be a string representing python function reference "
            "in the format <python-module>:<function-name>."
        ),
    ):
        KafkaSourceConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "foobar:9092",
                    "consumer_config": {
                        "oauth_cb": test_kafka_ignore_warnings_on_schema_type
                    },
                }
            }
        )


@patch("datahub.ingestion.source.kafka.kafka.get_kafka_consumer")
def test_validate_kafka_connectivity_success(mock_get_kafka_consumer):
    """Test that validate_kafka_connectivity succeeds when Kafka is reachable."""
    from datahub.configuration.kafka import KafkaConsumerConnectionConfig
    from datahub.ingestion.source.kafka.kafka import validate_kafka_connectivity

    # Setup mock consumer with brokers in metadata
    mock_consumer = MagicMock()
    mock_cluster_metadata = MagicMock()
    mock_broker = MagicMock()
    mock_broker.id = 1
    mock_cluster_metadata.brokers = {1: mock_broker}
    mock_consumer.list_topics.return_value = mock_cluster_metadata
    mock_get_kafka_consumer.return_value = mock_consumer

    # Create connection config
    connection = KafkaConsumerConnectionConfig(
        bootstrap="localhost:9092", client_timeout_seconds=30
    )

    # Should not raise any exception
    validate_kafka_connectivity(connection)

    # Verify the consumer was used correctly
    # The function uses max(10, client_timeout_seconds) so should use 30 here
    mock_get_kafka_consumer.assert_called_once_with(connection)
    mock_consumer.list_topics.assert_called_once_with(topic="", timeout=30)
    mock_consumer.close.assert_called_once()


@patch("datahub.ingestion.source.kafka.kafka.get_kafka_consumer")
def test_validate_kafka_connectivity_failure(mock_get_kafka_consumer):
    """Test that validate_kafka_connectivity raises KafkaConnectivityError when Kafka is unreachable."""
    from datahub.configuration.kafka import KafkaConsumerConnectionConfig
    from datahub.ingestion.source.kafka.kafka import (
        KafkaConnectivityError,
        validate_kafka_connectivity,
    )

    # Setup mock consumer to raise an exception
    mock_consumer = MagicMock()
    mock_consumer.list_topics.side_effect = Exception(
        "Failed to connect to broker at localhost:9092"
    )
    mock_get_kafka_consumer.return_value = mock_consumer

    # Create connection config with 60 second timeout
    connection = KafkaConsumerConnectionConfig(
        bootstrap="localhost:9092", client_timeout_seconds=60
    )

    # Should raise KafkaConnectivityError
    with pytest.raises(KafkaConnectivityError) as exc_info:
        validate_kafka_connectivity(connection)

    # Verify the error message contains expected information
    error_message = str(exc_info.value)
    assert "[FATAL] Failed to connect to Kafka" in error_message
    assert "localhost:9092" in error_message
    assert "Failed to connect to broker" in error_message

    # Verify the consumer was used correctly
    # The function uses max(10, client_timeout_seconds) so should use 60 here
    mock_get_kafka_consumer.assert_called_once_with(connection)
    mock_consumer.list_topics.assert_called_once_with(topic="", timeout=60)


@patch("datahub.ingestion.source.kafka.kafka.get_kafka_consumer")
def test_validate_kafka_connectivity_no_brokers(mock_get_kafka_consumer):
    """Test that validate_kafka_connectivity raises KafkaConnectivityError when no brokers are found."""
    from datahub.configuration.kafka import KafkaConsumerConnectionConfig
    from datahub.ingestion.source.kafka.kafka import (
        KafkaConnectivityError,
        validate_kafka_connectivity,
    )

    # Setup mock consumer with empty brokers
    mock_consumer = MagicMock()
    mock_cluster_metadata = MagicMock()
    mock_cluster_metadata.brokers = {}  # No brokers
    mock_consumer.list_topics.return_value = mock_cluster_metadata
    mock_get_kafka_consumer.return_value = mock_consumer

    # Create connection config
    connection = KafkaConsumerConnectionConfig(
        bootstrap="localhost:9092", client_timeout_seconds=30
    )

    # Should raise KafkaConnectivityError
    with pytest.raises(KafkaConnectivityError) as exc_info:
        validate_kafka_connectivity(connection)

    # Verify the error message
    assert "No brokers found in cluster metadata" in str(exc_info.value)

    # Verify the consumer was used correctly
    # The function uses max(10, client_timeout_seconds) so should use 30 here
    mock_get_kafka_consumer.assert_called_once_with(connection)
    mock_consumer.list_topics.assert_called_once_with(topic="", timeout=30)


@patch(
    "datahub.ingestion.source.confluent_schema_registry.SchemaRegistryClient",
    autospec=True,
)
@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_handles_non_iterable_schema_tags(
    mock_kafka_consumer, mock_schema_registry_client, mock_admin_client
):
    """Test that a TypeError from non-iterable schema_tags_field is handled gracefully.

    When the schema's tags field contains a non-iterable value (e.g., an integer),
    the source should log a warning and continue processing without crashing.
    """
    # Schema with non-iterable value in the tags field (integer instead of array)
    topic_subject_schema_map: Dict[str, Tuple[RegisteredSchema, RegisteredSchema]] = {
        "topic_with_bad_tags": (
            RegisteredSchema(
                guid=None,
                schema_id="schema_id_key",
                schema=Schema(
                    schema_str='{"type":"record", "name":"TopicKey", "namespace": "test.acryl", "fields": [{"name":"key", "type": "string"}]}',
                    schema_type="AVRO",
                ),
                subject="topic_with_bad_tags-key",
                version=1,
            ),
            RegisteredSchema(
                guid=None,
                schema_id="schema_id_value",
                schema=Schema(
                    # tags field is an integer (non-iterable) instead of an array
                    schema_str=json.dumps(
                        {
                            "type": "record",
                            "name": "TopicValue",
                            "namespace": "test.acryl",
                            "fields": [{"name": "value", "type": "string"}],
                            "tags": 12345,  # Invalid: should be an array of strings
                        }
                    ),
                    schema_type="AVRO",
                ),
                subject="topic_with_bad_tags-value",
                version=1,
            ),
        )
    }

    # Mock the kafka consumer
    mock_kafka_instance = mock_kafka_consumer.return_value
    mock_cluster_metadata = MagicMock()
    mock_cluster_metadata.topics = {k: None for k in topic_subject_schema_map}
    mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

    # Mock the schema registry client
    mock_schema_registry_client.return_value.get_subjects.return_value = [
        v.subject for v in chain(*topic_subject_schema_map.values())
    ]

    def mock_get_latest_version(subject_name: str) -> Optional[RegisteredSchema]:
        for registered_schema in chain(*topic_subject_schema_map.values()):
            if registered_schema.subject == subject_name:
                return registered_schema
        return None

    mock_schema_registry_client.return_value.get_latest_version = (
        mock_get_latest_version
    )

    ctx = PipelineContext(run_id="test_bad_tags")
    kafka_source = KafkaSource.create(
        {
            "connection": {"bootstrap": "localhost:9092"},
        },
        ctx,
    )

    # Should not raise an exception - the source should handle the TypeError gracefully
    workunits = list(kafka_source.get_workunits())

    assert len(workunits) >= 6

    for wu in workunits:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)

    aspect_names = [
        wu.metadata.aspectName
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
    ]
    expected_aspects = [
        "status",
        "schemaMetadata",
        "browsePathsV2",
        "datasetProperties",
        "subTypes",
        "dataPlatformInstance",
    ]
    for expected_aspect in expected_aspects:
        assert expected_aspect in aspect_names, (
            f"Expected aspect '{expected_aspect}' to be yielded, got: {aspect_names}"
        )

    report = kafka_source.get_report()
    warnings_list = list(report.warnings)
    assert len(warnings_list) > 0
    warning_found = any(
        "Unable to extract tags from schema field" in w.message
        and any("topic_with_bad_tags" in ctx for ctx in w.context)
        for w in warnings_list
    )
    assert warning_found, (
        f"Expected warning about tags extraction, got: {[(w.message, list(w.context)) for w in warnings_list]}"
    )

    tags_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "globalTags"
    ]
    assert len(tags_mcps) == 0


@patch(
    "datahub.ingestion.source.confluent_schema_registry.SchemaRegistryClient",
    autospec=True,
)
@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
def test_kafka_source_handles_valid_schema_tags(
    mock_kafka_consumer, mock_schema_registry_client, mock_admin_client
):
    """Test that valid schema tags are extracted correctly.

    This is a positive test to ensure the tag extraction works when
    the schema_tags_field contains a valid array of strings.
    """
    # Schema with valid tags field (array of strings)
    topic_subject_schema_map: Dict[str, Tuple[RegisteredSchema, RegisteredSchema]] = {
        "topic_with_good_tags": (
            RegisteredSchema(
                guid=None,
                schema_id="schema_id_key",
                schema=Schema(
                    schema_str='{"type":"record", "name":"TopicKey", "namespace": "test.acryl", "fields": [{"name":"key", "type": "string"}]}',
                    schema_type="AVRO",
                ),
                subject="topic_with_good_tags-key",
                version=1,
            ),
            RegisteredSchema(
                guid=None,
                schema_id="schema_id_value",
                schema=Schema(
                    schema_str=json.dumps(
                        {
                            "type": "record",
                            "name": "TopicValue",
                            "namespace": "test.acryl",
                            "fields": [{"name": "value", "type": "string"}],
                            "tags": ["pii", "sensitive", "internal"],
                        }
                    ),
                    schema_type="AVRO",
                ),
                subject="topic_with_good_tags-value",
                version=1,
            ),
        )
    }

    # Mock the kafka consumer
    mock_kafka_instance = mock_kafka_consumer.return_value
    mock_cluster_metadata = MagicMock()
    mock_cluster_metadata.topics = {k: None for k in topic_subject_schema_map}
    mock_kafka_instance.list_topics.return_value = mock_cluster_metadata

    # Mock the schema registry client
    mock_schema_registry_client.return_value.get_subjects.return_value = [
        v.subject for v in chain(*topic_subject_schema_map.values())
    ]

    def mock_get_latest_version(subject_name: str) -> Optional[RegisteredSchema]:
        for registered_schema in chain(*topic_subject_schema_map.values()):
            if registered_schema.subject == subject_name:
                return registered_schema
        return None

    mock_schema_registry_client.return_value.get_latest_version = (
        mock_get_latest_version
    )

    ctx = PipelineContext(run_id="test_good_tags")
    kafka_source = KafkaSource.create(
        {
            "connection": {"bootstrap": "localhost:9092"},
        },
        ctx,
    )

    workunits = list(kafka_source.get_workunits())

    assert len(workunits) >= 8

    tags_mcps = [
        wu.metadata
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.aspectName == "globalTags"
    ]
    assert len(tags_mcps) == 1

    expected_tags = make_global_tag_aspect_with_tag_list(
        ["pii", "sensitive", "internal"]
    )
    assert tags_mcps[0].aspect == expected_tags

    report = kafka_source.get_report()
    warnings_list = list(report.warnings)
    tag_extraction_warnings = [
        w
        for w in warnings_list
        if any("Unable to extract tags from schema field" in ctx for ctx in w.context)
    ]
    assert len(tag_extraction_warnings) == 0
