from typing import Dict, Iterator, List, Optional
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.schema_registry.schema_registry_client import (
    Schema,
    SchemaRegistryClient,
)

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kafka.kafka import KafkaSource
from datahub.ingestion.source.kafka.kafka_constants import ResolutionMethod
from datahub.ingestion.source.kafka.kafka_schema_registry_base import (
    KafkaSchemaRegistryBase,
    SchemaAndFields,
)
from datahub.ingestion.source.kafka.schema_resolution import SchemaResolutionResult
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    KafkaSchema,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
)

KAFKA_MODULE = "datahub.ingestion.source.kafka.kafka"
REGISTRY_MODULE = "datahub.ingestion.source.confluent_schema_registry"


def _field(path: str) -> SchemaField:
    return SchemaField(
        fieldPath=path,
        type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
        nativeDataType="long",
    )


def _resolved(fields: List[SchemaField]) -> SchemaResolutionResult:
    return SchemaResolutionResult(
        schema=None,
        fields=fields,
        resolution_method=ResolutionMethod.SCHEMA_INFERENCE,
    )


def _unresolved() -> SchemaResolutionResult:
    return SchemaResolutionResult(
        schema=None, fields=[], resolution_method=ResolutionMethod.NONE
    )


@pytest.fixture
def source() -> Iterator[KafkaSource]:
    with (
        patch(f"{KAFKA_MODULE}.confluent_kafka.Consumer", autospec=True) as consumer,
        patch(f"{KAFKA_MODULE}.AdminClient", autospec=True),
        patch(f"{REGISTRY_MODULE}.SchemaRegistryClient", autospec=True) as registry,
    ):
        registry.return_value.get_subjects.return_value = []
        metadata = MagicMock()
        metadata.topics = {"events": None}
        consumer.return_value.list_topics.return_value = metadata
        kafka_source = KafkaSource.create(
            {
                "connection": {"bootstrap": "localhost:9092"},
                "schema_resolution": {"enabled": True},
            },
            PipelineContext(run_id="test"),
        )
        yield kafka_source
        kafka_source.close()


def test_inferred_fields_are_emitted_for_schemaless_topics(source: KafkaSource) -> None:
    inferred = [_field("amount"), _field("customer.id")]

    def resolve(topics: List[str], is_key_schema: bool):  # type: ignore[no-untyped-def]
        return {
            t: _unresolved() if is_key_schema else _resolved(inferred) for t in topics
        }

    with patch(f"{KAFKA_MODULE}.KafkaSchemaResolver", autospec=True) as resolver_cls:
        resolver_cls.return_value.resolve_schemas_batch.side_effect = resolve
        schema_aspects = [
            wu.metadata.aspect
            for wu in source.get_workunits()
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, SchemaMetadataClass)
        ]

    # The registry knows nothing about "events", so without resolution there
    # would be no schemaMetadata at all.
    [schema] = schema_aspects
    assert [f.fieldPath for f in schema.fields] == ["amount", "customer.id"]
    resolver_cls.return_value.resolve_schemas_batch.assert_any_call(
        ["events"], is_key_schema=False
    )


def test_only_topics_without_registry_schema_are_resolved(source: KafkaSource) -> None:
    registered = SchemaAndFields(schema=Schema('{"type":"string"}', "AVRO"))
    value_schemas: Dict[str, SchemaAndFields] = {
        "registered": registered,
        "empty": SchemaAndFields(),
    }
    key_schemas: Dict[str, SchemaAndFields] = {}
    resolved_fields = [_field("a")]

    def resolve(topics: List[str], is_key_schema: bool):  # type: ignore[no-untyped-def]
        return {
            t: _unresolved() if is_key_schema else _resolved(resolved_fields)
            for t in topics
        }

    with patch(f"{KAFKA_MODULE}.KafkaSchemaResolver", autospec=True) as resolver_cls:
        resolver_cls.return_value.resolve_schemas_batch.side_effect = resolve
        source._resolve_missing_schemas(
            ["registered", "empty", "unknown"], value_schemas, key_schemas
        )
        # A second pass must reuse the resolver (it caches sampled messages and
        # registry subjects) rather than building a new one.
        source._resolve_missing_schemas(["unknown"], value_schemas, key_schemas)

    resolver_cls.assert_called_once()
    first_call = resolver_cls.return_value.resolve_schemas_batch.call_args_list[0]
    assert first_call.args[0] == ["empty", "unknown"]
    # Registry-backed topics are untouched; resolved ones get their fields; keys
    # that could not be resolved get an empty placeholder so lookups don't KeyError.
    assert value_schemas["registered"] is registered
    assert value_schemas["empty"].fields == resolved_fields
    assert value_schemas["unknown"].fields == resolved_fields
    assert key_schemas["empty"] == SchemaAndFields()
    assert key_schemas["unknown"] == SchemaAndFields()


def test_nothing_to_resolve_skips_building_the_resolver(source: KafkaSource) -> None:
    value_schemas = {"t": SchemaAndFields(fields=[_field("a")])}

    with patch(f"{KAFKA_MODULE}.KafkaSchemaResolver", autospec=True) as resolver_cls:
        source._resolve_missing_schemas(["t"], value_schemas, {})

    resolver_cls.assert_not_called()


class _MinimalRegistry(KafkaSchemaRegistryBase):
    """A custom `schema_registry_class` that only implements the abstract methods."""

    def __init__(self, metadata: Dict[str, Optional[SchemaMetadata]]) -> None:
        self._metadata = metadata
        self.calls: List[str] = []

    def get_schema_metadata(
        self, topic: str, platform_urn: str, is_subject: bool
    ) -> Optional[SchemaMetadata]:
        self.calls.append(topic)
        if topic == "broken":
            raise RuntimeError("lookup failed")
        return self._metadata.get(topic)

    def get_subjects(self) -> List[str]:
        return []

    def _get_subject_for_topic(
        self, dataset_subtype: str, is_key_schema: bool
    ) -> Optional[str]:
        return None

    def get_schema_registry_client(self) -> SchemaRegistryClient:
        raise NotImplementedError


def _schema_metadata(fields: List[SchemaField]) -> SchemaMetadata:
    return SchemaMetadata(
        schemaName="t",
        platform="urn:li:dataPlatform:kafka",
        version=0,
        hash="",
        platformSchema=KafkaSchema(documentSchema=""),
        fields=fields,
    )


def test_base_registry_batch_falls_back_to_per_topic_lookups() -> None:
    fields = [_field("a")]
    registry = _MinimalRegistry(
        {"with_fields": _schema_metadata(fields), "no_fields": _schema_metadata([])}
    )

    result = registry.get_schema_and_fields_batch(
        ["with_fields", "no_fields", "missing", "broken"]
    )

    assert result["with_fields"].fields == fields
    assert result["no_fields"] == SchemaAndFields()
    assert result["missing"] == SchemaAndFields()
    # A failing lookup degrades that topic to schemaless instead of failing the batch.
    assert result["broken"] == SchemaAndFields()


def test_base_registry_builds_schema_metadata_via_lookup() -> None:
    metadata = _schema_metadata([_field("a")])
    registry = _MinimalRegistry({"t": metadata})

    built = registry.build_schema_metadata_with_key(
        "t", "urn:li:dataPlatform:kafka", None, [], None, []
    )

    assert built is metadata
    assert registry.calls == ["t"]
