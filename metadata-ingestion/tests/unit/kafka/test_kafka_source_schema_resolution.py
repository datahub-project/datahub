from typing import Dict, List, Optional
from unittest.mock import patch

import pytest
from confluent_kafka.schema_registry.schema_registry_client import (
    Schema,
    SchemaRegistryClient,
)

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
from tests.unit.kafka.conftest import (
    KAFKA_MODULE,
    KafkaClientMocks,
    SourceFactory,
    emitted_aspects,
)


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
def source(
    kafka_clients: KafkaClientMocks, make_kafka_source: SourceFactory
) -> KafkaSource:
    """One topic ("events") the registry knows nothing about, with resolution on."""
    kafka_clients.set_topics(["events"])
    return make_kafka_source({"schema_resolution": {"enabled": True}})


def test_inferred_fields_are_emitted_for_schemaless_topics(source: KafkaSource) -> None:
    """Without resolution the registry-less topic would get no schemaMetadata at all."""
    inferred = [_field("amount"), _field("customer.id")]

    def resolve(topics: List[str], is_key_schema: bool):  # type: ignore[no-untyped-def]
        return {
            t: _unresolved() if is_key_schema else _resolved(inferred) for t in topics
        }

    with patch(f"{KAFKA_MODULE}.KafkaSchemaResolver", autospec=True) as resolver_cls:
        resolver_cls.return_value.resolve_schemas_batch.side_effect = resolve
        [schema] = emitted_aspects(source, SchemaMetadataClass)

    assert [f.fieldPath for f in schema.fields] == ["amount", "customer.id"]
    resolver_cls.return_value.resolve_schemas_batch.assert_any_call(
        ["events"], is_key_schema=False
    )


def test_only_topics_without_registry_schema_are_resolved(source: KafkaSource) -> None:
    """Registry-backed topics are untouched, resolved ones get their fields, keys
    that could not be resolved get an empty placeholder so lookups don't
    KeyError, and a second pass reuses the resolver (it caches sampled
    messages and registry subjects) rather than building a new one."""
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
        source._resolve_missing_schemas(["unknown"], value_schemas, key_schemas)

    resolver_cls.assert_called_once()
    first_call = resolver_cls.return_value.resolve_schemas_batch.call_args_list[0]
    assert first_call.args[0] == ["empty", "unknown"]
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
    """The base class's batch default is per-topic lookups; a failing lookup
    degrades that topic to schemaless instead of failing the batch."""
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
    assert result["broken"] == SchemaAndFields()


def test_base_registry_builds_schema_metadata_via_lookup() -> None:
    metadata = _schema_metadata([_field("a")])
    registry = _MinimalRegistry({"t": metadata})

    built = registry.build_schema_metadata_with_key(
        "t", "urn:li:dataPlatform:kafka", None, [], None, []
    )

    assert built is metadata
    assert registry.calls == ["t"]
