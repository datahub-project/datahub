import json
import time
from typing import Optional
from unittest.mock import MagicMock, patch

import datahub.metadata.schema_classes as models
import freezegun
import pytest
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    EntityChangeEvent,
    MetadataChangeLogEvent,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext

from datahub_integrations.propagation.propagation.generic_propagation_action import (
    GenericPropagationAction,
    PropertyPropagationConfig,
)
from datahub_integrations.propagation.propagation.propagation_rule_config import (
    AspectLookup,
    PropagatedMetadata,
    PropagationRelationships,
    PropagationRule,
    RelationshipLookup,
)
from datahub_integrations.propagation.propagation.propagation_utils import (
    DirectionType,
    RelationshipType,
    SourceDetails,
)


@pytest.fixture
def config() -> PropertyPropagationConfig:
    return PropertyPropagationConfig(
        enabled=True,
        propagation_rule=PropagationRule(
            metadataPropagated={
                PropagatedMetadata.DOCUMENTATION: {},
                PropagatedMetadata.TAGS: {},
            },
            entityTypes=["schemaField", "dataset"],
            targetUrnResolution=[
                AspectLookup(
                    aspect_name="Siblings",
                    field="siblings",
                )
            ],
        ),
    )


@pytest.fixture
def graph() -> MagicMock:
    return MagicMock()


@pytest.fixture
def ctx(graph: Optional[AcrylDataHubGraph]) -> PipelineContext:
    return PipelineContext(pipeline_name="test_pipeline", graph=graph)


@pytest.fixture
def action(
    config: PropertyPropagationConfig, ctx: PipelineContext
) -> GenericPropagationAction:
    return GenericPropagationAction(config=config, ctx=ctx)


@pytest.fixture
def event() -> EventEnvelope:
    return EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="DOCUMENTATION",
            entityType="schemaField",
            entityUrn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
            parameters={  # type: ignore
                "fieldPath": "0",
                "description": "test docs",
            },
            operation="UPDATE",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )


def mcl_event() -> EventEnvelope:
    source_details = SourceDetails(
        origin=None,
        via=None,
        actor="urn:li:corpuser:__datahub_system",
        propagated=None,
        propagation_started_at=int(time.time() * 1000),
        propagation_depth=1,
        propagation_relationship=RelationshipType.LINEAGE,
        propagation_direction=DirectionType.UP,
    )
    attribution = models.MetadataAttributionClass(
        source="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
        actor="urn:li:corpuser:__datahub_system",
        time=1234567890,
        sourceDetail=source_details.for_metadata_attribution(),
    )

    return EventEnvelope(
        event_type="MetadataChangeLogEvent_v1",
        event=MetadataChangeLogEvent(
            aspectName="documentation",
            entityType="schemaField",
            entityUrn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
            aspect=models.GenericAspectClass(
                contentType="application/json",
                value=json.dumps(
                    models.DocumentationClass(
                        documentations=[
                            models.DocumentationAssociationClass(
                                documentation="test docs", attribution=attribution
                            )
                        ]
                    ).to_obj()
                ).encode("utf-8"),
            ),
            changeType="UPDATE",
        ),
        meta=MagicMock(),
    )


@pytest.fixture
def tag_event() -> EventEnvelope:
    return EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="TAG",
            entityType="dataset",
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
            modifier="urn:li:tag:tag1",
            operation="ADD",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )


def create_action(
    graph_mock: Optional[MagicMock] = None,
    propagation_config: Optional[PropertyPropagationConfig] = None,
) -> GenericPropagationAction:
    config = (
        PropertyPropagationConfig(
            propagation_rule=PropagationRule(
                metadataPropagated={
                    PropagatedMetadata.DOCUMENTATION: {},
                    PropagatedMetadata.TAGS: {},
                },
                entityTypes=["schemaField", "dataset"],
                targetUrnResolution=[
                    RelationshipLookup(
                        type=PropagationRelationships.DOWNSTREAM,
                    ),
                    RelationshipLookup(
                        type=PropagationRelationships.UPSTREAM,
                    ),
                    AspectLookup(
                        aspect_name="Siblings",
                        field="siblings",
                    ),
                ],
            )
        )
        if not propagation_config
        else propagation_config
    )
    graph = MagicMock() if not graph_mock else graph_mock
    ctx = PipelineContext(pipeline_name="test_pipeline", graph=graph)
    return GenericPropagationAction(config=config, ctx=ctx)


def test_act_async_siblings() -> None:
    action = create_action()
    action._rate_limited_emit_mcp = MagicMock()
    graph_mock = MagicMock()
    graph_mock.graph = MagicMock()
    graph_mock.graph.get_aspect.configure_mock(
        side_effect=lambda urn, aspect_type: {
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
                models.SiblingsClass,
            ): models.SiblingsClass(
                siblings=[
                    "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_sibling1,PROD)"
                ],
                primary=True,
            ),
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_sibling1,PROD)",
                models.SchemaMetadataClass,
            ): models.SchemaMetadataClass(
                schemaName="my_database.my_table_sibling1",
                platform="urn:li:dataPlatform:hive",
                version=1,
                platformSchema=models.MySqlDDLClass(tableSchema="table_schema"),
                fields=[
                    models.SchemaFieldClass(
                        fieldPath="0",
                        type=models.SchemaFieldDataTypeClass(models.StringTypeClass()),
                        description="sibling's field",
                        nullable=True,
                        recursive=False,
                        nativeDataType="string",
                    )
                ],
                hash="hash",
            ),
        }.get((urn, aspect_type))
    )
    action = create_action(graph_mock)
    test_event = EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="DOCUMENTATION",
            entityType="schemaField",
            entityUrn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
            parameters={  # type: ignore
                "fieldPath": "0",
                "description": "test docs",
            },
            operation="UPDATE",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )
    results = list(action.act_async(test_event))
    assert len(results) == 1


@patch(
    "datahub_integrations.propagation.propagation.propagator.EntityPropagator.create_property_change_proposal"
)
def test_propagation_upstream(mock_create_property_change_proposal: MagicMock) -> None:
    action = create_action()
    action._rate_limited_emit_mcp = MagicMock()
    graph_mock = MagicMock()
    graph_mock.graph = MagicMock()
    mock_create_property_change_proposal.return_value = MagicMock()
    graph_mock.get_relationships.configure_mock(
        side_effect=lambda entity_urn, direction, relationship_types: {
            (
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
                "OUTGOING",
                ("DownstreamOf",),
            ): [
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD),0)"
            ]
        }.get((entity_urn, direction, tuple(relationship_types)), [])
    )
    graph_mock.graph.get_aspect.configure_mock(
        side_effect=lambda urn, aspect_type: {
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD)",
                models.SchemaMetadataClass,
            ): models.SchemaMetadataClass(
                schemaName="my_database.my_table_upstream1",
                platform="urn:li:dataPlatform:hive",
                version=1,
                platformSchema=models.MySqlDDLClass(tableSchema="table_schema"),
                fields=[
                    models.SchemaFieldClass(
                        fieldPath="0",
                        type=models.SchemaFieldDataTypeClass(models.StringTypeClass()),
                        description="upstream's field",
                        nullable=True,
                        recursive=False,
                        nativeDataType="string",
                    )
                ],
                hash="hash",
            )
        }.get((urn, aspect_type))
    )

    action = create_action(graph_mock)
    test_event = EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="DOCUMENTATION",
            entityType="schemaField",
            entityUrn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
            parameters={  # type: ignore
                "fieldPath": "0",
                "description": "test docs",
            },
            operation="UPDATE",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )
    results = list(action.act_async(test_event))
    assert len(results) == 1
    assert (
        results[0].entityUrn
        == "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD),0)"
    )
    assert results[0].aspectName == "documentation"
    documentation_aspect = results[0].aspect
    assert isinstance(documentation_aspect, models.DocumentationClass)
    assert documentation_aspect.documentations[0].documentation == "test docs"


@freezegun.freeze_time("2025-01-01 00:00:00+00:00")
@patch(
    "datahub_integrations.propagation.propagation.propagator.EntityPropagator.create_property_change_proposal"
)
def test_doc_propagation_upstream_mcl(
    mock_create_property_change_proposal: MagicMock,
) -> None:
    graph_mock = MagicMock()
    graph_mock.graph = MagicMock()
    mock_create_property_change_proposal.return_value = MagicMock()

    graph_mock.get_relationships.configure_mock(
        side_effect=lambda entity_urn, direction, relationship_types: {
            (
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
                "OUTGOING",
                ("DownstreamOf",),
            ): [
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD),0)"
            ]
        }.get((entity_urn, direction, tuple(relationship_types)), [])
    )

    graph_mock.graph.get_aspect.configure_mock(
        side_effect=lambda urn, aspect_type: {
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD)",
                models.SchemaMetadataClass,
            ): models.SchemaMetadataClass(
                schemaName="my_database.my_table_upstream1",
                platform="urn:li:dataPlatform:hive",
                version=1,
                platformSchema=models.MySqlDDLClass(tableSchema="table_schema"),
                fields=[
                    models.SchemaFieldClass(
                        fieldPath="0",
                        type=models.SchemaFieldDataTypeClass(models.StringTypeClass()),
                        description="upstream's field",
                        nullable=True,
                        recursive=False,
                        nativeDataType="string",
                    )
                ],
                hash="hash",
            )
        }.get((urn, aspect_type))
    )

    action = create_action(graph_mock)
    test_event = mcl_event()
    results = list(action.act_async(test_event))
    assert len(results) == 1
    res = results[0]
    assert (
        res.entityUrn
        == "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD),0)"
    )
    assert res.aspectName == "documentation"
    documentation_aspect = res.aspect
    assert isinstance(documentation_aspect, models.DocumentationClass)
    assert documentation_aspect.documentations[0].documentation == "test docs"


@freezegun.freeze_time("2025-01-01 00:00:00+00:00")
def test_tag_propagation_upstream() -> None:
    graph_mock = MagicMock()
    graph_mock.graph = MagicMock()

    graph_mock.get_relationships.configure_mock(
        side_effect=lambda entity_urn, direction, relationship_types: {
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
                "OUTGOING",
                ("DownstreamOf",),
            ): [
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD)"
            ]
        }.get((entity_urn, direction, tuple(relationship_types)), [])
    )
    graph_mock.graph.get_aspect.configure_mock(
        side_effect=lambda urn, aspect_type: None
    )

    action = create_action(graph_mock)
    test_event = EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="TAG",
            entityType="dataset",
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
            modifier="urn:li:tag:tag1",
            operation="ADD",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )
    list(action.act_async(test_event))
    graph_mock.add_tags_to_dataset.assert_called_with(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD)",
        ["urn:li:tag:tag1"],
        context={
            "origin": "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
            "via": "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
            "propagated": "true",
            "actor": "urn:li:corpuser:__datahub_system",
            "propagation_started_at": str(1735689600000),
            "propagation_depth": str(1),
            "propagation_relationship": RelationshipType.LINEAGE.value,
            "propagation_direction": DirectionType.UP.value,
        },
    )


@freezegun.freeze_time("2025-01-01 00:00:00+00:00")
def test_term_propagation_upstream(monkeypatch: pytest.MonkeyPatch) -> None:
    # Configure the mock's behavior
    config = PropertyPropagationConfig(
        enabled=True,
        propagation_rule=PropagationRule(
            targetUrnResolution=[
                RelationshipLookup(
                    type=PropagationRelationships.UPSTREAM,
                )
            ],
            entityTypes=["schemaField", "dataset"],
            metadataPropagated={
                PropagatedMetadata.TERMS: {},
            },
        ),
    )

    graph_mock = MagicMock()
    graph_mock.graph = MagicMock()
    graph_mock.get_relationships.configure_mock(
        side_effect=lambda entity_urn, direction, relationship_types: {
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
                "OUTGOING",
                ("DownstreamOf",),
            ): [
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD)"
            ]
        }.get((entity_urn, direction, tuple(relationship_types)), [])
    )

    action = create_action(graph_mock, config)
    test_event = EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="GLOSSARY_TERM",
            entityType="dataset",
            entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
            modifier="urn:li:term:my_glossary_term",
            operation="ADD",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )

    results = list(action.act_async(test_event))

    # Check that we got results (the real propagation should work)
    assert len(results) > 0


@freezegun.freeze_time("2025-01-01 00:00:00+00:00")
def test_field_term_propagation_upstream(monkeypatch: pytest.MonkeyPatch) -> None:
    # Configure the mock's behavior
    config = PropertyPropagationConfig(
        enabled=True,
        propagation_rule=PropagationRule(
            targetUrnResolution=[
                RelationshipLookup(
                    type=PropagationRelationships.UPSTREAM,
                )
            ],
            entityTypes=["schemaField", "dataset"],
            metadataPropagated={
                PropagatedMetadata.TERMS: {},
            },
        ),
    )

    graph_mock = MagicMock()
    graph_mock.graph = MagicMock()
    graph_mock.get_relationships.configure_mock(
        side_effect=lambda entity_urn, direction, relationship_types: {
            (
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
                "OUTGOING",
                ("DownstreamOf",),
            ): [
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD),0)"
            ]
        }.get((entity_urn, direction, tuple(relationship_types)), [])
    )

    graph_mock.graph.get_aspect.configure_mock(
        side_effect=lambda urn, aspect_type: {
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD)",
                models.SchemaMetadataClass,
            ): models.SchemaMetadataClass(
                schemaName="my_database.my_table_upstream1",
                platform="urn:li:dataPlatform:hive",
                version=1,
                platformSchema=models.MySqlDDLClass(tableSchema="table_schema"),
                fields=[
                    models.SchemaFieldClass(
                        fieldPath="0",
                        type=models.SchemaFieldDataTypeClass(models.StringTypeClass()),
                        description="upstream's field",
                        nullable=True,
                        recursive=False,
                        nativeDataType="string",
                    )
                ],
                hash="hash",
            )
        }.get((urn, aspect_type))
    )

    action = create_action(graph_mock, config)
    test_event = EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="GLOSSARY_TERM",
            entityType="schemaField",
            entityUrn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
            modifier="urn:li:term:my_glossary_term",
            operation="ADD",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )

    results = list(action.act_async(test_event))
    assert len(results) == 1
    res = results[0]
    assert (
        res.entityUrn
        == "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_upstream1,PROD)"
    )
    assert res.aspectName == "editableSchemaMetadata"
    assert isinstance(res.aspect, models.GenericAspectClass)
    patch_ops = json.loads(res.aspect.value)
    # There should be an op to add the fieldPath
    assert any(
        op["op"] == "add" and op["path"].endswith("fieldPath") and op["value"] == "0"
        for op in patch_ops
    )
    # There should be an op to add the glossary term
    assert any(
        op["op"] == "add"
        and op["path"].endswith("glossaryTerms/terms/urn:li:term:my_glossary_term")
        and "urn:li:term:my_glossary_term" in json.dumps(op["value"])
        for op in patch_ops
    )


def test_propagation_disabled(
    config: PropertyPropagationConfig,
    action: GenericPropagationAction,
    event: EventEnvelope,
) -> None:
    config.enabled = False
    action._rate_limited_emit_mcp = MagicMock()
    results = list(action.act_async(event))
    assert len(results) == 0
    assert action._rate_limited_emit_mcp.call_count == 0


def test_sibling_propagation() -> None:
    config = PropertyPropagationConfig(
        enabled=True,
        propagation_rule=PropagationRule(
            targetUrnResolution=[
                AspectLookup(
                    aspect_name="Siblings",
                    field="siblings",
                )
            ],
            entityTypes=["schemaField", "dataset"],
            metadataPropagated={
                PropagatedMetadata.DOCUMENTATION: {"columns_enabled": True},
                PropagatedMetadata.TAGS: {},
            },
        ),
    )

    graph_mock = MagicMock()
    graph_mock.graph = MagicMock()
    graph_mock.graph.get_aspect.configure_mock(
        side_effect=lambda urn, aspect_type: {
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
                models.SiblingsClass,
            ): models.SiblingsClass(
                siblings=[
                    "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_sibling1,PROD)"
                ],
                primary=True,
            ),
            (
                "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_sibling1,PROD)",
                models.SchemaMetadataClass,
            ): models.SchemaMetadataClass(
                schemaName="my_database.my_table_sibling1",
                platform="urn:li:dataPlatform:hive",
                version=1,
                platformSchema=models.MySqlDDLClass(tableSchema="table_schema"),
                fields=[
                    models.SchemaFieldClass(
                        fieldPath="0",
                        type=models.SchemaFieldDataTypeClass(models.StringTypeClass()),
                        description="sibling's field",
                        nullable=True,
                        recursive=False,
                        nativeDataType="string",
                    )
                ],
                hash="hash",
            ),
        }.get((urn, aspect_type))
    )
    action = create_action(graph_mock=graph_mock, propagation_config=config)
    action._rate_limited_emit_mcp = MagicMock()

    test_event = EventEnvelope(
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="DOCUMENTATION",
            entityType="schemaField",
            entityUrn="urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),0)",
            parameters={  # type: ignore
                "fieldPath": "0",
                "description": "test docs",
            },
            operation="UPDATE",
            version=1,
            auditStamp=models.AuditStampClass(
                time=1234567890, actor="urn:li:corpuser:__datahub_system"
            ),
        ),
        meta=MagicMock(),
    )
    results = list(action.act_async(test_event))

    assert len(results) == 1
    assert (
        results[0].entityUrn
        == "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_sibling1,PROD),0)"
    )
    assert results[0].aspectName == "documentation"
    documentation_aspect = results[0].aspect
    assert documentation_aspect.documentations[0].documentation == "test docs"  # type: ignore


def test_unsupported_property_type(action: GenericPropagationAction) -> None:
    aspect_value = MagicMock()
    aspect_value.value = json.dumps({"unsupported_property": "value"})

    result = action._extract_property_value("unsupported_property", aspect_value)
    assert result is None
