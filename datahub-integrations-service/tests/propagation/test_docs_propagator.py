import time
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.schema import MySqlDDL
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DocumentationAssociationClass,
    DocumentationClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    MetadataAttributionClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from datahub.utilities.urns.urn import Urn
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent

from datahub_integrations.propagation.propagation.docs.docs_propagator import (
    DocPropagationDirective,
    DocsPropagator,
    DocsPropagatorConfig,
)
from datahub_integrations.propagation.propagation.propagation_rule_config import (
    PropagationRelationships,
    PropagationRule,
    RelationshipLookup,
)
from datahub_integrations.propagation.propagation.propagation_utils import (
    SourceDetails,
)


# Documentation Propagator Tests
@pytest.fixture
def docs_propagator_setup() -> Dict[str, Any]:
    """Fixture to set up the docs propagator."""
    # Create mock graph and action URN
    graph = MagicMock(spec=AcrylDataHubGraph)
    action_urn = "urn:li:dataHubAction:test_action"
    actor_urn = "urn:li:corpuser:test_user"

    # Create propagation rule
    propagation_rule = PropagationRule(
        entityTypes=["dataset", "schemaField"],
        targetUrnResolution=[
            RelationshipLookup(type=PropagationRelationships.UPSTREAM)
        ],
    )

    # Create config
    config = DocsPropagatorConfig(
        enabled=True,
        columns_enabled=True,
        propagation_rule=propagation_rule,
    )

    # Create propagator
    docs_propagator = DocsPropagator(
        action_urn,
        graph,
        config,
    )

    return {
        "propagator": docs_propagator,
        "graph": graph,
        "action_urn": action_urn,
        "actor_urn": actor_urn,
        "rule": propagation_rule,
        "config": config,
    }


def test_bootstrap_dataset_documentation(docs_propagator_setup: Any) -> None:
    """Test bootstrapping documentation for dataset fields."""
    # Setup
    docs_propagator = docs_propagator_setup["propagator"]
    graph = docs_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = docs_propagator_setup["actor_urn"]

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Create schema metadata with field descriptions
    schema_metadata = SchemaMetadataClass(
        schemaName="test_schema",
        platform="test_platform",
        version=0,
        hash="test_hash",
        platformSchema=MySqlDDL(tableSchema="test_table_schema"),
        fields=[
            SchemaFieldClass(
                fieldPath="field1",
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="string",
                description="Field 1 description",
                lastModified=AuditStampClass(time=123456, actor=actor_urn),
            ),
            SchemaFieldClass(
                fieldPath="field2",
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="string",
                description="Field 2 description",
                lastModified=AuditStampClass(time=123456, actor=actor_urn),
            ),
        ],
    )

    # Create editable schema metadata with field descriptions
    editable_schema_metadata = EditableSchemaMetadataClass(
        editableSchemaFieldInfo=[
            EditableSchemaFieldInfoClass(
                fieldPath="field3", description="Field 3 description"
            )
        ],
        lastModified=AuditStampClass(time=123456, actor=actor_urn),
    )

    # Mock get_aspect to return our schema metadata
    graph.graph.get_aspect.side_effect = lambda urn, aspect_type: {
        (dataset_urn.urn(), SchemaMetadataClass): schema_metadata,
        (dataset_urn.urn(), EditableSchemaMetadataClass): editable_schema_metadata,
    }.get((urn, aspect_type))

    # Execute
    directives = list(docs_propagator.boostrap_asset(dataset_urn))

    # Assert
    assert len(directives) == 3  # One for each field

    field_paths = ["field1", "field2", "field3"]
    descriptions = ["Field 1 description", "Field 2 description", "Field 3 description"]

    for directive, field_path, description in zip(
        directives, field_paths, descriptions, strict=False
    ):
        assert isinstance(directive, DocPropagationDirective)
        assert directive.propagate is True
        assert directive.doc_string == description
        assert directive.operation == "ADD"
        expected_urn = f"urn:li:schemaField:({dataset_urn.urn()},{field_path})"
        assert directive.entity == expected_urn


def test_modify_docs_on_columns_new_documentation(docs_propagator_setup: Any) -> None:
    """Test adding new documentation to a column."""
    # Setup
    docs_propagator = docs_propagator_setup["propagator"]
    graph = docs_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = docs_propagator_setup["actor_urn"]

    schema_field_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),field1)"
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    field_doc = "New documentation"
    operation = "ADD"

    # Create source details
    source_details = SourceDetails(
        actor=actor_urn,
        origin="urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_origin,PROD)",
        propagation_depth=1,
        propagation_started_at=int(time.time() * 1000),
    )

    # Mock get_aspect to return None (no existing documentation)
    graph.graph.get_aspect.return_value = None

    # Execute
    mcps = list(
        docs_propagator.modify_docs_on_columns(
            graph, operation, schema_field_urn, dataset_urn, field_doc, source_details
        )
    )

    # Assert
    assert len(mcps) == 1
    mcp = mcps[0]
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert mcp.entityUrn == schema_field_urn
    assert isinstance(mcp.aspect, DocumentationClass)
    assert len(mcp.aspect.documentations) == 1
    assert mcp.aspect.documentations[0].documentation == "New documentation"
    assert mcp.aspect.documentations[0].attribution
    assert (
        mcp.aspect.documentations[0].attribution.source
        == docs_propagator_setup["action_urn"]
    )
    assert mcp.aspect.documentations[0].attribution.actor == actor_urn


def test_modify_docs_on_columns_existing_documentation(
    docs_propagator_setup: Any,
) -> None:
    """Test updating existing documentation on a column."""
    # Setup
    docs_propagator = docs_propagator_setup["propagator"]
    graph = docs_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = docs_propagator_setup["actor_urn"]
    action_urn = docs_propagator_setup["action_urn"]

    schema_field_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),field1)"
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    field_doc = "Updated documentation"
    operation = "MODIFY"

    # Create source details
    source_details = SourceDetails(
        actor=actor_urn,
        origin="urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_origin,PROD)",
        propagation_depth=1,
        propagation_started_at=int(time.time() * 1000),
    )

    # Create existing documentation with attribution
    existing_attribution = MetadataAttributionClass(
        source=action_urn,
        time=123456,
        actor=actor_urn,
        sourceDetail={
            "origin": "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table_origin,PROD)"
        },
    )

    existing_docs = DocumentationClass(
        documentations=[
            DocumentationAssociationClass(
                documentation="Original documentation", attribution=existing_attribution
            )
        ]
    )

    # Mock get_aspect to return existing documentation
    graph.graph.get_aspect.return_value = existing_docs

    # Execute
    mcps = list(
        docs_propagator.modify_docs_on_columns(
            graph, operation, schema_field_urn, dataset_urn, field_doc, source_details
        )
    )

    # Assert
    assert len(mcps) == 1
    mcp = mcps[0]
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert mcp.entityUrn == schema_field_urn
    assert isinstance(mcp.aspect, DocumentationClass)
    assert len(mcp.aspect.documentations) == 1
    assert mcp.aspect.documentations[0].documentation == "Updated documentation"
    assert mcp.aspect.documentations[0].attribution
    assert mcp.aspect.documentations[0].attribution.source == action_urn
    assert mcp.aspect.documentations[0].attribution.actor == actor_urn


def test_process_mce_for_chart_field(docs_propagator_setup: Any) -> None:
    """Test processing documentation MCE for chart fields."""
    # Setup
    docs_propagator = docs_propagator_setup["propagator"]
    actor_urn = docs_propagator_setup["actor_urn"]

    chart_urn = "urn:li:chart:test_chart"
    field_path = "test_field"
    schema_field_urn = f"urn:li:schemaField:({chart_urn},{field_path})"

    # Create event
    event = EventEnvelope(
        meta={},  # Not used
        event_type="EntityChangeEvent_v1",
        event=EntityChangeEvent(
            category="DOCUMENTATION",
            entityType="schemaField",
            entityUrn=schema_field_urn,
            parameters={
                "description": "Chart field documentation",
                "origin": "urn:li:chart:origin",
            },
            operation="ADD",
            auditStamp=AuditStampClass(time=123456, actor=actor_urn),
            version=0,
        ),
    )

    # Execute
    directive = docs_propagator.process_ece(event)

    # Assert
    assert isinstance(directive, DocPropagationDirective)
    assert directive.propagate is True
    assert directive.doc_string == "Chart field documentation"
    assert directive.operation == "ADD"
    assert directive.entity == schema_field_urn
    assert directive.origin == "urn:li:chart:origin"
    assert directive.actor == actor_urn
    assert directive.propagation_depth == 1


def test_skip_self_propagation(docs_propagator_setup: Any) -> None:
    """Test that propagation to self is skipped."""
    # Setup
    docs_propagator = docs_propagator_setup["propagator"]
    graph = docs_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = docs_propagator_setup["actor_urn"]

    schema_field_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD),field1)"
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    field_doc = "Test documentation"
    operation = "ADD"

    # Source origin matches schema_field_urn to trigger skip
    source_details = SourceDetails(
        actor=actor_urn,
        origin=schema_field_urn,
        propagation_depth=1,
        propagation_started_at=123456,
    )

    # Execute
    mcps = list(
        docs_propagator.modify_docs_on_columns(
            graph, operation, schema_field_urn, dataset_urn, field_doc, source_details
        )
    )

    # Assert - should not emit any MCPs when propagating to self
    assert len(mcps) == 0
