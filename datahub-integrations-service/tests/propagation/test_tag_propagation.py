import json
import time
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.common import GlobalTags
from datahub.metadata.com.linkedin.pegasus2avro.schema import MySqlDDL
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    MetadataAttributionClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TagAssociationClass,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.utilities.urns.urn import Urn
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent

from datahub_integrations.propagation.propagation.propagation_rule_config import (
    PropagationRelationships,
    PropagationRule,
    RelationshipLookup,
)
from datahub_integrations.propagation.propagation.propagation_utils import (
    SourceDetails,
)
from datahub_integrations.propagation.propagation.tag.tag_propagator import (
    TagPropagationDirective,
    TagPropagator,
    TagPropagatorConfig,
)


@pytest.fixture
def tag_propagator_setup() -> Dict:
    """Fixture to set up the tag propagator."""
    # Create mock graph and action URN
    graph = MagicMock(spec=AcrylDataHubGraph)
    action_urn = "urn:li:dataHubAction:test_action"
    actor_urn = "urn:li:corpuser:test_user"

    # Create propagation rule
    propagation_rule = PropagationRule(
        entity_types=["dataset", "schemaField"],
        target_urn_resolution=[
            RelationshipLookup(type=PropagationRelationships.UPSTREAM)
        ],
    )

    # Create config
    config = TagPropagatorConfig(
        enabled=True,
        propagation_rule=propagation_rule,
    )

    # Create propagator
    tag_propagator = TagPropagator(
        action_urn,
        graph,
        config,
    )

    return {
        "propagator": tag_propagator,
        "graph": graph,
        "action_urn": action_urn,
        "actor_urn": actor_urn,
        "rule": propagation_rule,
        "config": config,
    }


def test_generate_tag_directive(tag_propagator_setup: Any) -> None:
    """Test generating a tag propagation directive."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]
    actor_urn = tag_propagator_setup["actor_urn"]

    entity_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    old_global_tags = None
    current_global_tags = GlobalTags(
        tags=[TagAssociationClass(tag="urn:li:tag:test_tag")]
    )
    source_details = SourceDetails(
        actor=actor_urn,
        origin=entity_urn,
        propagation_depth=1,
        propagation_started_at=12345,
    )

    # Execute
    directive = tag_propagator.generate_directive(
        entity_urn, old_global_tags, current_global_tags, source_details
    )

    # Assert
    assert isinstance(directive, TagPropagationDirective)
    assert directive.propagate is True
    assert directive.tags == ["urn:li:tag:test_tag"]
    assert directive.operation == "ADD"
    assert directive.entity == entity_urn
    assert directive.origin == entity_urn
    assert directive.propagation_depth == 2  # Should increment by 1
    assert directive.actor == actor_urn


def test_process_mce_add_tag(tag_propagator_setup: Any) -> None:
    """Test processing a metadata change event for adding a tag."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]
    actor_urn = tag_propagator_setup["actor_urn"]
    test_tag = "urn:li:tag:test_tag"

    # Create a context in the parameters
    source_details = SourceDetails(
        actor=actor_urn,
        origin="urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
        propagation_depth=1,
        propagation_started_at=12345,
    )

    entity_change_event = EntityChangeEvent(
        category="TAG",
        operation="ADD",
        modifier=test_tag,
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
        parameters={"context": json.dumps(source_details.dict())},
        entityType="dataset",
        auditStamp=AuditStampClass(time=12345, actor=actor_urn),
        version=0,
    )
    event_envelope = EventEnvelope(
        meta={},  # Not needed for this test
        event_type="EntityChangeEvent_v1",
        event=entity_change_event,
    )

    # Execute
    directive = tag_propagator.process_ece(event_envelope)

    # Assert
    assert isinstance(directive, TagPropagationDirective)
    assert directive.propagate is True
    assert directive.tags == [test_tag]
    assert directive.operation == "ADD"
    assert (
        directive.entity
        == "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )


def test_create_property_change_proposal_dataset(tag_propagator_setup: Any) -> None:
    """Test creating property change proposals for a dataset."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]
    graph = tag_propagator_setup["graph"]
    actor_urn = tag_propagator_setup["actor_urn"]

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )
    tag = "urn:li:tag:test_tag"

    # Create directive
    directive = TagPropagationDirective(
        propagate=True,
        tags=[tag],
        operation="ADD",
        entity=dataset_urn.urn(),
        actor=actor_urn,
        origin=dataset_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
        relationships={},
    )

    # Create source details
    source_details = SourceDetails(
        actor=actor_urn,
        origin=dataset_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
    )

    # Execute
    list(
        tag_propagator.create_property_change_proposal(
            directive, dataset_urn, source_details
        )
    )

    # Assert
    graph.add_tags_to_dataset.assert_called_once_with(
        dataset_urn.urn(),
        [tag],
        context=source_details.for_metadata_attribution(),
        action_urn="urn:li:dataHubAction:test_action",
    )


def test_propagate_tag_to_schema_field(tag_propagator_setup: Any) -> None:
    """Test propagating a tag to a schema field."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]
    graph = tag_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = tag_propagator_setup["actor_urn"]

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )
    field_path = "field1"
    schema_field_urn = Urn.from_string(
        f"urn:li:schemaField:({dataset_urn.urn()},{field_path})"
    )
    tag = "urn:li:tag:test_tag"

    # Create directive
    directive = TagPropagationDirective(
        propagate=True,
        tags=[tag],
        operation="ADD",
        entity=schema_field_urn.urn(),
        actor=actor_urn,
        origin=schema_field_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
        relationships={},
    )

    # Create source details
    source_details = SourceDetails(
        actor=actor_urn,
        origin=schema_field_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
    )

    # Mock get_aspect to return None (no existing metadata)
    graph.graph.get_aspect.return_value = None

    # Execute
    mcps = list(
        tag_propagator.create_property_change_proposal(
            directive, schema_field_urn, source_details
        )
    )

    # Assert
    assert len(mcps) == 1
    mcp = mcps[0]
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert mcp.entityUrn == dataset_urn.urn()
    assert isinstance(mcp.aspect, EditableSchemaMetadataClass)
    assert len(mcp.aspect.editableSchemaFieldInfo) == 1
    assert mcp.aspect.editableSchemaFieldInfo[0].fieldPath == field_path
    assert isinstance(mcp.aspect.editableSchemaFieldInfo[0].globalTags, GlobalTags)
    assert len(mcp.aspect.editableSchemaFieldInfo[0].globalTags.tags) == 1
    assert mcp.aspect.editableSchemaFieldInfo[0].globalTags.tags[0].tag == tag


def test_add_tag_to_dataset_field_slow_new_metadata(tag_propagator_setup: Any) -> None:
    """Test adding a tag to a dataset field when no metadata exists."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]
    graph = tag_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = tag_propagator_setup["actor_urn"]
    action_urn = tag_propagator_setup["action_urn"]

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )
    field_path = "field1"
    tag = "urn:li:tag:test_tag"

    # Create field tags map
    field_tags = {field_path: [tag]}

    # Create directive
    directive = TagPropagationDirective(
        propagate=True,
        tags=[tag],
        operation="ADD",
        entity=f"urn:li:schemaField:({dataset_urn.urn()},{field_path})",
        actor=actor_urn,
        origin=dataset_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
        relationships={},
    )

    # Create source details
    source_details = SourceDetails(
        actor=actor_urn,
        origin=dataset_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
    )

    # Mock get_aspect to return None (no existing metadata)
    graph.graph.get_aspect.return_value = None

    # Execute
    mcps = list(
        tag_propagator._add_tag_to_dataset_field_slow(
            dataset_urn, field_tags, directive, source_details
        )
    )

    # Assert
    assert len(mcps) == 1
    mcp = mcps[0]
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert mcp.entityUrn == dataset_urn.urn()
    assert isinstance(mcp.aspect, EditableSchemaMetadataClass)
    assert len(mcp.aspect.editableSchemaFieldInfo) == 1
    assert mcp.aspect.editableSchemaFieldInfo[0].fieldPath == field_path
    assert isinstance(mcp.aspect.editableSchemaFieldInfo[0].globalTags, GlobalTags)
    assert len(mcp.aspect.editableSchemaFieldInfo[0].globalTags.tags) == 1
    assert mcp.aspect.editableSchemaFieldInfo[0].globalTags.tags[0].tag == tag
    assert mcp.aspect.editableSchemaFieldInfo[0].globalTags.tags[0].attribution
    assert (
        mcp.aspect.editableSchemaFieldInfo[0].globalTags.tags[0].attribution.source
        == action_urn
    )
    assert (
        mcp.aspect.editableSchemaFieldInfo[0].globalTags.tags[0].attribution.actor
        == actor_urn
    )


def test_add_tag_to_dataset_field_existing_metadata(tag_propagator_setup: Any) -> None:
    """Test adding a tag to a dataset field when metadata already exists."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]
    graph = tag_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = tag_propagator_setup["actor_urn"]

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )
    field_path = "field1"
    tag = "urn:li:tag:test_tag"

    # Create field tags map
    field_tags = {field_path: [tag]}

    # Create directive
    directive = TagPropagationDirective(
        propagate=True,
        tags=[tag],
        operation="ADD",
        entity=f"urn:li:schemaField:({dataset_urn.urn()},{field_path})",
        actor=actor_urn,
        origin=dataset_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
        relationships={},
    )

    # Create source details
    source_details = SourceDetails(
        actor=actor_urn,
        origin=dataset_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
    )

    # Create existing editable schema metadata
    existing_metadata = EditableSchemaMetadataClass(
        editableSchemaFieldInfo=[
            EditableSchemaFieldInfoClass(fieldPath=field_path, description="Test field")
        ]
    )

    # Mock get_aspect to return existing metadata
    graph.graph.get_aspect.return_value = existing_metadata

    # Execute
    mcps = list(
        tag_propagator._add_tag_to_dataset_field_slow(
            dataset_urn, field_tags, directive, source_details
        )
    )

    # Assert
    assert len(mcps) == 1
    mcp = mcps[0]
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert mcp.entityUrn == dataset_urn.urn()
    assert isinstance(mcp.aspect, EditableSchemaMetadataClass)
    assert len(mcp.aspect.editableSchemaFieldInfo) == 1
    assert mcp.aspect.editableSchemaFieldInfo[0].fieldPath == field_path
    assert isinstance(mcp.aspect.editableSchemaFieldInfo[0].globalTags, GlobalTags)
    assert len(mcp.aspect.editableSchemaFieldInfo[0].globalTags.tags) == 1
    assert mcp.aspect.editableSchemaFieldInfo[0].globalTags.tags[0].tag == tag


def test_add_tag_to_dataset_field_with_existing_tags(tag_propagator_setup: Any) -> None:
    """Test adding a tag to a dataset field that already has tags."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]
    graph = tag_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = tag_propagator_setup["actor_urn"]

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )
    field_path = "field1"
    existing_tag = "urn:li:tag:existing_tag"
    new_tag = "urn:li:tag:new_tag"

    # Create field tags map
    field_tags = {field_path: [new_tag]}

    # Create directive
    directive = TagPropagationDirective(
        propagate=True,
        tags=[new_tag],
        operation="ADD",
        entity=f"urn:li:schemaField:({dataset_urn.urn()},{field_path})",
        actor=actor_urn,
        origin=dataset_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
        relationships={},
    )

    # Create source details
    source_details = SourceDetails(
        actor=actor_urn,
        origin=dataset_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
    )

    # Create existing editable schema metadata with existing tag
    existing_metadata = EditableSchemaMetadataClass(
        editableSchemaFieldInfo=[
            EditableSchemaFieldInfoClass(
                fieldPath=field_path,
                description="Test field",
                globalTags=GlobalTags(
                    tags=[
                        TagAssociationClass(
                            tag=existing_tag,
                            attribution=MetadataAttributionClass(
                                source="other_source", time=123456, actor=actor_urn
                            ),
                        )
                    ]
                ),
            )
        ]
    )

    # Mock get_aspect to return existing metadata
    graph.graph.get_aspect.return_value = existing_metadata

    # Execute
    mcps = list(
        tag_propagator._add_tag_to_dataset_field_slow(
            dataset_urn, field_tags, directive, source_details
        )
    )

    # Assert
    assert len(mcps) == 1
    mcp = mcps[0]
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert mcp.entityUrn == dataset_urn.urn()
    assert isinstance(mcp.aspect, EditableSchemaMetadataClass)
    assert len(mcp.aspect.editableSchemaFieldInfo) == 1
    assert mcp.aspect.editableSchemaFieldInfo[0].fieldPath == field_path
    assert isinstance(mcp.aspect.editableSchemaFieldInfo[0].globalTags, GlobalTags)
    assert len(mcp.aspect.editableSchemaFieldInfo[0].globalTags.tags) == 2

    # Verify both tags are present
    tags = [tag.tag for tag in mcp.aspect.editableSchemaFieldInfo[0].globalTags.tags]
    assert existing_tag in tags
    assert new_tag in tags


def test_tag_filter_by_prefix(tag_propagator_setup: Any) -> None:
    """Test that tags can be filtered by prefix."""
    # Setup
    tag_propagator_setup["config"].tag_prefixes = ["urn:li:tag:important"]

    # Re-create the propagator with the updated config
    tag_propagator = TagPropagator(
        tag_propagator_setup["action_urn"],
        tag_propagator_setup["graph"],
        tag_propagator_setup["config"],
    )

    entity_change_event = EntityChangeEvent(
        version=0,
        auditStamp=AuditStampClass(
            time=int(time.time() * 1000), actor="urn:li:corpuser:test_user"
        ),
        entityType="dataset",
        category="TAG",
        operation="ADD",
        modifier="urn:li:tag:not_so_important_tag",
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
    )

    event_envelope = EventEnvelope(
        meta={}, event_type="EntityChangeEvent_v1", event=entity_change_event
    )

    # Execute
    directive = tag_propagator.process_ece(event_envelope)

    # Assert - should not propagate due to prefix mismatch
    assert directive is None


def test_remove_tag_from_dataset_field(tag_propagator_setup: Any) -> None:
    """Test removing a tag from a dataset field."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]
    graph = tag_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = tag_propagator_setup["actor_urn"]
    action_urn = tag_propagator_setup["action_urn"]

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )
    field_path = "field1"
    tag_to_remove = "urn:li:tag:test_tag"

    # Create field tags map
    field_tags = {field_path: [tag_to_remove]}

    # Create directive
    directive = TagPropagationDirective(
        propagate=True,
        tags=[tag_to_remove],
        operation="REMOVE",
        entity=f"urn:li:schemaField:({dataset_urn.urn()},{field_path})",
        actor=actor_urn,
        origin=dataset_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
        relationships={},
    )

    # Create source details
    source_details = SourceDetails(
        actor=actor_urn,
        origin=dataset_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
    )

    # Create existing editable schema metadata with the tag to be removed
    existing_metadata = EditableSchemaMetadataClass(
        editableSchemaFieldInfo=[
            EditableSchemaFieldInfoClass(
                fieldPath=field_path,
                description="Test field",
                globalTags=GlobalTags(
                    tags=[
                        TagAssociationClass(
                            tag=tag_to_remove,
                            attribution=MetadataAttributionClass(
                                source=action_urn, time=123456, actor=actor_urn
                            ),
                        )
                    ]
                ),
            )
        ]
    )

    # Mock get_aspect to return existing metadata
    graph.graph.get_aspect.return_value = existing_metadata

    # Execute
    mcps = list(
        tag_propagator._add_tag_to_dataset_field_slow(
            dataset_urn, field_tags, directive, source_details
        )
    )

    # Assert
    assert len(mcps) == 1
    mcp = mcps[0]
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert mcp.entityUrn == dataset_urn.urn()
    assert isinstance(mcp.aspect, EditableSchemaMetadataClass)
    assert len(mcp.aspect.editableSchemaFieldInfo) == 1
    assert mcp.aspect.editableSchemaFieldInfo[0].fieldPath == field_path
    assert isinstance(mcp.aspect.editableSchemaFieldInfo[0].globalTags, GlobalTags)
    assert len(mcp.aspect.editableSchemaFieldInfo[0].globalTags.tags) == 0


def test_rollback_asset_with_tags(tag_propagator_setup: Any) -> None:
    """Test rolling back tag associations from an asset."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]
    graph = tag_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = tag_propagator_setup["actor_urn"]
    action_urn = tag_propagator_setup["action_urn"]

    asset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Create mock global tags aspect
    global_tags = GlobalTags(
        tags=[
            TagAssociationClass(
                "urn:li:tag:action_tag",
                attribution=MetadataAttributionClass(
                    source=action_urn,
                    time=12345,
                    actor=actor_urn,
                ),
            ),
            TagAssociationClass(
                "urn:li:tag:other_tag",
                attribution=MetadataAttributionClass(
                    source="other_source",
                    time=12345,
                    actor=actor_urn,
                ),
            ),
        ]
    )

    # Also create mock editable schema metadata with field tags
    editable_schema_metadata = EditableSchemaMetadataClass(
        editableSchemaFieldInfo=[
            EditableSchemaFieldInfoClass(
                fieldPath="field1",
                globalTags=GlobalTags(
                    tags=[
                        TagAssociationClass(
                            "urn:li:tag:action_field_tag",
                            attribution=MetadataAttributionClass(
                                source=action_urn,
                                time=12345,
                                actor=actor_urn,
                            ),
                        ),
                        TagAssociationClass(
                            "urn:li:tag:other_field_tag",
                            attribution=MetadataAttributionClass(
                                source="other_source",
                                time=12345,
                                actor=actor_urn,
                            ),
                        ),
                    ]
                ),
            )
        ]
    )

    # Mock get_aspect to return global tags and editable schema metadata
    graph.graph.get_aspect.side_effect = lambda urn, aspect_type: {
        (asset_urn.urn(), GlobalTags): global_tags,
        (asset_urn.urn(), EditableSchemaMetadataClass): editable_schema_metadata,
    }.get((urn, aspect_type))

    # Execute
    tag_propagator.rollback_asset(asset_urn)

    # Assert that emit was called to update the tags
    assert graph.graph.emit.call_count >= 1

    # Check the editable schema metadata was updated correctly
    updated_metadata = None
    for call_args in graph.graph.emit.call_args_list:
        mcp = call_args[0][0]
        if isinstance(mcp.aspect, EditableSchemaMetadataClass):
            updated_metadata = mcp.aspect
            break

    # Verify that tags from our action were removed
    assert updated_metadata is not None
    assert updated_metadata.editableSchemaFieldInfo[0].globalTags
    field_tags = updated_metadata.editableSchemaFieldInfo[0].globalTags.tags
    assert len(field_tags) == 1
    assert field_tags[0].tag == "urn:li:tag:other_field_tag"


def test_bootstrap_dataset_tags(tag_propagator_setup: Dict) -> None:
    """Test bootstrapping tags from a dataset."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]
    graph = tag_propagator_setup["graph"]
    graph.graph = MagicMock()

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Create mock global tags
    global_tags = GlobalTags(
        tags=[
            TagAssociationClass(tag="urn:li:tag:dataset_tag1"),
            TagAssociationClass(tag="urn:li:tag:dataset_tag2"),
        ]
    )

    graph.graph.get_aspect.side_effect = lambda urn, aspect_type: {
        (dataset_urn.urn(), GlobalTags): global_tags,
        (dataset_urn.urn(), EditableSchemaMetadataClass): None,
    }.get((urn, aspect_type))

    # Execute
    directives = list(tag_propagator.boostrap_asset(dataset_urn))

    # Assert
    assert len(directives) == 1  # One for each tag

    # Check the directives
    tag_values = ["urn:li:tag:dataset_tag1", "urn:li:tag:dataset_tag2"]
    assert isinstance(directives[0], TagPropagationDirective)
    assert directives[0].propagate is True
    assert len(directives[0].tags) == 2
    assert directives[0].tags == tag_values
    assert directives[0].operation == "ADD"
    assert directives[0].entity == dataset_urn.urn()
    # In bootstrap, the origin is initially the dataset
    assert directives[0].origin == dataset_urn.urn()
    assert directives[0].propagation_depth == 1


def test_bootstrap_dataset_field_tags(tag_propagator_setup: Dict) -> None:
    """Test bootstrapping tags from dataset fields."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]
    graph = tag_propagator_setup["graph"]
    graph.graph = MagicMock()

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Create schema metadata with field tags
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
                globalTags=GlobalTags(
                    tags=[TagAssociationClass(tag="urn:li:tag:schema_field1_tag")]
                ),
            ),
            SchemaFieldClass(
                fieldPath="field2",
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="string",
                globalTags=GlobalTags(
                    tags=[TagAssociationClass(tag="urn:li:tag:schema_field2_tag")]
                ),
            ),
        ],
    )

    # Create editable schema metadata with field tags
    editable_schema_metadata = EditableSchemaMetadataClass(
        editableSchemaFieldInfo=[
            EditableSchemaFieldInfoClass(
                fieldPath="field3",
                globalTags=GlobalTags(
                    tags=[TagAssociationClass(tag="urn:li:tag:editable_field3_tag")]
                ),
            )
        ]
    )

    # Mock get_aspect to return schema metadata and editable schema metadata
    graph.graph.get_aspect.side_effect = lambda urn, aspect_type: {
        (dataset_urn.urn(), SchemaMetadataClass): schema_metadata,
        (dataset_urn.urn(), EditableSchemaMetadataClass): editable_schema_metadata,
    }.get((urn, aspect_type))

    # Execute
    directives = list(tag_propagator.boostrap_asset(dataset_urn))

    # Assert
    assert len(directives) == 3  # One for each field tag

    # Check that we have directives for each field
    field_paths = ["field1", "field2", "field3"]
    tag_values = [
        "urn:li:tag:schema_field1_tag",
        "urn:li:tag:schema_field2_tag",
        "urn:li:tag:editable_field3_tag",
    ]

    for directive in directives:
        assert isinstance(directive, TagPropagationDirective)
        assert directive.propagate is True
        assert len(directive.tags) == 1

        # Extract field path from the entity URN
        schema_urn = SchemaFieldUrn.from_string(directive.entity)
        field_path = schema_urn.field_path

        # Find the expected tag for this field
        idx = field_paths.index(field_path)
        expected_tag = tag_values[idx]

        assert directive.tags[0] == expected_tag
        assert directive.operation == "ADD"
        # Field URNs have the format urn:li:schemaField:(datasetUrn,fieldPath)
        assert directive.entity.startswith(f"urn:li:schemaField:({dataset_urn.urn()},")
        # In bootstrap, the origin is initially the dataset
        assert directive.origin == schema_urn.urn()
        assert directive.propagation_depth == 1


def test_bootstrap_dataset_and_field_tags(tag_propagator_setup: Dict) -> None:
    """Test bootstrapping tags from both dataset and its fields."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]
    graph = tag_propagator_setup["graph"]
    graph.graph = MagicMock()

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Create global tags for the dataset
    global_tags = GlobalTags(tags=[TagAssociationClass(tag="urn:li:tag:dataset_tag")])

    # Create schema metadata with field tags
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
                globalTags=GlobalTags(
                    tags=[TagAssociationClass(tag="urn:li:tag:field1_tag")]
                ),
            )
        ],
    )

    # Mock get_aspect to return global tags and schema metadata
    graph.graph.get_aspect.side_effect = lambda urn, aspect_type: {
        (dataset_urn.urn(), GlobalTags): global_tags,
        (dataset_urn.urn(), SchemaMetadataClass): schema_metadata,
        (
            dataset_urn.urn(),
            EditableSchemaMetadataClass,
        ): None,  # No editable schema metadata
    }.get((urn, aspect_type), None)

    # Execute
    directives = list(tag_propagator.boostrap_asset(dataset_urn))

    # Sort directives by entity type to ensure deterministic order
    directives.sort(key=lambda d: d.entity)

    # Assert
    assert len(directives) == 2  # One for dataset tag, one for field tag

    # Check the dataset directive
    dataset_directive = directives[0]
    assert isinstance(dataset_directive, TagPropagationDirective)
    assert dataset_directive.propagate is True
    assert len(dataset_directive.tags) == 1
    assert dataset_directive.tags[0] == "urn:li:tag:dataset_tag"
    assert dataset_directive.operation == "ADD"
    assert dataset_directive.entity == dataset_urn.urn()

    # Check the field directive
    field_directive = directives[1]
    assert isinstance(field_directive, TagPropagationDirective)
    assert field_directive.propagate is True
    assert len(field_directive.tags) == 1
    assert field_directive.tags[0] == "urn:li:tag:field1_tag"
    assert field_directive.operation == "ADD"
    assert field_directive.entity.startswith(
        f"urn:li:schemaField:({dataset_urn.urn()},"
    )


def test_bootstrap_dataset_no_tags(tag_propagator_setup: Dict) -> None:
    """Test bootstrapping a dataset with no tags."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]
    graph = tag_propagator_setup["graph"]
    graph.graph = MagicMock()

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Mock get_aspect to return None for all aspects
    graph.graph.get_aspect.return_value = None

    # Execute
    directives = list(tag_propagator.boostrap_asset(dataset_urn))

    # Assert
    assert len(directives) == 0  # No directives should be generated


def test_bootstrap_unsupported_entity(tag_propagator_setup: Dict) -> None:
    """Test bootstrapping an unsupported entity type."""
    # Setup
    tag_propagator = tag_propagator_setup["propagator"]

    # Create an unsupported entity type
    dashboard_urn = Urn.from_string("urn:li:dashboard:(tableau,test_dashboard)")

    # Execute
    directives = list(tag_propagator.boostrap_asset(dashboard_urn))

    # Assert
    assert (
        len(directives) == 0
    )  # No directives should be generated for unsupported entity
