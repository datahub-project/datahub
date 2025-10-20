# Term Propagator Tests
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeProposal
from datahub.metadata.com.linkedin.pegasus2avro.schema import MySqlDDL
from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    InputFieldClass,
    InputFieldsClass,
    MetadataAttributionClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
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
from datahub_integrations.propagation.propagation.term.term_propagator import (
    TermPropagationDirective,
    TermPropagator,
    TermPropagatorConfig,
)


@pytest.fixture
def term_propagator_setup() -> Dict:
    """Fixture to set up the term propagator."""
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
    config = TermPropagatorConfig(
        enabled=True,
        propagation_rule=propagation_rule,
    )

    # Create propagator
    with (
        patch(
            "datahub_integrations.propagation.propagation.term.term_propagator.GlossaryTermsResolver"
        ),
        patch(
            "datahub_integrations.propagation.propagation.term.term_propagator.TermPropagator._get_all_terms",
            return_value=["urn:li:glossaryTerm:test_term"],
        ),
    ):
        term_propagator = TermPropagator(
            action_urn,
            graph,
            config,
        )

        return {
            "propagator": term_propagator,
            "graph": graph,
            "action_urn": action_urn,
            "actor_urn": actor_urn,
            "rule": propagation_rule,
            "config": config,
        }


def test_generate_term_directive(term_propagator_setup: Any) -> None:
    """Test generating a term propagation directive."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    actor_urn = term_propagator_setup["actor_urn"]

    entity_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    terms = ["urn:li:glossaryTerm:test_term"]
    source_details = SourceDetails(
        actor=actor_urn,
        origin=entity_urn,
        propagation_depth=1,
        propagation_started_at=12345,
    )

    # Execute
    directive = term_propagator.generate_directive(entity_urn, terms, source_details)

    # Assert
    assert isinstance(directive, TermPropagationDirective)
    assert directive.propagate is True
    assert directive.terms == terms
    assert directive.operation == "ADD"
    assert directive.entity == entity_urn
    assert directive.origin == entity_urn
    assert directive.propagation_depth == 2  # Should increment by 1
    assert directive.actor == actor_urn


def test_add_term_to_dataset(term_propagator_setup: Any) -> None:
    """Test adding a term to a dataset."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = term_propagator_setup["actor_urn"]
    action_urn = term_propagator_setup["action_urn"]

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )
    term = "urn:li:glossaryTerm:test_term"

    # Create directive
    directive = TermPropagationDirective(
        propagate=True,
        terms=[term],
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

    # Mock DatasetPatchBuilder
    patch_builder_mock = MagicMock()
    graph.graph.emit = MagicMock()

    with patch(
        "datahub_integrations.propagation.propagation.term.term_propagator.DatasetPatchBuilder",
        return_value=patch_builder_mock,
    ) as mock_builder_class:
        # Mock to return a MetadataChangeProposal instead of MetadataChangeProposalWrapper
        mock_mcp = MagicMock(spec=MetadataChangeProposal)
        patch_builder_mock.build.return_value = [mock_mcp]

        # Execute
        mcps = list(
            term_propagator._add_term_to_dataset(dataset_urn, directive, source_details)
        )

        # Assert
        mock_builder_class.assert_called_once_with(urn=dataset_urn.urn())
        patch_builder_mock.add_term.assert_called_once()
        term_association = patch_builder_mock.add_term.call_args[0][0]
        assert isinstance(term_association, GlossaryTermAssociationClass)
        assert term_association.urn == term
        assert term_association.attribution
        assert term_association.attribution.source == action_urn
        assert term_association.attribution.actor == actor_urn
        patch_builder_mock.build.assert_called_once()

        # Check that we got the expected MCPs instead of checking emit calls
        assert len(mcps) == 1
        assert isinstance(mcps[0], MetadataChangeProposal)


def test_add_term_to_chart_field(term_propagator_setup: Any) -> None:
    """Test adding a term to a chart field."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = term_propagator_setup["actor_urn"]
    action_urn = term_propagator_setup["action_urn"]

    chart_urn = Urn.from_string("urn:li:chart:(looker,test_chart)")
    field_path = "test_field"
    term = "urn:li:glossaryTerm:test_term"

    # Create field terms map
    field_terms = {field_path: [term]}

    # Create directive
    directive = TermPropagationDirective(
        propagate=True,
        terms=[term],
        operation="ADD",
        entity=f"urn:li:schemaField:({chart_urn.urn()},{field_path})",
        actor=actor_urn,
        origin=chart_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
        relationships={},
    )

    # Create source details
    source_details = SourceDetails(
        actor=actor_urn,
        origin=chart_urn.urn(),
        propagation_depth=1,
        propagation_started_at=123456,
    )

    # Create input fields aspect with chart fields
    input_fields = InputFieldsClass(
        fields=[
            InputFieldClass(
                schemaFieldUrn=f"urn:li:schemaField:({chart_urn.urn()},{field_path})",
                schemaField=SchemaFieldClass(
                    fieldPath=field_path,
                    nullable=False,
                    nativeDataType="string",
                    type=SchemaFieldDataTypeClass(StringTypeClass()),
                ),
            )
        ]
    )

    # Mock get_aspect to return input fields
    graph.graph.get_aspect.return_value = input_fields

    # Execute
    mcps = list(
        term_propagator._add_term_to_chart_field_slow(
            chart_urn, field_terms, directive, source_details
        )
    )

    # Assert
    assert len(mcps) == 1
    mcp = mcps[0]
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert mcp.entityUrn == chart_urn.urn()
    assert isinstance(mcp.aspect, InputFieldsClass)
    assert len(mcp.aspect.fields) == 1
    assert mcp.aspect.fields[0].schemaField
    assert mcp.aspect.fields[0].schemaField.fieldPath == field_path
    assert isinstance(
        mcp.aspect.fields[0].schemaField.glossaryTerms, GlossaryTermsClass
    )
    assert len(mcp.aspect.fields[0].schemaField.glossaryTerms.terms) == 1
    assert mcp.aspect.fields[0].schemaField.glossaryTerms.terms[0].urn == term
    assert mcp.aspect.fields[0].schemaField.glossaryTerms.terms[0].attribution
    assert (
        mcp.aspect.fields[0].schemaField.glossaryTerms.terms[0].attribution.source
        == action_urn
    )
    assert (
        mcp.aspect.fields[0].schemaField.glossaryTerms.terms[0].attribution.actor
        == actor_urn
    )


def test_process_mce_add_term(term_propagator_setup: Any) -> None:
    """Test processing a metadata change event for adding a term."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = term_propagator_setup["actor_urn"]

    entity_change_event = EntityChangeEvent(
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)",
        entityType="dataset",
        category="GLOSSARY_TERM",
        operation="ADD",
        modifier="urn:li:glossaryTerm:test_term",
        auditStamp=AuditStampClass(time=12345, actor=actor_urn),
        version=0,
    )

    event_envelope = EventEnvelope(
        meta={},  # Not used
        event_type="EntityChangeEvent_v1",
        event=entity_change_event,
    )

    # Mock graph check_relationship to allow term propagation
    graph.check_relationship.return_value = True

    # Execute
    directive = term_propagator.process_ece(event_envelope)

    # Assert
    assert isinstance(directive, TermPropagationDirective)
    assert directive.propagate is True
    assert directive.terms == ["urn:li:glossaryTerm:test_term"]
    assert directive.operation == "ADD"
    assert (
        directive.entity
        == "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )
    assert directive.actor == actor_urn


def test_term_rollback_asset(term_propagator_setup: Any) -> None:
    """Test rolling back term associations from an asset."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = term_propagator_setup["actor_urn"]
    action_urn = term_propagator_setup["action_urn"]

    asset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )
    # Create mock glossary terms aspect
    glossary_terms = GlossaryTermsClass(
        auditStamp=AuditStampClass(time=12345, actor=actor_urn),
        terms=[
            GlossaryTermAssociationClass(
                "urn:li:glossaryTerm:test_term",
                attribution=MetadataAttributionClass(
                    source=action_urn,
                    time=12345,
                    actor=actor_urn,
                ),
            ),
            GlossaryTermAssociationClass(
                "urn:li:glossaryTerm:other_term",
                attribution=MetadataAttributionClass(
                    source="other_source",
                    time=12345,
                    actor=actor_urn,
                ),
            ),
        ],
    )

    graph.graph.get_aspect.configure_mock(
        side_effect=lambda urn, aspect_type: {
            (
                asset_urn.urn(),
                GlossaryTermsClass,
            ): glossary_terms,
            (
                asset_urn.urn(),
                EditableSchemaMetadataClass,
            ): [],
        }.get((urn, aspect_type))
    )

    # Execute
    term_propagator.rollback_asset(asset_urn)

    # Assert that emit was called to update the terms
    # Only the term not created by this action should remain
    graph.graph.emit.assert_called_once()
    mcp = graph.graph.emit.call_args[0][0]
    assert isinstance(mcp, MetadataChangeProposalWrapper)
    assert mcp.entityUrn == asset_urn.urn()


def test_bootstrap_dataset_terms(term_propagator_setup: Dict) -> None:
    """Test bootstrapping terms from a dataset."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = term_propagator_setup["actor_urn"]

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Create mock glossary terms
    glossary_terms = GlossaryTermsClass(
        auditStamp=AuditStampClass(time=12345, actor=actor_urn),
        terms=[
            GlossaryTermAssociationClass(
                urn="urn:li:glossaryTerm:dataset_term1", actor=actor_urn
            ),
            GlossaryTermAssociationClass(
                urn="urn:li:glossaryTerm:dataset_term2", actor=actor_urn
            ),
        ],
    )

    # Mock get_aspect to return glossary terms
    graph.graph.get_aspect.side_effect = lambda urn, aspect_type: {
        (dataset_urn.urn(), SchemaMetadataClass): None,
        (dataset_urn.urn(), EditableSchemaMetadataClass): None,
        (
            dataset_urn.urn(),
            GlossaryTermsClass,
        ): glossary_terms,  # No dataset-level terms
    }.get((urn, aspect_type))

    # Execute
    directives = list(term_propagator.boostrap_asset(dataset_urn))

    # Assert
    assert len(directives) == 1  # One for each term

    # Check the directives
    assert isinstance(directives[0], TermPropagationDirective)
    assert directives[0].propagate is True
    assert len(directives[0].terms) == 2
    assert directives[0].terms == [
        "urn:li:glossaryTerm:dataset_term1",
        "urn:li:glossaryTerm:dataset_term2",
    ]
    assert directives[0].operation == "ADD"
    assert directives[0].entity == dataset_urn.urn()
    # In bootstrap, the origin is initially the dataset
    assert directives[0].origin == dataset_urn.urn()
    assert directives[0].propagation_depth == 1


def test_bootstrap_dataset_field_terms(term_propagator_setup: Dict) -> None:
    """Test bootstrapping terms from dataset fields."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = term_propagator_setup["actor_urn"]

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Create schema metadata with field terms
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
                glossaryTerms=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(
                            urn="urn:li:glossaryTerm:schema_field1_term",
                            actor=actor_urn,
                        )
                    ],
                    auditStamp=AuditStampClass(time=12345, actor=actor_urn),
                ),
            ),
            SchemaFieldClass(
                fieldPath="field2",
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="string",
                glossaryTerms=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(
                            urn="urn:li:glossaryTerm:schema_field2_term",
                            actor=actor_urn,
                        )
                    ],
                    auditStamp=AuditStampClass(time=12345, actor=actor_urn),
                ),
            ),
        ],
    )

    # Create editable schema metadata with field terms
    editable_schema_metadata = EditableSchemaMetadataClass(
        editableSchemaFieldInfo=[
            EditableSchemaFieldInfoClass(
                fieldPath="field3",
                glossaryTerms=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(
                            urn="urn:li:glossaryTerm:editable_field3_term",
                            actor=actor_urn,
                        )
                    ],
                    auditStamp=AuditStampClass(time=12345, actor=actor_urn),
                ),
            )
        ]
    )

    # Mock get_aspect to return schema metadata and editable schema metadata
    graph.graph.get_aspect.side_effect = lambda urn, aspect_type: {
        (dataset_urn.urn(), SchemaMetadataClass): schema_metadata,
        (dataset_urn.urn(), EditableSchemaMetadataClass): editable_schema_metadata,
        (dataset_urn.urn(), GlossaryTermsClass): None,  # No dataset-level terms
    }.get((urn, aspect_type))

    # Execute
    directives = list(term_propagator.boostrap_asset(dataset_urn))

    # Assert
    assert len(directives) == 3  # One for each field term

    # Check that we have directives for each field
    field_paths = ["field1", "field2", "field3"]
    term_values = [
        "urn:li:glossaryTerm:schema_field1_term",
        "urn:li:glossaryTerm:schema_field2_term",
        "urn:li:glossaryTerm:editable_field3_term",
    ]

    for directive in directives:
        assert isinstance(directive, TermPropagationDirective)
        assert directive.propagate is True
        assert len(directive.terms) == 1

        # Extract field path from the entity URN
        schema_urn = SchemaFieldUrn.from_string(directive.entity)
        field_path = schema_urn.field_path

        # Find the expected term for this field
        idx = field_paths.index(field_path)
        expected_term = term_values[idx]

        assert directive.terms[0] == expected_term
        assert directive.operation == "ADD"
        # Field URNs have the format urn:li:schemaField:(datasetUrn,fieldPath)
        assert directive.entity.startswith(f"urn:li:schemaField:({dataset_urn.urn()},")
        assert directive.actor == actor_urn


def test_bootstrap_dataset_and_field_terms(term_propagator_setup: Dict) -> None:
    """Test bootstrapping terms from both dataset and its fields."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = term_propagator_setup["actor_urn"]

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Create glossary terms for the dataset
    glossary_terms = GlossaryTermsClass(
        terms=[
            GlossaryTermAssociationClass(
                urn="urn:li:glossaryTerm:dataset_term", actor=actor_urn
            )
        ],
        auditStamp=AuditStampClass(time=12345, actor=actor_urn),
    )

    # Create schema metadata with field terms
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
                glossaryTerms=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(
                            urn="urn:li:glossaryTerm:field1_term", actor=actor_urn
                        )
                    ],
                    auditStamp=AuditStampClass(time=12345, actor=actor_urn),
                ),
            )
        ],
    )

    # Mock get_aspect to return glossary terms and schema metadata
    graph.graph.get_aspect.side_effect = lambda urn, aspect_type: {
        (dataset_urn.urn(), GlossaryTermsClass): glossary_terms,
        (dataset_urn.urn(), SchemaMetadataClass): schema_metadata,
        (
            dataset_urn.urn(),
            EditableSchemaMetadataClass,
        ): None,  # No editable schema metadata
    }.get((urn, aspect_type))

    # Execute
    directives = list(term_propagator.boostrap_asset(dataset_urn))

    # Sort directives by entity type to ensure deterministic order
    directives.sort(key=lambda d: d.entity)

    # Assert
    assert len(directives) == 2  # One for dataset term, one for field term

    # Check the dataset directive
    dataset_directive = directives[0]
    assert isinstance(dataset_directive, TermPropagationDirective)
    assert dataset_directive.propagate is True
    assert (
        len(
            dataset_directive.tags
            if hasattr(dataset_directive, "tags")
            else dataset_directive.terms
        )
        == 1
    )
    assert dataset_directive.terms[0] == "urn:li:glossaryTerm:dataset_term"
    assert dataset_directive.operation == "ADD"
    assert dataset_directive.entity == dataset_urn.urn()

    # Check the field directive
    field_directive = directives[1]
    assert isinstance(field_directive, TermPropagationDirective)
    assert field_directive.propagate is True
    assert (
        len(
            field_directive.tags
            if hasattr(field_directive, "tags")
            else field_directive.terms
        )
        == 1
    )
    assert field_directive.terms[0] == "urn:li:glossaryTerm:field1_term"
    assert field_directive.operation == "ADD"
    assert field_directive.entity.startswith(
        f"urn:li:schemaField:({dataset_urn.urn()},"
    )


def test_bootstrap_dataset_apply_term_filters(term_propagator_setup: Dict) -> None:
    """Test bootstrapping with term filters applied."""
    # Setup
    with (
        patch(
            "datahub_integrations.propagation.propagation.term.term_propagator.TermPropagator._get_all_terms",
            return_value=["urn:li:glossaryTerm:allowed_term"],
        ),
    ):
        term_propagator = term_propagator_setup["propagator"]
        graph = term_propagator_setup["graph"]
        graph.graph = MagicMock()
        actor_urn = term_propagator_setup["actor_urn"]

        dataset_urn = Urn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
        )

        # Create glossary terms for the dataset with both allowed and non-allowed terms
        glossary_terms = GlossaryTermsClass(
            terms=[
                GlossaryTermAssociationClass(
                    urn="urn:li:glossaryTerm:allowed_term", actor=actor_urn
                ),
                GlossaryTermAssociationClass(
                    urn="urn:li:glossaryTerm:non_allowed_term", actor=actor_urn
                ),
            ],
            auditStamp=AuditStampClass(time=12345, actor=actor_urn),
        )

        # Mock get_aspect to return glossary terms
        graph.graph.get_aspect.side_effect = lambda urn, aspect_type: {
            (dataset_urn.urn(), GlossaryTermsClass): glossary_terms,
            (dataset_urn.urn(), SchemaMetadataClass): None,
            (dataset_urn.urn(), EditableSchemaMetadataClass): None,
        }.get((urn, aspect_type), None)

        # Execute
        directives = list(term_propagator.boostrap_asset(dataset_urn))

        # Assert
        assert len(directives) == 1  # Only the allowed term should generate a directive

        directive = directives[0]
        assert isinstance(directive, TermPropagationDirective)
        assert directive.propagate is True
        assert len(directive.terms) == 1
        assert directive.terms[0] == "urn:li:glossaryTerm:allowed_term"


def test_bootstrap_dataset_no_terms(term_propagator_setup: Dict) -> None:
    """Test bootstrapping a dataset with no terms."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()

    dataset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Mock get_aspect to return None for all aspects
    graph.graph.get_aspect.return_value = None

    # Execute
    directives = list(term_propagator.boostrap_asset(dataset_urn))

    # Assert
    assert len(directives) == 0  # No directives should be generated


def test_bootstrap_unsupported_entity_type(term_propagator_setup: Dict) -> None:
    """Test bootstrapping an unsupported entity type."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]

    # Create an unsupported entity type
    dashboard_urn = Urn.from_string("urn:li:dashboard:(tableau,test_dashboard)")

    # Execute
    directives = list(term_propagator.boostrap_asset(dashboard_urn))

    # Assert
    assert (
        len(directives) == 0
    )  # No directives should be generated for unsupported entity


def test_term_propagator_asset_filters() -> None:
    """Test that TermPropagator.asset_filters returns correct filters based on entity types."""
    # Setup
    graph = MagicMock()
    action_urn = "urn:li:dataHubAction:test_action"

    # Create propagation rule with multiple entity types
    rule = PropagationRule(
        entity_types=[
            "dataset",
            "schemaField",
            "chart",
            "dashboard",
            "dataJob",
            "dataFlow",
        ],
        target_urn_resolution=[
            RelationshipLookup(type=PropagationRelationships.UPSTREAM)
        ],
    )

    # Create config
    config = TermPropagatorConfig(
        enabled=True,
        propagation_rule=rule,
    )

    # Create propagator with mocked term resolution
    with (
        patch(
            "datahub_integrations.propagation.propagation.term.term_propagator.GlossaryTermsResolver"
        ),
        patch(
            "datahub_integrations.propagation.propagation.term.term_propagator.TermPropagator._get_all_terms",
            return_value=["urn:li:glossaryTerm:term1", "urn:li:glossaryTerm:term2"],
        ),
    ):
        term_propagator = TermPropagator(
            action_urn,
            graph,
            config,
        )

        # Execute
        filters = term_propagator.asset_filters()

        # Assert
        assert isinstance(filters, dict)

        # Check all expected entity types are present
        expected_entity_types = [
            "dataset",
            "schemaField",
            "chart",
            "dashboard",
            "dataJob",
            "dataFlow",
        ]
        for entity_type in expected_entity_types:
            assert entity_type in filters

        # Check schema field filters
        assert "dataset" in filters["schemaField"]
        schema_field_filters = filters["schemaField"]["dataset"]

        # Should have one filter per term for field glossary terms and edited field glossary terms
        assert len(schema_field_filters) == 4  # 2 terms × 2 fields

        # Check the fields and conditions
        fields = set(f.field for f in schema_field_filters)
        assert "fieldGlossaryTerms" in fields
        assert "editedFieldGlossaryTerms" in fields

        # All filters should have IN condition
        for f in schema_field_filters:
            assert f.condition == "IN"
            assert f.values[0] in [
                "urn:li:glossaryTerm:term1",
                "urn:li:glossaryTerm:term2",
            ]

        # Check dataset filters
        assert "dataset" in filters["dataset"]
        dataset_filters = filters["dataset"]["dataset"]

        # Should have one filter per term for dataset glossary terms
        assert len(dataset_filters) == 2  # 2 terms

        # Check the field and condition
        for f in dataset_filters:
            assert f.field == "glossaryTerms"
            assert f.condition == "IN"
            # Check that the values contain one of the expected terms
            assert len(f.values) == 1
            assert f.values[0] in [
                "urn:li:glossaryTerm:term1",
                "urn:li:glossaryTerm:term2",
            ]


def test_term_propagator_asset_filters_with_single_entity_type() -> None:
    """Test TermPropagator.asset_filters with a single entity type."""
    # Setup
    graph = MagicMock()
    action_urn = "urn:li:dataHubAction:test_action"

    # Create propagation rule with only dataset entity type
    rule = PropagationRule(
        entity_types=["dataset"],
        target_urn_resolution=[
            RelationshipLookup(type=PropagationRelationships.UPSTREAM)
        ],
    )

    # Create config
    config = TermPropagatorConfig(
        enabled=True,
        propagation_rule=rule,
    )

    # Create propagator with mocked term resolution
    with (
        patch(
            "datahub_integrations.propagation.propagation.term.term_propagator.GlossaryTermsResolver"
        ),
        patch(
            "datahub_integrations.propagation.propagation.term.term_propagator.TermPropagator._get_all_terms",
            return_value=["urn:li:glossaryTerm:term1"],
        ),
    ):
        term_propagator = TermPropagator(
            action_urn,
            graph,
            config,
        )

        # Execute
        filters = term_propagator.asset_filters()

        # Assert
        assert isinstance(filters, dict)

        # Only dataset should be present
        assert "dataset" in filters
        assert "schemaField" not in filters

        # Check dataset filters
        assert "dataset" in filters["dataset"]
        dataset_filters = filters["dataset"]["dataset"]

        # Should have one filter for the term
        assert len(dataset_filters) == 1

        # Check the filter
        assert dataset_filters[0].field == "glossaryTerms"
        assert dataset_filters[0].condition == "IN"
        assert dataset_filters[0].values == ["urn:li:glossaryTerm:term1"]


def test_term_propagator_asset_filters_with_no_terms() -> None:
    """Test TermPropagator.asset_filters when no terms are configured."""
    # Setup
    graph = MagicMock()
    action_urn = "urn:li:dataHubAction:test_action"

    # Create propagation rule
    rule = PropagationRule(
        entity_types=["dataset", "schemaField"],
        target_urn_resolution=[
            RelationshipLookup(type=PropagationRelationships.UPSTREAM)
        ],
    )

    # Create config
    config = TermPropagatorConfig(
        enabled=True,
        propagation_rule=rule,
    )

    # Create propagator with mock returning empty terms list
    with (
        patch(
            "datahub_integrations.propagation.propagation.term.term_propagator.GlossaryTermsResolver"
        ),
        patch(
            "datahub_integrations.propagation.propagation.term.term_propagator.TermPropagator._get_all_terms",
            return_value=[],
        ),
    ):
        term_propagator = TermPropagator(
            action_urn,
            graph,
            config,
        )

        # Execute
        filters = term_propagator.asset_filters()

        # Assert - should still return the structure but with empty lists of filters
        assert isinstance(filters, dict)
        assert "dataset" in filters
        assert "schemaField" in filters

        # Check dataset filters - should have an empty list
        assert "dataset" in filters["dataset"]
        assert filters["dataset"]["dataset"] == []

        # Check schema field filters - should have an empty list
        assert "dataset" in filters["schemaField"]
        assert filters["schemaField"]["dataset"] == []


def test_rollback_schema_terms_with_terms_from_action(
    term_propagator_setup: Dict,
) -> None:
    """Test rolling back schema terms that were added by this action."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()
    action_urn = term_propagator_setup["action_urn"]
    actor_urn = term_propagator_setup["actor_urn"]

    asset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Create schema metadata with fields that have terms from this action and other sources
    schema_metadata = SchemaMetadataClass(
        schemaName="test_schema",
        platform="test_platform",
        version=0,
        hash="test_hash",
        platformSchema=MySqlDDL(tableSchema="test_table_schema"),
        fields=[
            # Field 1 has a term from this action that should be removed
            SchemaFieldClass(
                fieldPath="field1",
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="string",
                glossaryTerms=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(
                            urn="urn:li:glossaryTerm:term_from_action",
                            attribution=MetadataAttributionClass(
                                source=action_urn, time=123456, actor=actor_urn
                            ),
                        )
                    ],
                    auditStamp=AuditStampClass(time=123456, actor=actor_urn),
                ),
            ),
            # Field 2 has terms from both this action and another source
            SchemaFieldClass(
                fieldPath="field2",
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="string",
                glossaryTerms=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(
                            urn="urn:li:glossaryTerm:term_from_action",
                            attribution=MetadataAttributionClass(
                                source=action_urn, time=123456, actor=actor_urn
                            ),
                        ),
                        GlossaryTermAssociationClass(
                            urn="urn:li:glossaryTerm:term_from_other_source",
                            attribution=MetadataAttributionClass(
                                source="other_source", time=123456, actor=actor_urn
                            ),
                        ),
                    ],
                    auditStamp=AuditStampClass(time=123456, actor=actor_urn),
                ),
            ),
            # Field 3 has only terms from another source that should be kept
            SchemaFieldClass(
                fieldPath="field3",
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="string",
                glossaryTerms=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(
                            urn="urn:li:glossaryTerm:term_from_other_source",
                            attribution=MetadataAttributionClass(
                                source="other_source", time=123456, actor=actor_urn
                            ),
                        )
                    ],
                    auditStamp=AuditStampClass(time=123456, actor=actor_urn),
                ),
            ),
            # Field 4 has no terms
            SchemaFieldClass(
                fieldPath="field4",
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="string",
            ),
        ],
    )

    # Mock get_aspect to return the schema metadata
    graph.graph.get_aspect.return_value = schema_metadata

    # Execute
    term_propagator._rollback_schema_terms(asset_urn)

    # Assert
    # Verify emit was called once with correct MetadataChangeProposal
    graph.graph.emit.assert_called_once()
    emit_call = graph.graph.emit.call_args[0][0]
    assert isinstance(emit_call, MetadataChangeProposalWrapper)
    assert emit_call.entityUrn == asset_urn.urn()

    # Get the updated schema metadata
    updated_schema = emit_call.aspect
    assert isinstance(updated_schema, SchemaMetadataClass)

    # Verify field 1 has no terms left
    field1 = next(f for f in updated_schema.fields if f.fieldPath == "field1")
    assert field1.glossaryTerms
    assert len(field1.glossaryTerms.terms) == 0

    # Verify field 2 only has the term from the other source
    field2 = next(f for f in updated_schema.fields if f.fieldPath == "field2")
    assert field2.glossaryTerms
    assert len(field2.glossaryTerms.terms) == 1
    assert (
        field2.glossaryTerms.terms[0].urn
        == "urn:li:glossaryTerm:term_from_other_source"
    )

    # Verify field 3 still has its term
    field3 = next(f for f in updated_schema.fields if f.fieldPath == "field3")
    assert field3.glossaryTerms
    assert len(field3.glossaryTerms.terms) == 1
    assert (
        field3.glossaryTerms.terms[0].urn
        == "urn:li:glossaryTerm:term_from_other_source"
    )

    # Verify field 4 is unchanged
    field4 = next(f for f in updated_schema.fields if f.fieldPath == "field4")
    assert not hasattr(field4, "glossaryTerms") or field4.glossaryTerms is None


def test_rollback_schema_terms_no_terms_from_action(
    term_propagator_setup: Dict,
) -> None:
    """Test rolling back schema terms when no terms from this action exist."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()
    actor_urn = term_propagator_setup["actor_urn"]

    asset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Create schema metadata with fields that have terms only from other sources
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
                glossaryTerms=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(
                            urn="urn:li:glossaryTerm:term_from_other_source",
                            attribution=MetadataAttributionClass(
                                source="other_source", time=123456, actor=actor_urn
                            ),
                        )
                    ],
                    auditStamp=AuditStampClass(time=123456, actor=actor_urn),
                ),
            ),
            SchemaFieldClass(
                fieldPath="field2",
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="string",
                glossaryTerms=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(
                            urn="urn:li:glossaryTerm:another_term",
                            attribution=MetadataAttributionClass(
                                source="another_source", time=123456, actor=actor_urn
                            ),
                        )
                    ],
                    auditStamp=AuditStampClass(time=123456, actor=actor_urn),
                ),
            ),
        ],
    )

    # Make a copy of the original schema metadata for comparison
    import copy

    original_schema = copy.deepcopy(schema_metadata)

    # Mock get_aspect to return the schema metadata
    graph.graph.get_aspect.return_value = schema_metadata

    # Execute
    term_propagator._rollback_schema_terms(asset_urn)

    # Assert
    # Emit should not be called since no changes were made
    graph.graph.emit.assert_not_called()

    # Verify schema metadata is unchanged
    assert schema_metadata.fields[0].glossaryTerms
    assert original_schema.fields[0].glossaryTerms
    assert (
        schema_metadata.fields[0].glossaryTerms.terms[0].urn
        == original_schema.fields[0].glossaryTerms.terms[0].urn
    )
    assert schema_metadata.fields[1].glossaryTerms
    assert original_schema.fields[1].glossaryTerms

    assert (
        schema_metadata.fields[1].glossaryTerms.terms[0].urn
        == original_schema.fields[1].glossaryTerms.terms[0].urn
    )


def test_rollback_schema_terms_no_schema_metadata(term_propagator_setup: Dict) -> None:
    """Test rolling back schema terms when no schema metadata exists."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()

    asset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Mock get_aspect to return None (no schema metadata)
    graph.graph.get_aspect.return_value = None

    # Execute
    term_propagator._rollback_schema_terms(asset_urn)

    # Assert
    # Emit should not be called since no schema metadata exists
    graph.graph.emit.assert_not_called()


def test_rollback_schema_terms_no_fields(term_propagator_setup: Dict) -> None:
    """Test rolling back schema terms when schema metadata has no fields."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()

    asset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Create schema metadata with no fields
    schema_metadata = SchemaMetadataClass(
        schemaName="test_schema",
        platform="test_platform",
        version=0,
        hash="test_hash",
        platformSchema=MySqlDDL(tableSchema="test_table_schema"),
        fields=[],
    )

    # Mock get_aspect to return the schema metadata
    graph.graph.get_aspect.return_value = schema_metadata

    # Execute
    term_propagator._rollback_schema_terms(asset_urn)

    # Assert
    # Emit should not be called since no fields exist
    graph.graph.emit.assert_not_called()


def test_rollback_schema_terms_fields_without_glossary_terms(
    term_propagator_setup: Dict,
) -> None:
    """Test rolling back schema terms when fields don't have glossary terms."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()

    asset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Create schema metadata with fields that don't have glossary terms
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
            ),
            SchemaFieldClass(
                fieldPath="field2",
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="string",
            ),
        ],
    )

    # Mock get_aspect to return the schema metadata
    graph.graph.get_aspect.return_value = schema_metadata

    # Execute
    term_propagator._rollback_schema_terms(asset_urn)

    # Assert
    # Emit should not be called since no glossary terms exist
    graph.graph.emit.assert_not_called()


def test_rollback_schema_terms_mixed_fields(term_propagator_setup: Dict) -> None:
    """Test rolling back schema terms with a mix of fields with and without terms."""
    # Setup
    term_propagator = term_propagator_setup["propagator"]
    graph = term_propagator_setup["graph"]
    graph.graph = MagicMock()
    action_urn = term_propagator_setup["action_urn"]
    actor_urn = term_propagator_setup["actor_urn"]

    asset_urn = Urn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:hive,my_database.my_table,PROD)"
    )

    # Create schema metadata with a mix of fields
    schema_metadata = SchemaMetadataClass(
        schemaName="test_schema",
        platform="test_platform",
        version=0,
        hash="test_hash",
        platformSchema=MySqlDDL(tableSchema="test_table_schema"),
        fields=[
            # Field with term from this action
            SchemaFieldClass(
                fieldPath="field1",
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="string",
                glossaryTerms=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(
                            urn="urn:li:glossaryTerm:term_from_action",
                            attribution=MetadataAttributionClass(
                                source=action_urn, time=123456, actor=actor_urn
                            ),
                        )
                    ],
                    auditStamp=AuditStampClass(time=123456, actor=actor_urn),
                ),
            ),
            # Field without any terms
            SchemaFieldClass(
                fieldPath="field2",
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="string",
            ),
            # Field with term from another source
            SchemaFieldClass(
                fieldPath="field3",
                type=SchemaFieldDataTypeClass(StringTypeClass()),
                nativeDataType="string",
                glossaryTerms=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(
                            urn="urn:li:glossaryTerm:term_from_other_source",
                            attribution=MetadataAttributionClass(
                                source="other_source", time=123456, actor=actor_urn
                            ),
                        )
                    ],
                    auditStamp=AuditStampClass(time=123456, actor=actor_urn),
                ),
            ),
        ],
    )

    # Mock get_aspect to return the schema metadata
    graph.graph.get_aspect.return_value = schema_metadata

    # Execute
    term_propagator._rollback_schema_terms(asset_urn)

    # Assert
    # Verify emit was called once with correct MetadataChangeProposal
    graph.graph.emit.assert_called_once()
    emit_call = graph.graph.emit.call_args[0][0]
    assert isinstance(emit_call, MetadataChangeProposalWrapper)
    assert emit_call.entityUrn == asset_urn.urn()

    # Get the updated schema metadata
    updated_schema = emit_call.aspect
    assert isinstance(updated_schema, SchemaMetadataClass)

    # Verify field 1 has no terms left
    field1 = next(f for f in updated_schema.fields if f.fieldPath == "field1")
    assert field1.glossaryTerms
    assert len(field1.glossaryTerms.terms) == 0

    # Verify field 2 is unchanged
    field2 = next(f for f in updated_schema.fields if f.fieldPath == "field2")
    assert not hasattr(field2, "glossaryTerms") or field2.glossaryTerms is None

    # Verify field 3 still has its term
    field3 = next(f for f in updated_schema.fields if f.fieldPath == "field3")
    assert field3.glossaryTerms
    assert len(field3.glossaryTerms.terms) == 1
    assert (
        field3.glossaryTerms.terms[0].urn
        == "urn:li:glossaryTerm:term_from_other_source"
    )
