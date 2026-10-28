from typing import Dict, Optional, Type
from unittest.mock import MagicMock

import pytest

from datahub.metadata.schema_classes import (
    AuditStampClass,
    EditableSchemaFieldInfoClass,
    EditableSchemaMetadataClass,
    EntityChangeEventClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    OtherSchemaClass,
    ParametersClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)
from datahub_actions.api.action_graph import AcrylDataHubGraph
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.action.term.term_propagation_action import (
    TermPropagationAction,
)

DATASET = "urn:li:dataset:(urn:li:dataPlatform:glue,db.table,PROD)"
# schemaField URN carrying a v2-annotated field path, as GMS emits for a
# column-level term application (see issue #19502).
SCHEMA_FIELD = (
    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:glue,db.table,PROD),"
    "[version=2.0].[type=string].first_name)"
)
DOWNSTREAM = "urn:li:dataset:(urn:li:dataPlatform:glue,db.table_downstream,PROD)"
TERM = "urn:li:glossaryTerm:Sensitive"
ACTION_URN = "urn:li:dataHubAction:test"


def _event(entity_urn: str, operation: str = "ADD") -> EventEnvelope:
    ece = EntityChangeEvent.from_class(
        EntityChangeEventClass(
            entityType="dataset"
            if entity_urn.startswith("urn:li:dataset:")
            else "schemaField",
            entityUrn=entity_urn,
            category="GLOSSARY_TERM",
            operation=operation,
            auditStamp=AuditStampClass(0, "urn:li:corpuser:datahub"),
            version=0,
            modifier=TERM,
            parameters=ParametersClass(),
        )
    )
    return EventEnvelope("EntityChangeEvent_v1", ece, {})


def _action(downstreams):
    graph = MagicMock()
    graph.get_downstreams.return_value = downstreams
    # Default: source no longer carries the term (only relevant for REMOVE).
    graph.dataset_has_term.return_value = False
    ctx = PipelineContext(pipeline_name="test", graph=graph)
    return TermPropagationAction.create({}, ctx), graph


def test_column_level_add_resolves_parent_dataset_for_lineage():
    # The core bug: lineage must be looked up against the parent DATASET, not the
    # schemaField URN (whose v2 field path never matches DownstreamOf edges).
    action, graph = _action([DOWNSTREAM])
    action.act(_event(SCHEMA_FIELD))
    graph.get_downstreams.assert_called_once_with(entity_urn=DATASET)
    graph.add_terms_to_dataset.assert_called_once()
    assert graph.add_terms_to_dataset.call_args.args[0] == DOWNSTREAM
    assert graph.add_terms_to_dataset.call_args.args[1] == [TERM]
    # Attributed to the action, so a later propagated removal can target only it.
    assert (
        graph.add_terms_to_dataset.call_args.kwargs["attribution"].source == ACTION_URN
    )


def test_dataset_level_add_propagates_to_downstream():
    action, graph = _action([DOWNSTREAM])
    action.act(_event(DATASET))
    graph.get_downstreams.assert_called_once_with(entity_urn=DATASET)
    assert graph.add_terms_to_dataset.call_args.args[0] == DOWNSTREAM


def test_column_level_remove_propagates_to_downstream():
    action, graph = _action([DOWNSTREAM])
    action.act(_event(SCHEMA_FIELD, operation="REMOVE"))
    graph.dataset_has_term.assert_called_once_with(DATASET, TERM)
    graph.get_downstreams.assert_called_once_with(entity_urn=DATASET)
    graph.remove_terms_from_dataset.assert_called_once()
    assert graph.remove_terms_from_dataset.call_args.args[0] == DOWNSTREAM
    assert graph.remove_terms_from_dataset.call_args.args[1] == [TERM]
    # Only the action's own entries are removed; a user-applied entry survives.
    assert (
        graph.remove_terms_from_dataset.call_args.kwargs["attribution_source"]
        == ACTION_URN
    )
    graph.add_terms_to_dataset.assert_not_called()


def test_remove_skipped_when_source_still_has_term():
    # Removing the term from one field must not strip downstreams while the source
    # still carries it elsewhere (another field or the dataset level).
    action, graph = _action([DOWNSTREAM])
    graph.dataset_has_term.return_value = True
    action.act(_event(SCHEMA_FIELD, operation="REMOVE"))
    graph.remove_terms_from_dataset.assert_not_called()
    graph.get_downstreams.assert_not_called()


def test_non_dataset_field_entity_is_skipped():
    action, graph = _action([DOWNSTREAM])
    action.act(_event("urn:li:dashboard:(looker,dashboards.1)"))
    graph.get_downstreams.assert_not_called()
    graph.add_terms_to_dataset.assert_not_called()


def _terms(*urns: str) -> GlossaryTermsClass:
    return GlossaryTermsClass(
        terms=[GlossaryTermAssociationClass(urn) for urn in urns],
        auditStamp=AuditStampClass(0, "urn:li:corpuser:datahub"),
    )


def _graph_with_aspects(aspects: Dict[Type, object]) -> AcrylDataHubGraph:
    base = MagicMock()
    base.get_aspect.side_effect = lambda urn, aspect_type: aspects.get(aspect_type)
    return AcrylDataHubGraph(base)


def _schema(field_terms: Optional[GlossaryTermsClass]) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName="table",
        platform="urn:li:dataPlatform:glue",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath="first_name",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
                glossaryTerms=field_terms,
            )
        ],
    )


def _editable(field_terms: Optional[GlossaryTermsClass]) -> EditableSchemaMetadataClass:
    return EditableSchemaMetadataClass(
        editableSchemaFieldInfo=[
            EditableSchemaFieldInfoClass(
                fieldPath="first_name", glossaryTerms=field_terms
            )
        ]
    )


@pytest.mark.parametrize(
    "aspects",
    [
        pytest.param({GlossaryTermsClass: _terms(TERM)}, id="dataset_level"),
        pytest.param(
            {EditableSchemaMetadataClass: _editable(_terms(TERM))}, id="editable_field"
        ),
        pytest.param({SchemaMetadataClass: _schema(_terms(TERM))}, id="ingested_field"),
    ],
)
def test_dataset_has_term_finds_term(aspects: Dict[Type, object]) -> None:
    assert _graph_with_aspects(aspects).dataset_has_term(DATASET, TERM)


@pytest.mark.parametrize(
    "aspects",
    [
        pytest.param({}, id="no_aspects"),
        pytest.param(
            {
                GlossaryTermsClass: _terms("urn:li:glossaryTerm:Other"),
                EditableSchemaMetadataClass: _editable(None),
                SchemaMetadataClass: _schema(_terms("urn:li:glossaryTerm:Other")),
            },
            id="only_other_terms",
        ),
    ],
)
def test_dataset_has_term_when_absent(aspects: Dict[Type, object]) -> None:
    assert not _graph_with_aspects(aspects).dataset_has_term(DATASET, TERM)
