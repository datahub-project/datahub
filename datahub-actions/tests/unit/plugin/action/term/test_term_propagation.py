from unittest.mock import MagicMock

from datahub.metadata.schema_classes import (
    AuditStampClass,
    EntityChangeEventClass,
    ParametersClass,
)
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


def test_dataset_level_add_propagates_to_downstream():
    action, graph = _action([DOWNSTREAM])
    action.act(_event(DATASET))
    graph.get_downstreams.assert_called_once_with(entity_urn=DATASET)
    assert graph.add_terms_to_dataset.call_args.args[0] == DOWNSTREAM


def test_column_level_remove_propagates_to_downstream():
    action, graph = _action([DOWNSTREAM])
    action.act(_event(SCHEMA_FIELD, operation="REMOVE"))
    graph.get_downstreams.assert_called_once_with(entity_urn=DATASET)
    graph.remove_terms_from_dataset.assert_called_once()
    assert graph.remove_terms_from_dataset.call_args.args[0] == DOWNSTREAM
    assert graph.remove_terms_from_dataset.call_args.args[1] == [TERM]
    graph.add_terms_to_dataset.assert_not_called()


def test_non_dataset_field_entity_is_skipped():
    action, graph = _action([DOWNSTREAM])
    action.act(_event("urn:li:dashboard:(looker,dashboards.1)"))
    graph.get_downstreams.assert_not_called()
    graph.add_terms_to_dataset.assert_not_called()
