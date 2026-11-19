from typing import Dict, List, Optional, Set
from unittest import mock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit_processor import WorkunitProcessorContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.workunit_processors.auto_materialize_referenced_tags_terms import (
    AutoMaterializeReferencedTagsTermsProcessor,
    AutoMaterializeReferencedTagsTermsProcessorReport,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermKeyClass,
    GlossaryTermsClass,
    TagAssociationClass,
    TagKeyClass,
)

DATASET = "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)"
TAG_EXISTING = "urn:li:tag:existing_tag"
TAG_NEW = "urn:li:tag:new_tag"
TERM_EXISTING = "urn:li:glossaryTerm:existing_term"
TERM_NEW = "urn:li:glossaryTerm:new_term"
ALL_REFERENCED = sorted([TAG_EXISTING, TAG_NEW, TERM_EXISTING, TERM_NEW])


def _run(
    graph: Optional[DataHubGraph],
) -> "tuple[List[str], AutoMaterializeReferencedTagsTermsProcessorReport]":
    ctx = WorkunitProcessorContext(
        source_report=SourceReport(),
        pipeline_context=PipelineContext(run_id="test", graph=graph),
        source_config=None,
        platform=None,
    )
    processor = AutoMaterializeReferencedTagsTermsProcessor.create(ctx)
    stream = [
        MetadataChangeProposalWrapper(
            entityUrn=DATASET,
            aspect=GlobalTagsClass(
                tags=[TagAssociationClass(TAG_EXISTING), TagAssociationClass(TAG_NEW)]
            ),
        ).as_workunit(),
        MetadataChangeProposalWrapper(
            entityUrn=DATASET,
            aspect=GlossaryTermsClass(
                terms=[
                    GlossaryTermAssociationClass(TERM_EXISTING),
                    GlossaryTermAssociationClass(TERM_NEW),
                ],
                auditStamp=AuditStampClass(time=0, actor="urn:li:corpuser:test"),
            ),
        ).as_workunit(),
    ]
    materialized = [
        wu.get_urn()
        for wu in processor.process(stream)
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, (TagKeyClass, GlossaryTermKeyClass))
    ]
    return materialized, processor.report


def _graph_with(existing: Set[str]) -> mock.MagicMock:
    graph = mock.MagicMock(spec=DataHubGraph)

    def get_entities(
        entity_name: str, urns: List[str], aspects: Optional[List[str]] = None
    ) -> Dict[str, Dict]:
        return {urn: {} for urn in urns if urn in existing}

    graph.get_entities.side_effect = get_entities
    return graph


def test_skips_existing_and_materializes_missing() -> None:
    graph = _graph_with({TAG_EXISTING, TERM_EXISTING})

    materialized, report = _run(graph)

    assert materialized == [TERM_NEW, TAG_NEW]
    assert report.num_skipped_existing == 2
    assert report.num_existence_check_failures == 0
    # One batched lookup per entity type, not one per urn.
    assert graph.get_entities.call_count == 2


def test_without_graph_materializes_everything() -> None:
    materialized, report = _run(None)

    assert sorted(materialized) == ALL_REFERENCED
    assert report.num_skipped_existing == 0


def test_failed_lookup_materializes_everything() -> None:
    graph = mock.MagicMock(spec=DataHubGraph)
    graph.get_entities.side_effect = RuntimeError("boom")

    materialized, report = _run(graph)

    assert sorted(materialized) == ALL_REFERENCED
    assert report.num_skipped_existing == 0
    assert report.num_existence_check_failures == 1
