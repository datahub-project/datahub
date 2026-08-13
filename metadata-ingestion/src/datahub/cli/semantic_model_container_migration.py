"""Backfill Metric → SMD lineage for Semantic Models on the older catalog shape.

For each ``semanticModel`` that still has ``semanticModelInfo.datasets`` populated
(the metrics-catalog / lineage-hop shape), this script finds metrics whose
``metricInfo.semanticModel`` points at that model and, when they lack
``metricUpstreams.datasetUpstreams``, writes dataset upstream edges to the
listed Semantic Model Dataset URNs.

1. Sets ``semanticModelProperties.semanticModel`` on each listed dataset when
   absent (idempotent set-if-absent; never overwrites an existing value).
2. For metrics whose ``metricInfo.semanticModel`` points at that model and that
   lack ``metricUpstreams.datasetUpstreams``, writes dataset upstream edges to
   the listed Semantic Model Dataset URNs.

Never clears or rewrites ``semanticModelInfo.datasets``. Safe to re-run.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Sequence

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import EmitMode
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter, SearchFilterRule
from datahub.metadata.schema_classes import (
    EdgeClass,
    MetricUpstreamsClass,
    SemanticModelInfoClass,
    _Aspect,
)
from datahub.utilities.urns.urn import guess_entity_type

log = logging.getLogger(__name__)

SEMANTIC_MODEL_FIELD = "semanticModel"


@dataclass
class ContainerMigrationResult:
    semantic_model_urn: str
    datasets_seen: List[str] = field(default_factory=list)
    metrics_seen: List[str] = field(default_factory=list)
    upstreams_written: List[str] = field(default_factory=list)
    upstreams_skipped: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    # True when semanticModelInfo.datasets is empty — already new-shape / nothing
    # to backfill from stored datasets (mirrors subtype_skipped in snowflake migrate).
    skipped_empty_datasets: bool = False
    error: Optional[str] = None


@dataclass
class ContainerMigrationReport:
    dry_run: bool
    results: List[ContainerMigrationResult] = field(default_factory=list)

    def __repr__(self) -> str:
        prefix = "[Dry Run] " if self.dry_run else ""
        failed = [r for r in self.results if r.error is not None]
        skipped = [
            r for r in self.results if r.error is None and r.skipped_empty_datasets
        ]
        migrated = [
            r for r in self.results if r.error is None and not r.skipped_empty_datasets
        ]
        upstreams_written = sum(len(r.upstreams_written) for r in migrated)
        upstreams_skipped = sum(len(r.upstreams_skipped) for r in migrated)
        lines = [
            f"{prefix}Semantic Model Container Migration Report:",
            "--------------",
            f"{prefix}Semantic models with stored datasets (old shape) = {len(migrated)}",
            f"{prefix}Semantic models skipped "
            f"(empty semanticModelInfo.datasets) = {len(skipped)}",
            f"{prefix}Semantic models errored = {len(failed)}",
            f"{prefix}metricUpstreams.datasetUpstreams written = {upstreams_written}",
            f"{prefix}metricUpstreams.datasetUpstreams skipped = {upstreams_skipped}",
        ]
        if skipped:
            lines.append(f"{prefix}Skipped (empty semanticModelInfo.datasets):")
            for r in skipped:
                lines.append(f"{prefix}  skipped: {r.semantic_model_urn}")
        lines.append(f"{prefix}Details:")
        for r in migrated:
            lines.append(f"{prefix}  {r.semantic_model_urn}")
            lines.append(
                f"{prefix}    datasets from semanticModelInfo.datasets: "
                f"{len(r.datasets_seen)}"
            )
            lines.append(f"{prefix}    metrics seen: {len(r.metrics_seen)}")
            if r.upstreams_written:
                lines.append(
                    f"{prefix}    upstreams written: {', '.join(r.upstreams_written)}"
                )
            if r.upstreams_skipped:
                lines.append(
                    f"{prefix}    upstreams skipped: {', '.join(r.upstreams_skipped)}"
                )
            for note in r.notes:
                lines.append(f"{prefix}    note: {note}")
        for r in failed:
            lines.append(f"{prefix}  {r.semantic_model_urn}: ERROR: {r.error}")
        return "\n".join(lines)


def _status_filter(
    include_soft_deleted: bool, only_soft_deleted: bool
) -> RemovedStatusFilter:
    if only_soft_deleted:
        return RemovedStatusFilter.ONLY_SOFT_DELETED
    if include_soft_deleted:
        return RemovedStatusFilter.ALL
    return RemovedStatusFilter.NOT_SOFT_DELETED


def _emit_aspect(
    graph: DataHubGraph, entity_urn: str, aspect: _Aspect, dry_run: bool
) -> None:
    if not dry_run:
        graph.emit_mcp(
            MetadataChangeProposalWrapper(entityUrn=entity_urn, aspect=aspect),
            emit_mode=EmitMode.SYNC_PRIMARY,
        )


def discover_semantic_model_urns(
    graph: DataHubGraph,
    platform: Optional[str] = None,
    platform_instance: Optional[str] = None,
    include_soft_deleted: bool = False,
    *,
    only_soft_deleted: bool = False,
) -> List[str]:
    status = _status_filter(include_soft_deleted, only_soft_deleted)
    return list(
        graph.get_urns_by_filter(
            entity_types=["semanticModel"],
            platform=platform,
            platform_instance=platform_instance,
            status=status,
        )
    )


def discover_metrics_for_semantic_model(
    graph: DataHubGraph,
    semantic_model_urn: str,
    include_soft_deleted: bool = False,
) -> List[str]:
    status = _status_filter(include_soft_deleted, only_soft_deleted=False)
    return list(
        graph.get_urns_by_filter(
            entity_types=["metric"],
            status=status,
            extraFilters=[
                SearchFilterRule(
                    field=SEMANTIC_MODEL_FIELD,
                    condition="EQUAL",
                    values=[semantic_model_urn],
                ).to_raw()
            ],
        )
    )


def _backfill_metric_upstreams(
    graph: DataHubGraph,
    *,
    metric_urn: str,
    dataset_urns: Sequence[str],
    dry_run: bool,
    result: ContainerMigrationResult,
) -> None:
    existing = graph.get_aspect(metric_urn, MetricUpstreamsClass)
    if (
        existing is not None
        and existing.datasetUpstreams is not None
        and len(existing.datasetUpstreams) > 0
    ):
        result.upstreams_skipped.append(f"{metric_urn} (datasetUpstreams already set)")
        return

    if not dataset_urns:
        result.upstreams_skipped.append(f"{metric_urn} (no datasets to link)")
        return

    edges = [EdgeClass(destinationUrn=urn) for urn in dataset_urns]
    if existing is None:
        aspect = MetricUpstreamsClass(datasetUpstreams=edges)
    else:
        existing.datasetUpstreams = edges
        aspect = existing

    _emit_aspect(graph, metric_urn, aspect, dry_run)
    result.upstreams_written.append(metric_urn)


def migrate_semantic_model(
    graph: DataHubGraph,
    semantic_model_urn: str,
    dry_run: bool,
    include_soft_deleted: bool = False,
) -> ContainerMigrationResult:
    result = ContainerMigrationResult(semantic_model_urn=semantic_model_urn)
    if guess_entity_type(semantic_model_urn) != "semanticModel":
        result.error = f"not a semanticModel URN: {semantic_model_urn}"
        return result
    if not graph.exists(semantic_model_urn):
        result.error = f"source entity does not exist: {semantic_model_urn}"
        return result

    try:
        info = graph.get_aspect(semantic_model_urn, SemanticModelInfoClass)
        dataset_urns: List[str] = list(info.datasets) if info is not None else []
        result.datasets_seen = list(dataset_urns)

        # Older modeling = non-empty semanticModelInfo.datasets. Empty means
        # already container-shaped (or never used the deprecated field) — skip
        # like snowflake-semantic-views skips non-Semantic-View subtypes.
        if not dataset_urns:
            result.skipped_empty_datasets = True
            result.notes.append(
                "semanticModelInfo.datasets empty or missing; "
                "nothing to backfill from stored datasets"
            )
            return result

        metric_urns = discover_metrics_for_semantic_model(
            graph,
            semantic_model_urn,
            include_soft_deleted=include_soft_deleted,
        )
        result.metrics_seen = list(metric_urns)
        for metric_urn in metric_urns:
            _backfill_metric_upstreams(
                graph,
                metric_urn=metric_urn,
                dataset_urns=dataset_urns,
                dry_run=dry_run,
                result=result,
            )
    except Exception as e:
        log.warning(f"Failed to migrate {semantic_model_urn}: {e}")
        result.error = str(e)
    return result


def run_migration(
    graph: DataHubGraph,
    urns: Sequence[str],
    dry_run: bool,
    include_soft_deleted: bool = False,
) -> ContainerMigrationReport:
    report = ContainerMigrationReport(dry_run=dry_run)
    for urn in urns:
        report.results.append(
            migrate_semantic_model(
                graph,
                urn,
                dry_run=dry_run,
                include_soft_deleted=include_soft_deleted,
            )
        )
    return report
