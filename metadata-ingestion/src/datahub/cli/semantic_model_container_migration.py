"""Report Semantic Model container-model migration status.

For each ``semanticModel`` that still has ``semanticModelInfo.datasets`` populated
(the older catalog shape), discover metrics whose ``metricInfo.semanticModel``
points at that model and report which lack ``metricUpstreams.datasetUpstreams``.

This command does not write metadata. Re-ingest (or an SDK write with
``upstream_datasets``) is the migration path — both emit per-metric
``metricUpstreams`` with correct routing. Safe to re-run.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Sequence, Tuple

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.filters import RemovedStatusFilter, SearchFilterRule
from datahub.metadata.schema_classes import MetricUpstreamsClass, SemanticModelInfoClass
from datahub.utilities.urns.urn import guess_entity_type

log = logging.getLogger(__name__)

SEMANTIC_MODEL_FIELD = "semanticModel"


@dataclass
class ContainerMigrationResult:
    semantic_model_urn: str
    datasets_seen: List[str] = field(default_factory=list)
    metrics_seen: List[str] = field(default_factory=list)
    metrics_missing_upstreams: List[str] = field(default_factory=list)
    metric_errors: List[Tuple[str, str]] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    # True when semanticModelInfo.datasets is empty — already new-shape / nothing
    # to report from stored datasets (mirrors subtype_skipped in snowflake migrate).
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
        old_shape = [
            r for r in self.results if r.error is None and not r.skipped_empty_datasets
        ]
        new_shape_ready = [
            r
            for r in old_shape
            if not r.metrics_missing_upstreams and not r.metric_errors
        ]
        needs_reingest = [r for r in old_shape if r.metrics_missing_upstreams]
        metric_error_count = sum(len(r.metric_errors) for r in self.results)
        lines = [
            f"{prefix}Semantic Model Container Migration Report:",
            "--------------",
            f"{prefix}Semantic models on the old shape "
            f"(semanticModelInfo.datasets populated) = {len(old_shape)}",
            f"{prefix}Semantic models with all metrics on the new shape "
            f"(metricUpstreams populated) = {len(new_shape_ready)}",
            f"{prefix}Semantic models with metrics still missing metricUpstreams "
            f"(re-ingest required) = {len(needs_reingest)}",
            f"{prefix}Semantic models skipped "
            f"(empty semanticModelInfo.datasets) = {len(skipped)}",
            f"{prefix}Semantic models errored = {len(failed)}",
            f"{prefix}metric-level errors = {metric_error_count}",
        ]
        if skipped:
            lines.append(f"{prefix}Skipped (empty semanticModelInfo.datasets):")
            for r in skipped:
                lines.append(f"{prefix}  skipped: {r.semantic_model_urn}")
        lines.append(f"{prefix}Details:")
        for r in old_shape:
            lines.append(f"{prefix}  {r.semantic_model_urn}")
            lines.append(
                f"{prefix}    datasets from semanticModelInfo.datasets: "
                f"{len(r.datasets_seen)}"
            )
            lines.append(f"{prefix}    metrics seen: {len(r.metrics_seen)}")
            if r.metrics_missing_upstreams:
                lines.append(
                    f"{prefix}    metrics missing metricUpstreams: "
                    f"{', '.join(r.metrics_missing_upstreams)}"
                )
            if r.metric_errors:
                for metric_urn, message in r.metric_errors:
                    lines.append(f"{prefix}    metric error: {metric_urn}: {message}")
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


def _metric_has_dataset_upstreams(graph: DataHubGraph, metric_urn: str) -> bool:
    upstreams = graph.get_aspect(metric_urn, MetricUpstreamsClass)
    return (
        upstreams is not None
        and upstreams.datasetUpstreams is not None
        and len(upstreams.datasetUpstreams) > 0
    )


def migrate_semantic_model(
    graph: DataHubGraph,
    semantic_model_urn: str,
    dry_run: bool,
    include_soft_deleted: bool = False,
) -> ContainerMigrationResult:
    del dry_run  # report-only; retained for CLI signature compatibility
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

        if not dataset_urns:
            result.skipped_empty_datasets = True
            result.notes.append(
                "semanticModelInfo.datasets empty or missing; "
                "nothing to report from stored datasets"
            )
            return result

        metric_urns = discover_metrics_for_semantic_model(
            graph,
            semantic_model_urn,
            include_soft_deleted=include_soft_deleted,
        )
        result.metrics_seen = list(metric_urns)
        for metric_urn in metric_urns:
            try:
                if not _metric_has_dataset_upstreams(graph, metric_urn):
                    result.metrics_missing_upstreams.append(metric_urn)
            except Exception as e:
                log.warning(
                    "Failed to inspect metricUpstreams for %s: %s",
                    metric_urn,
                    e,
                )
                result.metric_errors.append((metric_urn, str(e)))
    except Exception as e:
        log.warning(f"Failed to report on {semantic_model_urn}: {e}")
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
