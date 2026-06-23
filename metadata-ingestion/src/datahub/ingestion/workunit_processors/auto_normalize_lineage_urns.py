import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Tuple

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import (
    DashboardInfoClass,
    FineGrainedLineageClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import DataPlatformUrn, DatasetUrn
from datahub.sql_parsing.schema_resolver import (
    SchemaInfo,
    SchemaResolver,
    match_columns_to_schema,
)
from datahub.sql_parsing.schema_resolver_provider import provide_schema_resolver
from datahub.utilities.urns.urn import Urn, guess_entity_type

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.ingestion.run.pipeline_config import UpstreamPlatformCasing

logger = logging.getLogger(__name__)


@dataclass
class AutoNormalizeLineageUrnsProcessorReport(WorkunitProcessorReport):
    """Report for AutoNormalizeLineageUrnsProcessor metrics."""

    num_dataset_urns_normalized: int = 0  # Upstream dataset URNs rewritten
    num_column_urns_normalized: int = 0  # Fine-grained field URNs rewritten
    num_refs_unchanged: int = 0  # Left as-is (already canonical or no match)
    num_exceptions: int = 0  # Failed to process a workunit


def _parent_dataset_urn(field_urn: str) -> Optional[str]:
    """Return the parent dataset URN of a schemaField URN, or None if not parseable."""
    try:
        return Urn.from_string(field_urn).entity_ids[0]
    except Exception:
        return None


def _field_path(field_urn: str) -> Optional[str]:
    """Return the field path (column) of a schemaField URN, or None if not parseable."""
    try:
        return Urn.from_string(field_urn).entity_ids[1]
    except Exception:
        return None


class AutoNormalizeLineageUrnsProcessor(
    WorkunitProcessor[AutoNormalizeLineageUrnsProcessorReport]
):
    """Reconcile the casing of upstream warehouse URN references in lineage.

    Heals casing mismatches between sources (e.g. an uppercase Snowflake table
    referenced as lowercase by a BI tool, or vice versa) that would otherwise create
    two disconnected lineage nodes. For each configured upstream platform it
    bulk-loads that platform's URNs and schemas once (via ``SchemaResolverProvider``)
    and resolves every reference locally, in both directions, against the casing
    DataHub already stores — table-level (``UpstreamLineage``, ``DashboardInfo``) and
    column-level (``FineGrainedLineage`` field paths).

    Only references *to* warehouse assets found in this source's metadata are fixed;
    the entity the aspect is attached to and downstream fields are never touched. It
    must be enabled on BI-tool / cross-platform ingestions and configured with the
    upstream platform(s) — never on the warehouse ingestion, whose reported casing and
    identity must be respected.
    """

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        graph = ctx.pipeline_context.graph
        # Guaranteed non-None by should_enable(); assert for the type checker.
        assert graph is not None
        self._graph: DataHubGraph = graph
        self._config: List[UpstreamPlatformCasing] = (
            ctx.pipeline_context.flags.normalize_lineage_urn_casing.upstream_platforms
        )
        # Lazily bulk-initialized resolvers, keyed by platform name.
        self._resolvers_by_platform: Dict[str, List[SchemaResolver]] = {}

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        cfg = ctx.pipeline_context.flags.normalize_lineage_urn_casing
        return cfg.enabled and ctx.pipeline_context.graph is not None

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        for wu in stream:
            try:
                upstream = wu.get_aspect_of_type(UpstreamLineageClass)
                if upstream is not None:
                    self._normalize_upstream_lineage(upstream)
                dashboard = wu.get_aspect_of_type(DashboardInfoClass)
                if dashboard is not None:
                    self._normalize_dashboard_info(dashboard)
            except Exception as e:
                self.report.num_exceptions += 1
                logger.warning(
                    f"Failed to normalize lineage URN casing for {wu.id}: {e}",
                    exc_info=True,
                )
            yield wu

    # --- resolution -------------------------------------------------------------

    def _get_resolvers(self, platform: str) -> List[SchemaResolver]:
        """Bulk-initialized resolvers for every configured entry on this platform."""
        if platform not in self._resolvers_by_platform:
            self._resolvers_by_platform[platform] = [
                provide_schema_resolver(
                    graph=self._graph,
                    platform=entry.platform,
                    platform_instance=entry.platform_instance,
                    env=entry.env,
                )
                for entry in self._config
                if entry.platform == platform
            ]
        return self._resolvers_by_platform[platform]

    def _resolve_dataset(self, urn: str) -> Tuple[str, Optional[SchemaInfo]]:
        """Resolve `urn` to its existing casing in DataHub, with its schema info.

        Returns (resolved_urn, schema_info). Prefers an exact match (don't merge
        genuinely distinct case-sensitive entities); falls back to a unique
        case-insensitive match; leaves the URN unchanged when there is no match or an
        ambiguous collision across the configured catalogs.
        """
        try:
            platform = DataPlatformUrn.from_string(
                DatasetUrn.from_string(urn).platform
            ).platform_name
        except Exception:
            return urn, None
        resolvers = self._get_resolvers(platform)
        if not resolvers:
            return urn, None

        # Exact match anywhere wins (also gives us schema for column correction).
        for resolver in resolvers:
            if resolver.has_urn(urn):
                return urn, resolver.get_cached_schema_info(urn)

        # No exact match: try case-insensitive resolution across catalogs.
        candidates: Dict[str, Optional[SchemaInfo]] = {}
        for resolver in resolvers:
            resolved = resolver.resolve_urn_casing(urn)
            if resolved != urn:
                candidates[resolved] = resolver.get_cached_schema_info(resolved)
        if len(candidates) == 1:
            resolved, schema = next(iter(candidates.items()))
            return resolved, schema
        # No match, or ambiguous collision -> leave unchanged.
        return urn, None

    # --- aspect rewriters -------------------------------------------------------

    def _normalize_upstream_lineage(self, aspect: UpstreamLineageClass) -> None:
        for upstream in aspect.upstreams:
            dataset = getattr(upstream, "dataset", None)
            if dataset is None or guess_entity_type(dataset) != "dataset":
                continue
            resolved, _schema = self._resolve_dataset(dataset)
            if resolved != dataset:
                upstream.dataset = resolved
                self.report.num_dataset_urns_normalized += 1
            else:
                self.report.num_refs_unchanged += 1

        for fine_grained in aspect.fineGrainedLineages or []:
            self._normalize_fine_grained_upstreams(fine_grained)

    def _normalize_fine_grained_upstreams(
        self, fine_grained: FineGrainedLineageClass
    ) -> None:
        # Only upstream references are healed; downstream fields belong to the entity
        # this aspect describes and must keep its casing.
        if not fine_grained.upstreams:
            return
        fine_grained.upstreams = [
            self._resolve_field_urn(field_urn) for field_urn in fine_grained.upstreams
        ]

    def _resolve_field_urn(self, field_urn: str) -> str:
        parent = _parent_dataset_urn(field_urn)
        field_path = _field_path(field_urn)
        if parent is None or field_path is None:
            self.report.num_refs_unchanged += 1
            return field_urn

        resolved_parent, schema = self._resolve_dataset(parent)
        # Correct the column casing against the warehouse schema, if known.
        new_field_path = field_path
        if schema:
            new_field_path = match_columns_to_schema(schema, [field_path])[0]

        if resolved_parent == parent and new_field_path == field_path:
            self.report.num_refs_unchanged += 1
            return field_urn
        if new_field_path != field_path:
            self.report.num_column_urns_normalized += 1
        else:
            self.report.num_dataset_urns_normalized += 1
        return make_schema_field_urn(resolved_parent, new_field_path)

    def _normalize_dashboard_info(self, aspect: DashboardInfoClass) -> None:
        if aspect.datasets:
            rewritten: List[str] = []
            for dataset in aspect.datasets:
                resolved, _schema = self._resolve_dataset(dataset)
                if resolved != dataset:
                    self.report.num_dataset_urns_normalized += 1
                else:
                    self.report.num_refs_unchanged += 1
                rewritten.append(resolved)
            aspect.datasets = rewritten

        for edge in aspect.datasetEdges or []:
            destination = getattr(edge, "destinationUrn", None)
            if destination is None or guess_entity_type(destination) != "dataset":
                continue
            resolved, _schema = self._resolve_dataset(destination)
            if resolved != destination:
                edge.destinationUrn = resolved
                self.report.num_dataset_urns_normalized += 1
            else:
                self.report.num_refs_unchanged += 1
