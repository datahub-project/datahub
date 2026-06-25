# NOTE: `from __future__ import annotations` keeps the schema_resolver type hints
# (imported only under TYPE_CHECKING) as strings, so importing this module does not
# pull in sqlglot. The sqlglot-heavy schema_resolver imports are deferred into the
# methods that use them — importing this module (e.g. via the workunit_processors
# package) must stay lightweight so it never adds a transitive sqlglot requirement
# to connectors that don't declare it.
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Set, Tuple

from datahub.emitter.mce_builder import make_schema_field_urn

# _make_generic_aspect is the canonical typed-aspect -> GenericAspect serializer used by
# MetadataChangeProposalWrapper.make_mcp(); we reuse it to write a mutated aspect back
# into a raw MetadataChangeProposal (see _write_back).
from datahub.emitter.mcp import _make_generic_aspect
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import (
    DashboardInfoClass,
    DataJobInputOutputClass,
    EdgeClass,
    FineGrainedLineageClass,
    LineageMatchTypeClass,
    MetadataChangeProposalClass,
    UpstreamLineageClass,
    _Aspect,
)
from datahub.metadata.urns import DataPlatformUrn, DatasetUrn
from datahub.utilities.urns.urn import Urn, guess_entity_type
from datahub.utilities.urns.urn_iter import lowercase_dataset_urn

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.ingestion.run.pipeline_config import UpstreamPlatformCasing
    from datahub.sql_parsing.schema_resolver import SchemaInfo, SchemaResolver

logger = logging.getLogger(__name__)

# Above this many URNs per platform, the in-memory URN set + lowercase index are large
# enough (order of hundreds of MB) to warrant an explicit heads-up to operators rather
# than letting it surface as unexplained memory pressure / OOM.
_CATALOG_SIZE_WARN_THRESHOLD = 500_000


@dataclass
class AutoNormalizeLineageUrnsProcessorReport(WorkunitProcessorReport):
    """Report for AutoNormalizeLineageUrnsProcessor metrics."""

    num_dataset_urns_normalized: int = 0  # Upstream dataset URNs rewritten
    num_column_urns_normalized: int = 0  # Fine-grained field URNs rewritten
    num_refs_unchanged: int = 0  # Left as-is (already canonical or no match)
    num_exceptions: int = 0  # Failed to process a workunit


@dataclass
class _Resolution:
    """Outcome of resolving one dataset URN against the configured catalogs."""

    urn: str  # The (possibly rewritten) URN to emit.
    schema: Optional[SchemaInfo]  # Cached schema of the resolved entity, if known.
    # EXACT / NORMALIZED / None (no reconciliation performed).
    match_type: Optional[str]


@dataclass
class _Catalog:
    """A platform's catalog used for casing resolution.

    `urns` is the *complete* set of dataset URNs for the platform (independent of
    whether they have a schema), so exact-match and ambiguity checks see every real
    entity. `index` maps lowercase(urn) -> the real URNs sharing that form. The
    `resolvers` carry schemas (a subset — only schema-bearing entities) and are used
    solely to correct column-name casing.
    """

    urns: Set[str]
    index: Dict[str, List[str]]
    resolvers: List["SchemaResolver"]


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
        # Lazily bulk-initialized catalogs, keyed by normalized platform name.
        self._catalog_by_platform: Dict[str, _Catalog] = {}

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        cfg = ctx.pipeline_context.flags.normalize_lineage_urn_casing
        if not cfg.enabled:
            return False
        if not cfg.upstream_platforms:
            # Enabled but unconfigured: every reference would no-op. Skip the
            # per-platform bulk catalog load entirely and tell the operator why.
            logger.warning(
                "normalize_lineage_urn_casing is enabled but no upstream_platforms "
                "are configured; the processor will not run. Configure the warehouse "
                "platform(s) this source references to enable casing reconciliation."
            )
            return False
        # Use getattr for graph: it's a no-op without a backend, and `graph` is a
        # PipelineContext instance attribute (absent from MagicMock(spec=...) used by
        # some connector tests).
        return getattr(ctx.pipeline_context, "graph", None) is not None

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        for wu in stream:
            try:
                upstream = wu.get_aspect_of_type(UpstreamLineageClass)
                if upstream is not None:
                    self._normalize_upstream_lineage(upstream)
                    self._write_back(wu, upstream)
                dashboard = wu.get_aspect_of_type(DashboardInfoClass)
                if dashboard is not None:
                    self._normalize_dashboard_info(dashboard)
                    self._write_back(wu, dashboard)
                datajob = wu.get_aspect_of_type(DataJobInputOutputClass)
                if datajob is not None:
                    self._normalize_datajob_io(datajob)
                    self._write_back(wu, datajob)
            except Exception as e:
                self.report.num_exceptions += 1
                logger.warning(
                    f"Failed to normalize lineage URN casing for {wu.id}: {e}",
                    exc_info=True,
                )
            yield wu

    @staticmethod
    def _write_back(wu: MetadataWorkUnit, aspect: _Aspect) -> None:
        # get_aspect_of_type returns the *live* aspect for MCE/MCPW workunits, so the
        # in-place mutation above is already reflected in what gets emitted. For a raw
        # MetadataChangeProposal (e.g. workunits from the file source) it returns a
        # throwaway deserialized copy, so the mutation would be silently dropped unless
        # we re-serialize it back into the proposal's generic aspect.
        if isinstance(wu.metadata, MetadataChangeProposalClass):
            wu.metadata.aspect = _make_generic_aspect(aspect)

    # --- resolution -------------------------------------------------------------

    def _get_catalog(self, platform: str) -> _Catalog:
        """Bulk-initialized catalog for every configured entry on this platform."""
        # Deferred import: schema_resolver_provider pulls in sqlglot, which must not
        # be a module-load-time dependency (see the note at the top of this file).
        from datahub.sql_parsing.schema_resolver_provider import provide_schema_resolver

        if platform not in self._catalog_by_platform:
            # Emitted before the (potentially long, paginated) fetch so operators see a
            # signal during the stall on the first lineage work unit, not only after.
            logger.info(
                f"Loading '{platform}' catalog from DataHub for lineage casing "
                f"reconciliation; this may take a while on large warehouses..."
            )
            urns: Set[str] = set()
            resolvers: List[SchemaResolver] = []
            for entry in self._config:
                # Normalize the configured platform: it may be a bare name
                # ("snowflake") or a full URN ("urn:li:dataPlatform:snowflake"), both
                # of which the resolver accepts. `platform` here is the normalized
                # name parsed from the dataset URN, so compare like-for-like.
                if DataPlatformUrn(entry.platform).platform_name != platform:
                    continue
                resolvers.append(
                    provide_schema_resolver(
                        graph=self._graph,
                        platform=entry.platform,
                        platform_instance=entry.platform_instance,
                        env=entry.env,
                    )
                )
                # The full URN set is fetched independently of schema presence, so
                # exact-match and ambiguity checks see every real entity — not only
                # the schema-bearing ones the resolver caches.
                urns.update(
                    self._graph.get_urns_by_filter(
                        entity_types=["dataset"],
                        platform=entry.platform,
                        platform_instance=entry.platform_instance,
                        env=entry.env,
                    )
                )
            index: Dict[str, List[str]] = {}
            for existing in urns:
                try:
                    index.setdefault(lowercase_dataset_urn(existing), []).append(
                        existing
                    )
                except Exception:
                    continue
            # The full URN set and its lowercase index are held in memory for the
            # lifetime of the pipeline. On very large warehouses this is the
            # processor's main memory cost; log the size so operators can gauge it,
            # escalating to WARNING once the catalog is large enough to matter.
            count = len(urns)
            message = (
                f"Loaded {count} '{platform}' dataset URNs for lineage casing "
                f"reconciliation (held in memory for the pipeline's lifetime)."
            )
            if count > _CATALOG_SIZE_WARN_THRESHOLD:
                logger.warning(
                    f"{message} This is a large catalog and may use significant "
                    f"memory; consider narrowing upstream_platforms "
                    f"(platform_instance / env) to the assets this source references."
                )
            else:
                logger.info(message)
            self._catalog_by_platform[platform] = _Catalog(urns, index, resolvers)
        return self._catalog_by_platform[platform]

    @staticmethod
    def _schema_for(
        urn: str, resolvers: List["SchemaResolver"]
    ) -> Optional[SchemaInfo]:
        for resolver in resolvers:
            schema = resolver.get_cached_schema_info(urn)
            if schema is not None:
                return schema
        return None

    def _resolve_dataset(self, urn: str) -> _Resolution:
        """Resolve `urn` to its existing casing in DataHub, with its schema info.

        Prefers an exact match against the full URN set (don't merge genuinely
        distinct case-sensitive entities, and record it as EXACT); falls back to a
        unique case-insensitive match (recorded as NORMALIZED); leaves the URN
        unchanged with no match type when there is no match or an ambiguous collision.
        Membership is schema-independent, so a real but schemaless entity still wins
        the exact match.
        """
        try:
            platform = DataPlatformUrn.from_string(
                DatasetUrn.from_string(urn).platform
            ).platform_name
        except Exception:
            return _Resolution(urn, None, None)
        catalog = self._get_catalog(platform)
        if not catalog.resolvers:
            return _Resolution(urn, None, None)

        # Exact match wins (schema-independent). Schema, if any, is for column casing.
        if urn in catalog.urns:
            return _Resolution(
                urn,
                self._schema_for(urn, catalog.resolvers),
                LineageMatchTypeClass.EXACT,
            )

        # No exact match: a unique case-insensitive match heals; ambiguity is left alone.
        try:
            normalized = lowercase_dataset_urn(urn)
        except Exception:
            return _Resolution(urn, None, None)
        candidates = [c for c in catalog.index.get(normalized, []) if c != urn]
        if len(candidates) == 1:
            resolved = candidates[0]
            return _Resolution(
                resolved,
                self._schema_for(resolved, catalog.resolvers),
                LineageMatchTypeClass.NORMALIZED,
            )
        # No match, or ambiguous collision -> leave unchanged.
        return _Resolution(urn, None, None)

    # --- aspect rewriters -------------------------------------------------------

    def _normalize_upstream_lineage(self, aspect: UpstreamLineageClass) -> None:
        for upstream in aspect.upstreams:
            dataset = getattr(upstream, "dataset", None)
            if dataset is None or guess_entity_type(dataset) != "dataset":
                continue
            res = self._resolve_dataset(dataset)
            if res.urn != dataset:
                upstream.dataset = res.urn
                upstream.matchType = res.match_type  # NORMALIZED
                self.report.num_dataset_urns_normalized += 1
            else:
                # Exact / no match: leave the upstream untouched. We deliberately do
                # NOT stamp matchType=EXACT — a clean edge then stays byte-identical to
                # the previous ingest so GMS dedupes it (no MCL churn). Absence of
                # matchType means "exact / not reconciled".
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
        rewritten: List[str] = []
        match_types: List[Optional[str]] = []
        for field_urn in fine_grained.upstreams:
            new_urn, match_type = self._resolve_field_urn(field_urn)
            rewritten.append(new_urn)
            match_types.append(match_type)
        fine_grained.upstreams = rewritten
        # Aggregate: stamp NORMALIZED only if at least one field was reconciled. We
        # don't stamp EXACT (absence means exact / not reconciled), so an unchanged
        # fine-grained lineage stays byte-identical and GMS dedupes it (no MCL churn).
        if LineageMatchTypeClass.NORMALIZED in match_types:
            fine_grained.matchType = LineageMatchTypeClass.NORMALIZED

    def _resolve_field_urn(self, field_urn: str) -> Tuple[str, Optional[str]]:
        parent = _parent_dataset_urn(field_urn)
        field_path = _field_path(field_urn)
        if parent is None or field_path is None:
            self.report.num_refs_unchanged += 1
            return field_urn, None

        res = self._resolve_dataset(parent)
        # Correct the column casing against the warehouse schema, if known.
        new_field_path = field_path
        if res.schema:
            # Deferred import: keeps sqlglot off this module's load path.
            from datahub.sql_parsing.schema_resolver import match_columns_to_schema

            new_field_path = match_columns_to_schema(res.schema, [field_path])[0]

        if res.urn == parent and new_field_path == field_path:
            self.report.num_refs_unchanged += 1
            return field_urn, res.match_type
        # A field (schemaField) URN is a single column-level reference, so any rewrite
        # is counted under the column bucket — whether the parent dataset casing, the
        # column casing, or both changed. num_dataset_urns_normalized is reserved for
        # table-level references. A corrected column path is itself a normalization
        # even when the parent dataset matched exactly, so report NORMALIZED in that
        # case rather than the parent's (EXACT) match type.
        self.report.num_column_urns_normalized += 1
        match_type = (
            LineageMatchTypeClass.NORMALIZED
            if new_field_path != field_path
            else res.match_type
        )
        return make_schema_field_urn(res.urn, new_field_path), match_type

    def _normalize_dashboard_info(self, aspect: DashboardInfoClass) -> None:
        if aspect.datasets:
            aspect.datasets = self._heal_dataset_urns(aspect.datasets)
        self._heal_dataset_edges(aspect.datasetEdges or [])

    def _normalize_datajob_io(self, aspect: DataJobInputOutputClass) -> None:
        # A DataJob's *inputs* are upstream warehouse references (the dbt / Airflow /
        # Spark warehouse-upstream path) and are healed like any other upstream. The
        # job's outputs are its declared products and are left untouched, matching the
        # processor's rule of never rewriting an entity's own / downstream side.
        if aspect.inputDatasets:
            aspect.inputDatasets = self._heal_dataset_urns(aspect.inputDatasets)
        self._heal_dataset_edges(aspect.inputDatasetEdges or [])
        for fine_grained in aspect.fineGrainedLineages or []:
            self._normalize_fine_grained_upstreams(fine_grained)

    def _heal_dataset_urns(self, urns: List[str]) -> List[str]:
        healed: List[str] = []
        for dataset in urns:
            # Guard non-dataset URNs (consistent with _normalize_upstream_lineage and
            # _heal_dataset_edges): leave them untouched without attempting resolution.
            if guess_entity_type(dataset) != "dataset":
                healed.append(dataset)
                continue
            res = self._resolve_dataset(dataset)
            if res.urn != dataset:
                self.report.num_dataset_urns_normalized += 1
            else:
                self.report.num_refs_unchanged += 1
            healed.append(res.urn)
        return healed

    def _heal_dataset_edges(self, edges: List[EdgeClass]) -> None:
        for edge in edges:
            destination = getattr(edge, "destinationUrn", None)
            if destination is None or guess_entity_type(destination) != "dataset":
                continue
            res = self._resolve_dataset(destination)
            if res.urn != destination:
                edge.destinationUrn = res.urn
                self.report.num_dataset_urns_normalized += 1
            else:
                self.report.num_refs_unchanged += 1
