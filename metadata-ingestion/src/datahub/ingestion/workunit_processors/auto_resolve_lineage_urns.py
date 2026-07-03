# NOTE: `from __future__ import annotations` keeps the schema_resolver type hints
# (imported only under TYPE_CHECKING) as strings, so importing this module does not
# pull in sqlglot. The sqlglot-heavy schema_resolver imports are deferred into the
# methods that use them — importing this module (e.g. via the workunit_processors
# package) must stay lightweight so it never adds a transitive sqlglot requirement
# to connectors that don't declare it.
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Type,
)

from typing_extensions import TypeGuard

from datahub.emitter.mce_builder import make_schema_field_urn

# _make_generic_aspect is the canonical typed-aspect -> GenericAspect serializer used by
# MetadataChangeProposalWrapper.make_mcp(); we reuse it to write a mutated aspect back
# into a raw MetadataChangeProposal (see _write_back_if_mcp).
from datahub.emitter.mcp import _make_generic_aspect
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import (
    ChartInfoClass,
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
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.urns.urn import Urn, guess_entity_type
from datahub.utilities.urns.urn_iter import lowercase_dataset_urn

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.ingestion.run.pipeline_config import UpstreamPlatformCasing
    from datahub.sql_parsing.schema_resolver import SchemaInfo, SchemaResolver

logger = logging.getLogger(__name__)

# Above this many URNs per platform, the in-memory case-insensitive index is large
# enough to warrant an explicit heads-up to operators rather than letting it surface as
# unexplained memory pressure. (A disk-backed, casing-aware index owned by SchemaResolver
# is the planned follow-up; see the schemaless/membership backlog task.)
_CATALOG_SIZE_WARN_THRESHOLD = 500_000

# The closed set of matchType verdicts, as a Literal so the if/elif verdict chains that
# drive correctness can be typo- and exhaustiveness-checked. LineageMatchTypeClass
# renders these as plain ``str`` (codegen), so we bind Literal-typed aliases and assert
# they stay in sync with the generated class.
MatchType = Literal["EXACT", "NORMALIZED", "UNRESOLVED"]
_EXACT: MatchType = "EXACT"
_NORMALIZED: MatchType = "NORMALIZED"
_UNRESOLVED: MatchType = "UNRESOLVED"
assert (_EXACT, _NORMALIZED, _UNRESOLVED) == (
    LineageMatchTypeClass.EXACT,
    LineageMatchTypeClass.NORMALIZED,
    LineageMatchTypeClass.UNRESOLVED,
), "MatchType literals drifted from LineageMatchTypeClass"


@dataclass
class AutoResolveLineageUrnsProcessorReport(WorkunitProcessorReport):
    """Report for AutoResolveLineageUrnsProcessor metrics."""

    num_dataset_urns_normalized: int = 0  # Upstream dataset URNs rewritten
    num_column_urns_normalized: int = 0  # Fine-grained field URNs rewritten
    num_refs_unchanged: int = 0  # Left as-is (exact match, or out of scope)
    num_refs_unresolved: int = 0  # Configured platform, no unique match (flagged)
    num_exceptions: int = 0  # Failed to process a workunit
    # Workunit-level counts for the deser/reser cost ratio: a raw MCP is deserialized
    # once per lineage-bearing workunit and re-serialized only when mutated. The ratio
    # num_workunits_modified / num_workunits_with_lineage tells whether skipping the
    # deserialization when nothing needs fixing would be worthwhile.
    num_workunits_with_lineage: int = (
        0  # Carried a lineage aspect (deserialization paid)
    )
    num_workunits_modified: int = (
        0  # A lineage aspect was actually mutated (re-serialized)
    )


@dataclass
class _Resolution:
    """Outcome of resolving one dataset URN against the configured platform(s)."""

    urn: str  # The (possibly rewritten) URN to emit.
    schema: Optional[SchemaInfo]  # Cached schema of the resolved entity, if known.
    # EXACT / NORMALIZED / UNRESOLVED / None (no reconciliation performed).
    match_type: Optional[MatchType]


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


def _is_dataset_urn(urn: Optional[str]) -> TypeGuard[str]:
    """True iff `urn` is a well-formed dataset URN.

    Non-raising: ``guess_entity_type`` asserts on a malformed / empty / non-URN
    string, so calling it unguarded inside a per-reference loop would let one bad
    reference abort resolution for every valid sibling in the aspect. A stray
    reference is skipped instead.
    """
    if not urn:
        return False
    try:
        return guess_entity_type(urn) == "dataset"
    except Exception:
        return False


class AutoResolveLineageUrnsProcessor(
    WorkunitProcessor[AutoResolveLineageUrnsProcessorReport]
):
    """Resolve the casing of upstream warehouse URN references in lineage.

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
            ctx.pipeline_context.flags.auto_resolve_lineage_urns.upstream_platforms
        )
        # Per-platform state, lazily bulk-initialized on first reference. The
        # SchemaResolvers are the source of truth for schema + membership; the casing
        # index is a derived lowercase(urn) -> real URNs map used to reconcile arbitrary
        # casings and detect ambiguous collisions. NOTE: this local index duplicates
        # membership the SchemaResolver could own and only exists until SchemaResolver
        # gains casing-aware resolution (tracked follow-up), at which point it is deleted.
        self._resolvers_by_platform: Dict[str, List["SchemaResolver"]] = {}
        self._casing_index_by_platform: Dict[str, Dict[str, List[str]]] = {}
        # Platforms whose catalog load has been attempted. Doubles as the set of
        # platforms actually referenced this run (every reference routes through
        # _ensure_platform_loaded), so _warn_unmatched_platforms uses it to flag
        # configured platforms that no reference matched.
        self._loaded_platforms: Set[str] = set()
        # A bounded sample of URNs left UNRESOLVED, for the aggregated end-of-run
        # warning (the counter num_refs_unresolved gives the full total).
        self._unresolved_sample: LossyList[str] = LossyList()
        # (aspect class -> in-place normalizer, returns True iff it mutated the aspect).
        # These are the aspects a BI / orchestration source emits that carry *upstream
        # dataset* references — the only refs affected by cross-source casing mismatch:
        # upstreamLineage (table + fineGrained columns), dashboardInfo / chartInfo inputs,
        # and dataJobInputOutput inputs (dbt / Airflow / Spark). Other lineage aspects
        # don't target datasets or are the entity's own outputs (see the dev guide).
        self._normalizers: List[Tuple[Type[_Aspect], Callable[..., bool]]] = [
            (UpstreamLineageClass, self._normalize_upstream_lineage),
            (DashboardInfoClass, self._normalize_dashboard_info),
            (DataJobInputOutputClass, self._normalize_datajob_io),
            (ChartInfoClass, self._normalize_chart_info),
        ]

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        cfg = ctx.pipeline_context.flags.auto_resolve_lineage_urns
        # This processor is in the shared chain for *every* source, and some tests build
        # a source with a bare Mock() ctx where cfg.enabled / cfg.upstream_platforms are
        # truthy Mocks. Fail closed on a degenerate/mock config: require enabled to be
        # exactly True and upstream_platforms to be a real, non-empty list — otherwise
        # the processor would initialize with a Mock self._config and crash mid-run.
        if cfg.enabled is not True:
            return False
        if not isinstance(cfg.upstream_platforms, list) or not cfg.upstream_platforms:
            if isinstance(cfg.upstream_platforms, list):
                # Genuinely enabled but unconfigured (empty list): every reference would
                # no-op — skip the bulk catalog load entirely and tell the operator why.
                logger.warning(
                    "auto_resolve_lineage_urns is enabled but no upstream_platforms "
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
                self._resolve_workunit(wu)
            except Exception as e:
                self.report.num_exceptions += 1
                # Surface in the pipeline report (not just this processor's sub-report),
                # so a run that fails to reconcile part of a source's lineage doesn't
                # look clean. Keeps the processor counter above; logs via the report.
                self.ctx.source_report.warning(
                    title="Lineage URN casing not reconciled",
                    message="Failed to reconcile lineage URN casing for a work unit; "
                    "its lineage is emitted unchanged.",
                    context=wu.id,
                    exc=e,
                )
            yield wu
        self._warn_unmatched_platforms()
        self._warn_unresolved_refs()

    def _resolve_workunit(self, wu: MetadataWorkUnit) -> None:
        """Reconcile casing on each lineage aspect the workunit carries, in place.

        We edit the typed aspect (via get_aspect_of_type) rather than the uniform
        transform_urns() helper: we must be selective (only upstream refs, never the
        entity or downstream fields) and set a non-URN field (matchType). For MCE/MCPW
        get_aspect_of_type returns the *live* aspect (in-place edit, no (de)serialization);
        a raw MCP is deserialized to inspect, and re-serialized (via _write_back_if_mcp)
        only when something actually changed.
        """
        had_lineage = False
        modified = False
        for aspect_cls, normalize in self._normalizers:
            aspect = wu.get_aspect_of_type(aspect_cls)
            if aspect is None:
                continue
            had_lineage = True
            if normalize(aspect):
                self._write_back_if_mcp(wu, aspect)
                modified = True
        if had_lineage:
            self.report.num_workunits_with_lineage += 1
        if modified:
            self.report.num_workunits_modified += 1

    def _warn_unmatched_platforms(self) -> None:
        """Warn about configured platforms that no reference used this run.

        The usual cause is a case/spelling mismatch in the config (platform names are
        compared case-sensitively), which would otherwise silently heal nothing.
        """
        # Defense in depth: this runs outside the per-workunit try/except, so guard
        # against a non-list config (should_enable already fails closed on a mock ctx).
        if not isinstance(self._config, list):
            return
        unmatched = {entry.platform for entry in self._config} - self._loaded_platforms
        if unmatched:
            logger.warning(
                f"auto_resolve_lineage_urns: configured upstream platform(s) "
                f"{sorted(unmatched)} matched no lineage references in this run. If "
                f"unexpected, check the platform name — it must match the dataset URN's "
                f"platform exactly (case-sensitive), e.g. 'snowflake', not 'Snowflake'."
            )

    def _warn_unresolved_refs(self) -> None:
        """Surface UNRESOLVED references in the pipeline report, once, aggregated.

        UNRESOLVED is the "this lineage is likely broken" signal; a per-reference
        warning would be too noisy, so emit one end-of-run warning with the total count
        and a bounded sample of the URNs left unchanged.
        """
        if self.report.num_refs_unresolved == 0:
            return
        self.ctx.source_report.warning(
            title="Lineage references not resolved to an existing entity",
            message="Some upstream lineage references could not be reconciled to a "
            "single existing entity (no case-insensitive match, or an ambiguous casing "
            "collision) and were left unchanged; that lineage may be broken.",
            context=f"{self.report.num_refs_unresolved} reference(s); "
            f"sample: {list(self._unresolved_sample)}",
        )

    @staticmethod
    def _write_back_if_mcp(wu: MetadataWorkUnit, aspect: _Aspect) -> None:
        # get_aspect_of_type returns the *live* aspect for MCE/MCPW workunits, so the
        # in-place mutation is already reflected in what gets emitted — nothing to do.
        # A raw MetadataChangeProposal (e.g. workunits from the file source) instead
        # hands back a throwaway deserialized copy, so the mutation would be silently
        # dropped unless we re-serialize it into the proposal's generic aspect. Callers
        # invoke this only when a mutation actually happened, so an unchanged raw MCP is
        # never re-serialized.
        if isinstance(wu.metadata, MetadataChangeProposalClass):
            wu.metadata.aspect = _make_generic_aspect(aspect)

    # --- resolution -------------------------------------------------------------

    def _ensure_platform_loaded(self, platform: str) -> None:
        """Bulk-load every configured entry on this platform, once.

        Built from what ``SchemaResolver`` already provides — ``get_urns()``, i.e. the
        platform's **schema-bearing** entities — so resolution uses the resolver's
        existing single bulk scroll and adds no framework changes. Entities that exist
        in DataHub without a schema are therefore not reconciled (a tracked follow-up to
        enrich SchemaResolver with membership).

        Keyed by platform only: ``platform_instance`` and ``env`` are fused into each
        dataset URN's name, so the lowercase index distinguishes them at full-URN
        granularity. A reference for a platform instance / env that isn't configured
        simply won't appear in the index and resolves to UNRESOLVED.
        """
        # Load a platform's catalog at most once. Recording the platform here (before
        # the entries check) also means _loaded_platforms captures every referenced
        # platform, which _warn_unmatched_platforms relies on.
        if platform in self._loaded_platforms:
            return
        self._loaded_platforms.add(platform)

        # Deferred import: schema_resolver_provider pulls in sqlglot, which must not
        # be a module-load-time dependency (see the note at the top of this file).
        from datahub.sql_parsing.schema_resolver_provider import provide_schema_resolver

        # `platform` here is the normalized platform name parsed from the dataset URN;
        # entry.platform is normalized to the same bare-name form by the config
        # validator, so compare like-for-like.
        entries = [entry for entry in self._config if entry.platform == platform]
        if not entries:
            # Platform not configured: references to it are out of scope.
            return
        # Emitted before the (potentially long, paginated) fetch so operators see a
        # signal during the stall on the first lineage work unit, not only after.
        logger.info(
            f"Loading '{platform}' catalog from DataHub for lineage casing "
            f"reconciliation; this may take a while on large warehouses..."
        )
        index: Dict[str, List[str]] = {}
        resolvers: List[SchemaResolver] = []
        for entry in entries:
            resolver = provide_schema_resolver(
                graph=self._graph,
                platform=entry.platform,
                platform_instance=entry.platform_instance,
                env=entry.env,
            )
            resolvers.append(resolver)
            # get_urns() is the schema-bearing entity set the resolver already
            # loaded; fold it into a lowercase index. A URN is "present exactly" iff
            # it appears in its own lowercase bucket, so no separate set is needed.
            for existing in resolver.get_urns():
                try:
                    bucket = index.setdefault(lowercase_dataset_urn(existing), [])
                except Exception:
                    continue
                if existing not in bucket:
                    bucket.append(existing)
        # The index is held in memory for the pipeline's lifetime; log its size,
        # escalating to WARNING once it's large enough to matter.
        count = sum(len(bucket) for bucket in index.values())
        message = (
            f"Loaded {count} '{platform}' dataset URNs for lineage casing "
            f"reconciliation."
        )
        if count > _CATALOG_SIZE_WARN_THRESHOLD:
            logger.warning(
                f"{message} This is a large catalog and may use significant memory; "
                f"consider narrowing upstream_platforms (platform_instance / env) to "
                f"the assets this source references."
            )
        else:
            logger.info(message)
        self._resolvers_by_platform[platform] = resolvers
        self._casing_index_by_platform[platform] = index

    @staticmethod
    def _schema_for(
        urn: str, resolvers: List["SchemaResolver"]
    ) -> Optional[SchemaInfo]:
        # The URN came from get_urns() (schema-bearing, already cached), so resolve_urn
        # returns its schema from cache without a graph call.
        for resolver in resolvers:
            _, schema = resolver.resolve_urn(urn)
            if schema is not None:
                return schema
        return None

    def _resolve_dataset(
        self, urn: str, *, include_schema: bool = False
    ) -> _Resolution:
        """Resolve `urn` to its existing casing in DataHub, optionally with its schema.

        Prefers an exact match (don't merge genuinely distinct case-sensitive entities,
        and record it as EXACT); falls back to a unique case-insensitive match (recorded
        as NORMALIZED); leaves the URN unchanged and flags UNRESOLVED when there is no
        match or an ambiguous collision. Membership is the resolver's schema-bearing
        entities, so a reference to an existing-but-schemaless entity is reported
        UNRESOLVED (reconciling those is a tracked follow-up).

        `include_schema` fetches the resolved entity's schema (only needed for
        column-casing correction); table-level callers leave it off to skip a cache read.
        """
        try:
            platform = DataPlatformUrn.from_string(
                DatasetUrn.from_string(urn).platform
            ).platform_name
        except Exception:
            return _Resolution(urn, None, None)
        self._ensure_platform_loaded(platform)
        resolvers = self._resolvers_by_platform.get(platform)
        if not resolvers:
            return _Resolution(urn, None, None)

        # The lowercase bucket for this URN holds every known entity sharing its
        # case-insensitive form. Compute it once and derive the verdict from it.
        try:
            bucket = self._casing_index_by_platform[platform].get(
                lowercase_dataset_urn(urn), []
            )
        except Exception:
            return _Resolution(urn, None, None)

        # Exact match wins: the URN is present in its own bucket. Don't merge genuinely
        # distinct case-sensitive entities. Schema, if any, is for column casing.
        if urn in bucket:
            return _Resolution(
                urn,
                self._schema_for(urn, resolvers) if include_schema else None,
                _EXACT,
            )

        # No exact match: a unique case-insensitive match heals; ambiguity is left alone.
        candidates = [c for c in bucket if c != urn]
        if len(candidates) == 1:
            resolved = candidates[0]
            return _Resolution(
                resolved,
                self._schema_for(resolved, resolvers) if include_schema else None,
                _NORMALIZED,
            )
        # On a configured platform but no unique match (none, or an ambiguous casing
        # collision): leave the URN unchanged but flag it UNRESOLVED so potentially
        # broken lineage is visible rather than indistinguishable from a clean edge.
        # Sampled here (the single UNRESOLVED site) for the aggregated end-of-run warning.
        self._unresolved_sample.append(urn)
        return _Resolution(urn, None, _UNRESOLVED)

    # --- aspect rewriters -------------------------------------------------------
    #
    # Each returns True iff it mutated the aspect (rewrote a reference or stamped a
    # matchType), so process() can skip the raw-MCP re-serialization when nothing in the
    # aspect was in scope.

    def _normalize_upstream_lineage(self, aspect: UpstreamLineageClass) -> bool:
        changed = False
        for upstream in aspect.upstreams:
            dataset = getattr(upstream, "dataset", None)
            if not _is_dataset_urn(dataset):
                continue
            res = self._resolve_dataset(dataset)
            # Stamp the verdict (EXACT / NORMALIZED / UNRESOLVED) for any reference on
            # a configured platform; out-of-scope refs get res.match_type=None and are
            # left untouched.
            if res.match_type is not None:
                upstream.matchType = res.match_type
                changed = True
            if res.match_type == _NORMALIZED:
                upstream.dataset = res.urn
                self.report.num_dataset_urns_normalized += 1
            elif res.match_type == _UNRESOLVED:
                self.report.num_refs_unresolved += 1
            else:
                self.report.num_refs_unchanged += 1

        for fine_grained in aspect.fineGrainedLineages or []:
            if self._normalize_fine_grained_upstreams(fine_grained):
                changed = True
        return changed

    def _normalize_fine_grained_upstreams(
        self, fine_grained: FineGrainedLineageClass
    ) -> bool:
        # Only upstream references are healed; downstream fields belong to the entity
        # this aspect describes and must keep its casing.
        if not fine_grained.upstreams:
            return False
        changed = False
        rewritten: List[str] = []
        match_types: List[Optional[MatchType]] = []
        for field_urn in fine_grained.upstreams:
            new_urn, match_type = self._resolve_field_urn(field_urn)
            rewritten.append(new_urn)
            match_types.append(match_type)
            if new_urn != field_urn:
                changed = True
        fine_grained.upstreams = rewritten
        # Aggregate a single verdict for the fine-grained lineage, surfacing the most
        # actionable signal first: NORMALIZED (something was healed) > UNRESOLVED (a
        # field couldn't be matched) > EXACT (all verified). Absent only when every
        # field was out of scope.
        aggregate: Optional[str] = None
        if _NORMALIZED in match_types:
            aggregate = _NORMALIZED
        elif _UNRESOLVED in match_types:
            aggregate = _UNRESOLVED
        elif _EXACT in match_types:
            aggregate = _EXACT
        if aggregate is not None:
            fine_grained.matchType = aggregate
            changed = True
        return changed

    def _resolve_field_urn(self, field_urn: str) -> Tuple[str, Optional[MatchType]]:
        parent = _parent_dataset_urn(field_urn)
        field_path = _field_path(field_urn)
        if parent is None or field_path is None:
            self.report.num_refs_unchanged += 1
            return field_urn, None

        # Column-level: we need the parent's schema to correct the column casing.
        res = self._resolve_dataset(parent, include_schema=True)
        new_field_path = field_path
        if res.schema:
            # Deferred import: keeps sqlglot off this module's load path.
            from datahub.sql_parsing.schema_resolver import match_columns_to_schema

            new_field_path = match_columns_to_schema(res.schema, [field_path])[0]

        if res.urn == parent and new_field_path == field_path:
            if res.match_type == _UNRESOLVED:
                self.report.num_refs_unresolved += 1
            else:
                self.report.num_refs_unchanged += 1
            return field_urn, res.match_type
        # A field (schemaField) URN is a single column-level reference, so any rewrite
        # is counted under the column bucket — whether the parent dataset casing, the
        # column casing, or both changed. num_dataset_urns_normalized is reserved for
        # table-level references. A corrected column path is itself a normalization
        # even when the parent dataset matched exactly, so report NORMALIZED in that
        # case rather than the parent's (EXACT) match type.
        self.report.num_column_urns_normalized += 1
        match_type = _NORMALIZED if new_field_path != field_path else res.match_type
        return make_schema_field_urn(res.urn, new_field_path), match_type

    def _normalize_dashboard_info(self, aspect: DashboardInfoClass) -> bool:
        changed = False
        if aspect.datasets:
            aspect.datasets, c = self._heal_dataset_urns(aspect.datasets)
            changed = changed or c
        if self._heal_dataset_edges(aspect.datasetEdges or []):
            changed = True
        return changed

    def _normalize_datajob_io(self, aspect: DataJobInputOutputClass) -> bool:
        # A DataJob's *inputs* are upstream warehouse references (the dbt / Airflow /
        # Spark warehouse-upstream path) and are healed like any other upstream. The
        # job's outputs are its declared products and are left untouched, matching the
        # processor's rule of never rewriting an entity's own / downstream side.
        changed = False
        if aspect.inputDatasets:
            aspect.inputDatasets, c = self._heal_dataset_urns(aspect.inputDatasets)
            changed = changed or c
        if self._heal_dataset_edges(aspect.inputDatasetEdges or []):
            changed = True
        for fine_grained in aspect.fineGrainedLineages or []:
            if self._normalize_fine_grained_upstreams(fine_grained):
                changed = True
        return changed

    def _normalize_chart_info(self, aspect: ChartInfoClass) -> bool:
        # A chart's `inputs` / `inputEdges` are the upstream datasets it reads from.
        # For BI tools that query the warehouse directly (e.g. Superset, Mode, Redash,
        # Metabase) these point straight at warehouse tables, so casing mismatches
        # there break lineage just like any other upstream reference.
        changed = False
        if aspect.inputs:
            aspect.inputs, c = self._heal_dataset_urns(aspect.inputs)
            changed = changed or c
        if self._heal_dataset_edges(aspect.inputEdges or []):
            changed = True
        return changed

    def _heal_dataset_urns(self, urns: List[str]) -> Tuple[List[str], bool]:
        healed: List[str] = []
        changed = False
        for dataset in urns:
            # Guard non-dataset / malformed URNs (consistent with
            # _normalize_upstream_lineage and _heal_dataset_edges): leave them untouched
            # without attempting resolution.
            if not _is_dataset_urn(dataset):
                healed.append(dataset)
                continue
            res = self._resolve_dataset(dataset)
            # A plain URN list / Edge has no matchType field to stamp, but the report
            # counters must still distinguish UNRESOLVED (broken) from a clean ref so a
            # dashboard/datajob pointing at broken lineage isn't invisible in the report.
            if res.match_type == _NORMALIZED:
                self.report.num_dataset_urns_normalized += 1
                changed = True
            elif res.match_type == _UNRESOLVED:
                self.report.num_refs_unresolved += 1
            else:
                self.report.num_refs_unchanged += 1
            healed.append(res.urn)
        return healed, changed

    def _heal_dataset_edges(self, edges: List[EdgeClass]) -> bool:
        changed = False
        for edge in edges:
            destination = getattr(edge, "destinationUrn", None)
            if not _is_dataset_urn(destination):
                continue
            res = self._resolve_dataset(destination)
            if res.match_type == _NORMALIZED:
                edge.destinationUrn = res.urn
                self.report.num_dataset_urns_normalized += 1
                changed = True
            elif res.match_type == _UNRESOLVED:
                self.report.num_refs_unresolved += 1
            else:
                self.report.num_refs_unchanged += 1
        return changed
