# NOTE: `from __future__ import annotations` keeps the schema_resolver type hints
# (imported only under TYPE_CHECKING) as strings, so importing this module does not
# pull in sqlglot. This module is imported eagerly on every source's
# get_workunit_processors() path, so module load must stay sqlglot-free (guarded by
# test_module_import_does_not_pull_sqlglot). The sqlglot-heavy schema_resolver imports
# are therefore deferred to a single chokepoint in __init__, which runs only after
# should_enable() confirms the feature is on and a graph exists — off the module-load
# path, but honest about the dependency (see __init__).
from __future__ import annotations

import logging
from dataclasses import dataclass, field
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
from datahub.metadata.urns import DataPlatformUrn, DatasetUrn, SchemaFieldUrn
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.urns.error import InvalidUrnError

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.ingestion.run.pipeline_config import UpstreamPlatformCasing
    from datahub.sql_parsing.schema_resolver import SchemaInfo, SchemaResolver

logger = logging.getLogger(__name__)

# Above this many URNs per platform, the bulk-loaded SchemaResolver cache is large
# enough to warrant an explicit heads-up to operators rather than letting it surface as
# unexplained memory pressure. (A disk-backed, casing-aware resolver owned by
# SchemaResolver is the planned follow-up; see the normalizedUrn backlog task.)
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
    num_workunits_with_lineage_aspect: int = 0
    num_workunits_modified: int = 0
    # Bounded sample of references left UNRESOLVED, alongside the num_refs_unresolved
    # count, so the report shows *which* lineage looks broken, not just how much.
    unresolved_refs_sample: LossyList[str] = field(default_factory=LossyList)


@dataclass
class _Resolution:
    """Outcome of resolving one dataset URN against the configured platform(s)."""

    urn: str  # The (possibly rewritten) URN to emit.
    schema: Optional[SchemaInfo]  # Cached schema of the resolved entity, if known.
    # EXACT / NORMALIZED / UNRESOLVED / None (no reconciliation performed).
    match_type: Optional[MatchType]


def _parent_dataset_urn(field_urn: str) -> Optional[str]:
    """Return the parent dataset URN of a schemaField URN, or None if it isn't one.

    Uses the typed ``SchemaFieldUrn`` (full validation) rather than positional
    ``entity_ids[0]``; ``from_string`` raises ``InvalidUrnError`` on any non-schemaField
    URN, so a stray reference correctly yields None instead of a bogus value.
    """
    try:
        return SchemaFieldUrn.from_string(field_urn).parent
    except InvalidUrnError:
        return None


def _field_path(field_urn: str) -> Optional[str]:
    """Return the field path (column) of a schemaField URN, or None if it isn't one.

    Uses ``SchemaFieldUrn.field_path`` rather than positional ``entity_ids[1]``. This
    also closes a latent bug: with the positional access, a stray *dataset* URN returned
    its name (e.g. ``DB.SCHEMA.TABLE``) as a bogus field path; ``from_string`` raises
    ``InvalidUrnError`` on a non-schemaField URN, so we correctly return None.
    """
    try:
        return SchemaFieldUrn.from_string(field_urn).field_path
    except InvalidUrnError:
        return None


def _is_dataset_urn(urn: Optional[str]) -> TypeGuard[str]:
    """True iff `urn` is a well-formed dataset URN.

    Uses the typed ``DatasetUrn`` primitive (full structural validation) rather than the
    naive ``guess_entity_type`` splitter. Non-raising: ``from_string`` raises
    ``InvalidUrnError`` on a malformed / empty / non-dataset URN, so a stray reference
    is skipped rather than aborting resolution for its valid siblings in the aspect.
    """
    if not urn:
        return False
    try:
        DatasetUrn.from_string(urn)
        return True
    except InvalidUrnError:
        return False


class AutoResolveLineageUrnsProcessor(
    WorkunitProcessor[AutoResolveLineageUrnsProcessorReport]
):
    """Resolve the casing of upstream warehouse URN references in lineage.

    Heals casing mismatches between sources (e.g. a lowercase-stored Snowflake table
    referenced in a different casing by a BI tool) that would otherwise create two
    disconnected lineage nodes. For each configured upstream platform it bulk-loads that
    platform's URNs and schemas once (via ``SchemaResolverProvider``) and resolves every
    reference locally via ``SchemaResolver.resolve_table`` (which tries the original,
    lowercased, and mixed-instance casings — see ``_resolve_dataset``) against the casing
    DataHub already stores — table-level (``UpstreamLineage``,
    ``DashboardInfo``) and column-level (``FineGrainedLineage`` field paths). Broader
    any-casing resolution is a tracked SchemaResolver follow-up (the ``normalizedUrn``
    aspect).

    Only references *to* warehouse assets found in this source's metadata are fixed;
    the entity the aspect is attached to and downstream fields are never touched. It
    must be enabled on BI-tool / cross-platform ingestions and configured with the
    upstream platform(s) — never on the warehouse ingestion, whose reported casing and
    identity must be respected.
    """

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        graph = ctx.pipeline_context.graph
        # assert for the type checker.
        assert graph is not None
        self._graph: DataHubGraph = graph
        self._config: List[UpstreamPlatformCasing] = (
            ctx.pipeline_context.flags.auto_resolve_lineage_urns.upstream_platforms
        )
        # Resolve the sqlglot-backed schema_resolver callables once, here — a single
        # honest chokepoint rather than imports buried in two leaf methods. Deferred into
        # __init__ (not module level) so importing this module stays sqlglot-free
        # (guarded by test_module_import_does_not_pull_sqlglot); __init__ runs only after
        # should_enable() confirmed the feature is on. The sqlglot dependency itself is
        # validated up front by AutoResolveLineageUrnsConfig (fail-fast at config parse
        # when enabled), so these imports are guaranteed to succeed here.
        from datahub.sql_parsing.schema_resolver import match_columns_to_schema
        from datahub.sql_parsing.schema_resolver_provider import provide_schema_resolver

        self._provide_schema_resolver: Callable[..., SchemaResolver] = (
            provide_schema_resolver
        )
        self._match_columns_to_schema: Callable[[SchemaInfo, List[str]], List[str]] = (
            match_columns_to_schema
        )
        # Per-platform SchemaResolvers, bulk-initialized up front by _load_catalogs().
        # Casing matching is delegated to SchemaResolver.resolve_table (which tries the
        # reference's original, lowercased, and mixed-instance casings — see
        # _resolve_dataset) — the processor keeps no parallel casing index of its own.
        # Broader casing coverage (a non-lowercase table stored as Pascal / Mixed /
        # arbitrary) and casing-aware resolution are a tracked SchemaResolver follow-up
        # (the planned `normalizedUrn` aspect). Until that lands, only casings resolve_table
        # covers are reconciled here.
        self._resolvers_by_platform: Dict[str, List["SchemaResolver"]] = {}
        # Platforms actually referenced by this source's lineage, so
        # _warn_unmatched_platforms can flag configured platforms that no reference used
        # (usually a case/spelling typo in the config).
        self._seen_reference_platforms: Set[str] = set()
        # (aspect class -> in-place normalizer, returns True iff it mutated the aspect).
        # These are the aspects a BI / orchestration source emits that carry *upstream
        # dataset* references — the only refs affected by cross-source casing mismatch:
        # upstreamLineage (table + fineGrained columns), dashboardInfo / chartInfo inputs,
        # and dataJobInputOutput inputs (dbt / Airflow / Spark). Other lineage aspects
        # don't target datasets or are the entity's own outputs (see the dev guide).
        # Covering four aspects is cheap per work unit: get_aspect_of_type is one type
        # check for MCE/MCPW (live aspect) and, for a raw MCP, short-circuits on aspectName
        # before any deserialization — so a work unit is deserialized at most once (for the
        # aspect it actually carries), and covering four vs. one adds only three constant
        # comparisons. The one real cost, the up-front catalog load, is independent of this.
        # Callable[..., bool] (not Callable[[_Aspect], bool]): each normalizer takes a
        # specific aspect subtype, and function args are contravariant, so the precise
        # signature won't accept them in a heterogeneous table (mypy list-item error).
        self._normalizers: List[Tuple[Type[_Aspect], Callable[..., bool]]] = [
            (UpstreamLineageClass, self._normalize_upstream_lineage),
            (DashboardInfoClass, self._normalize_dashboard_info),
            (DataJobInputOutputClass, self._normalize_datajob_io),
            (ChartInfoClass, self._normalize_chart_info),
        ]
        self._load_catalogs()

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        cfg = ctx.pipeline_context.flags.auto_resolve_lineage_urns
        # Fail closed on a degenerate/mock config: this processor is in the shared chain
        # for *every* source, and some connector tests build a source with a bare Mock()
        # ctx where cfg.enabled / cfg.upstream_platforms are truthy Mocks that bypass
        # pydantic validation. Require enabled to be exactly True and upstream_platforms a
        # real, non-empty list. (A real enabled config is guaranteed a non-empty
        # upstream_platforms list by AutoResolveLineageUrnsConfig's validator, which fails
        # config parse otherwise.)
        if cfg.enabled is not True:
            return False
        if not isinstance(cfg.upstream_platforms, list) or not cfg.upstream_platforms:
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
                    context=wu.get_urn(),
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
        # At most one of the four aspects is present per work unit (each belongs to a
        # different entity type — dataset / dashboard / chart / dataJob — and a work unit
        # targets one entity), so incrementing inside the loop still counts work units.
        for aspect_cls, normalize in self._normalizers:
            aspect = wu.get_aspect_of_type(aspect_cls)
            if aspect is None:
                continue
            self.report.num_workunits_with_lineage_aspect += 1
            if normalize(aspect):
                self._write_back_if_mcp(wu, aspect)
                self.report.num_workunits_modified += 1

    def _warn_unmatched_platforms(self) -> None:
        """Surface configured platforms that no reference used, in the pipeline report.

        The usual cause is a case/spelling mismatch in the config (platform names are
        compared case-sensitively). Emitting it as a structured (UI-visible) warning lets
        the operator either fix the platform name or drop the platform if it isn't
        actually referenced by this source.
        """
        # Defense in depth: this runs outside the per-workunit try/except, so guard
        # against a non-list config (should_enable already fails closed on a mock ctx).
        if not isinstance(self._config, list):
            return
        unmatched = {
            entry.platform for entry in self._config
        } - self._seen_reference_platforms
        if not unmatched:
            return
        self.ctx.source_report.warning(
            title="Configured upstream platform matched no lineage references",
            message="An upstream platform configured under auto_resolve_lineage_urns was "
            "not referenced by any lineage in this run, so nothing was reconciled for it. "
            "Platform names are matched case-sensitively against the dataset URN's "
            "platform (e.g. 'snowflake', not 'Snowflake') — fix the name if it's a typo, "
            "or remove the platform from upstream_platforms if this source doesn't "
            "reference it.",
            context=f"{sorted(unmatched)}",
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
            f"sample: {list(self.report.unresolved_refs_sample)}",
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

    def _load_catalogs(self) -> None:
        """Bulk-load every configured platform's SchemaResolver once, up front.

        ``provide_schema_resolver`` does a single bulk scroll per (platform, instance, env)
        and is globally cached, warming each resolver's schema cache so the per-reference
        ``resolve_table`` calls in _resolve_dataset stay local (no per-reference round
        trips). Matching itself is delegated to SchemaResolver; this processor keeps no
        casing index of its own.
        """
        entries_by_platform: Dict[str, List["UpstreamPlatformCasing"]] = {}
        for entry in self._config:
            entries_by_platform.setdefault(entry.platform, []).append(entry)

        for platform, entries in entries_by_platform.items():
            # Emitted before the (potentially long, paginated) fetch so operators see a
            # signal during the stall, not only after.
            logger.info(
                f"Loading '{platform}' catalog from DataHub for lineage casing "
                f"reconciliation; this may take a while on large warehouses..."
            )
            resolvers: List[SchemaResolver] = []
            try:
                for entry in entries:
                    resolvers.append(
                        self._provide_schema_resolver(
                            graph=self._graph,
                            platform=entry.platform,
                            platform_instance=entry.platform_instance,
                            env=entry.env,
                        )
                    )
            except Exception as e:
                # A catalog-load failure must not crash the pipeline: report it and leave
                # the platform unloaded, so its references are emitted unchanged.
                self.ctx.source_report.warning(
                    title="Lineage URN casing: upstream catalog not loaded",
                    message="Failed to bulk-load an upstream platform's catalog from "
                    "DataHub; references to it are emitted unchanged.",
                    context=platform,
                    exc=e,
                )
                continue
            # The resolver caches are held for the pipeline's lifetime; log their size,
            # escalating to WARNING once large enough to matter.
            count = sum(len(r.get_urns()) for r in resolvers)
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

    @staticmethod
    def _strip_platform_instance(name: str, platform_instance: Optional[str]) -> str:
        # A dataset URN name fuses any platform_instance as a leading ``<instance>.``
        # prefix; resolve_table re-prepends the resolver's instance, so strip it here
        # (matched case-insensitively, since that prefix's own casing may differ) to avoid
        # a doubled prefix.
        if platform_instance and name.lower().startswith(
            f"{platform_instance.lower()}."
        ):
            return name[len(platform_instance) + 1 :]
        return name

    def _resolve_dataset(self, urn: str) -> _Resolution:
        """Resolve `urn` to the casing DataHub already stores, via SchemaResolver.

        Delegates matching to ``SchemaResolver.resolve_table``, which tries three casing
        candidates for the reference and returns the first that exists in DataHub:

        1. **original** — the reference's name exactly as given.
        2. **lowercased** — name *and* platform_instance lowercased.
        3. **mixed** — name lowercased but the platform_instance's casing kept.

        (2) and (3) differ only when a platform_instance has non-lowercase casing. Example,
        instance ``ProdWarehouse`` stored as ``ProdWarehouse.db.schema.table``, reference
        ``ProdWarehouse.DB.SCHEMA.TABLE``: (1) misses (table cased wrong), (2) misses
        (instance lowercased to ``prodwarehouse``), (3) matches.

        A hit under the reference's own casing is EXACT; a hit under a different candidate
        is NORMALIZED; no hit is UNRESOLVED — the latter includes casings none of the three
        candidates reach (e.g. an UPPER/Pascal/Mixed-cased *table* in the warehouse), which
        are the tracked ``normalizedUrn`` SchemaResolver follow-up. The resolved entity's
        schema is returned too, for column-casing correction.
        """
        try:
            dataset_urn = DatasetUrn.from_string(urn)
            platform = DataPlatformUrn.from_string(dataset_urn.platform).platform_name
        except Exception:
            return _Resolution(urn, None, None)
        # Track referenced platforms so _warn_unmatched_platforms can flag configured
        # platforms that no reference used (usually a case/spelling typo in the config).
        self._seen_reference_platforms.add(platform)
        resolvers = self._resolvers_by_platform.get(platform)
        if not resolvers:
            return _Resolution(urn, None, None)

        name = dataset_urn.name
        for resolver in resolvers:
            table = self._strip_platform_instance(name, resolver.platform_instance)
            try:
                resolved_urn, schema = resolver.resolve_table_parts(
                    database=None, db_schema=None, table=table
                )
            except Exception:
                continue
            # resolve_table returns a best-effort URN even on a miss; a non-None schema is
            # the signal that an existing entity actually matched.
            if schema is not None:
                match_type = _EXACT if resolved_urn == urn else _NORMALIZED
                return _Resolution(resolved_urn, schema, match_type)
        # On a configured platform but no existing entity matched under a casing
        # resolve_table covers: leave the URN unchanged but flag it UNRESOLVED so
        # potentially broken lineage is visible rather than indistinguishable from clean.
        self.report.unresolved_refs_sample.append(urn)
        return _Resolution(urn, None, _UNRESOLVED)

    # --- aspect rewriters -------------------------------------------------------
    #
    # Each returns True iff it mutated the aspect (rewrote a reference or stamped a
    # matchType), so process() can skip the raw-MCP re-serialization when nothing in the
    # aspect was in scope.

    def _tally_table_ref(self, res: _Resolution) -> bool:
        """Record report counters for a table-level reference; return True iff it was
        normalized (so the caller can rewrite the URN). Shared by the three table-level
        paths; the column-level path (_resolve_field_urn) counts separately."""
        if res.match_type == _NORMALIZED:
            self.report.num_dataset_urns_normalized += 1
            return True
        if res.match_type == _UNRESOLVED:
            self.report.num_refs_unresolved += 1
        else:
            self.report.num_refs_unchanged += 1
        return False

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
            if self._tally_table_ref(res):
                # We overwrite the reference in place; the original (pre-normalization)
                # casing is not retained. If provenance/auditing of the original URN is
                # ever needed, stash it in the Upstream.properties map (already on this
                # record) rather than a dedicated URN field, to avoid the per-edge
                # overhead. Deferred per review — the NORMALIZED matchType already
                # signals that a rewrite happened.
                upstream.dataset = res.urn

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
        res = self._resolve_dataset(parent)
        new_field_path = field_path
        if res.schema:
            new_field_path = self._match_columns_to_schema(res.schema, [field_path])[0]

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
            # A plain URN list / Edge has no matchType field to stamp, but the counters
            # must still distinguish UNRESOLVED (broken) from a clean ref so a
            # dashboard/datajob pointing at broken lineage isn't invisible in the report.
            if self._tally_table_ref(res):
                changed = True
            healed.append(res.urn)
        return healed, changed

    def _heal_dataset_edges(self, edges: List[EdgeClass]) -> bool:
        changed = False
        for edge in edges:
            destination = getattr(edge, "destinationUrn", None)
            if not _is_dataset_urn(destination):
                continue
            res = self._resolve_dataset(destination)
            if self._tally_table_ref(res):
                edge.destinationUrn = res.urn
                changed = True
        return changed
