# NOTE: `from __future__ import annotations` keeps the schema_resolver type hints (imported
# only under TYPE_CHECKING) as strings, so importing this module does not pull in sqlglot.
# This module is imported eagerly on every source's get_workunit_processors() path, so
# module load must stay sqlglot-free (guarded by test_module_import_does_not_pull_sqlglot).
# The sqlglot-heavy imports are therefore deferred to a single chokepoint in __init__, which
# runs only after should_enable() confirms the feature is on and a graph exists.
from __future__ import annotations

from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
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
)
from datahub.ingestion.workunit_processors.auto_resolve_lineage_urns.models import (
    EXACT,
    NORMALIZED,
    UNRESOLVED,
    AutoResolveLineageUrnsProcessorReport,
    MatchType,
    Resolution,
    ResolutionStrategy,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ChartInfoClass,
    DashboardInfoClass,
    DataJobInputOutputClass,
    EdgeClass,
    FineGrainedLineageClass,
    MetadataChangeProposalClass,
    UpstreamLineageClass,
    _Aspect,
)
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn
from datahub.utilities.urns.error import InvalidUrnError

if TYPE_CHECKING:
    from datahub.sql_parsing.schema_resolver import SchemaInfo


@dataclass(frozen=True)
class _AspectHandler:
    """A lineage aspect and the paired passes that read and then rewrite it.

    Collect and apply must agree on exactly which references they touch: a reference the
    rewriter resolves but the collector never gathered is absent from the resolution map,
    which the rewriter treats as a bug rather than silently skipping. Pairing them in one
    record is what keeps them from drifting.
    """

    aspect_cls: Type[_Aspect]
    # (aspect, urns, schema_urns) -> None. Gathers references, mutating nothing.
    collect: Callable[..., None]
    # (aspect, resolutions) -> True iff the aspect was mutated.
    apply: Callable[..., bool]


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
    disconnected lineage nodes. Reconciliation covers table-level (``UpstreamLineage``,
    ``DashboardInfo``, ``ChartInfo``, ``DataJobInputOutput``) and column-level
    (``FineGrainedLineage`` field paths) references.

    Finding the URN DataHub actually stores is delegated to a :class:`ResolutionStrategy`;
    this class owns everything that is the same regardless of how a reference resolves —
    which aspects carry upstream references, how they are rewritten, and how verdicts are
    counted and reported.

    Only references *to* warehouse assets are fixed; the entity the aspect is attached to
    and downstream fields are never touched. It must be enabled on BI-tool / cross-platform
    ingestions — never on the warehouse ingestion, whose reported casing and identity must
    be respected.
    """

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        # Deferred imports, both for the same reason the module comment gives: these
        # reach sqlglot-backed code, and this __init__ runs only once should_enable() has
        # confirmed the feature is on.
        from datahub.ingestion.workunit_processors.auto_resolve_lineage_urns.bulk import (
            BulkCatalogStrategy,
        )
        from datahub.sql_parsing.schema_resolver import match_columns_to_schema

        self._match_columns_to_schema: Callable[[SchemaInfo, List[str]], List[str]] = (
            match_columns_to_schema
        )
        self._strategy: ResolutionStrategy = BulkCatalogStrategy(ctx)
        # The aspects a BI / orchestration source emits that carry *upstream dataset*
        # references — the only refs affected by cross-source casing mismatch:
        # upstreamLineage (table + fineGrained columns), dashboardInfo / chartInfo inputs,
        # and dataJobInputOutput inputs (dbt / Airflow / Spark). Other lineage aspects
        # don't target datasets or are the entity's own outputs (see the dev guide).
        # Covering four aspects is cheap per work unit: get_aspect_of_type is one type
        # check for MCE/MCPW (live aspect) and, for a raw MCP, short-circuits on aspectName
        # before any deserialization — so a work unit is deserialized at most once (for the
        # aspect it actually carries), and covering four vs. one adds only three constant
        # comparisons.
        self._handlers: List[_AspectHandler] = [
            _AspectHandler(
                UpstreamLineageClass,
                self._collect_upstream_lineage,
                self._normalize_upstream_lineage,
            ),
            _AspectHandler(
                DashboardInfoClass,
                self._collect_dashboard_info,
                self._normalize_dashboard_info,
            ),
            _AspectHandler(
                DataJobInputOutputClass,
                self._collect_datajob_io,
                self._normalize_datajob_io,
            ),
            _AspectHandler(
                ChartInfoClass, self._collect_chart_info, self._normalize_chart_info
            ),
        ]
        # Aspect names of the above, used to detect a lineage aspect that arrived as a
        # PATCH (which get_aspect_of_type can't surface — see _is_unreconcilable_patch).
        self._lineage_aspect_names: Set[str] = {
            handler.aspect_cls.ASPECT_NAME for handler in self._handlers
        }

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
                self._reconcile(wu)
            except Exception as e:
                self._report_workunit_failure(wu, e)
            yield wu
        self._strategy.finish()
        self._warn_unresolved_refs()
        self._warn_patch_lineage_skipped()

    def _reconcile(self, wu: MetadataWorkUnit) -> None:
        """Collect this work unit's references, resolve them together, then rewrite.

        Two passes rather than resolving inline: the batch has to be known before any of it
        can be resolved. The batch is currently one work unit, which already collapses a
        dashboard's forty upstreams into a single resolve_many call. Widening it across work
        units is a change to this method alone.
        """
        urns: Set[str] = set()
        schema_urns: Set[str] = set()
        self._collect_workunit(wu, urns, schema_urns)
        resolutions = self._resolve(urns, schema_urns)
        # None distinguishes a failed resolve from one that legitimately found nothing to
        # do: on failure the aspect is left alone, but an empty batch must still run the
        # rewriters, whose per-reference bookkeeping covers references that were never
        # resolvable (a malformed schemaField URN, say).
        if resolutions is not None:
            self._apply_workunit(wu, resolutions)

    def _resolve(
        self, urns: Set[str], schema_urns: Set[str]
    ) -> Optional[Dict[str, Resolution]]:
        """Resolve a batch, or None if the strategy failed outright."""
        if not urns:
            return {}
        try:
            return self._strategy.resolve_many(urns=urns, schema_urns=schema_urns)
        except Exception as e:
            self.report.num_exceptions += 1
            self.ctx.source_report.warning(
                title="Lineage URN casing not reconciled",
                message="Failed to resolve a batch of upstream lineage references; the "
                "affected lineage is emitted unchanged.",
                context=f"{len(urns)} reference(s)",
                exc=e,
            )
            return None

    def _report_workunit_failure(self, wu: MetadataWorkUnit, e: Exception) -> None:
        self.report.num_exceptions += 1
        # Surface in the pipeline report (not just this processor's sub-report), so a run
        # that fails to reconcile part of a source's lineage doesn't look clean.
        self.ctx.source_report.warning(
            title="Lineage URN casing not reconciled",
            message="Failed to reconcile lineage URN casing for a work unit; "
            "its lineage is emitted unchanged.",
            context=wu.get_urn(),
            exc=e,
        )

    def _is_unreconcilable_patch(self, wu: MetadataWorkUnit) -> bool:
        """Whether this is a lineage aspect we can't reconcile because it's a PATCH.

        get_aspect_of_type routes a raw MCP through try_from_mcpc, which returns None for
        non-upserts, so a patched aspect is invisible to both passes and would pass through
        silently. (dataJobInputOutput can be emitted as a patch by dbt/Airflow/Spark; BI
        sources emit full upserts and are unaffected.)
        """
        md = wu.metadata
        return (
            isinstance(md, MetadataChangeProposalClass)
            and md.changeType != ChangeTypeClass.UPSERT
            and md.aspectName in self._lineage_aspect_names
        )

    def _collect_workunit(
        self, wu: MetadataWorkUnit, urns: Set[str], schema_urns: Set[str]
    ) -> None:
        """Pass 1: gather the upstream references this work unit carries.

        Mutates nothing. Counts the work unit as carrying lineage, and counts a patch skip,
        here rather than in pass 2 so those totals hold even if resolution then fails.
        """
        if self._is_unreconcilable_patch(wu):
            self.report.num_patch_lineage_skipped += 1
            return
        # At most one of the four aspects is present per work unit (each belongs to a
        # different entity type — dataset / dashboard / chart / dataJob — and a work unit
        # targets one entity), so incrementing inside the loop still counts work units.
        for handler in self._handlers:
            aspect = wu.get_aspect_of_type(handler.aspect_cls)
            if aspect is None:
                continue
            self.report.num_workunits_with_lineage_aspect += 1
            handler.collect(aspect, urns, schema_urns)

    def _apply_workunit(
        self, wu: MetadataWorkUnit, resolutions: Dict[str, Resolution]
    ) -> None:
        """Pass 2: reconcile casing on each lineage aspect the work unit carries, in place.

        We edit the typed aspect (via get_aspect_of_type) rather than the uniform
        transform_urns() helper: we must be selective (only upstream refs, never the
        entity or downstream fields) and set a non-URN field (matchType). For MCE/MCPW
        get_aspect_of_type returns the *live* aspect (in-place edit, no (de)serialization);
        a raw MCP is deserialized to inspect, and re-serialized (via _write_back_if_mcp)
        only when something actually changed.
        """
        if self._is_unreconcilable_patch(wu):
            return
        for handler in self._handlers:
            aspect = wu.get_aspect_of_type(handler.aspect_cls)
            if aspect is None:
                continue
            if handler.apply(aspect, resolutions):
                self._write_back_if_mcp(wu, aspect)
                self.report.num_workunits_modified += 1

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

    def _warn_patch_lineage_skipped(self) -> None:
        """Surface patch-based lineage aspects that couldn't be reconciled, once.

        The processor only reconciles UPSERT aspects; a lineage aspect emitted as a PATCH
        is passed through unchanged (see _resolve_workunit). Emit one end-of-run warning so
        the skip is visible rather than silent.
        """
        if self.report.num_patch_lineage_skipped == 0:
            return
        self.ctx.source_report.warning(
            title="Patch-based lineage not reconciled for casing",
            message="Some lineage aspects were emitted as PATCH change proposals rather "
            "than full upserts; casing reconciliation only applies to upserts, so these "
            "were emitted unchanged. This affects patch-based lineage (e.g. "
            "dataJobInputOutput from dbt/Airflow/Spark); sources that emit full aspects "
            "are unaffected.",
            context=f"{self.report.num_patch_lineage_skipped} aspect(s)",
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

    # --- collectors (pass 1) ----------------------------------------------------
    #
    # Each gathers the upstream dataset references its aspect carries, without touching
    # anything. `schema_urns` is the subset reached by column-level lineage, i.e. the only
    # parents whose schema needs fetching. Each must gather exactly the references its
    # paired rewriter below resolves — see _AspectHandler.

    def _collect_upstream_lineage(
        self, aspect: UpstreamLineageClass, urns: Set[str], schema_urns: Set[str]
    ) -> None:
        for upstream in aspect.upstreams:
            dataset = getattr(upstream, "dataset", None)
            if _is_dataset_urn(dataset):
                urns.add(dataset)
        for fine_grained in aspect.fineGrainedLineages or []:
            self._collect_fine_grained(fine_grained, urns, schema_urns)

    def _collect_fine_grained(
        self,
        fine_grained: FineGrainedLineageClass,
        urns: Set[str],
        schema_urns: Set[str],
    ) -> None:
        for field_urn in fine_grained.upstreams or []:
            parent = _parent_dataset_urn(field_urn)
            # Deliberately not filtered by _is_dataset_urn, matching _resolve_field_urn:
            # it resolves whatever parent it finds and lets the strategy decline. Filtering
            # here would leave that lookup absent from the map.
            if parent is not None and _field_path(field_urn) is not None:
                urns.add(parent)
                schema_urns.add(parent)

    def _collect_dashboard_info(
        self, aspect: DashboardInfoClass, urns: Set[str], schema_urns: Set[str]
    ) -> None:
        self._collect_dataset_urns(aspect.datasets or [], urns)
        self._collect_dataset_edges(aspect.datasetEdges or [], urns)

    def _collect_datajob_io(
        self, aspect: DataJobInputOutputClass, urns: Set[str], schema_urns: Set[str]
    ) -> None:
        self._collect_dataset_urns(aspect.inputDatasets or [], urns)
        self._collect_dataset_edges(aspect.inputDatasetEdges or [], urns)
        for fine_grained in aspect.fineGrainedLineages or []:
            self._collect_fine_grained(fine_grained, urns, schema_urns)

    def _collect_chart_info(
        self, aspect: ChartInfoClass, urns: Set[str], schema_urns: Set[str]
    ) -> None:
        self._collect_dataset_urns(aspect.inputs or [], urns)
        self._collect_dataset_edges(aspect.inputEdges or [], urns)

    @staticmethod
    def _collect_dataset_urns(refs: List[str], urns: Set[str]) -> None:
        for dataset in refs:
            if _is_dataset_urn(dataset):
                urns.add(dataset)

    @staticmethod
    def _collect_dataset_edges(edges: List[EdgeClass], urns: Set[str]) -> None:
        for edge in edges:
            destination = getattr(edge, "destinationUrn", None)
            if _is_dataset_urn(destination):
                urns.add(destination)

    # --- aspect rewriters (pass 2) ----------------------------------------------
    #
    # Each returns True iff it mutated the aspect (rewrote a reference or stamped a
    # matchType), so _apply_workunit can skip the raw-MCP re-serialization when nothing in
    # the aspect was in scope. `resolutions` is indexed directly rather than with .get():
    # the strategy contract guarantees an entry per collected reference, so a missing key
    # means collect and apply have drifted and should fail loudly, not silently skip.

    def _tally_table_ref(self, res: Resolution) -> bool:
        """Record report counters for a table-level reference; return True iff it was
        normalized (so the caller can rewrite the URN). Shared by the three table-level
        paths; the column-level path (_resolve_field_urn) counts separately."""
        if res.match_type == NORMALIZED:
            self.report.num_dataset_urns_normalized += 1
            return True
        if res.match_type == UNRESOLVED:
            self.report.num_refs_unresolved += 1
            self.report.unresolved_refs_sample.append(res.urn)
        else:
            self.report.num_refs_unchanged += 1
        return False

    def _normalize_upstream_lineage(
        self, aspect: UpstreamLineageClass, resolutions: Dict[str, Resolution]
    ) -> bool:
        changed = False
        for upstream in aspect.upstreams:
            dataset = getattr(upstream, "dataset", None)
            if not _is_dataset_urn(dataset):
                continue
            res = resolutions[dataset]
            # Stamp the verdict (EXACT / NORMALIZED / UNRESOLVED) for any reference in
            # scope; out-of-scope refs get res.match_type=None and are left untouched.
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
            if self._normalize_fine_grained_upstreams(fine_grained, resolutions):
                changed = True
        return changed

    def _normalize_fine_grained_upstreams(
        self,
        fine_grained: FineGrainedLineageClass,
        resolutions: Dict[str, Resolution],
    ) -> bool:
        # Only upstream references are healed; downstream fields belong to the entity
        # this aspect describes and must keep its casing.
        if not fine_grained.upstreams:
            return False
        changed = False
        rewritten: List[str] = []
        match_types: List[Optional[MatchType]] = []
        for field_urn in fine_grained.upstreams:
            new_urn, match_type = self._resolve_field_urn(field_urn, resolutions)
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
        if NORMALIZED in match_types:
            aggregate = NORMALIZED
        elif UNRESOLVED in match_types:
            aggregate = UNRESOLVED
        elif EXACT in match_types:
            aggregate = EXACT
        if aggregate is not None:
            fine_grained.matchType = aggregate
            changed = True
        return changed

    def _resolve_field_urn(
        self, field_urn: str, resolutions: Dict[str, Resolution]
    ) -> Tuple[str, Optional[MatchType]]:
        parent = _parent_dataset_urn(field_urn)
        field_path = _field_path(field_urn)
        if parent is None or field_path is None:
            self.report.num_refs_unchanged += 1
            return field_urn, None

        # Column-level: we need the parent's schema to correct the column casing.
        res = resolutions[parent]
        new_field_path = field_path
        if res.schema:
            new_field_path = self._match_columns_to_schema(res.schema, [field_path])[0]

        if res.urn == parent and new_field_path == field_path:
            if res.match_type == UNRESOLVED:
                self.report.num_refs_unresolved += 1
                self.report.unresolved_refs_sample.append(res.urn)
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
        match_type = NORMALIZED if new_field_path != field_path else res.match_type
        return make_schema_field_urn(res.urn, new_field_path), match_type

    def _normalize_dashboard_info(
        self, aspect: DashboardInfoClass, resolutions: Dict[str, Resolution]
    ) -> bool:
        changed = False
        if aspect.datasets:
            aspect.datasets, c = self._heal_dataset_urns(aspect.datasets, resolutions)
            changed = changed or c
        if self._heal_dataset_edges(aspect.datasetEdges or [], resolutions):
            changed = True
        return changed

    def _normalize_datajob_io(
        self, aspect: DataJobInputOutputClass, resolutions: Dict[str, Resolution]
    ) -> bool:
        # A DataJob's *inputs* are upstream warehouse references (the dbt / Airflow /
        # Spark warehouse-upstream path) and are healed like any other upstream. The
        # job's outputs are its declared products and are left untouched, matching the
        # processor's rule of never rewriting an entity's own / downstream side.
        changed = False
        if aspect.inputDatasets:
            aspect.inputDatasets, c = self._heal_dataset_urns(
                aspect.inputDatasets, resolutions
            )
            changed = changed or c
        if self._heal_dataset_edges(aspect.inputDatasetEdges or [], resolutions):
            changed = True
        for fine_grained in aspect.fineGrainedLineages or []:
            if self._normalize_fine_grained_upstreams(fine_grained, resolutions):
                changed = True
        return changed

    def _normalize_chart_info(
        self, aspect: ChartInfoClass, resolutions: Dict[str, Resolution]
    ) -> bool:
        # A chart's `inputs` / `inputEdges` are the upstream datasets it reads from.
        # For BI tools that query the warehouse directly (e.g. Superset, Mode, Redash,
        # Metabase) these point straight at warehouse tables, so casing mismatches
        # there break lineage just like any other upstream reference.
        changed = False
        if aspect.inputs:
            aspect.inputs, c = self._heal_dataset_urns(aspect.inputs, resolutions)
            changed = changed or c
        if self._heal_dataset_edges(aspect.inputEdges or [], resolutions):
            changed = True
        return changed

    def _heal_dataset_urns(
        self, urns: List[str], resolutions: Dict[str, Resolution]
    ) -> Tuple[List[str], bool]:
        healed: List[str] = []
        changed = False
        for dataset in urns:
            # Guard non-dataset / malformed URNs (consistent with
            # _normalize_upstream_lineage and _heal_dataset_edges): leave them untouched
            # without attempting resolution.
            if not _is_dataset_urn(dataset):
                healed.append(dataset)
                continue
            res = resolutions[dataset]
            # A plain URN list / Edge has no matchType field to stamp, but the counters
            # must still distinguish UNRESOLVED (broken) from a clean ref so a
            # dashboard/datajob pointing at broken lineage isn't invisible in the report.
            if self._tally_table_ref(res):
                changed = True
            healed.append(res.urn)
        return healed, changed

    def _heal_dataset_edges(
        self, edges: List[EdgeClass], resolutions: Dict[str, Resolution]
    ) -> bool:
        changed = False
        for edge in edges:
            destination = getattr(edge, "destinationUrn", None)
            if not _is_dataset_urn(destination):
                continue
            res = resolutions[destination]
            if self._tally_table_ref(res):
                edge.destinationUrn = res.urn
                changed = True
        return changed
