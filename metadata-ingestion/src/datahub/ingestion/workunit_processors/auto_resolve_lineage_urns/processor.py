# NOTE: `from __future__ import annotations` keeps the schema_resolver type hints (imported
# only under TYPE_CHECKING) as strings, so importing this module does not pull in sqlglot.
# This module is imported eagerly on every source's get_workunit_processors() path, so
# module load must stay sqlglot-free (guarded by test_module_import_does_not_pull_sqlglot).
# The sqlglot-heavy imports are therefore deferred: match_columns_to_schema to a chokepoint
# in __init__, and the strategy module to the same place — both run only after
# should_enable() confirms the feature is on and a graph exists.
from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Callable,
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
from datahub.ingestion.run.pipeline_config import LineageUrnResolutionMode
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
        from datahub.sql_parsing.schema_resolver import match_columns_to_schema

        self._match_columns_to_schema: Callable[[SchemaInfo, List[str]], List[str]] = (
            match_columns_to_schema
        )
        self._strategy: ResolutionStrategy = self._make_strategy(ctx)
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
        # comparisons.
        # Callable[..., bool] (not Callable[[_Aspect], bool]): each normalizer takes a
        # specific aspect subtype, and function args are contravariant, so the precise
        # signature won't accept them in a heterogeneous table (mypy list-item error).
        self._normalizers: List[Tuple[Type[_Aspect], Callable[..., bool]]] = [
            (UpstreamLineageClass, self._normalize_upstream_lineage),
            (DashboardInfoClass, self._normalize_dashboard_info),
            (DataJobInputOutputClass, self._normalize_datajob_io),
            (ChartInfoClass, self._normalize_chart_info),
        ]
        # Aspect names of the above, used to detect a lineage aspect that arrived as a
        # PATCH (which get_aspect_of_type can't surface — see _resolve_workunit).
        self._lineage_aspect_names: Set[str] = {
            aspect_cls.ASPECT_NAME for aspect_cls, _ in self._normalizers
        }

    @staticmethod
    def _make_strategy(ctx: WorkunitProcessorContext) -> ResolutionStrategy:
        """The resolution strategy the config selects. Lazily imported, as above."""
        mode = ctx.pipeline_context.flags.auto_resolve_lineage_urns.mode
        if mode is LineageUrnResolutionMode.ALIAS_LOOKUP:
            from datahub.ingestion.workunit_processors.auto_resolve_lineage_urns.alias_lookup import (
                AliasLookupStrategy,
            )

            return AliasLookupStrategy(ctx)
        from datahub.ingestion.workunit_processors.auto_resolve_lineage_urns.bulk import (
            BulkCatalogStrategy,
        )

        return BulkCatalogStrategy(ctx)

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        cfg = ctx.pipeline_context.flags.auto_resolve_lineage_urns
        # Fail closed on a Mock ctx: this processor is in the shared chain for *every*
        # source, and some connector tests pass a bare Mock() whose cfg fields are truthy
        # Mocks bypassing pydantic — hence exact-True / isinstance over plain truthiness.
        if cfg.enabled is not True:
            return False
        # Only bulk_catalog needs platforms. The config validator runs at construction only,
        # so a later `enabled = True` can still reach here with an empty list.
        if cfg.mode is LineageUrnResolutionMode.BULK_CATALOG and (
            not isinstance(cfg.upstream_platforms, list) or not cfg.upstream_platforms
        ):
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
        self._strategy.finish()
        self._warn_unresolved_refs()
        self._warn_patch_lineage_skipped()

    def _resolve_workunit(self, wu: MetadataWorkUnit) -> None:
        """Reconcile casing on each lineage aspect the workunit carries, in place.

        We edit the typed aspect (via get_aspect_of_type) rather than the uniform
        transform_urns() helper: we must be selective (only upstream refs, never the
        entity or downstream fields) and set a non-URN field (matchType). For MCE/MCPW
        get_aspect_of_type returns the *live* aspect (in-place edit, no (de)serialization);
        a raw MCP is deserialized to inspect, and re-serialized (via _write_back_if_mcp)
        only when something actually changed.
        """
        # A lineage aspect emitted as a raw MCP PATCH (not UPSERT) can't be reconciled:
        # get_aspect_of_type routes a raw MCP through try_from_mcpc, which returns None for
        # non-upserts, so the aspect is invisible to the normalizers below and would pass
        # through silently. Count it so the skip surfaces in the report. (dataJobInputOutput
        # can be emitted as a patch by dbt/Airflow/Spark; BI sources emit full upserts and
        # are unaffected.)
        md = wu.metadata
        if (
            isinstance(md, MetadataChangeProposalClass)
            and md.changeType != ChangeTypeClass.UPSERT
            and md.aspectName in self._lineage_aspect_names
        ):
            self.report.num_patch_lineage_skipped += 1
            return
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

    # --- aspect rewriters -------------------------------------------------------
    #
    # Each returns True iff it mutated the aspect (rewrote a reference or stamped a
    # matchType), so process() can skip the raw-MCP re-serialization when nothing in the
    # aspect was in scope.

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

    def _normalize_upstream_lineage(self, aspect: UpstreamLineageClass) -> bool:
        changed = False
        for upstream in aspect.upstreams:
            dataset = getattr(upstream, "dataset", None)
            if not _is_dataset_urn(dataset):
                continue
            res = self._strategy.resolve(dataset)
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

    def _resolve_field_urn(self, field_urn: str) -> Tuple[str, Optional[MatchType]]:
        parent = _parent_dataset_urn(field_urn)
        field_path = _field_path(field_urn)
        if parent is None or field_path is None:
            self.report.num_refs_unchanged += 1
            return field_urn, None

        # Column-level: we need the parent's schema to correct the column casing.
        res = self._strategy.resolve(parent, need_schema=True)
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
            res = self._strategy.resolve(dataset)
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
            res = self._strategy.resolve(destination)
            if self._tally_table_ref(res):
                edge.destinationUrn = res.urn
                changed = True
        return changed
