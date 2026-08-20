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
    ChangeTypeClass,
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
from datahub.utilities.dataset_aliases.provider import (
    graph_urn_alias_resolver,
    provide_urn_alias_resolver,
)
from datahub.utilities.dataset_aliases.resolver import (
    UrnAliasResolver,
    maintains_dataset_aliases,
)
from datahub.utilities.lossy_collections import LossySet
from datahub.utilities.urns.error import InvalidUrnError

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.ingestion.run.pipeline_config import UpstreamPlatformCasing
    from datahub.sql_parsing.schema_resolver import SchemaInfo, SchemaResolver

logger = logging.getLogger(__name__)

# Above this many URNs per platform, the bulk-loaded catalog is large enough to warrant
# an explicit heads-up to operators rather than letting it surface as unexplained disk
# use and load time.
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
    num_refs_verified_exact: int = 0  # Checked; the exact URN exists in DataHub
    num_refs_out_of_scope: int = 0  # Never attempted (platform not in scope)
    num_refs_skipped_malformed: int = 0  # Not a well-formed dataset / schemaField URN
    num_refs_unresolved: int = 0  # In scope, no unique match (flagged)
    # In scope, but the lookup failed, so there is no verdict (not UNRESOLVED).
    num_refs_lookup_failed: int = 0
    num_exceptions: int = 0  # Failed to process a workunit
    # Per-URN schema fetch failed; table casing still healed, column casing left alone.
    num_schema_fetches_failed: int = 0
    # Lineage aspect emitted as a PATCH (not UPSERT); can't be reconciled, so skipped.
    num_patch_lineage_skipped: int = 0
    num_workunits_with_lineage_aspect: int = 0
    num_workunits_modified: int = 0
    # Bounded sample of references left UNRESOLVED, alongside the num_refs_unresolved
    # count, so the report shows *which* lineage looks broken, not just how much.
    unresolved_refs_sample: LossySet[str] = field(default_factory=LossySet)


@dataclass
class _Resolution:
    """Outcome of resolving one dataset URN against the platforms in scope."""

    urn: str  # The (possibly rewritten) URN to emit.
    # Schema of the resolved entity, when the caller asked for it (the column-level path)
    # and DataHub holds one. None on a table-level resolution, which needs no columns.
    schema: Optional[SchemaInfo]
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
    disconnected lineage nodes. Identity comes from a ``UrnAliasResolver`` and columns from a
    ``SchemaResolver``, each preloaded per configured platform and asked per reference
    otherwise. Both table-level (``UpstreamLineage``, ``DashboardInfo``) and column-level
    (``FineGrainedLineage`` field paths) references are reconciled. Any stored casing is
    reachable, and a reference matching two entities that differ only by case resolves to the
    lowercase-named one, or is left alone if neither is lowercase.

    Scope is the platform names in ``upstream_platforms``; their ``platform_instance`` and
    ``env`` say only what to preload. ``resolve_all_platforms`` widens scope to every
    platform a reference points at.

    Only references *to* warehouse assets found in this source's metadata are fixed;
    the entity the aspect is attached to and downstream fields are never touched. It
    must be enabled on BI-tool / cross-platform ingestions — never on the warehouse
    ingestion, whose reported casing and identity must be respected. Under
    ``resolve_all_platforms`` nothing but that rule keeps a warehouse ingestion off its
    own platform's references, since no list excludes it.
    """

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        graph = ctx.pipeline_context.graph
        # assert for the type checker.
        assert graph is not None
        self._graph: DataHubGraph = graph
        cfg = ctx.pipeline_context.flags.auto_resolve_lineage_urns
        self._config: List[UpstreamPlatformCasing] = cfg.upstream_platforms
        self._resolve_all_platforms: bool = cfg.resolve_all_platforms
        # The platforms in scope, independent of what loaded.
        self._platforms: Set[str] = {entry.platform for entry in self._config}
        # Preloaded URN indexes per platform, one per configured entry that loaded. A cache
        # only: a miss is asked of DataHub.
        self._alias_resolvers: Dict[str, List[UrnAliasResolver]] = {}
        # Resolve the sqlglot-backed schema_resolver callables once, here — a single
        # honest chokepoint rather than imports buried in two leaf methods. Deferred into
        # __init__ (not module level) so importing this module stays sqlglot-free
        # (guarded by test_module_import_does_not_pull_sqlglot); __init__ runs only after
        # should_enable() confirmed the feature is on. The sqlglot dependency itself is
        # validated up front by AutoResolveLineageUrnsConfig (fail-fast at config parse
        # when enabled), so these imports are guaranteed to succeed here.
        from datahub.sql_parsing.schema_resolver import (
            SchemaResolver as _SchemaResolver,
            match_columns_to_schema,
        )
        from datahub.sql_parsing.schema_resolver_provider import provide_schema_resolver

        self._provide_schema_resolver: Callable[..., SchemaResolver] = (
            provide_schema_resolver
        )
        self._schema_resolver_cls: Type[SchemaResolver] = _SchemaResolver
        self._match_columns_to_schema: Callable[[SchemaInfo, List[str]], List[str]] = (
            match_columns_to_schema
        )
        # Preloaded schemas per platform, also a cache: a miss is fetched per URN.
        self._schema_resolvers: Dict[str, List["SchemaResolver"]] = {}
        # Lazily built for the per-URN fetches. See _graph_resolver_for.
        self._graph_resolvers_by_platform: Dict[str, "SchemaResolver"] = {}
        # Platforms actually referenced by this source's lineage, so
        # _warn_unmatched_platforms can flag configured platforms that no reference used
        # (usually a case/spelling typo in the config).
        self._seen_reference_platforms: Set[str] = set()
        # (aspect class -> in-place normalizer). These four are the only aspects that
        # carry *upstream dataset* references; docs/dev_guides/lineage_urn_casing.md has
        # the coverage table and the deliberate exclusions.
        # Trying all four per work unit is cheap: get_aspect_of_type short-circuits a raw
        # MCP on aspectName before deserializing, so a work unit is deserialized at most
        # once.
        # Callable[..., bool] (not Callable[[_Aspect], bool]): args are contravariant, so
        # the precise signature is rejected in a heterogeneous table (mypy list-item).
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
        self._load_catalogs()

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        cfg = ctx.pipeline_context.flags.auto_resolve_lineage_urns
        # Fail closed on a degenerate/mock config: this processor is in the shared chain
        # for *every* source, and some connector tests build a source with a bare Mock()
        # ctx where cfg.enabled / cfg.upstream_platforms / cfg.resolve_all_platforms are
        # truthy Mocks that bypass pydantic validation. Hence the `is True` tests and the
        # isinstance check rather than plain truthiness. (A real enabled config is
        # guaranteed a non-empty upstream_platforms list or resolve_all_platforms by
        # AutoResolveLineageUrnsConfig's validator, which fails config parse otherwise.)
        if cfg.enabled is not True:
            return False
        if not isinstance(cfg.upstream_platforms, list):
            return False
        if not cfg.upstream_platforms and cfg.resolve_all_platforms is not True:
            return False
        # Use getattr for graph: it's a no-op without a backend, and `graph` is a
        # PipelineContext instance attribute (absent from MagicMock(spec=...) used by
        # some connector tests).
        graph = getattr(ctx.pipeline_context, "graph", None)
        if graph is None:
            return False
        # Resolution reads the `aliases` aspect GMS maintains, so a server too old to
        # have it cannot answer at all.
        if not maintains_dataset_aliases(graph):
            ctx.source_report.warning(
                title="Lineage URN casing resolution disabled",
                message="This server does not maintain the dataset `aliases` aspect that "
                "URN casing resolution reads, so lineage is emitted unchanged. Requires "
                "DataHub Cloud 2.2.0 or DataHub 1.8.0 and later.",
            )
            return False
        return True

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

    # --- resolution -------------------------------------------------------------

    def _load_catalogs(self) -> None:
        """Read each configured entry's URNs and schemas up front, as a cache.

        Best-effort: whatever fails to load is asked of DataHub per reference instead, so a
        failure here costs speed rather than correctness.
        """
        for entry in self._config:
            scope = (
                f"{entry.platform}, platform_instance={entry.platform_instance}, "
                f"env={entry.env}"
            )
            alias_resolver = provide_urn_alias_resolver(
                graph=self._graph,
                platform=entry.platform,
                platform_instance=entry.platform_instance,
                env=entry.env,
            )
            if alias_resolver is None:
                self.ctx.source_report.warning(
                    title="Lineage URN casing: upstream URNs not loaded",
                    message="Failed to load an upstream platform's URNs from DataHub; "
                    "its references are resolved one at a time instead.",
                    context=scope,
                )
            else:
                self._alias_resolvers.setdefault(entry.platform, []).append(
                    alias_resolver
                )

            try:
                schema_resolver = self._provide_schema_resolver(
                    graph=self._graph,
                    platform=entry.platform,
                    platform_instance=entry.platform_instance,
                    env=entry.env,
                )
            except Exception as e:
                self.ctx.source_report.warning(
                    title="Lineage URN casing: upstream catalog not loaded",
                    message="Failed to bulk-load an upstream platform's catalog from "
                    "DataHub; its columns are fetched one at a time instead.",
                    context=scope,
                    exc=e,
                )
                continue
            self._schema_resolvers.setdefault(entry.platform, []).append(
                schema_resolver
            )
            count = schema_resolver.schema_count()
            message = f"Loaded {count} dataset schemas for {scope}."
            if count > _CATALOG_SIZE_WARN_THRESHOLD:
                logger.warning(
                    f"{message} Its cache uses significant disk; consider narrowing "
                    f"upstream_platforms (platform_instance / env) to the assets this "
                    f"source references."
                )
            else:
                logger.info(message)

    def _resolve_dataset(self, urn: str, *, with_schema: bool = False) -> _Resolution:
        """Resolve `urn` to the casing DataHub already stores, via the URN alias index.

        ``UrnAliasResolver.resolve`` returns the stored URN matching the reference under
        any casing: a hit under the reference's own casing is EXACT, a hit under a different
        casing is NORMALIZED, and None is UNRESOLVED. It is called with
        ``prefer_lowercased=True``, so two stored entities differing only by case heal to
        the lowercase-named one rather than leaving the lineage broken; None is left for a
        reference nothing matches, or a collision with no lowercase-named side to prefer. A
        reference outside scope is never looked up at all, so it abstains rather than
        reporting an absence.

        Matching whole URNs means platform_instance and env are part of the comparison, so a
        reference is never healed across either.

        With `with_schema`, the resolved entity's schema is returned too, for column-casing
        correction — see _schema_of, which is where identity and columns are paired back up.
        Only the column-level path asks for it: fetching a schema costs a query per entity
        outside the preload, and a table-level reference has no columns to reconcile.
        """
        try:
            dataset_urn = DatasetUrn.from_string(urn)
            platform = DataPlatformUrn.from_string(dataset_urn.platform).platform_name
        except Exception:
            return _Resolution(urn, None, None)
        # Track referenced platforms so _warn_unmatched_platforms can flag configured
        # platforms that no reference used (usually a case/spelling typo in the config).
        self._seen_reference_platforms.add(platform)
        if platform not in self._platforms and not self._resolve_all_platforms:
            # Out of scope: left untouched, and unstamped (no verdict == not processed).
            self.report.num_refs_out_of_scope += 1
            return _Resolution(urn, None, None)
        try:
            resolved = self._resolve_alias(urn, platform)
        except Exception as e:
            # Nothing was answered, so nothing is stamped: UNRESOLVED would claim DataHub
            # holds no such entity, on the strength of a query that failed.
            self.report.num_refs_lookup_failed += 1
            self.ctx.source_report.warning(
                title="Lineage URN casing not checked",
                message="Failed to look an upstream lineage reference up in DataHub; its "
                "casing is left unchanged and unflagged.",
                context=urn,
                exc=e,
                log=False,
            )
            return _Resolution(urn, None, None)
        if resolved is None:
            # In scope but no single existing entity matched: leave the URN unchanged
            # but flag it UNRESOLVED so potentially broken lineage is visible rather
            # than indistinguishable from clean.
            self.report.unresolved_refs_sample.add(urn)
            return _Resolution(urn, None, _UNRESOLVED)
        match_type = _EXACT if resolved == urn else _NORMALIZED
        schema = self._schema_of(resolved, platform) if with_schema else None
        return _Resolution(resolved, schema, match_type)

    def _resolve_alias(self, urn: str, platform: str) -> Optional[str]:
        """The stored casing of `urn`, from a preloaded catalog or by asking DataHub.

        A preload covers one platform_instance / env, so a miss in it is not an absence and
        has to be asked about. A preload that raises is likewise not an absence: DataHub is
        asked below either way, and its search is exhaustive, so the reference still heals.
        """
        for resolver in self._alias_resolvers.get(platform) or []:
            try:
                resolved = resolver.resolve(urn, prefer_lowercased=True)
            except Exception:
                logger.debug(
                    f"Preloaded URN catalog failed for {urn}; asking DataHub instead",
                    exc_info=True,
                )
                continue
            if resolved is not None:
                return resolved
        return graph_urn_alias_resolver(self._graph).resolve(
            urn, prefer_lowercased=True
        )

    def _schema_of(self, urn: str, platform: str) -> Optional[SchemaInfo]:
        """The columns DataHub stores for `urn`, or None for an entity that has none.

        Preloaded catalogs first, then a fetch — the same miss-means-ask as _resolve_alias.

        A preloaded catalog that raises is one that did not answer, not an absence: the
        fetch below runs regardless and reads the entity itself, so carrying on loses
        nothing. A failed fetch answers None: columns enrich an identity that is already
        resolved, so losing them must not unwind the reference.
        """
        for resolver in self._schema_resolvers.get(platform) or []:
            try:
                schema = resolver.resolve_urn(urn)[1]
            except Exception:
                logger.debug(
                    f"Preloaded schema catalog failed for {urn}; asking DataHub instead",
                    exc_info=True,
                )
                continue
            if schema is not None:
                return schema
        try:
            return self._graph_resolver_for(platform).resolve_urn(urn)[1]
        except Exception as e:
            self.report.num_schema_fetches_failed += 1
            self.ctx.source_report.warning(
                title="Lineage URN casing: upstream schema not fetched",
                message="Failed to fetch an upstream dataset's schema from DataHub; "
                "its table casing is still reconciled, but its column casing is left "
                "unchanged.",
                context=urn,
                exc=e,
                log=False,
            )
            return None

    def _graph_resolver_for(self, platform: str) -> "SchemaResolver":
        """A lazily-fetching SchemaResolver for `platform`.

        Keyed by platform alone: ``resolve_urn`` looks an entity up by its exact URN and
        reads neither ``platform_instance`` nor ``env``, and an instance cannot be
        recovered from a URN to key on anyway.
        """
        resolver = self._graph_resolvers_by_platform.get(platform)
        if resolver is None:
            resolver = self._schema_resolver_cls(platform=platform, graph=self._graph)
            self._graph_resolvers_by_platform[platform] = resolver
        return resolver

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
        elif res.match_type == _EXACT:
            self.report.num_refs_verified_exact += 1
        return False

    def _normalize_upstream_lineage(self, aspect: UpstreamLineageClass) -> bool:
        changed = False
        for upstream in aspect.upstreams:
            dataset = getattr(upstream, "dataset", None)
            if not _is_dataset_urn(dataset):
                self.report.num_refs_skipped_malformed += 1
                continue
            res = self._resolve_dataset(dataset)
            # Stamp the verdict (EXACT / NORMALIZED / UNRESOLVED) for any reference on
            # a configured platform; out-of-scope refs get res.match_type=None and are
            # left untouched.
            if res.match_type is not None:
                upstream.matchType = res.match_type
                changed = True
            if self._tally_table_ref(res):
                # Rewritten in place: the original casing is not retained. The
                # NORMALIZED matchType is the record that a rewrite happened.
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
        if parent is None or field_path is None or not _is_dataset_urn(parent):
            # A schemaField's parent is not necessarily a dataset.
            self.report.num_refs_skipped_malformed += 1
            return field_urn, None

        # Column-level: we need the parent's schema to correct the column casing.
        res = self._resolve_dataset(parent, with_schema=True)
        new_field_path = field_path
        if res.schema:
            new_field_path = self._match_columns_to_schema(res.schema, [field_path])[0]

        if res.urn == parent and new_field_path == field_path:
            if res.match_type == _UNRESOLVED:
                self.report.num_refs_unresolved += 1
            elif res.match_type == _EXACT:
                self.report.num_refs_verified_exact += 1
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
                self.report.num_refs_skipped_malformed += 1
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
                self.report.num_refs_skipped_malformed += 1
                continue
            res = self._resolve_dataset(destination)
            if self._tally_table_ref(res):
                edge.destinationUrn = res.urn
                changed = True
        return changed
