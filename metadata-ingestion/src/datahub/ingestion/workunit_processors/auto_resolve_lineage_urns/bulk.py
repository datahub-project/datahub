# NOTE: `from __future__ import annotations` keeps the schema_resolver type hints (imported
# only under TYPE_CHECKING) as strings. The sqlglot-heavy schema_resolver import is deferred
# to a single chokepoint in __init__, and the processor imports this module lazily, so
# neither happens on the module-load path (guarded by
# test_module_import_does_not_pull_sqlglot).
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set

from datahub.ingestion.api.workunit_processor import WorkunitProcessorContext
from datahub.ingestion.workunit_processors.auto_resolve_lineage_urns.models import (
    EXACT,
    NORMALIZED,
    UNRESOLVED,
    Resolution,
)
from datahub.metadata.urns import DataPlatformUrn, DatasetUrn

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph
    from datahub.ingestion.run.pipeline_config import UpstreamPlatformCasing
    from datahub.sql_parsing.schema_resolver import SchemaResolver

logger = logging.getLogger(__name__)

# Above this many URNs per platform, the bulk-loaded SchemaResolver cache is large
# enough to warrant an explicit heads-up to operators rather than letting it surface as
# unexplained memory pressure.
_CATALOG_SIZE_WARN_THRESHOLD = 500_000


class BulkCatalogStrategy:
    """Resolve casing against each configured platform's catalog, downloaded up front.

    For every configured upstream platform this bulk-loads that platform's URNs and
    schemas once (via ``SchemaResolverProvider``) and resolves every reference locally
    via ``SchemaResolver.resolve_table``, which tries the reference's original,
    lowercased, and mixed-instance casings (see :meth:`resolve`). No casing index of its
    own is kept here — matching is entirely delegated to SchemaResolver.

    Casings none of those three candidates reach (an UPPER / Pascal / Mixed-cased table)
    are unreachable by this strategy and reported UNRESOLVED.
    """

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        self._ctx = ctx
        graph = ctx.pipeline_context.graph
        # assert for the type checker; should_enable guarantees a graph exists.
        assert graph is not None
        self._graph: DataHubGraph = graph
        self._config: List[UpstreamPlatformCasing] = (
            ctx.pipeline_context.flags.auto_resolve_lineage_urns.upstream_platforms
        )
        # Resolve the sqlglot-backed schema_resolver callable once, here — a single honest
        # chokepoint rather than an import buried in a leaf method. Deferred into __init__
        # (not module level) so importing the processor package stays sqlglot-free;
        # __init__ runs only after should_enable() confirmed the feature is on. The sqlglot
        # dependency itself is validated up front by AutoResolveLineageUrnsConfig (fail-fast
        # at config parse when enabled), so this import is guaranteed to succeed here.
        from datahub.sql_parsing.schema_resolver_provider import provide_schema_resolver

        self._provide_schema_resolver: Callable[..., SchemaResolver] = (
            provide_schema_resolver
        )
        # Per-platform SchemaResolvers, bulk-initialized up front by _load_catalogs().
        self._resolvers_by_platform: Dict[str, List[SchemaResolver]] = {}
        # Platforms actually referenced by this source's lineage, so finish() can flag
        # configured platforms that no reference used (usually a case/spelling typo).
        self._seen_reference_platforms: Set[str] = set()
        self._load_catalogs()

    def _load_catalogs(self) -> None:
        """Bulk-load every configured platform's SchemaResolver once, up front.

        ``provide_schema_resolver`` does a single bulk scroll per (platform, instance, env)
        and is globally cached, warming each resolver's schema cache so the per-reference
        ``resolve_table`` calls in :meth:`resolve` stay local (no per-reference round
        trips).
        """
        entries_by_platform: Dict[str, List[UpstreamPlatformCasing]] = {}
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
                self._ctx.source_report.warning(
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

    def resolve(self, urn: str, *, need_schema: bool = False) -> Resolution:
        """Resolve `urn` to the casing DataHub already stores, via SchemaResolver.

        ``need_schema`` is ignored: the bulk catalog already holds every schema it loaded,
        so a schema costs nothing extra here.

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
        is NORMALIZED; no hit is UNRESOLVED. The resolved entity's schema is returned too,
        for column-casing correction.
        """
        try:
            dataset_urn = DatasetUrn.from_string(urn)
            platform = DataPlatformUrn.from_string(dataset_urn.platform).platform_name
        except Exception:
            return Resolution(urn, None, None)
        # Track referenced platforms so finish() can flag configured platforms that no
        # reference used (usually a case/spelling typo in the config).
        self._seen_reference_platforms.add(platform)
        resolvers = self._resolvers_by_platform.get(platform)
        if not resolvers:
            return Resolution(urn, None, None)

        name = dataset_urn.name
        # A platform can have several configured resolvers (one per platform_instance /
        # env); we iterate and take the first schema match. We deliberately don't index by
        # platform_instance because it isn't recoverable from the URN — it's fused into the
        # name, and separating it would require fetching the dataPlatformInstance aspect
        # (overkill). env *is* recoverable from the URN, so we could additionally
        # disambiguate on it (e.g. avoid healing a PROD ref to a DEV entity), but that
        # collision is unlikely in practice, so it's left as a possible follow-up.
        for resolver in resolvers:
            table = self._strip_platform_instance(name, resolver.platform_instance)
            try:
                # We pass the whole name as `table` and rely on get_urn_for_table
                # concatenating the parts back into the name. This leans on SchemaResolver
                # internals and is a bit fragile (get_urn_for_table carries a TODO about
                # 2/3-layer hierarchy).
                resolved_urn, schema = resolver.resolve_table_parts(
                    database=None, db_schema=None, table=table
                )
            except Exception:
                continue
            # resolve_table returns a best-effort URN even on a miss; a non-None schema is
            # the signal that an existing entity actually matched.
            if schema is not None:
                match_type = EXACT if resolved_urn == urn else NORMALIZED
                return Resolution(resolved_urn, schema, match_type)
        # On a configured platform but no existing entity matched under a casing
        # resolve_table covers: leave the URN unchanged but flag it UNRESOLVED so
        # potentially broken lineage is visible rather than indistinguishable from clean.
        return Resolution(urn, None, UNRESOLVED)

    def finish(self) -> None:
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
        self._ctx.source_report.warning(
            title="Configured upstream platform matched no lineage references",
            message="An upstream platform configured under auto_resolve_lineage_urns was "
            "not referenced by any lineage in this run, so nothing was reconciled for it. "
            "Platform names are matched case-sensitively against the dataset URN's "
            "platform (e.g. 'snowflake', not 'Snowflake') — fix the name if it's a typo, "
            "or remove the platform from upstream_platforms if this source doesn't "
            "reference it.",
            context=f"{sorted(unmatched)}",
        )
