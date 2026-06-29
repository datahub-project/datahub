import functools
import logging
from typing import TYPE_CHECKING, Optional

from datahub.sql_parsing.schema_resolver import (
    SchemaResolver,
    SchemaResolverReport,
)
from datahub.utilities.perf_timer import PerfTimer

if TYPE_CHECKING:
    from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


@functools.lru_cache
def provide_schema_resolver(
    graph: "DataHubGraph",
    platform: str,
    platform_instance: Optional[str],
    env: str,
    batch_size: int = 100,
    populate_membership: bool = False,
) -> SchemaResolver:
    """Return a bulk-initialized SchemaResolver, cached globally per (graph, platform, platform_instance, env).

    Using a module-level cache ensures deduplication across all callers in the same
    process, even when different SchemaResolverProvider instances are created.

    :param populate_membership: also populate the resolver's casing/membership index
        (including schemaless entities) from the same bulk scroll, so callers needing
        existence/casing lookups don't have to run a second scroll. Default False keeps
        the schema-only behavior for SQL-parsing callers.
    """
    return SchemaResolverProvider(graph=graph, batch_size=batch_size).get(
        platform=platform,
        platform_instance=platform_instance,
        env=env,
        populate_membership=populate_membership,
    )


class SchemaResolverProvider:
    """Creates and bulk-initializes SchemaResolver instances from DataHub.

    Separates SchemaResolver lifecycle management from DataHubGraph, which is
    responsible only for fetching raw schema data via _bulk_fetch_schema_info_by_filter().
    """

    def __init__(
        self,
        graph: "DataHubGraph",
        batch_size: int = 100,
        report: Optional[SchemaResolverReport] = None,
    ) -> None:
        self._graph = graph
        self._batch_size = batch_size
        self._report = report

    @functools.lru_cache
    def get(
        self,
        platform: str,
        platform_instance: Optional[str],
        env: str,
        populate_membership: bool = False,
    ) -> SchemaResolver:
        """Return a bulk-initialized SchemaResolver, cached per (platform, platform_instance, env).

        When ``populate_membership`` is True, the single bulk scroll also records every
        existing URN (including schemaless ones) in the resolver's casing index — so a
        caller that needs membership/casing lookups avoids a second scroll over the
        same datasets.
        """
        resolver = SchemaResolver(
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=None,
            report=self._report,
        )
        logger.info(f"Fetching schemas for platform {platform}, env {env}")
        count = 0
        membership_count = 0
        with PerfTimer() as timer:
            for urn, schema_info in self._graph._bulk_fetch_schema_info_by_filter(
                platform=platform,
                platform_instance=platform_instance,
                env=env,
                batch_size=self._batch_size,
                include_schemaless=populate_membership,
            ):
                try:
                    if populate_membership:
                        resolver.add_known_urn(urn)
                        membership_count += 1
                    if schema_info is not None:
                        resolver.add_graphql_schema_metadata(urn, schema_info)
                        count += 1
                except Exception:
                    logger.warning(
                        f"Failed to add schema info for {urn}", exc_info=True
                    )

                if count > 0 and count % 1000 == 0:
                    logger.debug(
                        f"Loaded {count} schema info in {timer.elapsed_seconds()} seconds"
                    )
            logger.info(
                f"Finished loading {count} schema info"
                f"{f' and {membership_count} known URNs' if populate_membership else ''}"
                f" in {timer.elapsed_seconds()} seconds"
            )

        # Only a schema-only fetch returning nothing is a problem worth flagging for SQL
        # lineage; a membership fetch may legitimately find existing-but-schemaless URNs.
        if count == 0 and not populate_membership:
            logger.warning(
                f"Bulk schema fetch returned 0 results for platform={platform}, "
                f"platform_instance={platform_instance}, env={env}. "
                "Schema resolver will be empty — SQL lineage may be incomplete."
            )
        return resolver

    def close(self) -> None:
        self.get.cache_clear()
