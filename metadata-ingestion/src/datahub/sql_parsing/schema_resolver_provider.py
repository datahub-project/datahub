import functools
import logging
from typing import Iterable, Optional, Protocol, Tuple

from datahub.sql_parsing.schema_resolver import (
    GraphQLSchemaMetadata,
    SchemaResolver,
    SchemaResolverReport,
)
from datahub.utilities.perf_timer import PerfTimer

logger = logging.getLogger(__name__)


class _SchemaFetcher(Protocol):
    def _bulk_fetch_schema_info_by_filter(
        self,
        *,
        platform: str,
        platform_instance: Optional[str],
        env: str,
        batch_size: int,
    ) -> Iterable[Tuple[str, GraphQLSchemaMetadata]]: ...


class SchemaResolverProvider:
    """Creates and bulk-initializes SchemaResolver instances from DataHub.

    Separates SchemaResolver lifecycle management from DataHubGraph, which is
    responsible only for fetching raw schema data.
    """

    def __init__(
        self,
        graph: _SchemaFetcher,
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
    ) -> SchemaResolver:
        """Return a bulk-initialized SchemaResolver, cached per (platform, platform_instance, env)."""
        resolver = SchemaResolver(
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=None,
            report=self._report,
        )
        logger.info(f"Fetching schemas for platform {platform}, env {env}")
        count = 0
        with PerfTimer() as timer:
            for urn, schema_info in self._graph._bulk_fetch_schema_info_by_filter(
                platform=platform,
                platform_instance=platform_instance,
                env=env,
                batch_size=self._batch_size,
            ):
                try:
                    resolver.add_graphql_schema_metadata(urn, schema_info)
                    count += 1
                except Exception:
                    logger.warning("Failed to add schema info", exc_info=True)

                if count % 1000 == 0:
                    logger.debug(
                        f"Loaded {count} schema info in {timer.elapsed_seconds()} seconds"
                    )
            logger.info(
                f"Finished loading {count} schema info in {timer.elapsed_seconds()} seconds"
            )
        return resolver

    def close(self) -> None:
        self.get.cache_clear()
