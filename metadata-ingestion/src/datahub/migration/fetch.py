"""Stage 1 of the migration framework: discover the source URNs to migrate.

Each fetcher is a generator over source URNs for a single entity type; a caller
combines them with a stage-2 transform (or supplies target URNs directly) to
produce :class:`MigrationPair` objects for the engine.
"""

import logging
from typing import Iterator

from datahub.cli.migration_utils import ENV_ENTITY_TYPES
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import DataPlatformInstanceClass

log = logging.getLogger(__name__)


def fetch_platform_urns(
    graph: DataHubGraph,
    *,
    platform: str,
    env: str,
    entity_type: str,
    skip_if_has_instance: bool = True,
) -> Iterator[str]:
    """URNs of a platform's entities that have no platform instance yet.

    Used by ``dataplatform2instance``: only entities still lacking a
    ``dataPlatformInstance.instance`` are candidates for being assigned one.
    """
    for src_urn in graph.get_urns_by_filter(
        platform=platform,
        env=env if entity_type in ENV_ENTITY_TYPES else None,
        entity_types=[entity_type],
    ):
        if skip_if_has_instance:
            instance_aspect = graph.get_aspect(src_urn, DataPlatformInstanceClass)
            if instance_aspect is not None and instance_aspect.instance:
                log.debug(f"{src_urn} already has instance, skipping")
                continue
        yield src_urn


def fetch_instance_urns(
    graph: DataHubGraph,
    *,
    platform: str,
    old_instance: str,
    env: str,
    entity_type: str,
) -> Iterator[str]:
    """URNs of a platform instance's entities of a given type."""
    yield from graph.get_urns_by_filter(
        platform=platform,
        platform_instance=old_instance,
        env=env if entity_type in ENV_ENTITY_TYPES else None,
        entity_types=[entity_type],
    )
