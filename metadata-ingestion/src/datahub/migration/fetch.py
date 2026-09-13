"""Stage 1 of the migration framework: discover the source URNs to migrate.

Each fetcher returns a list of source URNs for a single entity type; a caller
combines them with a stage-2 transform (or supplies target URNs directly) to
produce :class:`MigrationPair` objects for the engine.
"""

import logging
from typing import List

from datahub.cli.migration_utils import ENV_ENTITY_TYPES
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import DataPlatformInstanceClass

log = logging.getLogger(__name__)


def _matches_pipeline_name(graph: DataHubGraph, urn: str, pipeline_name: str) -> bool:
    """Check if any aspect on *urn* was last written by *pipeline_name*.

    We look at the systemMetadata attached to each aspect on the entity.
    If at least one aspect was produced by the given pipeline, we consider
    the entity as belonging to that pipeline.
    """
    mcpws = graph.get_entity_as_mcps(urn)
    return any(
        mcpw.systemMetadata and mcpw.systemMetadata.pipelineName == pipeline_name
        for mcpw in mcpws
    )


def fetch_platform_urns(
    graph: DataHubGraph,
    *,
    platform: str,
    env: str,
    entity_type: str,
    skip_if_has_instance: bool = True,
) -> List[str]:
    """URNs of a platform's entities that have no platform instance yet.

    Used by ``dataplatform2instance``: only entities still lacking a
    ``dataPlatformInstance.instance`` are candidates for being assigned one.
    """
    candidates: List[str] = []
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
        candidates.append(src_urn)

    return candidates


def fetch_instance_urns(
    graph: DataHubGraph,
    *,
    platform: str,
    old_instance: str,
    env: str,
    entity_type: str,
) -> List[str]:
    """URNs of a platform instance's entities of a given type."""
    return list(
        graph.get_urns_by_filter(
            platform=platform,
            platform_instance=old_instance,
            env=env if entity_type in ENV_ENTITY_TYPES else None,
            entity_types=[entity_type],
        )
    )
