import logging
from typing import Optional

from datahub.api.entities.external.external_entities import (
    PlatformResourceRepository,
    SyncContext,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.unity.tag_entities import (
    UnityCatalogTagPlatformResource,
    UnityCatalogTagPlatformResourceId,
)

logger = logging.getLogger(__name__)


class UnityCatalogPlatformResourceRepository(
    PlatformResourceRepository[
        UnityCatalogTagPlatformResourceId, UnityCatalogTagPlatformResource
    ]
):
    """Unity Catalog-specific platform resource repository with tag-related operations."""

    def __init__(self, graph: DataHubGraph, platform_instance: Optional[str] = None):
        super().__init__(graph)
        self.platform_instance = platform_instance

    def extract_platform_instance(self, sync_context: SyncContext) -> Optional[str]:
        """Get platform instance from repository configuration."""
        return self.platform_instance
