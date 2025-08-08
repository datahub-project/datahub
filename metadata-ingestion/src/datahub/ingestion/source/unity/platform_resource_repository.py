import logging
from typing import Any, Optional

from datahub.api.entities.external.external_entities import (
    LinkedResourceSet,
    PlatformResourceRepository,
    SyncContext,
)
from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


class UnityCatalogPlatformResourceRepository(PlatformResourceRepository):
    """Unity Catalog-specific platform resource repository with tag-related operations."""

    def __init__(self, graph: DataHubGraph):
        super().__init__(graph)

    def get_resource_type(self) -> str:
        """Get the Unity Catalog tag resource type for filtering."""
        return "UnityCatalogTagPlatformResource"

    def get_entity_class(self) -> type:
        """Get the Unity Catalog tag entity class for deserialization."""
        # Import locally to avoid circular dependency
        from datahub.ingestion.source.unity.tag_entities import (
            UnityCatalogTagPlatformResource,
        )

        return UnityCatalogTagPlatformResource

    def create_default_entity(self, entity_id: Any, managed_by_datahub: bool) -> Any:
        """Create a default Unity Catalog tag entity when none found in DataHub."""
        # Import locally to avoid circular dependency
        from datahub.ingestion.source.unity.tag_entities import (
            UnityCatalogTagPlatformResource,
        )

        return UnityCatalogTagPlatformResource(
            id=entity_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=managed_by_datahub,
            allowed_values=None,
        )

    def extract_platform_instance(self, sync_context: SyncContext) -> Optional[str]:
        """Extract platform instance from Unity Catalog sync context."""
        return sync_context.platform_instance

    def configure_entity_for_return(self, entity_id: Any) -> Any:
        """Configure Unity Catalog entity ID for return."""
        # Mark entity as existing in Unity Catalog and persisted
        entity_id.exists_in_unity_catalog = True
        entity_id.persisted = True
        return entity_id
