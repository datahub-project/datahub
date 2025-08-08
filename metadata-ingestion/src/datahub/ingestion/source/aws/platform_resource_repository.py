import logging
from typing import Any, Optional

from datahub.api.entities.external.external_entities import (
    LinkedResourceSet,
    PlatformResourceRepository,
    SyncContext,
)
from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)


class GluePlatformResourceRepository(PlatformResourceRepository):
    """AWS Glue-specific platform resource repository with tag-related operations."""

    def __init__(self, graph: DataHubGraph):
        super().__init__(graph)

    def get_resource_type(self) -> str:
        """Get the Lake Formation tag resource type for filtering."""
        return "LakeFormationTagPlatformResource"

    def get_entity_class(self) -> type:
        """Get the Lake Formation tag entity class for deserialization."""
        # Import locally to avoid circular dependency
        from datahub.ingestion.source.aws.tag_entities import (
            LakeFormationTagPlatformResource,
        )

        return LakeFormationTagPlatformResource

    def create_default_entity(self, entity_id: Any, managed_by_datahub: bool) -> Any:
        """Create a default Lake Formation tag entity when none found in DataHub."""
        # Import locally to avoid circular dependency
        from datahub.ingestion.source.aws.tag_entities import (
            LakeFormationTagPlatformResource,
        )

        return LakeFormationTagPlatformResource(
            id=entity_id,
            datahub_urns=LinkedResourceSet(urns=[]),
            managed_by_datahub=managed_by_datahub,
            allowed_values=None,
        )

    def extract_platform_instance(self, sync_context: SyncContext) -> Optional[str]:
        """Extract platform instance from Lake Formation sync context."""
        return sync_context.platform_instance

    def configure_entity_for_return(self, entity_id: Any) -> Any:
        """Configure Lake Formation entity ID for return."""
        # Mark entity as existing in Lake Formation and persisted
        entity_id.exists_in_lake_formation = True
        entity_id.persisted = True
        return entity_id
