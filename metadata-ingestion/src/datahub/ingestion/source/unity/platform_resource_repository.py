import logging

from datahub.api.entities.external.external_entities import (
    PlatformResourceRepository,
)
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
