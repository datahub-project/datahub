import logging
from typing import Optional

from datahub.api.entities.external.external_entities import (
    PlatformResourceRepository,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.aws.tag_entities import (
    LakeFormationTagPlatformResource,
    LakeFormationTagPlatformResourceId,
)

logger = logging.getLogger(__name__)


class GluePlatformResourceRepository(
    PlatformResourceRepository[
        LakeFormationTagPlatformResourceId, LakeFormationTagPlatformResource
    ]
):
    """AWS Glue-specific platform resource repository with tag-related operations."""

    def __init__(
        self,
        graph: DataHubGraph,
        platform_instance: Optional[str] = None,
        catalog: Optional[str] = None,
    ):
        super().__init__(graph, platform_instance)
        self.catalog = catalog
