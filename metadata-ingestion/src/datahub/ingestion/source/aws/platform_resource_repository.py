# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
