# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
