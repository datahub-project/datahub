import logging

from datahub.api.entities.external.external_entities import PlatformResourceRepository
from datahub.ingestion.source.dataplex.dataplex_external_entities import (
    DataplexAspectId,
    DataplexAspectPlatformResource,
)

logger = logging.getLogger(__name__)


class DataplexPlatformResourceRepository(
    PlatformResourceRepository[DataplexAspectId, DataplexAspectPlatformResource]
):
    """Reads/writes the Dataplex sync-back platform-resource side-index.

    ``get_resource_type()`` returns ``entity_class.__name__`` — i.e.
    ``DataplexAspectPlatformResource`` — which is exactly the resource type the
    sync-back wrote, so lookups match without extra configuration.
    """
