from typing import Optional

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import DatasetSourceConfigMixin
from datahub.ingestion.source.salesforce import SalesforceConnectionConfig
from datahub.ingestion.source.salesforce_data360.constants import DEFAULT_DATASPACE
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class SalesforceData360Config(
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    SalesforceConnectionConfig,
):
    dataspace: str = Field(
        default=DEFAULT_DATASPACE,
        description="Data 360 dataspace to read DMO relationships from.",
    )
    dmo_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for Data Model Object API names (e.g. "
        "'ssot__Individual__dlm') to filter in ingestion.",
    )
    include_disabled_dmos: bool = Field(
        default=False,
        description="Also ingest standard DMOs that are not enabled in the org. A "
        "new org lists about a thousand of them. Custom DMOs are always included.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Stateful ingestion / stale-entity removal config."
    )
