from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source
from datahub.ingestion.source.airbyte.config import AirbyteConfig


@platform_name("Airbyte")
@config_class(AirbyteConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class AirbyteSource(Source):
    """
    This plugin extracts airbyte workspace, connections, sources, destinations and jobs.
    This plugin is in beta and has only been tested on PostgreSQL.
    """
