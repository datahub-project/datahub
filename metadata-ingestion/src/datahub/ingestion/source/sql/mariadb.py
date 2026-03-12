from datahub.ingestion.api.decorators import (
    capability,
    config_class,
    IngestionSourceCategory,
    platform_name,
    SourceCapability,
    SourceCategory,
    support_status,
    SupportStatus,
)
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource


@SourceCategory(IngestionSourceCategory.DATA_WAREHOUSE)  # pragma: no cover
@platform_name("MariaDB")
@config_class(MySQLConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class MariaDBSource(MySQLSource):
    def get_platform(self):
        return "mariadb"
