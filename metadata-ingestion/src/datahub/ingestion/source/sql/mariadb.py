from datahub.ingestion.api.decorators import (
    IngestionSourceCategory,
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    source_category,
    support_status,
)
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource


@source_category(IngestionSourceCategory.DATA_WAREHOUSE)
@platform_name("MariaDB")
@config_class(MySQLConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class MariaDBSource(MySQLSource):
    def get_platform(self):
        return "mariadb"
