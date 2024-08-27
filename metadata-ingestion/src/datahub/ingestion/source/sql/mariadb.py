from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource


@platform_name("MariaDB")
@config_class(MySQLConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class MariaDBSource(MySQLSource):
    def get_platform(self):
        return "mariadb"
