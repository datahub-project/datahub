from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.sql.mysql import MySQLConfig, MySQLSource

# MariaDB uses MySQLConfig directly (via @config_class below), so it intentionally inherits
# MySQLProfilingConfig's four overrides (max_workers=5, profile_table_row_limit=None,
# profile_table_size_limit=None, report_expensive_tables=True). MariaDB is a MySQL fork — same
# single-primary row-store architecture, same InnoDB undo-log / history-list growth that produces
# the long-transaction problem, same information_schema.tables estimates. The MySQL defaults are
# correct for MariaDB; do NOT revert them. (If a new override is added to MySQLProfilingConfig,
# MariaDB should pick it up too — see test_mysql_profiling.py::test_mysql_profiling_overrides_do_not_drift.)


@platform_name("MariaDB")
@config_class(MySQLConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.USAGE_STATS,
    "Optionally enabled via `include_usage_statistics`. Reads query history from "
    "`performance_schema` digests (default) or `mysql.general_log` "
    "(`usage_source: general_log`), which also yields query-based table lineage.",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default for views via `include_view_lineage`. Table-level lineage is "
    "also derived from query history when `include_usage_statistics` is enabled.",
    subtype_modifier=[
        SourceCapabilityModifier.VIEW,
        SourceCapabilityModifier.TABLE,
    ],
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default for views via `include_view_column_lineage`. Column-level "
    "lineage is also derived from query history when `include_usage_statistics` is "
    "enabled.",
    subtype_modifier=[
        SourceCapabilityModifier.VIEW,
        SourceCapabilityModifier.TABLE,
    ],
)
class MariaDBSource(MySQLSource):
    def get_platform(self):
        return "mariadb"
