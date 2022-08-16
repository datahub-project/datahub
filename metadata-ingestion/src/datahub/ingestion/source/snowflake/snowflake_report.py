from datahub.ingestion.source_report.sql.snowflake import SnowflakeReport
from datahub.ingestion.source_report.usage.snowflake_usage import SnowflakeUsageReport


class SnowflakeV2Report(SnowflakeReport, SnowflakeUsageReport):
    include_usage_stats: bool = False
    include_operational_stats: bool = False

    usage_aggregation_query_secs: float = -1

    rows_zero_objects_modified: int = 0
