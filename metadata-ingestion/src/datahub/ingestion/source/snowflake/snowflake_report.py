from typing import Optional

from datahub.ingestion.source.sql.sql_generic_profiler import ProfilingSqlReport
from datahub.ingestion.source_report.sql.snowflake import SnowflakeReport
from datahub.ingestion.source_report.usage.snowflake_usage import SnowflakeUsageReport


class SnowflakeV2Report(SnowflakeReport, SnowflakeUsageReport, ProfilingSqlReport):

    account_locator: Optional[str] = None
    region: Optional[str] = None

    schemas_scanned: int = 0
    databases_scanned: int = 0

    include_usage_stats: bool = False
    include_operational_stats: bool = False
    include_technical_schema: bool = False
    include_column_lineage: bool = False

    usage_aggregation_query_secs: float = -1
    table_lineage_query_secs: float = -1
    view_upstream_lineage_query_secs: float = -1
    view_downstream_lineage_query_secs: float = -1
    external_lineage_queries_secs: float = -1

    # These will be non-zero if snowflake information_schema queries fail with error -
    # "Information schema query returned too much data. Please repeat query with more selective predicates.""
    # This will result in overall increase in time complexity
    num_get_tables_for_schema_queries: int = 0
    num_get_views_for_schema_queries: int = 0
    num_get_columns_for_table_queries: int = 0

    rows_zero_objects_modified: int = 0

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a view or a table or a schema or a database
        """
        if ent_type == "table":
            self.tables_scanned += 1
        elif ent_type == "view":
            self.views_scanned += 1
        elif ent_type == "schema":
            self.schemas_scanned += 1
        elif ent_type == "database":
            self.databases_scanned += 1
        else:
            raise KeyError(f"Unknown entity {ent_type}.")
