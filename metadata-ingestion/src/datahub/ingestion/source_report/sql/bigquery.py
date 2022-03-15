from dataclasses import dataclass

from datahub.ingestion.source.sql.sql_common import SQLSourceReport


@dataclass
class BigQueryReport(SQLSourceReport):
    num_total_log_entries: int = -1
    num_parsed_log_entires: int = -1
    num_total_audit_entries: int = -1
    num_parsed_audit_entires: int = -1
    lineage_metadata_entries: int = -1
    use_v2_audit_metadata: bool = False
    use_exported_bigquery_audit_metadata: bool = False
    start_time: str = ""
    end_time: str = ""
