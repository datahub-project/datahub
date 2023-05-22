import collections
import dataclasses
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Counter, Dict, List, Optional

import pydantic

from datahub.ingestion.source.sql.sql_generic_profiler import ProfilingSqlReport
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.stats_collections import TopKDict, int_top_k_dict

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class BigQueryV2Report(ProfilingSqlReport):
    num_total_lineage_entries: TopKDict[str, int] = field(default_factory=TopKDict)
    num_skipped_lineage_entries_missing_data: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    num_skipped_lineage_entries_not_allowed: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    num_lineage_entries_sql_parser_failure: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    num_skipped_lineage_entries_other: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    num_total_log_entries: TopKDict[str, int] = field(default_factory=int_top_k_dict)
    num_parsed_log_entries: TopKDict[str, int] = field(default_factory=int_top_k_dict)
    num_lineage_log_parse_failures: TopKDict[str, int] = field(
        default_factory=int_top_k_dict
    )
    bigquery_audit_metadata_datasets_missing: Optional[bool] = None
    lineage_failed_extraction: LossyList[str] = field(default_factory=LossyList)
    lineage_metadata_entries: TopKDict[str, int] = field(default_factory=TopKDict)
    lineage_mem_size: Dict[str, str] = field(default_factory=TopKDict)
    lineage_extraction_sec: Dict[str, float] = field(default_factory=TopKDict)
    usage_extraction_sec: Dict[str, float] = field(default_factory=TopKDict)
    usage_error_count: Dict[str, int] = field(default_factory=int_top_k_dict)
    num_usage_resources_dropped: int = 0
    num_usage_operations_dropped: int = 0
    operation_dropped: LossyList[str] = field(default_factory=LossyList)
    usage_failed_extraction: LossyList[str] = field(default_factory=LossyList)
    num_project_datasets_to_scan: Dict[str, int] = field(default_factory=TopKDict)
    metadata_extraction_sec: Dict[str, float] = field(default_factory=TopKDict)
    include_table_lineage: Optional[bool] = None
    use_date_sharded_audit_log_tables: Optional[bool] = None
    log_page_size: Optional[pydantic.PositiveInt] = None
    use_exported_bigquery_audit_metadata: Optional[bool] = None
    end_time: Optional[datetime] = None
    log_entry_start_time: Optional[str] = None
    log_entry_end_time: Optional[str] = None
    audit_start_time: Optional[str] = None
    audit_end_time: Optional[str] = None
    upstream_lineage: LossyDict = field(default_factory=LossyDict)
    partition_info: Dict[str, str] = field(default_factory=TopKDict)
    profile_table_selection_criteria: Dict[str, str] = field(default_factory=TopKDict)
    selected_profile_tables: Dict[str, List[str]] = field(default_factory=TopKDict)
    invalid_partition_ids: Dict[str, str] = field(default_factory=TopKDict)
    allow_pattern: Optional[str] = None
    deny_pattern: Optional[str] = None
    num_usage_workunits_emitted: int = 0
    total_query_log_entries: int = 0
    num_read_events: int = 0
    num_query_events: int = 0
    num_filtered_read_events: int = 0
    num_filtered_query_events: int = 0
    num_usage_query_hash_collisions: int = 0
    num_operational_stats_workunits_emitted: int = 0
    read_reasons_stat: Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    operation_types_stat: Counter[str] = dataclasses.field(
        default_factory=collections.Counter
    )
    usage_state_size: Optional[str] = None
    ingestion_stage: Optional[str] = None
    ingestion_stage_durations: Dict[str, str] = field(default_factory=TopKDict)

    _timer: Optional[PerfTimer] = field(
        default=None, init=False, repr=False, compare=False
    )

    def set_ingestion_stage(self, project: str, stage: str) -> None:
        if self._timer:
            elapsed = f"{self._timer.elapsed_seconds():.2f}"
            logger.info(
                f"Time spent in stage <{self.ingestion_stage}>: {elapsed} seconds"
            )
            if self.ingestion_stage:
                self.ingestion_stage_durations[self.ingestion_stage] = elapsed
        else:
            self._timer = PerfTimer()

        self.ingestion_stage = f"{project}: {stage} at {datetime.now(timezone.utc)}"
        self._timer.start()
