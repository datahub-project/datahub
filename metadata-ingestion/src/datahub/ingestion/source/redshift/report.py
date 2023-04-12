from dataclasses import dataclass, field
from typing import Dict, Optional

from datahub.ingestion.source.sql.sql_generic_profiler import ProfilingSqlReport
from datahub.utilities.lossy_collections import LossyDict
from datahub.utilities.stats_collections import TopKDict


@dataclass
class RedshiftReport(ProfilingSqlReport):
    num_usage_workunits_emitted: Optional[int] = None
    num_operational_stats_workunits_emitted: Optional[int] = None
    upstream_lineage: LossyDict = field(default_factory=LossyDict)
    usage_extraction_sec: Dict[str, float] = field(default_factory=TopKDict)
    lineage_extraction_sec: Dict[str, float] = field(default_factory=TopKDict)
    table_processed: TopKDict[str, int] = field(default_factory=TopKDict)
    view_processed: TopKDict[str, int] = field(default_factory=TopKDict)
    metadata_extraction_sec: TopKDict[str, float] = field(default_factory=TopKDict)
    operational_metadata_extraction_sec: TopKDict[str, float] = field(
        default_factory=TopKDict
    )
    lineage_mem_size: Dict[str, str] = field(default_factory=TopKDict)
    tables_in_mem_size: Dict[str, str] = field(default_factory=TopKDict)
    views_in_mem_size: Dict[str, str] = field(default_factory=TopKDict)
    num_operational_stats_skipped: int = 0
    num_usage_stat_skipped: int = 0

    def report_dropped(self, key: str) -> None:
        self.filtered.append(key)
