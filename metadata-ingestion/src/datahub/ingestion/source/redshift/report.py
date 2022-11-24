import collections
import dataclasses
from dataclasses import dataclass, field
from datetime import datetime
from typing import Counter, Dict, List, Optional, Set

import pydantic

from datahub.ingestion.source.sql.sql_common import SQLSourceReport
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.stats_collections import TopKDict


@dataclass
class RedshiftReport(SQLSourceReport):
    num_usage_workunits_emitted: Optional[int] = None
    num_operational_stats_workunits_emitted: Optional[int] = None
    upstream_lineage: LossyDict = field(default_factory=LossyDict)

    def report_dropped(self, key: str) -> None:
        self.filtered.append(key)
