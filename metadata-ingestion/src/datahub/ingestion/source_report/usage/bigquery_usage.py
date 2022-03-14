import collections
import dataclasses
from typing import Counter

from datahub.ingestion.api.source import SourceReport


@dataclasses.dataclass
class BigQueryUsageSourceReport(SourceReport):
    dropped_table: Counter[str] = dataclasses.field(default_factory=collections.Counter)

    def report_dropped(self, key: str) -> None:
        self.dropped_table[key] += 1
