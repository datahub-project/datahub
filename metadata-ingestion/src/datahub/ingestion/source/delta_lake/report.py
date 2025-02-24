import dataclasses
from dataclasses import field as dataclass_field
from typing import List

from datahub.ingestion.api.source import SourceReport


@dataclasses.dataclass
class DeltaLakeSourceReport(SourceReport):
    files_scanned = 0
    filtered: List[str] = dataclass_field(default_factory=list)

    def report_file_scanned(self) -> None:
        self.files_scanned += 1

    def report_file_dropped(self, file: str) -> None:
        self.filtered.append(file)
