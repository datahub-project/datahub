import dataclasses
from dataclasses import field as dataclass_field

from datahub.ingestion.api.source import SourceReport
from datahub.utilities.lossy_collections import LossyList


@dataclasses.dataclass
class DeltaLakeSourceReport(SourceReport):
    files_scanned = 0
    filtered: LossyList[str] = dataclass_field(default_factory=LossyList)

    def report_file_scanned(self) -> None:
        self.files_scanned += 1

    def report_file_dropped(self, file: str) -> None:
        self.filtered.append(file)
