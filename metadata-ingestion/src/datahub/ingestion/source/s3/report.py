import dataclasses
from dataclasses import field as dataclass_field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclasses.dataclass
class DataLakeSourceReport(StaleEntityRemovalSourceReport):
    files_scanned = 0
    filtered: LossyList[str] = dataclass_field(default_factory=LossyList)
    number_of_files_filtered: int = 0

    objects_listed: int = 0
    folders_scanned: int = 0
    # Number of tables for which tags were fetched (one get_s3_tags call each,
    # which may issue multiple AWS requests internally), not a literal API count.
    tables_tagged: int = 0
    # Wall-clock for the whole listing phase (S3 list API + path matching +
    # folder accumulation), not pure network I/O.
    listing_time_taken_secs: float = 0.0
    schema_inference_time_taken_secs: float = 0.0

    def report_file_scanned(self) -> None:
        self.files_scanned += 1

    def report_folder_scanned(self) -> None:
        self.folders_scanned += 1

    def report_file_dropped(self, file: str) -> None:
        self.filtered.append(file)
        self.number_of_files_filtered += 1
