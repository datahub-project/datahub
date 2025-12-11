# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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

    def report_file_scanned(self) -> None:
        self.files_scanned += 1

    def report_file_dropped(self, file: str) -> None:
        self.filtered.append(file)
        self.number_of_files_filtered += 1
