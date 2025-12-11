# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from dataclasses import dataclass
from typing import List


@dataclass
class ColumnLineage:
    source_column: str
    destination_column: str


@dataclass
class TableLineage:
    source_table: str
    destination_table: str
    column_lineage: List[ColumnLineage]


@dataclass
class Connector:
    connector_id: str
    connector_name: str
    connector_type: str
    paused: bool
    sync_frequency: int
    destination_id: str
    user_id: str
    lineage: List[TableLineage]
    jobs: List["Job"]


@dataclass
class Job:
    job_id: str
    start_time: int
    end_time: int
    status: str
