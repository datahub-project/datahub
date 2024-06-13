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
    table_lineage: List[TableLineage]
    jobs: List["Job"]


@dataclass
class Job:
    job_id: str
    start_time: int
    end_time: int
    status: str
