from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class Connector:
    connector_id: str
    connector_name: str
    connector_type: str
    paused: bool
    sync_frequency: int
    destination_id: str
    user_name: str
    table_lineage: List[Tuple[str, str]]
    jobs: List["Job"]


@dataclass
class Job:
    job_id: str
    start_time: int
    end_time: int
    status: str
