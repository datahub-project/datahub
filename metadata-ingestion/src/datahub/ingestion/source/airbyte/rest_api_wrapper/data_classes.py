from dataclasses import dataclass
from typing import List


@dataclass
class Workspace:
    workspace_id: str
    name: str
    connections: List["Connection"]

    def get_urn_part(self):
        return f"workspaces.{self.workspace_id}"


@dataclass
class Connection:
    connection_id: str
    name: str
    source: "Source"
    destination: "Destination"
    status: str
    namespace_definition: str
    namespace_format: str
    prefix: str
    jobs: List["Job"]


@dataclass
class Source:
    source_id: str
    name: str
    source_type: str


@dataclass
class Destination:
    destination_id: str
    name: str
    destination_type: str


@dataclass
class Job:
    job_id: str
    status: str
    job_type: str
    start_time: int
    end_time: int
    last_updated_at: str
    bytes_synced: int
    rows_synced: int
