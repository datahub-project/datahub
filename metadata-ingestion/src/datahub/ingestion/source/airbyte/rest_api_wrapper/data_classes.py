from dataclasses import dataclass


@dataclass
class Workspace:
    workspace_id: str
    name: str

    def get_urn_part(self):
        return f"workspaces.{self.workspace_id}"


@dataclass
class Connection:
    connection_id: str
    name: str
    source_id: str
    destination_id: str
    workspace_id: str
    status: str
    namespace_definition: str
    namespace_format: str
    prefix: str


@dataclass
class Source:
    source_id: str
    name: str
    source_type: str
    workspace_id: str


@dataclass
class Destination:
    destination_id: str
    name: str
    destination_type: str
    workspace_id: str


@dataclass
class Job:
    job_id: str
    status: str
    job_type: str
    start_time: int
    end_time: int
    connection_id: str
    last_updated_at: str
    bytes_synced: int
    rows_synced: int
