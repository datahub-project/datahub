from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ColumnLineage(BaseModel):
    source_column: str
    destination_column: str


class TableLineage(BaseModel):
    source_table: str
    destination_table: str
    column_lineage: List[ColumnLineage]


class Job(BaseModel):
    job_id: str
    start_time: int
    end_time: int
    status: str


class Connector(BaseModel):
    connector_id: str
    connector_name: str
    connector_type: str
    paused: bool
    sync_frequency: int
    destination_id: str
    user_id: Optional[str]
    lineage: List[TableLineage]
    jobs: List[Job]
    additional_properties: Dict[str, Any] = Field(default_factory=dict)
