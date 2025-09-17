from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ColumnLineage(BaseModel):
    source_column: str
    destination_column: str
    # Column type metadata for lineage
    source_column_type: Optional[str] = None
    destination_column_type: Optional[str] = None


class TableLineage(BaseModel):
    source_table: str
    destination_table: str
    column_lineage: List[ColumnLineage]

    # Comprehensive lineage metadata
    source_schema: Optional[str] = None
    destination_schema: Optional[str] = None
    source_database: Optional[str] = None
    destination_database: Optional[str] = None
    source_platform: Optional[str] = None
    destination_platform: Optional[str] = None
    source_env: Optional[str] = None
    destination_env: Optional[str] = None
    connector_type_id: Optional[str] = None
    connector_name: Optional[str] = None
    destination_id: Optional[str] = None


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
    user_id: Optional[str] = None
    lineage: List[TableLineage] = Field(default_factory=list)
    jobs: List[Job] = Field(default_factory=list)
    additional_properties: Dict[str, Any] = Field(default_factory=dict)
