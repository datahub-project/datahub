import base64
from typing import Dict, List, Optional, Set

from pydantic import BaseModel, Field, root_validator

from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.source.ms_fabric.constants import (
    ItemType,
    LakehouseTableType,
    SqlEndpointProvisioningStatus,
)


class Workspace(BaseModel):
    """Class to represent a workspace in Microsoft Fabric"""

    id: str
    display_name: str = Field(alias="displayName")
    type: ItemType
    capacity_id: Optional[str] = Field(default=None, alias="capacityId")


class SqlEndpointProperties(BaseModel):
    connection_string: str = Field(alias="connectionString")
    id: str
    provisioning_status: SqlEndpointProvisioningStatus = Field(
        alias="provisioningStatus"
    )


class LakehouseProperties(BaseModel):
    default_schema: Optional[str] = Field(default=None, alias="defaultSchema")
    one_lake_files_path: Optional[str] = Field(alias="oneLakeFilesPath")
    one_lake_tables_path: str = Field(alias="oneLakeTablesPath")
    sql_endpoint_properties: SqlEndpointProperties = Field(
        alias="sqlEndpointProperties"
    )
    _type: str = Field(alias="$type")


class Lakehouse(BaseModel):
    description: str
    display_name: str = Field(alias="displayName")
    id: str
    properties: LakehouseProperties
    type: ItemType
    workspace_id: str = Field(alias="workspaceId")


class LakehouseTable(BaseModel):
    format: str
    location: str
    name: str
    type: LakehouseTableType


class WarehouseProperties(BaseModel):
    connection_string: str = Field(alias="connectionString")
    created_date: str = Field(alias="createdDate")
    last_updated_time: str = Field(alias="lastUpdatedTime")


class Warehouse(BaseModel):
    description: str
    display_name: str = Field(alias="displayName")
    id: str
    properties: WarehouseProperties
    type: ItemType
    workspace_id: str = Field(alias="workspaceId")


class MirroredDatabase(BaseModel):
    description: str
    display_name: str = Field(alias="displayName")
    id: str
    properties: LakehouseProperties
    type: ItemType
    workspace_id: str = Field(alias="workspaceId")


class SemanticModelContainerKey(ContainerKey):
    key: str


class SemanticModelPart(BaseModel):
    path: str
    payload: str
    payload_type: str = Field(alias="payloadType")

    @root_validator(pre=False)
    def decode_payload(cls, values):
        try:
            values["payload"] = base64.b64decode(values["payload"]).decode("utf-8")
        except Exception as e:
            raise ValueError(f"Failed to decode base64 payload: {str(e)}")
        return values


class SemanticModelParts(BaseModel):
    parts: List[SemanticModelPart]


class SemanticModel(BaseModel):
    description: str
    display_name: str = Field(alias="displayName")
    id: str
    type: ItemType
    workspace_id: str = Field(alias="workspaceId")
    definition: Optional[SemanticModelParts]


class TmdlMeasure(BaseModel):
    name: str
    formula: str
    description: str = ""
    annotations: Dict[str, str] = None


class TmdlColumn(BaseModel):
    name: str
    data_type: str = ""
    source_provider_type: Optional[str] = None
    lineage_tag: str = ""
    summarize_by: str = ""
    source_column: Optional[str] = None
    source_lineage_tag: Optional[str] = None
    is_hidden: bool = False
    annotations: Dict[str, str] = None


class TmdlPartition(BaseModel):
    name: str
    mode: str = ""
    source_query: Optional[str] = None
    source_type: Optional[str] = None
    expression_source: Optional[str] = None


class TmdlCalculatedColumn(BaseModel):
    name: str
    formula: str
    referenced_columns: Set[str]
    description: str = ""


class TmdlTable(BaseModel):
    name: str
    lineage_tag: str = ""
    columns: List[TmdlColumn]
    measures: List[TmdlMeasure]
    partitions: List[TmdlPartition]
    calculated_columns: List[TmdlCalculatedColumn]
    annotations: Dict[str, str]
    source_lineage_tag: Optional[str] = None


class TmdlModel(BaseModel):
    name: str
    culture: Optional[str] = "en-US"
    tables: List[TmdlTable]
    compatibility_level: Optional[int] = 1550
    annotations: Dict[str, str]
