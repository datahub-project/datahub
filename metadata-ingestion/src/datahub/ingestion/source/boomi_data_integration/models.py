from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class DIModel(BaseModel):
    model_config = ConfigDict(extra="ignore")


class PageResponse(DIModel):
    # Items stay raw so that one malformed record only drops that record.
    items: List[object] = Field(default_factory=list)
    next_page: Optional[str] = None


class Environment(DIModel):
    cross_id: str
    environment_name: str
    is_default: bool = False
    is_deleted: Optional[bool] = False


class RiverSummary(DIModel):
    river_cross_id: str
    name: str
    river_type: str
    river_status: Optional[str] = None
    group_id: Optional[str] = None
    group_name: Optional[str] = None
    datasource_id: Optional[str] = None
    description: Optional[str] = None
    river_schedulers: List[str] = Field(default_factory=list)
    is_api_v2: Optional[bool] = None
    created_at: Optional[datetime] = None
    last_updated_at: Optional[datetime] = None


class RiverMetadata(DIModel):
    description: Optional[str] = None
    river_status: Optional[str] = None
    created_at: Optional[datetime] = None
    last_updated_at: Optional[datetime] = None


class RiverSchedule(DIModel):
    cron_expression: Optional[str] = None
    is_enabled: bool = False


class River(DIModel):
    cross_id: str
    name: str
    type: str
    group_id: Optional[str] = None
    group_name: Optional[str] = None
    metadata: RiverMetadata = Field(default_factory=RiverMetadata)
    # Parsed per river type by river_parser, because the discriminator
    # (`properties_type`) is optional in responses.
    properties: Dict[str, object] = Field(default_factory=dict)
    schedulers: Optional[List[RiverSchedule]] = None


class SourceSettings(DIModel):
    name: str
    connection_id: Optional[str] = None
    run_type: Optional[str] = None


class TargetSettings(DIModel):
    name: str
    connection_id: Optional[str] = None
    table_name: Optional[str] = None
    target_prefix: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    dataset_id: Optional[str] = None
    catalog_name: Optional[str] = None


class TableDetails(DIModel):
    # Database sources name the table `name`; predefined reports use `table_name`.
    name: Optional[str] = None
    table_name: Optional[str] = None
    is_selected: bool = False
    target_table: Optional[str] = None

    @property
    def source_table(self) -> Optional[str]:
        return self.name or self.table_name


class TableEntry(DIModel):
    details: TableDetails


class SourceSchema(DIModel):
    name: Optional[str] = None
    tables: List[TableEntry] = Field(default_factory=list)


class SourceToTargetProperties(DIModel):
    source: SourceSettings
    target: TargetSettings
    schemas: List[SourceSchema] = Field(default_factory=list)


class LogicProperties(DIModel):
    logic_steps: List[object] = Field(default_factory=list)


class StepTargetSettings(DIModel):
    target_type: Optional[str] = None
    table_name: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    dataset_id: Optional[str] = None
    catalog: Optional[str] = None


class LogicNode(DIModel):
    type: str
    name: Optional[str] = None
    is_enabled: bool = True
    step_id: Optional[str] = None
    container_id: Optional[str] = None
    is_parallel: bool = False
    # Children stay raw so that a malformed child only drops that child.
    steps: List[object] = Field(default_factory=list)
    river_id: Optional[str] = None
    connection_id: Optional[str] = None
    target_settings: Optional[StepTargetSettings] = None


class ConditionBranch(DIModel):
    condition_name: Optional[str] = None
    is_else: bool = False
    step: Optional[object] = None
