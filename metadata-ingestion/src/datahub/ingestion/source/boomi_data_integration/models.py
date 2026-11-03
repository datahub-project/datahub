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
    # Display names, not emails or user ids.
    created_by: Optional[str] = None
    last_updated_by: Optional[str] = None
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


class CustomQuerySettings(DIModel):
    query: Optional[str] = None
    # Output column mapping; when set, target columns may not match the
    # query's output names.
    fields: Optional[List[object]] = None


class SourceSettings(DIModel):
    name: str
    connection_id: Optional[str] = None
    run_type: Optional[str] = None
    # Kept raw and validated where used, so a malformed value only drops
    # source lineage, not the river's target lineage.
    custom_query_source_settings: Optional[object] = None


class TargetSettings(DIModel):
    name: str
    connection_id: Optional[str] = None
    table_name: Optional[str] = None
    target_prefix: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    dataset_id: Optional[str] = None
    catalog_name: Optional[str] = None


class ModifiedColumn(DIModel):
    name: Optional[str] = None
    alias: Optional[str] = None
    is_selected: bool = True
    expression: Optional[str] = None
    is_rivery_metadata: Optional[bool] = None


class TableDetails(DIModel):
    # Database sources name the table `name`; predefined reports use `table_name`.
    name: Optional[str] = None
    table_name: Optional[str] = None
    is_selected: bool = False
    target_table: Optional[str] = None
    # Only columns renamed, retyped, unselected or added are listed; other
    # columns load under their source name.
    # Kept raw so that a malformed column only drops that column.
    modified_columns: Optional[List[object]] = None

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
    fields: Optional[List[object]] = None


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
    sql_query: Optional[str] = None
    # The spec puts script SQL in sql_query too; sql_script is read as a
    # fallback in case a response uses that name.
    sql_script: Optional[str] = None
    target_settings: Optional[StepTargetSettings] = None

    @property
    def sql(self) -> Optional[str]:
        return self.sql_query or self.sql_script


class ConditionBranch(DIModel):
    condition_name: Optional[str] = None
    is_else: bool = False
    step: Optional[object] = None


class Activity(DIModel):
    cross_id: str
    river_id: Optional[str] = None
    master_river_id: Optional[str] = None
    is_sub_river: bool = False
    pending: int = 0
    failed: int = 0
    running: int = 0
    succeeded: int = 0
    canceled: int = 0

    @property
    def emitted_river_id(self) -> str:
        # A sub river is ingested as part of its master river.
        if self.is_sub_river and self.master_river_id:
            return self.master_river_id
        return self.river_id or self.cross_id

    @property
    def run_count(self) -> int:
        return (
            self.pending + self.failed + self.running + self.succeeded + self.canceled
        )


class TriggerEvent(DIModel):
    trigger_name: Optional[str] = None
    run_id: Optional[str] = None


class RiverRun(DIModel):
    run_id: str
    river_cross_id: str
    status: str
    datasource_id: Optional[str] = None
    start_date_in_milliseconds: int
    end_date_in_milliseconds: Optional[int] = None
    run_group_id: Optional[str] = None
    error_description: Optional[str] = None
    units: Optional[float] = None
    rpu: Optional[float] = None
    target_name: Optional[str] = None
    sub_river_id: Optional[str] = None
    is_sub_river_run: bool = False
    trigger_name: Optional[str] = None
    triggered_by_run_id: Optional[str] = None
    trigger_chain: Optional[List[TriggerEvent]] = None

    @property
    def group_id(self) -> str:
        return self.run_group_id or self.run_id


class DIUser(DIModel):
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    user_name: Optional[str] = None
