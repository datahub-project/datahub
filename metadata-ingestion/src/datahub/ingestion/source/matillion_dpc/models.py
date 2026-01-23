from datetime import datetime
from typing import List, Optional, Union

from pydantic import BaseModel, Field, field_validator

from datahub.ingestion.source.matillion_dpc.constants import TWO_TIER_PLATFORMS
from datahub.metadata.urns import DatasetUrn


class MatillionPlatformInstanceInfo(BaseModel):
    platform_instance: Optional[str] = None
    env: str = "PROD"
    database: Optional[str] = None
    default_schema: Optional[str] = None
    convert_urns_to_lowercase: bool = False


class MatillionProject(BaseModel):
    id: str
    name: str
    description: Optional[str] = None

    class Config:
        populate_by_name = True


class MatillionEnvironment(BaseModel):
    name: str
    default_agent_id: Optional[str] = Field(default=None, alias="defaultAgentId")
    default_agent_name: Optional[str] = Field(default=None, alias="defaultAgentName")

    class Config:
        populate_by_name = True


class MatillionPipeline(BaseModel):
    name: str
    published_time: Optional[datetime] = Field(default=None, alias="publishedTime")

    class Config:
        populate_by_name = True


class MatillionPipelineExecution(BaseModel):
    pipeline_execution_id: str = Field(alias="pipelineExecutionId")
    pipeline_name: str = Field(alias="pipelineName")
    status: str
    started_at: Optional[datetime] = Field(default=None, alias="startedAt")
    finished_at: Optional[datetime] = Field(default=None, alias="finishedAt")
    environment_name: Optional[str] = Field(default=None, alias="environmentName")
    project_id: Optional[str] = Field(default=None, alias="projectId")
    pipeline_type: Optional[str] = Field(default=None, alias="pipelineType")
    trigger: Optional[str] = None
    schedule_id: Optional[str] = Field(default=None, alias="scheduleId")
    message: Optional[str] = None

    class Config:
        populate_by_name = True


class MatillionSchedule(BaseModel):
    schedule_id: Optional[str] = Field(default=None, alias="scheduleId")
    name: str
    pipeline_name: str = Field(alias="pipelineName")
    cron_expression: Optional[str] = Field(default=None, alias="cronExpression")
    schedule_enabled: Optional[bool] = Field(default=None, alias="scheduleEnabled")
    environment_name: Optional[str] = Field(default=None, alias="environmentName")

    class Config:
        populate_by_name = True


class MatillionStreamingPipeline(BaseModel):
    streaming_pipeline_id: Optional[str] = Field(
        default=None, alias="streamingPipelineId"
    )
    name: str
    project_id: str = Field(alias="projectId")
    agent_id: Optional[str] = Field(default=None, alias="agentId")

    class Config:
        populate_by_name = True


class PaginationParams(BaseModel):
    page: int = Field(default=0, ge=0)
    size: int = Field(default=25, ge=1, le=100)

    @field_validator("size")
    @classmethod
    def validate_size(cls, v: int) -> int:
        if v < 1 or v > 100:
            raise ValueError("size must be between 1 and 100")
        return v


class TokenPaginationParams(BaseModel):
    limit: int = Field(default=25, ge=1, le=100)
    pagination_token: Optional[str] = Field(default=None)

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, v: int) -> int:
        if v < 1 or v > 100:
            raise ValueError("limit must be between 1 and 100")
        return v


class MatillionDatasetInfo(BaseModel):
    platform: str
    name: str
    namespace: str
    platform_instance: Optional[str] = None
    env: str = Field(default="PROD")

    @staticmethod
    def normalize_name(
        name: str,
        platform: str,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> str:
        parts = name.split(".")
        is_two_tier = platform in TWO_TIER_PLATFORMS

        # If we have 3+ parts, assume it's fully qualified
        if len(parts) >= 3:
            return name

        # If we have 2 parts (e.g., "schema.table" or "database.table")
        if len(parts) == 2:
            if is_two_tier:
                # 2-tier platforms: schema.table is complete, don't add database
                return name
            else:
                # 3-tier platforms: if we have a database, prepend it
                if database:
                    return f"{database}.{name}"
                return name

        # Single part name - prepend based on platform type and config
        if is_two_tier:
            # 2-tier: only prepend schema (or database, which acts as schema)
            return f"{schema or database}.{name}" if (schema or database) else name
        else:
            # 3-tier: build database.schema.table
            if database and schema:
                return f"{database}.{schema}.{name}"
            elif schema:
                return f"{schema}.{name}"
            elif database:
                return f"{database}.{name}"

        return name


class MatillionColumnLineageInfo(BaseModel):
    downstream_field: str
    upstream_datasets: List[MatillionDatasetInfo]
    upstream_fields: List[str]


class PipelineLineageResult(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    input_urns: List[Union[str, DatasetUrn]] = Field(
        default_factory=list,
    )
    output_urns: List[Union[str, DatasetUrn]] = Field(
        default_factory=list,
    )
    sql_queries: List[str] = Field(
        default_factory=list,
    )
