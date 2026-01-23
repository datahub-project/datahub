from datetime import datetime
from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from datahub.ingestion.source.matillion_dpc.constants import (
    API_MAX_PAGE_SIZE,
    TWO_TIER_PLATFORMS,
)
from datahub.metadata.urns import DatasetUrn


class MatillionPlatformInstanceInfo(BaseModel):
    platform_instance: Optional[str] = None
    env: str = "PROD"
    database: Optional[str] = None
    default_schema: Optional[str] = None
    convert_urns_to_lowercase: bool = False


class MatillionProject(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    name: str
    description: Optional[str] = None


class MatillionEnvironment(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    default_agent_id: Optional[str] = Field(default=None, alias="defaultAgentId")
    default_agent_name: Optional[str] = Field(default=None, alias="defaultAgentName")


class MatillionPipeline(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    published_time: Optional[datetime] = Field(default=None, alias="publishedTime")


class MatillionPipelineExecution(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

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


class MatillionSchedule(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schedule_id: Optional[str] = Field(default=None, alias="scheduleId")
    name: str
    pipeline_name: str = Field(alias="pipelineName")
    cron_expression: Optional[str] = Field(default=None, alias="cronExpression")
    schedule_enabled: Optional[bool] = Field(default=None, alias="scheduleEnabled")
    environment_name: Optional[str] = Field(default=None, alias="environmentName")


class MatillionStreamingPipeline(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    streaming_pipeline_id: Optional[str] = Field(
        default=None, alias="streamingPipelineId"
    )
    name: str
    project_id: str = Field(alias="projectId")
    agent_id: Optional[str] = Field(default=None, alias="agentId")


class PaginationParams(BaseModel):
    page: int = Field(default=0, ge=0)
    size: int = Field(default=25, ge=1, le=API_MAX_PAGE_SIZE)


class TokenPaginationParams(BaseModel):
    limit: int = Field(default=25, ge=1, le=API_MAX_PAGE_SIZE)
    pagination_token: Optional[str] = Field(default=None)


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

        if len(parts) >= 3:
            return name

        if len(parts) == 2:
            if is_two_tier:
                return name
            else:
                if database:
                    return f"{database}.{name}"
                return name

        if is_two_tier:
            return f"{schema or database}.{name}" if (schema or database) else name
        else:
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


class ArtifactDetails(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    branch: Optional[str] = None
    commit_hash: Optional[str] = Field(default=None, alias="commitHash")
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    deployed_at: Optional[datetime] = Field(default=None, alias="deployedAt")
    enabled: Optional[bool] = None
    provider: Optional[str] = None
    version_name: Optional[str] = Field(default=None, alias="versionName")


class ArtifactDetailsWithAssets(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    assets: Optional[List[str]] = None
    details: Optional[ArtifactDetails] = None
