from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class MatillionProject(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    updated_at: Optional[datetime] = Field(default=None, alias="updatedAt")

    class Config:
        populate_by_name = True


class MatillionEnvironment(BaseModel):
    id: str
    name: str
    project_id: str = Field(alias="projectId")
    description: Optional[str] = None
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")

    class Config:
        populate_by_name = True


class MatillionAgent(BaseModel):
    id: str
    name: str
    agent_type: Optional[str] = Field(default=None, alias="agentType")
    status: Optional[str] = None
    version: Optional[str] = None
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")

    class Config:
        populate_by_name = True


class MatillionConnection(BaseModel):
    id: str
    name: str
    connection_type: Optional[str] = Field(default=None, alias="type")
    project_id: Optional[str] = Field(default=None, alias="projectId")
    environment_id: Optional[str] = Field(default=None, alias="environmentId")
    description: Optional[str] = None
    configuration: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    updated_at: Optional[datetime] = Field(default=None, alias="updatedAt")

    class Config:
        populate_by_name = True


class MatillionPipeline(BaseModel):
    id: str
    name: str
    project_id: str = Field(alias="projectId")
    environment_id: Optional[str] = Field(default=None, alias="environmentId")
    pipeline_type: Optional[str] = Field(default=None, alias="type")
    description: Optional[str] = None
    version: Optional[str] = None
    branch: Optional[str] = None
    repository_id: Optional[str] = Field(default=None, alias="repositoryId")
    configuration: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    updated_at: Optional[datetime] = Field(default=None, alias="updatedAt")
    created_by: Optional[str] = Field(default=None, alias="createdBy")
    updated_by: Optional[str] = Field(default=None, alias="updatedBy")

    class Config:
        populate_by_name = True


class MatillionPipelineExecution(BaseModel):
    id: str
    pipeline_id: str = Field(alias="pipelineId")
    status: str
    started_at: Optional[datetime] = Field(default=None, alias="startedAt")
    completed_at: Optional[datetime] = Field(default=None, alias="completedAt")
    duration_ms: Optional[int] = Field(default=None, alias="durationMs")
    triggered_by: Optional[str] = Field(default=None, alias="triggeredBy")
    trigger_type: Optional[str] = Field(default=None, alias="triggerType")
    agent_id: Optional[str] = Field(default=None, alias="agentId")
    error_message: Optional[str] = Field(default=None, alias="errorMessage")
    rows_processed: Optional[int] = Field(default=None, alias="rowsProcessed")

    class Config:
        populate_by_name = True


class MatillionSchedule(BaseModel):
    id: str
    name: str
    pipeline_id: str = Field(alias="pipelineId")
    cron_expression: Optional[str] = Field(default=None, alias="cronExpression")
    enabled: bool = True
    description: Optional[str] = None
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    updated_at: Optional[datetime] = Field(default=None, alias="updatedAt")

    class Config:
        populate_by_name = True


class MatillionRepository(BaseModel):
    id: str
    name: str
    project_id: str = Field(alias="projectId")
    repository_url: Optional[str] = Field(default=None, alias="url")
    branch: Optional[str] = None
    provider: Optional[str] = None
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")

    class Config:
        populate_by_name = True


class MatillionStreamingPipeline(BaseModel):
    id: str
    name: str
    project_id: str = Field(alias="projectId")
    environment_id: str = Field(alias="environmentId")
    description: Optional[str] = None
    source_type: Optional[str] = Field(default=None, alias="sourceType")
    target_type: Optional[str] = Field(default=None, alias="targetType")
    source_connection_id: Optional[str] = Field(
        default=None, alias="sourceConnectionId"
    )
    target_connection_id: Optional[str] = Field(
        default=None, alias="targetConnectionId"
    )
    status: Optional[str] = None
    agent_id: Optional[str] = Field(default=None, alias="agentId")
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    updated_at: Optional[datetime] = Field(default=None, alias="updatedAt")

    class Config:
        populate_by_name = True


class MatillionLineageNode(BaseModel):
    id: str
    name: str
    node_type: str = Field(alias="type")
    platform: Optional[str] = None
    schema_name: Optional[str] = Field(default=None, alias="schema")
    table_name: Optional[str] = Field(default=None, alias="table")
    column_name: Optional[str] = Field(default=None, alias="column")

    class Config:
        populate_by_name = True


class MatillionLineageEdge(BaseModel):
    source_id: str = Field(alias="sourceId")
    target_id: str = Field(alias="targetId")
    edge_type: Optional[str] = Field(default=None, alias="type")

    class Config:
        populate_by_name = True


class MatillionLineageGraph(BaseModel):
    pipeline_id: str = Field(alias="pipelineId")
    nodes: List[MatillionLineageNode] = Field(default_factory=list)
    edges: List[MatillionLineageEdge] = Field(default_factory=list)

    class Config:
        populate_by_name = True


class MatillionConsumption(BaseModel):
    project_id: str = Field(alias="projectId")
    environment_id: Optional[str] = Field(default=None, alias="environmentId")
    pipeline_id: Optional[str] = Field(default=None, alias="pipelineId")
    credits_consumed: Optional[float] = Field(default=None, alias="creditsConsumed")
    period_start: Optional[datetime] = Field(default=None, alias="periodStart")
    period_end: Optional[datetime] = Field(default=None, alias="periodEnd")

    class Config:
        populate_by_name = True


class MatillionAuditEvent(BaseModel):
    id: str
    event_type: str = Field(alias="eventType")
    resource_type: Optional[str] = Field(default=None, alias="resourceType")
    resource_id: Optional[str] = Field(default=None, alias="resourceId")
    user_id: Optional[str] = Field(default=None, alias="userId")
    timestamp: Optional[datetime] = None
    details: Optional[Dict[str, Any]] = None

    class Config:
        populate_by_name = True


class MatillionConnector(BaseModel):
    id: str
    name: str
    connector_type: str = Field(alias="type")
    category: Optional[str] = None
    description: Optional[str] = None
    is_available: Optional[bool] = Field(default=None, alias="isAvailable")

    class Config:
        populate_by_name = True
