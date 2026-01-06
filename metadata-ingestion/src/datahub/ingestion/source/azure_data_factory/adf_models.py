"""Pydantic models for Azure Data Factory API responses.

These models provide type safety and validation for ADF REST API responses.
Field names match the Azure API response structure (camelCase).

API Documentation: https://learn.microsoft.com/en-us/rest/api/datafactory/
"""

from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import TypedDict

# Type aliases for common JSON value types in ADF API responses
# Azure API parameters and variables can contain primitive types
JsonPrimitive = Union[str, int, float, bool, None]


# TypedDict for well-known structures in ADF API responses.
# These provide type hints for commonly-used nested dictionaries from Azure SDK.
# Using total=False makes all fields optional, matching Azure's inconsistent responses.
class FolderInfo(TypedDict, total=False):
    """Folder organization structure used by pipelines, datasets, etc."""

    name: str


class InvokedByInfo(TypedDict, total=False):
    """Information about what triggered a pipeline run."""

    name: str
    id: str
    invokedByType: str


class UserProperty(TypedDict, total=False):
    """User-defined property on an activity."""

    name: str
    value: str


class IntegrationRuntimeReference(TypedDict, total=False):
    """Reference to an integration runtime."""

    referenceName: str
    type: str


class ActivityPolicy(TypedDict, total=False):
    """Execution policy for an activity."""

    timeout: str
    retry: int
    retryIntervalInSeconds: int
    secureInput: bool
    secureOutput: bool


class SchemaColumn(TypedDict, total=False):
    """Column definition in a dataset schema."""

    name: str
    type: str
    physicalType: str
    precision: int
    scale: int


class AdfResource(BaseModel):
    """Base model for Azure Data Factory resources."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    id: str = Field(description="Azure resource ID")
    name: str = Field(description="Resource name")
    type: str = Field(description="Azure resource type")
    etag: Optional[str] = Field(default=None, description="Resource ETag")


class FactoryProperties(BaseModel):
    """Properties of a Data Factory."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    provisioning_state: Optional[str] = Field(
        default=None, alias="provisioningState", description="Provisioning state"
    )
    create_time: Optional[datetime] = Field(
        default=None, alias="createTime", description="Factory creation time"
    )
    version: Optional[str] = Field(default=None, description="Factory version")


class Factory(AdfResource):
    """Azure Data Factory resource.

    API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/factories/get
    """

    location: str = Field(description="Azure region")
    tags: dict[str, str] = Field(default_factory=dict, description="Resource tags")
    properties: Optional[FactoryProperties] = Field(
        default=None, description="Factory properties"
    )


class ActivityDependency(BaseModel):
    """Dependency between activities in a pipeline."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    activity: str = Field(description="Name of the dependent activity")
    dependency_conditions: list[str] = Field(
        default_factory=list,
        alias="dependencyConditions",
        description="Conditions for dependency (Succeeded, Failed, Skipped, Completed)",
    )


class DatasetReference(BaseModel):
    """Reference to an ADF dataset."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    reference_name: str = Field(alias="referenceName", description="Dataset name")
    type: str = Field(default="DatasetReference", description="Reference type")
    parameters: dict[str, JsonPrimitive] = Field(
        default_factory=dict, description="Dataset parameters"
    )


class LinkedServiceReference(BaseModel):
    """Reference to a linked service."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    reference_name: str = Field(
        alias="referenceName", description="Linked service name"
    )
    type: str = Field(default="LinkedServiceReference", description="Reference type")
    parameters: dict[str, JsonPrimitive] = Field(
        default_factory=dict, description="Linked service parameters"
    )


class ActivityInput(BaseModel):
    """Input configuration for an activity."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    # For Copy activities - source config varies by source type (SQL, Blob, etc.)
    source: Optional[dict[str, Any]] = Field(
        default=None, description="Source configuration"
    )

    # Dataset reference (common)
    dataset: Optional[DatasetReference] = Field(
        default=None, description="Input dataset reference"
    )


class ActivityOutput(BaseModel):
    """Output configuration for an activity."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    # For Copy activities - sink config varies by sink type
    sink: Optional[dict[str, Any]] = Field(
        default=None, description="Sink configuration"
    )

    # Dataset reference (common)
    dataset: Optional[DatasetReference] = Field(
        default=None, description="Output dataset reference"
    )


class Activity(BaseModel):
    """Activity within an ADF pipeline.

    API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/pipelines/get
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    name: str = Field(description="Activity name")
    type: str = Field(
        description="Activity type (e.g., Copy, DataFlow, ExecutePipeline)"
    )
    description: Optional[str] = Field(default=None, description="Activity description")

    # Dependencies
    depends_on: list[ActivityDependency] = Field(
        default_factory=list, alias="dependsOn", description="Activity dependencies"
    )

    # Type-specific properties vary by activity type (Copy, DataFlow, ExecutePipeline, etc.)
    # Contains nested structures like {"pipeline": {"referenceName": "...", "type": "..."}}
    # Uses Any due to deeply nested and varying structures from Azure API
    type_properties: Optional[dict[str, Any]] = Field(
        default=None, alias="typeProperties", description="Type-specific properties"
    )

    # Inputs/Outputs (for Copy and other data activities)
    inputs: list[DatasetReference] = Field(
        default_factory=list, description="Input dataset references"
    )
    outputs: list[DatasetReference] = Field(
        default_factory=list, description="Output dataset references"
    )

    # Linked service (for some activities)
    linked_service_name: Optional[LinkedServiceReference] = Field(
        default=None,
        alias="linkedServiceName",
        description="Linked service for activity",
    )

    # Policy
    policy: Optional[ActivityPolicy] = Field(
        default=None, description="Activity execution policy"
    )

    # User properties
    user_properties: list[UserProperty] = Field(
        default_factory=list,
        alias="userProperties",
        description="User-defined properties",
    )


class PipelineProperties(BaseModel):
    """Properties of an ADF pipeline."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    description: Optional[str] = Field(default=None, description="Pipeline description")
    activities: list[Activity] = Field(
        default_factory=list, description="Pipeline activities"
    )
    # Parameters have complex structure: {"name": {"type": "String", "defaultValue": ...}}
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Pipeline parameters"
    )
    # Variables have complex structure similar to parameters
    variables: dict[str, Any] = Field(
        default_factory=dict, description="Pipeline variables"
    )
    concurrency: Optional[int] = Field(default=None, description="Max concurrent runs")
    annotations: list[str] = Field(
        default_factory=list, description="Pipeline annotations"
    )
    folder: Optional[FolderInfo] = Field(
        default=None, description="Folder path for organization"
    )


class Pipeline(AdfResource):
    """Azure Data Factory pipeline.

    API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/pipelines/get

    Note: The Azure SDK may return pipeline data with properties at the root level
    or nested under 'properties'. This model handles both cases.
    """

    # Properties can be nested or at root level depending on Azure SDK version
    properties: Optional[PipelineProperties] = Field(
        default=None, description="Pipeline properties"
    )

    # Root-level fields (used when properties are flattened)
    description: Optional[str] = Field(default=None, description="Pipeline description")
    activities: list[Activity] = Field(
        default_factory=list, description="Pipeline activities"
    )
    # Parameters have complex structure: {"name": {"type": "String", "defaultValue": ...}}
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Pipeline parameters"
    )
    # Variables have complex structure similar to parameters
    variables: dict[str, Any] = Field(
        default_factory=dict, description="Pipeline variables"
    )
    concurrency: Optional[int] = Field(default=None, description="Max concurrent runs")
    annotations: list[str] = Field(
        default_factory=list, description="Pipeline annotations"
    )
    folder: Optional[FolderInfo] = Field(
        default=None, description="Folder path for organization"
    )

    @model_validator(mode="after")
    def normalize_properties(self) -> "Pipeline":
        """Ensure properties are accessible whether nested or flat."""
        if self.properties is None:
            # Properties are at root level, create a PipelineProperties object
            self.properties = PipelineProperties(
                description=self.description,
                activities=self.activities,
                parameters=self.parameters,
                variables=self.variables,
                concurrency=self.concurrency,
                annotations=self.annotations,
                folder=self.folder,
            )
        return self


class DatasetProperties(BaseModel):
    """Properties of an ADF dataset."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    description: Optional[str] = Field(default=None, description="Dataset description")
    linked_service_name: LinkedServiceReference = Field(
        alias="linkedServiceName", description="Associated linked service"
    )
    # Parameters can have complex structure: {"name": {"type": "String"}}
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Dataset parameters"
    )
    annotations: list[str] = Field(
        default_factory=list, description="Dataset annotations"
    )
    folder: Optional[FolderInfo] = Field(
        default=None, description="Folder path for organization"
    )
    type: str = Field(
        description="Dataset type (e.g., AzureBlobDataset, DelimitedTextDataset)"
    )

    # Type-specific properties vary by dataset type (AzureBlobDataset, SqlTable, etc.)
    # Contains nested structures for connection details, file paths, etc.
    # Uses Any due to deeply nested and varying structures from Azure API
    type_properties: Optional[dict[str, Any]] = Field(
        default=None, alias="typeProperties", description="Type-specific properties"
    )

    # Schema (optional) - named schema_definition to avoid conflict with Pydantic's schema method
    schema_definition: Optional[list[SchemaColumn]] = Field(
        default=None, alias="schema", description="Dataset schema definition"
    )

    # Structure (legacy schema format)
    structure: Optional[list[SchemaColumn]] = Field(
        default=None, description="Dataset structure (legacy)"
    )


class Dataset(AdfResource):
    """Azure Data Factory dataset.

    API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/datasets/get
    """

    properties: DatasetProperties = Field(description="Dataset properties")


class LinkedServiceProperties(BaseModel):
    """Properties of a linked service."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    description: Optional[str] = Field(
        default=None, description="Linked service description"
    )
    type: str = Field(
        description="Linked service type (e.g., AzureBlobStorage, AzureSqlDatabase)"
    )
    # Type-specific properties vary by linked service type (SQL, Blob, etc.)
    # Uses Any due to deeply nested and varying structures from Azure API
    type_properties: Optional[dict[str, Any]] = Field(
        default=None, alias="typeProperties", description="Type-specific properties"
    )
    annotations: list[str] = Field(
        default_factory=list, description="Linked service annotations"
    )
    connect_via: Optional[IntegrationRuntimeReference] = Field(
        default=None, alias="connectVia", description="Integration runtime reference"
    )


class LinkedService(AdfResource):
    """Azure Data Factory linked service (connection).

    API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/linked-services/get
    """

    properties: LinkedServiceProperties = Field(description="Linked service properties")


class DataFlowSource(BaseModel):
    """Source definition in a data flow."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    name: str = Field(description="Source name")
    dataset: Optional[DatasetReference] = Field(
        default=None, description="Source dataset"
    )
    linked_service: Optional[LinkedServiceReference] = Field(
        default=None, alias="linkedService", description="Inline linked service"
    )
    schema_linked_service: Optional[LinkedServiceReference] = Field(
        default=None, alias="schemaLinkedService", description="Schema linked service"
    )


class DataFlowSink(BaseModel):
    """Sink definition in a data flow."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    name: str = Field(description="Sink name")
    dataset: Optional[DatasetReference] = Field(
        default=None, description="Sink dataset"
    )
    linked_service: Optional[LinkedServiceReference] = Field(
        default=None, alias="linkedService", description="Inline linked service"
    )
    schema_linked_service: Optional[LinkedServiceReference] = Field(
        default=None, alias="schemaLinkedService", description="Schema linked service"
    )


class DataFlowTransformation(TypedDict, total=False):
    """Transformation step in a data flow."""

    name: str
    description: str


class DataFlowProperties(BaseModel):
    """Properties of a mapping data flow."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    description: Optional[str] = Field(
        default=None, description="Data flow description"
    )
    type: str = Field(default="MappingDataFlow", description="Data flow type")
    # Type-specific properties contain sources, sinks, transformations, scripts
    # Uses Any due to deeply nested and varying structures from Azure API
    type_properties: Optional[dict[str, Any]] = Field(
        default=None, alias="typeProperties", description="Type-specific properties"
    )
    annotations: list[str] = Field(
        default_factory=list, description="Data flow annotations"
    )
    folder: Optional[FolderInfo] = Field(
        default=None, description="Folder path for organization"
    )

    # Sources and sinks for lineage extraction
    sources: list[DataFlowSource] = Field(
        default_factory=list, description="Data flow sources"
    )
    sinks: list[DataFlowSink] = Field(
        default_factory=list, description="Data flow sinks"
    )

    # Transformations and script
    transformations: list[DataFlowTransformation] = Field(
        default_factory=list, description="Data flow transformations"
    )
    script_lines: list[str] = Field(
        default_factory=list,
        alias="scriptLines",
        description="Data flow script lines (DSL)",
    )

    def get_script(self) -> Optional[str]:
        """Get the complete Data Flow script as a single string."""
        if self.script_lines:
            return "\n".join(self.script_lines)
        return None


class DataFlow(AdfResource):
    """Azure Data Factory mapping data flow.

    API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/data-flows/get
    """

    properties: DataFlowProperties = Field(description="Data flow properties")


class TriggerPipelineReference(TypedDict, total=False):
    """Reference to a pipeline from a trigger."""

    pipelineReference: dict[str, str]
    parameters: dict[str, str]


class TriggerProperties(BaseModel):
    """Properties of a trigger."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    description: Optional[str] = Field(default=None, description="Trigger description")
    type: str = Field(
        description="Trigger type (e.g., ScheduleTrigger, BlobEventsTrigger)"
    )
    runtime_state: Optional[str] = Field(
        default=None,
        alias="runtimeState",
        description="Trigger state (Started, Stopped)",
    )
    # Type-specific properties vary by trigger type (Schedule, BlobEvents, etc.)
    # Uses Any due to deeply nested and varying structures from Azure API
    type_properties: Optional[dict[str, Any]] = Field(
        default=None, alias="typeProperties", description="Type-specific properties"
    )
    annotations: list[str] = Field(
        default_factory=list, description="Trigger annotations"
    )
    pipelines: list[TriggerPipelineReference] = Field(
        default_factory=list, description="Pipelines triggered"
    )


class Trigger(AdfResource):
    """Azure Data Factory trigger.

    API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/triggers/get
    """

    properties: TriggerProperties = Field(description="Trigger properties")


class PipelineRun(BaseModel):
    """Pipeline run execution record.

    API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/pipeline-runs/get
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    run_id: str = Field(alias="runId", description="Unique run identifier")
    pipeline_name: str = Field(alias="pipelineName", description="Pipeline name")
    status: str = Field(description="Run status (Succeeded, Failed, InProgress, etc.)")
    run_start: Optional[datetime] = Field(
        default=None, alias="runStart", description="Run start time"
    )
    run_end: Optional[datetime] = Field(
        default=None, alias="runEnd", description="Run end time"
    )
    duration_in_ms: Optional[int] = Field(
        default=None, alias="durationInMs", description="Duration in milliseconds"
    )
    message: Optional[str] = Field(default=None, description="Run message or error")
    parameters: dict[str, str] = Field(
        default_factory=dict, description="Run parameters"
    )
    invoked_by: Optional[InvokedByInfo] = Field(
        default=None,
        alias="invokedBy",
        description="Trigger or user that invoked the run",
    )
    last_updated: Optional[datetime] = Field(
        default=None, alias="lastUpdated", description="Last update time"
    )
    run_group_id: Optional[str] = Field(
        default=None, alias="runGroupId", description="Run group identifier"
    )
    is_latest: Optional[bool] = Field(
        default=None, alias="isLatest", description="Is this the latest run"
    )


class ActivityRun(BaseModel):
    """Activity run execution record.

    API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/activity-runs/query-by-pipeline-run
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    activity_run_id: str = Field(
        alias="activityRunId", description="Unique run identifier"
    )
    activity_name: str = Field(alias="activityName", description="Activity name")
    activity_type: str = Field(alias="activityType", description="Activity type")
    pipeline_run_id: str = Field(
        alias="pipelineRunId", description="Parent pipeline run ID"
    )
    pipeline_name: str = Field(alias="pipelineName", description="Parent pipeline name")
    status: str = Field(description="Run status")
    activity_run_start: Optional[datetime] = Field(
        default=None, alias="activityRunStart", description="Activity start time"
    )
    activity_run_end: Optional[datetime] = Field(
        default=None, alias="activityRunEnd", description="Activity end time"
    )
    duration_in_ms: Optional[int] = Field(
        default=None, alias="durationInMs", description="Duration in milliseconds"
    )
    # Input/output/error contain runtime data that varies by activity type
    # These can contain deeply nested structures from Azure API
    input: Optional[dict[str, Any]] = Field(default=None, description="Activity input")
    output: Optional[dict[str, Any]] = Field(
        default=None, description="Activity output"
    )
    error: Optional[dict[str, Any]] = Field(
        default=None, description="Error details if failed"
    )


class ListResponse(BaseModel):
    """Generic list response with pagination.

    API Reference: https://learn.microsoft.com/en-us/rest/api/datafactory/factories/list
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    # Resources contain nested structures that vary by type
    value: list[dict[str, Any]] = Field(description="List of resources")
    next_link: Optional[str] = Field(
        default=None, alias="nextLink", description="URL for next page of results"
    )
