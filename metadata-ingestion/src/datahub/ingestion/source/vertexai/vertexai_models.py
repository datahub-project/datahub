from datetime import datetime, timedelta
from typing import Annotated, Dict, List, Optional

from google.cloud.aiplatform import Endpoint, Model
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.models import VersionInfo
from google.cloud.aiplatform_v1.types import PipelineTaskDetail
from google.protobuf import timestamp_pb2
from pydantic import BaseModel, ConfigDict, Field, SkipValidation, model_validator

from datahub.emitter.mcp_builder import ProjectIdKey
from datahub.ingestion.source.vertexai.vertexai_constants import MLMetadataDefaults
from datahub.metadata.schema_classes import EdgeClass, MLHyperParamClass, MLMetricClass
from datahub.metadata.urns import DataFlowUrn, DataJobUrn


class VertexAIResourceCategoryKey(ProjectIdKey):
    """Container key for Vertex AI resource categories (Models, Training Jobs, etc.)."""

    category: str


class ModelGroupKey(ProjectIdKey):
    """Container key for a Vertex AI model group."""

    model_group_name: str


class ExecutionMetadata(BaseModel):
    """Metadata for an execution."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    execution_name: str
    hyperparams: List[MLHyperParamClass] = Field(default_factory=list)
    metrics: List[MLMetricClass] = Field(default_factory=list)
    input_artifact_urns: List[str] = Field(
        default_factory=list
    )  # Can be DatasetUrn or MlModelUrn
    output_artifact_urns: List[str] = Field(
        default_factory=list
    )  # Can be DatasetUrn or MlModelUrn
    custom_properties: Dict[str, str] = Field(default_factory=dict)


class LineageMetadata(BaseModel):
    """Metadata for lineage tracking."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    input_urns: List[str] = Field(
        default_factory=list
    )  # Can be DatasetUrn or MlModelUrn
    output_urns: List[str] = Field(
        default_factory=list
    )  # Can be DatasetUrn or MlModelUrn
    hyperparams: List[MLHyperParamClass] = Field(default_factory=list)
    metrics: List[MLMetricClass] = Field(default_factory=list)

    def merge(self, other: "LineageMetadata") -> None:
        self.input_urns.extend(other.input_urns)
        self.output_urns.extend(other.output_urns)
        self.hyperparams.extend(other.hyperparams)
        self.metrics.extend(other.metrics)

    def deduplicate(self) -> None:
        self.input_urns = list(set(self.input_urns))
        self.output_urns = list(set(self.output_urns))
        hyperparam_dict = {hp.name: hp for hp in self.hyperparams}
        self.hyperparams = list(hyperparam_dict.values())
        metric_dict = {m.name: m for m in self.metrics}
        self.metrics = list(metric_dict.values())


class MLMetadataConfig(BaseModel):
    """Configuration for ML Metadata service."""

    project_id: str
    region: str
    metadata_store: str = "default"
    enable_lineage_extraction: bool = True
    enable_metrics_extraction: bool = True
    max_executions_per_job: Optional[int] = None
    max_execution_search_limit: int = MLMetadataDefaults.MAX_EXECUTION_SEARCH_RESULTS

    def get_parent_path(self) -> str:
        return MLMetadataDefaults.METADATA_STORE_PATH_TEMPLATE.format(
            project_id=self.project_id,
            region=self.region,
            metadata_store=self.metadata_store,
        )


class ArtifactInfo(BaseModel):
    """Information about an artifact."""

    name: str
    uri: Optional[str] = None
    schema_title: Optional[str] = None
    display_name: Optional[str] = None

    @model_validator(mode="after")
    def validate_name(self) -> "ArtifactInfo":
        if not self.name:
            raise ValueError("Artifact name cannot be empty")
        return self


class ExecutionInfo(BaseModel):
    """Information about an execution."""

    name: str
    display_name: Optional[str] = None
    schema_title: Optional[str] = None
    metadata: Optional[Dict] = None

    @model_validator(mode="after")
    def validate_name(self) -> "ExecutionInfo":
        if not self.name:
            raise ValueError("Execution name cannot be empty")
        return self


class TrainingJobMetadata(BaseModel):
    """Metadata for a Vertex AI training job.

    Note: Google Cloud SDK types use SkipValidation to allow both real objects
    and test mocks while preserving type hints for IDE support.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    job: Annotated[VertexAiResourceNoun, SkipValidation()]
    input_dataset: Optional[Annotated[VertexAiResourceNoun, SkipValidation()]] = None
    output_model: Optional[Annotated[Model, SkipValidation()]] = None
    output_model_version: Optional[Annotated[VersionInfo, SkipValidation()]] = None
    external_input_urns: Optional[List[str]] = None
    external_output_urns: Optional[List[str]] = None


class ModelMetadata(BaseModel):
    """Metadata for a Vertex AI model.

    Note: Google Cloud SDK types use SkipValidation to allow both real objects
    and test mocks while preserving type hints for IDE support.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    model: Annotated[Model, SkipValidation()]
    model_version: Annotated[VersionInfo, SkipValidation()]
    training_job_urn: Optional[str] = None
    training_data_urns: Optional[List[str]] = None
    endpoints: Optional[List[Annotated[Endpoint, SkipValidation()]]] = None


class PipelineTaskArtifacts(BaseModel):
    """Artifacts (datasets and models) for a pipeline task."""

    input_dataset_urns: Optional[List[str]] = None
    output_dataset_urns: Optional[List[str]] = None
    output_model_urns: Optional[List[str]] = None


class PipelineTaskMetadata(BaseModel):
    """Metadata for a Vertex AI pipeline task."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    urn: DataJobUrn
    id: Optional[int] = None
    type: Optional[str] = None
    state: Optional[PipelineTaskDetail.State] = None
    start_time: Optional[timestamp_pb2.Timestamp] = None
    create_time: Optional[timestamp_pb2.Timestamp] = None
    end_time: Optional[timestamp_pb2.Timestamp] = None
    upstreams: Optional[List[DataJobUrn]] = None
    duration: Optional[int] = None
    input_dataset_urns: Optional[List[str]] = None
    output_dataset_urns: Optional[List[str]] = None
    output_model_urns: Optional[List[str]] = None


class PipelineMetadata(BaseModel):
    """Metadata for a Vertex AI pipeline."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    resource_name: str
    tasks: List[PipelineTaskMetadata]
    urn: DataFlowUrn
    id: Optional[str] = None
    labels: Optional[Dict[str, str]] = None
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    duration: Optional[timedelta] = None
    region: Optional[str] = None


class RunTimestamps(BaseModel):
    """Timestamps for an experiment run."""

    created_time_ms: Optional[int] = Field(
        None, description="Created timestamp in milliseconds"
    )
    duration_ms: Optional[int] = Field(None, description="Duration in milliseconds")


class PipelineProperties(BaseModel):
    """Properties for pipeline custom properties."""

    resource_name: str = ""
    create_time: str = ""
    update_time: str = ""
    duration: str = ""
    location: str = ""
    labels: str = ""

    def to_custom_properties(self) -> Dict[str, str]:
        """Convert to custom properties dict."""
        return {k: v for k, v in self.model_dump().items() if v}


class PipelineTaskProperties(BaseModel):
    """Properties for pipeline task custom properties."""

    created_time: str = ""

    def to_custom_properties(self) -> Dict[str, str]:
        """Convert to custom properties dict."""
        return {k: v for k, v in self.model_dump().items() if v}


class ModelEvaluationCustomProperties(BaseModel):
    """Custom properties for model evaluation."""

    evaluation_id: str = Field(alias="evaluationId")
    model_name: str = Field(alias="modelName")
    model_resource_name: str = Field(alias="modelResourceName")

    model_config = ConfigDict(populate_by_name=True)

    def to_custom_properties(self) -> Dict[str, str]:
        """Convert to custom properties dict using camelCase keys."""
        return {
            "evaluationId": self.evaluation_id,
            "modelName": self.model_name,
            "modelResourceName": self.model_resource_name,
        }


class TrainingJobCustomProperties(BaseModel):
    """Custom properties for training jobs."""

    job_type: str = Field(alias="jobType")

    model_config = ConfigDict(populate_by_name=True)

    def to_custom_properties(self) -> Dict[str, str]:
        """Convert to custom properties dict."""
        return {"jobType": self.job_type}


class DatasetCustomProperties(BaseModel):
    """Custom properties for datasets."""

    resource_name: str = Field(alias="resourceName")

    model_config = ConfigDict(populate_by_name=True)

    def to_custom_properties(self) -> Dict[str, str]:
        """Convert to custom properties dict."""
        return {"resourceName": self.resource_name}


class EndpointDeploymentCustomProperties(BaseModel):
    """Custom properties for endpoint deployments."""

    display_name: str = Field(alias="displayName")

    model_config = ConfigDict(populate_by_name=True)

    def to_custom_properties(self) -> Dict[str, str]:
        """Convert to custom properties dict."""
        return {"displayName": self.display_name}


class MLModelCustomProperties(BaseModel):
    """Custom properties for ML models."""

    version_id: str = Field(alias="versionId")
    resource_name: str = Field(alias="resourceName")

    model_config = ConfigDict(populate_by_name=True)

    def to_custom_properties(self) -> Dict[str, str]:
        """Convert to custom properties dict."""
        return {
            "versionId": self.version_id,
            "resourceName": self.resource_name,
        }


class ArtifactURNs(BaseModel):
    """Input and output artifact URNs."""

    input_urns: List[str] = Field(default_factory=list)
    output_urns: List[str] = Field(default_factory=list)


class TrainingJobURNsAndEdges(BaseModel):
    """URNs and edges for training job inputs and outputs."""

    dataset_urn: Optional[str] = None
    model_urn: Optional[str] = None
    input_edges: List[EdgeClass] = Field(default_factory=list)
    output_edges: List[EdgeClass] = Field(default_factory=list)

    model_config = ConfigDict(arbitrary_types_allowed=True)


class MLMetrics(BaseModel):
    """Hyperparameters and metrics extracted from ML Metadata."""

    hyperparams: List[MLHyperParamClass] = Field(default_factory=list)
    metrics: List[MLMetricClass] = Field(default_factory=list)

    model_config = ConfigDict(arbitrary_types_allowed=True)


class InputDataConfig(BaseModel):
    """Input data configuration for AutoML training jobs."""

    datasetId: Optional[str] = Field(None, alias="datasetId")

    model_config = ConfigDict(populate_by_name=True)


class ModelToUpload(BaseModel):
    """Model upload configuration for AutoML training jobs."""

    name: Optional[str] = None
    versionId: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


class AutoMLJobConfig(BaseModel):
    """Configuration for AutoML training jobs parsed from job.to_dict()."""

    inputDataConfig: Optional[InputDataConfig] = None
    modelToUpload: Optional[ModelToUpload] = None

    model_config = ConfigDict(populate_by_name=True, extra="ignore")
