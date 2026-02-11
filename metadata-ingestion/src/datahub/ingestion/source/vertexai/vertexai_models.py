import dataclasses
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from google.cloud.aiplatform import Endpoint, Model
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.models import VersionInfo
from google.cloud.aiplatform_v1.types import PipelineTaskDetail
from google.protobuf import timestamp_pb2
from pydantic import BaseModel, Field

from datahub.emitter.mcp_builder import ProjectIdKey
from datahub.ingestion.source.vertexai.vertexai_constants import MLMetadataDefaults
from datahub.metadata.schema_classes import MLHyperParamClass, MLMetricClass
from datahub.metadata.urns import DataFlowUrn, DataJobUrn


class VertexAIResourceCategoryKey(ProjectIdKey):
    """Container key for Vertex AI resource categories (Models, Training Jobs, etc.)."""

    category: str


@dataclasses.dataclass
class ExecutionMetadata:
    execution_name: str
    hyperparams: List[MLHyperParamClass] = dataclasses.field(default_factory=list)
    metrics: List[MLMetricClass] = dataclasses.field(default_factory=list)
    input_artifact_urns: List[str] = dataclasses.field(default_factory=list)
    output_artifact_urns: List[str] = dataclasses.field(default_factory=list)
    custom_properties: Dict[str, str] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class LineageMetadata:
    input_urns: List[str] = dataclasses.field(default_factory=list)
    output_urns: List[str] = dataclasses.field(default_factory=list)
    hyperparams: List[MLHyperParamClass] = dataclasses.field(default_factory=list)
    metrics: List[MLMetricClass] = dataclasses.field(default_factory=list)

    def merge(self, other: "LineageMetadata") -> None:
        self.input_urns.extend(other.input_urns)
        self.output_urns.extend(other.output_urns)
        self.hyperparams.extend(other.hyperparams)
        self.metrics.extend(other.metrics)

    def deduplicate(self) -> None:
        self.input_urns = list(set(self.input_urns))
        self.output_urns = list(set(self.output_urns))
        # Deduplicate by name (keep last)
        hyperparam_dict = {hp.name: hp for hp in self.hyperparams}
        self.hyperparams = list(hyperparam_dict.values())
        metric_dict = {m.name: m for m in self.metrics}
        self.metrics = list(metric_dict.values())


@dataclasses.dataclass
class MLMetadataConfig:
    project_id: str
    region: str
    metadata_store: str = "default"
    enable_lineage_extraction: bool = True
    enable_metrics_extraction: bool = True
    max_executions_per_job: Optional[int] = None

    def get_parent_path(self) -> str:
        return MLMetadataDefaults.METADATA_STORE_PATH_TEMPLATE.format(
            project_id=self.project_id,
            region=self.region,
            metadata_store=self.metadata_store,
        )


@dataclasses.dataclass
class ArtifactInfo:
    name: str
    uri: Optional[str]
    schema_title: Optional[str]
    display_name: Optional[str]

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Artifact name cannot be empty")


@dataclasses.dataclass
class ExecutionInfo:
    name: str
    display_name: Optional[str]
    schema_title: Optional[str]
    metadata: Optional[Dict] = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Execution name cannot be empty")


@dataclasses.dataclass
class TrainingJobMetadata:
    """Metadata for a Vertex AI training job."""

    job: VertexAiResourceNoun
    input_dataset: Optional[VertexAiResourceNoun] = None
    output_model: Optional[Model] = None
    output_model_version: Optional[VersionInfo] = None
    external_input_urns: Optional[List[str]] = None
    external_output_urns: Optional[List[str]] = None


@dataclasses.dataclass
class ModelMetadata:
    """Metadata for a Vertex AI model."""

    model: Model
    model_version: VersionInfo
    training_job_urn: Optional[str] = None
    endpoints: Optional[List[Endpoint]] = None


@dataclasses.dataclass
class PipelineTaskMetadata:
    """Metadata for a Vertex AI pipeline task."""

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


@dataclasses.dataclass
class PipelineMetadata:
    """Metadata for a Vertex AI pipeline."""

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
        return {k: v for k, v in self.dict().items() if v}


class PipelineTaskProperties(BaseModel):
    """Properties for pipeline task custom properties."""

    created_time: str = ""

    def to_custom_properties(self) -> Dict[str, str]:
        """Convert to custom properties dict."""
        return {k: v for k, v in self.dict().items() if v}


class ModelEvaluationCustomProperties(BaseModel):
    """Custom properties for model evaluation."""

    evaluation_id: str = Field(..., alias="evaluationId")
    model_name: str = Field(..., alias="modelName")
    model_resource_name: str = Field(..., alias="modelResourceName")

    class Config:
        populate_by_name = True

    def to_custom_properties(self) -> Dict[str, str]:
        """Convert to custom properties dict using camelCase keys."""
        return {
            "evaluationId": self.evaluation_id,
            "modelName": self.model_name,
            "modelResourceName": self.model_resource_name,
        }


class TrainingJobCustomProperties(BaseModel):
    """Custom properties for training jobs."""

    job_type: str = Field(..., alias="jobType")

    class Config:
        populate_by_name = True

    def to_custom_properties(self) -> Dict[str, str]:
        """Convert to custom properties dict."""
        return {"jobType": self.job_type}


class DatasetCustomProperties(BaseModel):
    """Custom properties for datasets."""

    resource_name: str = Field(..., alias="resourceName")

    class Config:
        populate_by_name = True

    def to_custom_properties(self) -> Dict[str, str]:
        """Convert to custom properties dict."""
        return {"resourceName": self.resource_name}


class EndpointDeploymentCustomProperties(BaseModel):
    """Custom properties for endpoint deployments."""

    display_name: str = Field(..., alias="displayName")

    class Config:
        populate_by_name = True

    def to_custom_properties(self) -> Dict[str, str]:
        """Convert to custom properties dict."""
        return {"displayName": self.display_name}


class MLModelCustomProperties(BaseModel):
    """Custom properties for ML models."""

    version_id: str = Field(..., alias="versionId")
    resource_name: str = Field(..., alias="resourceName")

    class Config:
        populate_by_name = True

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


class InputDataConfig(BaseModel):
    """Input data configuration for AutoML training jobs."""

    datasetId: Optional[str] = Field(None, alias="datasetId")

    class Config:
        populate_by_name = True


class ModelToUpload(BaseModel):
    """Model upload configuration for AutoML training jobs."""

    name: Optional[str] = None
    versionId: Optional[str] = None

    class Config:
        populate_by_name = True


class AutoMLJobConfig(BaseModel):
    """Configuration for AutoML training jobs parsed from job.to_dict()."""

    inputDataConfig: Optional[InputDataConfig] = None
    modelToUpload: Optional[ModelToUpload] = None

    class Config:
        populate_by_name = True
        extra = "ignore"
