import logging
from datetime import datetime, timedelta
from typing import Annotated, Dict, Iterable, List, Optional, Protocol, Set

from google.cloud.aiplatform import Endpoint, Experiment, ExperimentRun, Model
from google.cloud.aiplatform.base import VertexAiResourceNoun
from google.cloud.aiplatform.models import VersionInfo
from google.cloud.aiplatform_v1.types import PipelineTaskDetail
from google.protobuf import timestamp_pb2
from pydantic import BaseModel, ConfigDict, Field, SkipValidation

from datahub.emitter.mcp_builder import ProjectIdKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.vertexai.vertexai_constants import (
    MLMetadataDefaults,
    ResourceCategory,
    ResourceCategoryType,
)
from datahub.metadata.schema_classes import EdgeClass, MLHyperParamClass, MLMetricClass
from datahub.metadata.urns import DataFlowUrn, DataJobUrn

logger = logging.getLogger(__name__)


class YieldCommonAspectsProtocol(Protocol):
    """Protocol for the yield_common_aspects function signature."""

    def __call__(
        self,
        entity_urn: str,
        subtype: str,
        include_container: bool = True,
        include_platform: bool = True,
        resource_category: Optional[ResourceCategoryType] = None,
        include_subtypes: bool = True,
    ) -> Iterable[MetadataWorkUnit]:
        pass


class VertexAIResourceCategoryKey(ProjectIdKey):
    """Container key for Vertex AI resource categories (Models, Training Jobs, etc.)."""

    category: ResourceCategoryType


class ModelGroupKey(ProjectIdKey):
    """Container key for a Vertex AI model group."""

    model_group_name: str


class PipelineContainerKey(ProjectIdKey):
    """Container key for a Vertex AI pipeline."""

    pipeline_name: str

    def parent_key(self) -> Optional[VertexAIResourceCategoryKey]:
        return VertexAIResourceCategoryKey(
            project_id=self.project_id,
            platform=self.platform,
            instance=self.instance,
            env=self.env,
            category=ResourceCategory.PIPELINES,
        )


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
    lineage: Optional[LineageMetadata] = None


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


class ExperimentMetadata(BaseModel):
    """Metadata for a Vertex AI experiment.

    Note: Google Cloud SDK types use SkipValidation to allow both real objects
    and test mocks while preserving type hints for IDE support.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    experiment: Annotated[Experiment, SkipValidation()]
    name: str
    update_time: Optional[datetime] = None


class ExperimentRunMetadata(BaseModel):
    """Metadata for a Vertex AI experiment run.

    Note: Google Cloud SDK types use SkipValidation to allow both real objects
    and test mocks while preserving type hints for IDE support.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    run: Annotated[ExperimentRun, SkipValidation()]
    name: str
    update_time: Optional[datetime] = None
    experiment_name: str


class PipelineTaskArtifacts(BaseModel):
    """Artifacts (datasets and models) for a pipeline task."""

    input_dataset_urns: Optional[List[str]] = None
    output_dataset_urns: Optional[List[str]] = None
    output_model_resource_names: Optional[List[str]] = None


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
    output_model_resource_names: Optional[List[str]] = None


class PipelineMetadata(BaseModel):
    """Metadata for a Vertex AI pipeline."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    display_name: str
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


class ModelLineageRelationships(BaseModel):
    """Lineage relationships for a single model or model group."""

    downstream_jobs: List[str] = Field(default_factory=list)
    training_jobs: List[str] = Field(default_factory=list)

    def add_downstream_job(self, job_urn: str) -> None:
        if job_urn not in self.downstream_jobs:
            self.downstream_jobs.append(job_urn)

    def add_training_job(self, job_urn: str) -> None:
        if job_urn not in self.training_jobs:
            self.training_jobs.append(job_urn)

    def set_training_job(self, job_urn: str) -> None:
        """Set the single training job (for models with one training job)."""
        if not self.training_jobs or job_urn not in self.training_jobs:
            self.training_jobs = [job_urn]


class PendingResourceUsage(BaseModel):
    """Pending lineage for a model resource before URN is resolved."""

    downstream_job_urns: List[str] = Field(default_factory=list)

    def add_downstream_job(self, job_urn: str) -> None:
        if job_urn not in self.downstream_job_urns:
            self.downstream_job_urns.append(job_urn)


class ModelUsageTracker(BaseModel):
    """Tracks ML Model and ML Model Group lineage relationships with DataJobs."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    relationships: Dict[str, ModelLineageRelationships] = Field(default_factory=dict)
    pending_resources: Dict[str, PendingResourceUsage] = Field(default_factory=dict)
    emitted_urns: Set[str] = Field(default_factory=set)

    def _get_or_create_relationships(self, urn: str) -> ModelLineageRelationships:
        if urn not in self.relationships:
            self.relationships[urn] = ModelLineageRelationships()
        return self.relationships[urn]

    def _get_or_create_pending(self, resource_name: str) -> PendingResourceUsage:
        if resource_name not in self.pending_resources:
            self.pending_resources[resource_name] = PendingResourceUsage()
        return self.pending_resources[resource_name]

    def track_model_usage(self, model_urn: str, used_in_urn: str) -> None:
        self._get_or_create_relationships(model_urn).add_downstream_job(used_in_urn)

    def track_model_group_training_job(
        self, model_group_urn: str, training_job_urn: str
    ) -> None:
        self._get_or_create_relationships(model_group_urn).add_training_job(
            training_job_urn
        )

    def track_model_training_job(self, model_urn: str, training_job_urn: str) -> None:
        self._get_or_create_relationships(model_urn).set_training_job(training_job_urn)

    def track_resource_usage(self, resource_name: str, used_in_urn: str) -> None:
        self._get_or_create_pending(resource_name).add_downstream_job(used_in_urn)

    def resolve_and_track_resource(
        self, resource_name: str, model_urn: str, model_group_urn: str
    ) -> None:
        """
        Resolve a pending resource to actual model URNs and track usage.
        Call this when processing a model to link it with jobs that referenced it by resource name.
        """
        if resource_name in self.pending_resources:
            pending = self.pending_resources[resource_name]
            for job_urn in pending.downstream_job_urns:
                self.track_model_usage(model_urn, job_urn)
                self.track_model_usage(model_group_urn, job_urn)

    def get_model_usage(self, model_urn: str) -> List[str]:
        return (
            self.relationships[model_urn].downstream_jobs
            if model_urn in self.relationships
            else []
        )

    def get_model_group_usage(self, model_group_urn: str) -> List[str]:
        return (
            self.relationships[model_group_urn].downstream_jobs
            if model_group_urn in self.relationships
            else []
        )

    def get_model_group_training_jobs(self, model_group_urn: str) -> List[str]:
        return (
            self.relationships[model_group_urn].training_jobs
            if model_group_urn in self.relationships
            else []
        )

    def get_model_training_job(self, model_urn: str) -> Optional[str]:
        if model_urn in self.relationships:
            jobs = self.relationships[model_urn].training_jobs
            return jobs[0] if jobs else None
        return None

    def mark_emitted(self, urn: str) -> None:
        """Mark a URN as having been emitted (to avoid duplicates in incremental mode)."""
        self.emitted_urns.add(urn)

    def get_pending_lineage_urns(self) -> List[str]:
        """
        Get all URNs with tracked lineage that haven't been emitted yet.
        Used in incremental mode to emit lineage updates for models that weren't re-processed.
        """
        return [
            urn
            for urn in self.relationships
            if urn not in self.emitted_urns
            and (
                self.relationships[urn].downstream_jobs
                or self.relationships[urn].training_jobs
            )
        ]
