from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict, Field, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
)
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.ingestion.source.common.gcp_project_filter import (
    GCP_LABEL_PATTERN,
    GCP_PROJECT_ID_PATTERN,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.vertexai.vertexai_constants import (
    IngestionLimits,
    MLMetadataDefaults,
)

logger = logging.getLogger(__name__)


class PlatformDetail(ConfigModel):
    """Platform instance configuration for external datasets referenced in Vertex AI lineage."""

    model_config = ConfigDict(extra="forbid")

    platform_instance: Optional[str] = Field(
        default=None,
        description="Platform instance for URN generation. If not set, no platform instance in URN.",
    )
    env: str = Field(
        default=DEFAULT_ENV,
        description="Environment for all assets from this platform",
    )
    convert_urns_to_lowercase: bool = Field(
        default=False,
        description="Convert dataset names to lowercase. Set to true for Snowflake (which defaults to lowercase URNs). "
        "Leave false for GCS, BigQuery, S3, and ABS.",
    )


class VertexAIConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
    IncrementalLineageConfigMixin,
):
    normalize_external_dataset_paths: bool = Field(
        default=False,
        description="Strip partition segments from external dataset paths (GCS/S3/ABS) to create stable dataset URNs. "
        "When enabled, 'gs://bucket/data/year=2024/month=01/' becomes 'gs://bucket/data/'. "
        "Partition-level information is captured via DataProcessInstance. "
        "Default is False for backward compatibility. Will default to True in a future major version.",
    )

    partition_pattern_rules: List[str] = Field(
        default=[
            r"/[^/]+=([^/]+)",  # Hive-style: /year=2024/month=01/
            r"/dt=\d{4}-\d{2}-\d{2}",  # Date partitions: /dt=2024-01-15/
            r"/\d{4}/\d{2}/\d{2}",  # Date hierarchy: /2024/01/15/
        ],
        description="Regex patterns to identify and strip partition segments from paths. "
        "Applied when normalize_external_dataset_paths is enabled. Patterns are applied in order.",
    )

    credential: Optional[GCPCredential] = Field(
        default=None, description="GCP credential information"
    )

    region: str = Field(
        description=(
            "[deprecated] Single Vertex AI region. Prefer 'regions' or 'discover_regions'."
        ),
    )
    _deprecate_region = pydantic_field_deprecated("region")

    # Feature flags to control ingestion scope
    include_models: bool = Field(
        default=True,
        description="Ingest models and model versions from the registry.",
    )
    include_training_jobs: bool = Field(
        default=True,
        description="Ingest training jobs and related run events.",
    )
    include_experiments: bool = Field(
        default=True,
        description="Ingest experiments and experiment runs.",
    )
    include_pipelines: bool = Field(
        default=True,
        description="Ingest pipelines and tasks.",
    )
    include_evaluations: bool = Field(
        default=True,
        description="Ingest model evaluations and evaluation metrics.",
    )
    # Advanced metadata extraction options
    use_ml_metadata_for_lineage: bool = Field(
        default=True,
        description="Extract lineage from Vertex AI ML Metadata API for CustomJob and other training jobs. "
        "This enables input dataset → training job → output model lineage for non-AutoML jobs.",
    )
    extract_execution_metrics: bool = Field(
        default=True,
        description="Extract hyperparameters and metrics from ML Metadata Executions. "
        "Useful for training jobs that don't use Experiments but log to ML Metadata.",
    )
    # Name/type filters
    experiment_name_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex allow/deny pattern for experiment names.",
    )
    training_job_type_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex allow/deny pattern for training job class names (e.g., CustomJob).",
    )
    model_name_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex allow/deny pattern for model display names.",
    )

    # Pagination / safety knobs
    max_models: Optional[int] = Field(
        default=IngestionLimits.DEFAULT_MAX_MODELS,
        le=IngestionLimits.ABSOLUTE_MAX_MODELS,
        description=f"Maximum number of models to ingest. Models are ordered by update_time descending "
        f"(most recently updated first). Default: {IngestionLimits.DEFAULT_MAX_MODELS}, "
        f"Max: {IngestionLimits.ABSOLUTE_MAX_MODELS}. Set to None for unlimited (not recommended).",
    )
    max_training_jobs_per_type: Optional[int] = Field(
        default=IngestionLimits.DEFAULT_MAX_TRAINING_JOBS_PER_TYPE,
        le=IngestionLimits.ABSOLUTE_MAX_TRAINING_JOBS_PER_TYPE,
        description=f"Maximum training jobs per type (CustomJob, AutoML, etc.). Jobs are ordered by update_time "
        f"descending (most recently updated first). Default: {IngestionLimits.DEFAULT_MAX_TRAINING_JOBS_PER_TYPE}, "
        f"Max: {IngestionLimits.ABSOLUTE_MAX_TRAINING_JOBS_PER_TYPE}. Set to None for unlimited (not recommended).",
    )
    max_experiments: Optional[int] = Field(
        default=IngestionLimits.DEFAULT_MAX_EXPERIMENTS,
        le=IngestionLimits.ABSOLUTE_MAX_EXPERIMENTS,
        description=f"Maximum number of experiments to ingest. Experiments are ordered by update_time descending "
        f"(most recently updated first). Default: {IngestionLimits.DEFAULT_MAX_EXPERIMENTS}, "
        f"Max: {IngestionLimits.ABSOLUTE_MAX_EXPERIMENTS}. Set to None for unlimited (not recommended).",
    )
    max_runs_per_experiment: Optional[int] = Field(
        default=IngestionLimits.DEFAULT_MAX_RUNS_PER_EXPERIMENT,
        le=IngestionLimits.ABSOLUTE_MAX_RUNS_PER_EXPERIMENT,
        description=f"Maximum experiment runs per experiment. Runs are ordered by update_time descending "
        f"(most recently updated first). Default: {IngestionLimits.DEFAULT_MAX_RUNS_PER_EXPERIMENT}, "
        f"Max: {IngestionLimits.ABSOLUTE_MAX_RUNS_PER_EXPERIMENT}. Set to None for unlimited (not recommended).",
    )
    max_evaluations_per_model: Optional[int] = Field(
        default=IngestionLimits.DEFAULT_MAX_EVALUATIONS_PER_MODEL,
        le=IngestionLimits.ABSOLUTE_MAX_EVALUATIONS_PER_MODEL,
        description=f"Maximum evaluations per model. Default: {IngestionLimits.DEFAULT_MAX_EVALUATIONS_PER_MODEL}, "
        f"Max: {IngestionLimits.ABSOLUTE_MAX_EVALUATIONS_PER_MODEL}. Set to None for unlimited (not recommended).",
    )
    ml_metadata_max_execution_search_limit: int = Field(
        default=MLMetadataDefaults.MAX_EXECUTION_SEARCH_RESULTS,
        description="Maximum number of ML Metadata executions to retrieve when searching for a training job. "
        "Executions are ordered by LAST_UPDATE_TIME descending (most recently updated first), so if the limit is reached, "
        "you'll get the most recently completed/updated executions. Prevents excessive API calls and timeouts. "
        "Default: 500. The API will automatically paginate results (100 per page).",
    )
    rate_limit: bool = Field(
        default=False,
        description="Slow down ingestion to avoid hitting Vertex AI API quota limits. "
        "Enable if you see '429 Quota Exceeded' errors during ingestion.",
    )
    requests_per_min: int = Field(
        default=60,
        description="How many Vertex AI API calls to allow per minute when rate_limit is enabled. "
        "Start low (30–60) and increase only if ingestion is too slow — some calls fetch multiple "
        "pages of results internally, so the real quota usage is higher than this number suggests.",
    )
    # Optional multi-project / filter support
    project_ids: List[str] = Field(
        default_factory=list,
        description=("Ingest specified GCP project ids. Overrides project_id_pattern."),
    )
    project_labels: List[str] = Field(
        default_factory=list,
        description=(
            "Ingest projects with these labels (key:value). Applied before project_id_pattern."
        ),
    )
    project_id_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for project ids to include/exclude.",
    )
    # Optional multi-region support
    regions: List[str] = Field(
        default_factory=list,
        description=(
            "List of Vertex AI regions to scan. If empty and discover_regions is false, falls back to 'region'."
        ),
    )
    discover_regions: bool = Field(
        default=False,
        description=(
            "If true, discover available Vertex AI regions per project and scan all."
        ),
    )
    bucket_uri: Optional[str] = Field(
        default=None,
        description=("Bucket URI used in your project"),
    )
    vertexai_url: Optional[str] = Field(
        default="https://console.cloud.google.com/vertex-ai",
        description=("VertexUI URI"),
    )

    platform_instance_map: Dict[str, PlatformDetail] = Field(
        default_factory=dict,
        description="Map external platform names (gcs, bigquery, s3, azure_blob_storage, snowflake) "
        "to their platform instance and env. Ensures URNs match native connectors for lineage connectivity.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration for tracking and removing stale metadata.",
    )

    @field_validator("project_ids", mode="before")
    @classmethod
    def _validate_project_ids(cls, v: Any) -> List[str]:
        if not v:
            return v or []
        invalid = [pid for pid in v if not GCP_PROJECT_ID_PATTERN.match(pid)]
        if invalid:
            raise ValueError(
                f"Invalid project_ids format: {invalid}. "
                "Must be 6-30 chars, lowercase letters/numbers/hyphens, "
                "start with letter, end with letter or number."
            )
        # Deduplicate while preserving order
        return list(dict.fromkeys(v))

    @field_validator("project_labels", mode="before")
    @classmethod
    def _validate_project_labels(cls, v: Any) -> List[str]:
        if not v:
            return v or []
        invalid = [label for label in v if not GCP_LABEL_PATTERN.match(label)]
        if invalid:
            raise ValueError(
                f"Invalid project_labels format: {invalid}. "
                "Must be 'key:value' with lowercase letters, digits, underscores, or hyphens."
            )
        return list(dict.fromkeys(v))

    @field_validator("project_id_pattern")
    @classmethod
    def _validate_project_id_pattern_syntax(
        cls, v: AllowDenyPattern
    ) -> AllowDenyPattern:
        invalid_patterns = []
        for pattern in v.allow + v.deny:
            try:
                re.compile(pattern)
            except re.error as e:
                invalid_patterns.append(f"'{pattern}': {e}")
        if invalid_patterns:
            raise ValueError(
                f"Invalid regex in project_id_pattern: {', '.join(invalid_patterns)}. "
                "Check your allow/deny patterns for syntax errors."
            )
        return v

    @model_validator(mode="before")
    @classmethod
    def _migrate_project_id_to_project_ids(cls, values: Any) -> Any:
        """Migrate deprecated 'project_id' to 'project_ids' and remove it."""
        if not isinstance(values, dict):
            return values
        project_id = values.pop("project_id", None)
        if not project_id:
            return values
        project_ids = values.get("project_ids")
        if not project_ids:
            logger.warning(
                "Config field 'project_id' is deprecated, use 'project_ids: [\"%s\"]' instead.",
                project_id,
            )
            values["project_ids"] = [project_id]
        elif project_id not in project_ids:
            raise ValueError(
                f"Conflicting config: 'project_id' is '{project_id}' "
                f"but 'project_ids' is {project_ids}. "
                "Remove the deprecated 'project_id' field and use only 'project_ids'."
            )
        return values

    @model_validator(mode="after")
    def _validate_pattern_does_not_filter_all(self) -> VertexAIConfig:
        """Raise early if project_id_pattern would filter out all explicitly configured project_ids."""
        if not self.project_ids:
            return self
        allowed = [
            pid for pid in self.project_ids if self.project_id_pattern.allowed(pid)
        ]
        if not allowed:
            raise ValueError(
                f"All {len(self.project_ids)} configured project_ids were filtered out "
                "by project_id_pattern. Check your allow/deny patterns."
            )
        return self

    def get_credentials(self) -> Optional[Dict[str, Any]]:
        if self.credential:
            return self.credential.to_dict()
        return None
