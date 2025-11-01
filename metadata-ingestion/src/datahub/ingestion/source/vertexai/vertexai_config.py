from typing import Dict, List, Optional

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import EnvConfigMixin
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential


class VertexAIConfig(EnvConfigMixin):
    credential: Optional[GCPCredential] = Field(
        default=None, description="GCP credential information"
    )
    project_id: str = Field(description=("Project ID in Google Cloud Platform"))
    region: str = Field(
        description=(
            "[deprecated] Single Vertex AI region. Prefer 'regions' or 'discover_regions'."
        ),
    )
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
        default=None,
        description="Optional cap on number of models to ingest.",
    )
    max_training_jobs_per_type: Optional[int] = Field(
        default=None,
        description="Optional cap per training job type.",
    )
    max_experiments: Optional[int] = Field(
        default=None,
        description="Optional cap on number of experiments to ingest.",
    )
    max_runs_per_experiment: Optional[int] = Field(
        default=None,
        description="Optional cap on runs ingested per experiment.",
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

    def get_credentials(self) -> Optional[Dict[str, str]]:
        if self.credential:
            return self.credential.to_dict(self.project_id)
        return None
