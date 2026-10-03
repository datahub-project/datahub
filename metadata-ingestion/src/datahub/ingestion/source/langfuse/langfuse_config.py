from typing import Optional

import pydantic
from pydantic import Field, SecretStr, ValidationInfo

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class LangfuseConnectionConfig(ConfigModel):
    """Connection configuration for a single Langfuse project.

    Langfuse Basic Auth resolves to exactly one project per key pair, so one
    recipe ingests exactly one project. Users with multiple Langfuse projects
    run one recipe per project.
    """

    host: str = Field(
        description=(
            "Langfuse base URL, e.g. 'http://localhost:3000' for a "
            "self-hosted instance, or the Langfuse Cloud region URL "
            "(e.g. 'https://cloud.langfuse.com'). No trailing slash."
        ),
    )
    public_key: str = Field(
        description=(
            "Langfuse project Public Key (Basic Auth username). Find it in "
            "Project Settings > API Keys."
        ),
    )
    secret_key: SecretStr = Field(
        description=(
            "Langfuse project Secret Key (Basic Auth password). Find it in "
            "Project Settings > API Keys. Only shown once at creation time "
            "in the Langfuse UI."
        ),
    )

    @pydantic.field_validator("host", mode="after")
    @classmethod
    def validate_host(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError(
                f"connection.host must start with http:// or https://, got '{v}'"
            )
        return v.rstrip("/")


class LangfuseSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    connection: LangfuseConnectionConfig = Field(
        description="Connection details for the Langfuse project to ingest from."
    )

    window: BaseTimeWindowConfig = Field(
        # BaseTimeWindowConfig's own bare default is "one bucket_duration back
        # from now" (i.e. ~1 day, floored to midnight), NOT 7 days - confirmed
        # empirically while testing against the live instance. The "-7d"
        # string must be passed explicitly to get the documented default.
        default_factory=lambda: BaseTimeWindowConfig(start_time="-7d"),
        description=(
            "Time window for Trace/Observation/Score retrieval. Defaults to "
            "the last 7 days. Langfuse's Observations API v2 is optimized "
            "for bounded, recent time ranges; querying the full history on "
            "every run is neither necessary nor efficient for this use case."
        ),
    )

    include_traces: bool = Field(
        default=True,
        description=(
            "Extract Traces, and their 'generation'-type Observations, "
            "within the configured time window as DataProcessInstance "
            "entities nested under a Project container. Non-generation "
            "Observations (plain spans/events) are summarized as counts on "
            "the parent Trace rather than emitted individually."
        ),
    )
    include_scores: bool = Field(
        default=True,
        description=(
            "Attach Langfuse Scores as MLMetric entries on the Trace or "
            "Generation they were recorded against. Session- and dataset-run"
            "-level scores are not attached in this version (Sessions and "
            "Dataset Runs are not emitted as entities) and are dropped with "
            "a reported count. Requires include_traces=True."
        ),
    )
    include_sessions: bool = Field(
        default=True,
        description=(
            "Record each Trace's Langfuse sessionId and userId as "
            "customProperties on the Trace's DataProcessInstance. Sessions "
            "and Users are not emitted as separate DataHub entities in this "
            "version."
        ),
    )
    include_prompts: bool = Field(
        default=True,
        description=(
            "Extract Prompts as versioned Dataset entities, one per prompt "
            "version, linked together via a shared VersionSet."
        ),
    )

    trace_name_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter Traces to ingest, by trace name.",
    )
    prompt_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns to filter Prompts to ingest, by prompt name.",
    )

    page_size: int = Field(
        default=50,
        description=(
            "Number of items to fetch per API call when paginating through "
            "observations, scores, and prompts. Langfuse's v2/v3 APIs cap "
            "this at 100-1000 depending on endpoint; values above the "
            "endpoint's cap are rejected by the Langfuse server."
        ),
    )

    @pydantic.field_validator("page_size", mode="after")
    @classmethod
    def validate_page_size(cls, v: int) -> int:
        if not (1 <= v <= 1000):
            raise ValueError(f"page_size must be between 1 and 1000, got {v}")
        return v

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description=(
            "Stateful ingestion configuration for stale-entity soft "
            "deletion. Applies only to Prompt entities, which are fully "
            "enumerated on every run. It does NOT apply to Trace/Generation "
            "entities: those are retrieved through the rolling time window "
            "above, so a naive full-history comparison would incorrectly "
            "flag entities that simply aged out of the window as deleted. "
            "Trace/Generation workunits are emitted with "
            "is_primary_source=False specifically to exclude them from "
            "this comparison."
        ),
    )

    @pydantic.field_validator("include_scores", mode="after")
    @classmethod
    def scores_require_traces(cls, v: bool, info: ValidationInfo) -> bool:
        if v and not info.data.get("include_traces", True):
            raise ValueError(
                "include_traces must be True for include_scores to be set."
            )
        return v
