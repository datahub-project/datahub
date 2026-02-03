"""Configuration for Google Dataplex source."""

import logging
from typing import Dict, List, Optional

from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulLineageConfigMixin,
)

logger = logging.getLogger(__name__)


class EntriesFilterConfig(ConfigModel):
    """Filter configuration specific to Dataplex Entries API (Universal Catalog)."""

    dataset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for entry IDs to filter in ingestion.",
    )


class DataplexFilterConfig(ConfigModel):
    """Filter configuration for Dataplex ingestion."""

    entries: EntriesFilterConfig = Field(
        default_factory=EntriesFilterConfig,
        description="Filters specific to Dataplex Entries API (Universal Catalog).",
    )


class DataplexConfig(
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
    StatefulIngestionConfigBase,
    StatefulLineageConfigMixin,
):
    """Configuration for Google Dataplex source."""

    credential: Optional[GCPCredential] = Field(
        default=None,
        description="GCP credential information. If not specified, uses Application Default Credentials.",
    )

    project_ids: List[str] = Field(
        default_factory=list,
        description="List of Google Cloud Project IDs to ingest Dataplex resources from. "
        "If not specified, uses project_id or attempts to detect from credentials.",
    )

    entries_location: str = Field(
        default="us",
        description="GCP location for Universal Catalog entries extraction. "
        "Must be a multi-region location (us, eu, asia) to access system-managed entry groups like @bigquery. "
        "Regional locations (us-central1, etc.) only contain placeholder entries and will miss BigQuery tables. "
        "Default: 'us' (recommended for most users).",
    )

    filter_config: DataplexFilterConfig = Field(
        default_factory=DataplexFilterConfig,
        description="Filters to control which Dataplex resources are ingested.",
    )

    include_schema: bool = Field(
        default=True,
        description="Whether to extract and ingest schema metadata (columns, types, descriptions). "
        "Set to False to skip schema extraction for faster ingestion when only basic dataset metadata is needed. "
        "Disabling schema extraction can improve performance for large deployments. Default: True.",
    )

    include_lineage: bool = Field(
        default=True,
        description="Whether to extract lineage information using Dataplex Lineage API. "
        "Extracts table-level lineage relationships between entries. "
        "Lineage API calls automatically retry transient errors (timeouts, rate limits) with exponential backoff.",
    )

    lineage_max_retries: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum number of retry attempts for lineage API calls when encountering transient errors "
        "(timeouts, rate limits, service unavailable). Each attempt uses exponential backoff. "
        "Higher values increase resilience but may slow down ingestion. Default: 3.",
    )

    lineage_retry_backoff_multiplier: float = Field(
        default=1.0,
        ge=0.1,
        le=10.0,
        description="Multiplier for exponential backoff between lineage API retry attempts (in seconds). "
        "Wait time formula: multiplier * (2 ^ attempt_number), capped between 2-10 seconds. "
        "Higher values reduce API load but increase ingestion time. Default: 1.0.",
    )

    batch_size: Optional[int] = Field(
        default=1000,
        description="Batch size for metadata emission and lineage extraction. "
        "Entries are emitted in batches to prevent memory issues in large deployments. "
        "Lower values reduce memory usage but may increase processing time. "
        "Set to None to disable batching (process all entries at once). "
        "Recommended: 1000 for large deployments (>10k entries), None for small deployments (<1k entries). Default: 1000.",
    )

    dataplex_url: str = Field(
        default="https://console.cloud.google.com/dataplex",
        description="Base URL for Dataplex console (for generating external links).",
    )

    @model_validator(mode="before")
    @classmethod
    def project_id_backward_compatibility(cls, values: Dict) -> Dict:
        """Handle backward compatibility for project_id -> project_ids migration."""
        # Pydantic passes the raw input dict to mode="before" validators.
        # We create a new dict to avoid mutating the input (important for dict reuse in tests).
        project_id = values.get("project_id")
        project_ids = values.get("project_ids")

        if not project_ids and project_id:
            # Create a new dict without project_id, adding project_ids
            result = {k: v for k, v in values.items() if k != "project_id"}
            result["project_ids"] = [project_id]
            return result
        elif project_ids and project_id:
            logging.warning(
                "Both project_id and project_ids are set. Using project_ids. "
                "The project_id config is deprecated, please use project_ids instead."
            )
            # Remove project_id from the dict
            return {k: v for k, v in values.items() if k != "project_id"}

        return values

    @model_validator(mode="after")
    def validate_project_ids(self) -> "DataplexConfig":
        """Ensure at least one project is configured."""
        if not self.project_ids:
            raise ValueError(
                "At least one project must be specified. "
                "Please set project_ids or project_id in your configuration."
            )
        return self

    @model_validator(mode="after")
    def validate_location_configuration(self) -> "DataplexConfig":
        """Validate location configuration and warn about common mistakes."""
        # Warn if entries_location appears to be a regional location
        if self.entries_location:
            if "-" in self.entries_location:
                logger.warning(
                    f"entries_location='{self.entries_location}' appears to be a regional location (contains '-'). "
                    "System-managed entry groups like @bigquery require multi-region locations (us, eu, asia). "
                    "You may miss BigQuery tables and other system resources. "
                    "Recommended: Change entries_location to 'us', 'eu', or 'asia'."
                )

        return self

    def get_credentials(self) -> Optional[Dict[str, str]]:
        """Get credentials dictionary for authentication."""
        if self.credential:
            # Use the first project_id for credential context
            project_id = self.project_ids[0] if self.project_ids else None
            return self.credential.to_dict(project_id)
        return None
