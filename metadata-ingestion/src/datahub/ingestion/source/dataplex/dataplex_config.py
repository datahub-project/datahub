"""Configuration for Google Dataplex source."""

import logging
from typing import Dict, List, Optional

from pydantic import AliasChoices, Field, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulLineageConfigMixin,
)

logger = logging.getLogger(__name__)

DEFAULT_LINEAGE_LOCATIONS = [
    "us",
    "eu",
    "asia",
    "us-central1",
    "us-east1",
    "us-east4",
    "us-east5",
    "us-south1",
    "us-west1",
    "us-west2",
    "us-west3",
    "us-west4",
    "northamerica-northeast1",
    "northamerica-northeast2",
    "southamerica-east1",
    "southamerica-west1",
    "europe-central2",
    "europe-north1",
    "europe-southwest1",
    "europe-west1",
    "europe-west2",
    "europe-west3",
    "europe-west4",
    "europe-west6",
    "europe-west8",
    "europe-west9",
    "europe-west10",
    "europe-west12",
    "me-central1",
    "me-central2",
    "me-west1",
    "asia-east1",
    "asia-east2",
    "asia-northeast1",
    "asia-northeast2",
    "asia-northeast3",
    "asia-south1",
    "asia-south2",
    "asia-southeast1",
    "asia-southeast2",
    "australia-southeast1",
    "australia-southeast2",
    "africa-south1",
]


class EntriesFilterConfig(ConfigModel):
    """Filter configuration specific to Dataplex Entries API (Universal Catalog)."""

    pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        validation_alias=AliasChoices("pattern", "dataset_pattern"),
        description="Regex patterns for Dataplex entry names to filter in ingestion.",
    )
    fqn_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for Dataplex fully-qualified names to filter in ingestion.",
    )


class EntryGroupFilterConfig(ConfigModel):
    """Filter configuration for Dataplex entry groups."""

    pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for entry group resource names to include/exclude.",
    )


class DataplexFilterConfig(ConfigModel):
    """Filter configuration for Dataplex ingestion."""

    entry_groups: EntryGroupFilterConfig = Field(
        default_factory=EntryGroupFilterConfig,
        description="Filters for Dataplex entry group names.",
    )
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

    entries_locations: List[str] = Field(
        default_factory=lambda: ["us", "eu", "asia", "global"],
        description="List of GCP regions to scan for Universal Catalog entries extraction. "
        "This list may include multi-regions (for example 'us', 'eu', 'asia') and "
        "single regions (for example 'us-central1'). "
        "Entries scanning runs across all configured entries_locations. "
        "Default: ['us', 'eu', 'asia', 'global'].",
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

    lineage_locations: List[str] = Field(
        default_factory=lambda: list(DEFAULT_LINEAGE_LOCATIONS),
        description="List of GCP regions to scan for Dataplex lineage data. "
        "By default, includes all supported multi-regions and regions. "
        "Narrowing this list from the default is critical for better performance "
        "because lineage API calls scale with configured project/location pairs. "
        "This list may include multi-regions and single regions. "
        "In practice, lineage often resides in job regions while entries may be in "
        "multi-regions, so entries_locations and lineage_locations are configured separately. "
        "Example: ['eu', 'us-central1', 'europe-west1'].",
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

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration for stale metadata removal.",
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
        if not self.entries_locations:
            raise ValueError(
                "At least one entries location must be specified via entries_locations."
            )
        if not self.lineage_locations:
            raise ValueError(
                "At least one lineage location must be specified via lineage_locations."
            )

        return self

    def get_credentials(self) -> Optional[Dict[str, str]]:
        """Get credentials dictionary for authentication."""
        if self.credential:
            # Use the first project_id for credential context
            project_id = self.project_ids[0] if self.project_ids else None
            return self.credential.to_dict(project_id)
        return None
