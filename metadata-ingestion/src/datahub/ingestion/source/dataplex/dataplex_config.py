"""Configuration for Google Dataplex source."""

import logging
from typing import Dict, List, Optional

from pydantic import Field, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.ingestion.source.common.gcp_project_filter import (
    GcpProjectFilterConfig,
    GCPValidationError,
    validate_project_label_list,
)
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
    GcpProjectFilterConfig,
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
    StatefulIngestionConfigBase,
    StatefulLineageConfigMixin,
):
    """Configuration for Google Dataplex source.

    Project selection (`project_ids`, `project_labels`, `project_id_pattern`) is
    inherited from `GcpProjectFilterConfig` and consumed by the shared
    `resolve_gcp_projects` helper. Auto-discovery (when `project_ids` is empty)
    requires `roles/resourcemanager.folderViewer` on the parent folder/organization.
    """

    credential: Optional[GCPCredential] = Field(
        default=None,
        description="GCP credential information. If not specified, uses Application Default Credentials.",
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

    max_workers_entries: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Number of parallel worker threads for fetching entry details "
        "(get_entry API calls). Entry detail fetching is the main bottleneck in the "
        "entries stage because each entry requires one blocking RPC. Increasing this "
        "value reduces wall-clock time proportionally up to the API quota limit. "
        "Increase for large deployments (>1k entries). Default: 10.",
    )

    max_workers_lineage: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Number of parallel worker threads for lineage lookups "
        "(search_links API calls). Lineage lookup volume scales with entries × "
        "lineage_locations, so parallelism here has a large impact on total "
        "ingestion time. Increase for large entry × location matrices. Default: 10.",
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration for stale metadata removal.",
    )

    include_glossaries: bool = Field(
        default=True,
        description=(
            "Whether to ingest Dataplex Business Glossary entities as DataHub GlossaryNodes "
            "and GlossaryTerms. Glossaries, categories, and terms are emitted with correct "
            "parent hierarchy. Default: True."
        ),
    )

    include_glossary_term_associations: bool = Field(
        default=False,
        description=(
            "Whether to ingest term-to-asset associations via the Dataplex lookupEntryLinks API. "
            "For each ingested term, all entries_locations are queried per project to find linked "
            "assets. Requires roles/resourcemanager.projectViewer on all configured projects to "
            "resolve GCP project numbers needed by the lookupEntryLinks API."
        ),
    )

    glossary_locations: List[str] = Field(
        default_factory=lambda: ["global"],
        description=(
            "GCP locations to scan for Dataplex Business Glossaries. "
            "Dataplex glossaries are typically created in 'global' but can exist in any location. "
            "Default: ['global']."
        ),
    )

    max_workers_glossary: int = Field(
        default=10,
        ge=1,
        le=100,
        description=(
            "Number of parallel worker threads for glossary ingestion (fetching terms and "
            "categories per glossary) and term-asset association traversal "
            "(lookupEntryLinks calls). Default: 10."
        ),
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

    @field_validator("project_labels")
    @classmethod
    def _validate_project_labels_format(cls, v: List[str]) -> List[str]:
        try:
            validate_project_label_list(v)
        except GCPValidationError as e:
            raise ValueError(str(e)) from e
        return v

    @model_validator(mode="after")
    def validate_project_ids(self) -> "DataplexConfig":
        """Ensure at least one means of selecting projects is configured."""
        has_non_default_pattern = (
            self.project_id_pattern != AllowDenyPattern.allow_all()
        )
        if (
            not self.project_ids
            and not self.project_labels
            and not has_non_default_pattern
        ):
            raise ValueError(
                "At least one project selector must be specified. Set project_ids "
                "explicitly, or use project_id_pattern / project_labels to "
                "auto-discover projects."
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
        if self.include_glossaries and not self.glossary_locations:
            raise ValueError(
                "At least one glossary location must be specified via glossary_locations "
                "when include_glossaries is enabled."
            )

        return self

    def get_credentials(self) -> Optional[Dict[str, str]]:
        """Get credentials dictionary for authentication."""
        if self.credential:
            # Use the first project_id for credential context
            project_id = self.project_ids[0] if self.project_ids else None
            return self.credential.to_dict(project_id)
        return None
