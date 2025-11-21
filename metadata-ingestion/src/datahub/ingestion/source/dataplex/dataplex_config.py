"""Configuration for Google Dataplex source."""

import logging
from copy import deepcopy
from typing import Dict, List, Optional

from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential

logger = logging.getLogger(__name__)


class DataplexFilterConfig(ConfigModel):
    """Filter configuration for Dataplex ingestion."""

    lake_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for lake names to filter in ingestion.",
    )

    zone_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for zone names to filter in ingestion.",
    )

    asset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for asset names to filter in ingestion.",
    )

    entity_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for entity names to filter in ingestion.",
    )


class DataplexConfig(EnvConfigMixin, PlatformInstanceConfigMixin):
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

    location: str = Field(
        default="us-central1",
        description="GCP location/region where Dataplex resources are located (e.g., us-central1, us, eu).",
    )

    filter_config: DataplexFilterConfig = Field(
        default=DataplexFilterConfig(),
        description="Filters to control which Dataplex resources are ingested.",
    )

    extract_lakes: bool = Field(
        default=True,
        description="Whether to extract Lake metadata as Containers.",
    )

    extract_zones: bool = Field(
        default=True,
        description="Whether to extract Zone metadata as Containers.",
    )

    extract_assets: bool = Field(
        default=True,
        description="Whether to extract Asset metadata as Containers.",
    )

    extract_entities: bool = Field(
        default=True,
        description="Whether to extract Entity metadata (discovered tables/filesets) as Datasets.",
    )

    extract_entry_groups: bool = Field(
        default=False,
        description="Whether to extract Entry Groups from Universal Catalog. (Phase 2 feature)",
    )

    extract_entries: bool = Field(
        default=False,
        description="Whether to extract Entries from Universal Catalog. (Phase 2 feature)",
    )

    extract_lineage: bool = Field(
        default=True,
        description="Whether to extract lineage information using Dataplex Lineage API. "
        "Extracts table-level lineage relationships between entities.",
    )

    lineage_fail_fast: bool = Field(
        default=False,
        description="If True, fail immediately on lineage extraction errors. "
        "If False (default), log errors and continue processing other entities. "
        "Recommended: False for development, True for production to catch issues early.",
    )

    max_lineage_failures: int = Field(
        default=10,
        description="Maximum number of consecutive lineage extraction failures before stopping. "
        "Acts as a circuit breaker to prevent excessive API calls when there's a systemic issue. "
        "Set to -1 to disable circuit breaker. Default: 10.",
    )

    lineage_batch_size: int = Field(
        default=1000,
        description="Number of entities to process in each lineage extraction batch. "
        "Lower values reduce memory usage but may increase processing time. "
        "Set to -1 to disable batching (process all entities at once). "
        "Recommended: 1000 for large deployments, -1 for small deployments. Default: 1000.",
    )

    create_sibling_relationships: bool = Field(
        default=True,
        description="Whether to create sibling relationships between Dataplex entities and native platform datasets (BigQuery/GCS).",
    )

    dataplex_is_primary_sibling: bool = Field(
        default=False,
        description="Controls sibling relationship primary designation between Dataplex entities and source platform entities. "
        "When False (default), source platform entities (BigQuery/GCS) are primary and Dataplex entities are secondary. "
        "When True, Dataplex entities are primary and source platform entities are secondary. "
        "Default is False since source platforms are typically the canonical data source.",
    )

    apply_zone_type_tags: bool = Field(
        default=True,
        description="Whether to apply tags based on zone types (RAW, CURATED).",
    )

    apply_label_tags: bool = Field(
        default=True,
        description="Whether to convert Dataplex labels to DataHub tags.",
    )

    max_workers: int = Field(
        default=10,
        description="Number of worker threads to use to parallelize zone entity extraction. Set to 1 to disable parallelization.",
    )

    dataplex_url: str = Field(
        default="https://console.cloud.google.com/dataplex",
        description="Base URL for Dataplex console (for generating external links).",
    )

    @model_validator(mode="before")
    @classmethod
    def project_id_backward_compatibility(cls, values: Dict) -> Dict:
        """Handle backward compatibility for project_id -> project_ids migration."""
        # Create a copy to avoid modifying the input dictionary, preventing state contamination in tests
        values = deepcopy(values)
        project_id = values.pop("project_id", None)
        project_ids = values.get("project_ids")

        if not project_ids and project_id:
            values["project_ids"] = [project_id]
        elif project_ids and project_id:
            logging.warning(
                "Both project_id and project_ids are set. Using project_ids. "
                "The project_id config is deprecated, please use project_ids instead."
            )

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

    def get_credentials(self) -> Optional[Dict[str, str]]:
        """Get credentials dictionary for authentication."""
        if self.credential:
            # Use the first project_id for credential context
            project_id = self.project_ids[0] if self.project_ids else None
            return self.credential.to_dict(project_id)
        return None
