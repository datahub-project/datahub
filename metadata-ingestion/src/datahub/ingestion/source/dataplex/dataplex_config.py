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

    location: str = Field(
        default="us-central1",
        description="GCP location/region where Dataplex resources are located (e.g., us-central1, us, eu).",
    )

    filter_config: DataplexFilterConfig = Field(
        default=DataplexFilterConfig(),
        description="Filters to control which Dataplex resources are ingested.",
    )

    include_entries: bool = Field(
        default=True,
        description="Whether to extract Entries from Universal Catalog. "
        "This is the primary source of metadata and is always processed first.",
    )

    include_entities: bool = Field(
        default=False,
        description="Whether to include Entity metadata from Lakes/Zones (discovered tables/filesets) as Datasets. "
        "This is optional and complements the Entries API data. "
        "When both include_entries and include_entities are enabled, entities already found in Entries API will be skipped to avoid duplicates.",
    )

    include_lineage: bool = Field(
        default=True,
        description="Whether to extract lineage information using Dataplex Lineage API. "
        "Extracts table-level lineage relationships between entities. "
        "Lineage API calls automatically retry transient errors (timeouts, rate limits) with exponential backoff.",
    )

    lineage_batch_size: int = Field(
        default=1000,
        description="Number of entities to process in each lineage extraction batch. "
        "Lower values reduce memory usage but may increase processing time. "
        "Set to -1 to disable batching (process all entities at once). "
        "Recommended: 1000 for large deployments, -1 for small deployments. Default: 1000.",
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

    def get_credentials(self) -> Optional[Dict[str, str]]:
        """Get credentials dictionary for authentication."""
        if self.credential:
            # Use the first project_id for credential context
            project_id = self.project_ids[0] if self.project_ids else None
            return self.credential.to_dict(project_id)
        return None
