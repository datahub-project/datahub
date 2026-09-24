"""Data pack models for the DataHub CLI."""

from typing import List, Optional

from pydantic import ConfigDict, Field

from datahub.configuration.common import ConfigModel
from datahub.utilities.str_enum import StrEnum


class TrustTier(StrEnum):
    """Trust level of a data pack."""

    VERIFIED = "verified"
    COMMUNITY = "community"
    CUSTOM = "custom"


class DataPackInfo(ConfigModel):
    """Metadata about a data pack available for loading into DataHub."""

    name: str = Field(
        description="Pack identifier (e.g. 'bootstrap', 'showcase-ecommerce')"
    )
    description: str = Field(
        description="Human-readable description of the pack contents"
    )
    url: str = Field(description="HTTP(S) URL to the MCP/MCE JSON data file")
    sha256: Optional[str] = Field(
        default=None,
        description="Hex-encoded SHA256 checksum of the data file for integrity verification",
    )
    size_hint: Optional[str] = Field(
        default=None,
        description="Approximate download size for display (e.g. '3.2 MB')",
    )
    tags: List[str] = Field(default_factory=list, description="Categorization tags")
    source_type: str = Field(
        default="http",
        description="Source type: 'http', 's3', or 'huggingface' (all resolve to HTTP in v1)",
    )
    trust: TrustTier = Field(
        default=TrustTier.CUSTOM,
        description="Trust tier: verified (DataHub official), community, or custom",
    )
    reference_timestamp: Optional[int] = Field(
        default=None,
        description="Epoch millis when the pack was captured. Used as the anchor for time-shifting.",
    )
    min_server_version: Optional[str] = Field(
        default=None,
        description="Minimum DataHub OSS server version required (e.g. '0.14.0')",
    )
    min_cloud_version: Optional[str] = Field(
        default=None,
        description="Minimum Acryl Cloud version required (e.g. '0.3.5')",
    )
    pack_format_version: str = Field(
        default="1", description="Schema version of the pack manifest"
    )


class RegistryManifest(ConfigModel):
    """Top-level structure of the remote registry JSON file."""

    model_config = ConfigDict(extra="allow")

    schema_version: int = Field(
        default=1,
        description="Registry schema version. Old clients ignore unknown fields.",
    )
    packs: dict[str, DataPackInfo] = Field(
        default_factory=dict, description="Map of pack name to pack info"
    )


class LoadRecord(ConfigModel):
    """Tracks a data pack load for unload support."""

    pack_name: str = Field(description="Name of the loaded pack")
    run_id: str = Field(description="Ingestion run ID used during load")
    loaded_at: str = Field(description="ISO 8601 timestamp of when the pack was loaded")
    pack_url: str = Field(description="URL the pack was downloaded from")
    pack_sha256: Optional[str] = Field(
        default=None, description="SHA256 of the data file that was loaded"
    )
