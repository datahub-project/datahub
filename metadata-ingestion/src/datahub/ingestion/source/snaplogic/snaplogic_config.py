from typing import Optional

from pydantic import Field, SecretStr

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulIngestionConfigBase,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulLineageConfigMixin,
    StatefulUsageConfigMixin,
)


class SnaplogicConfig(
    StatefulIngestionConfigBase, StatefulLineageConfigMixin, StatefulUsageConfigMixin
):
    platform: str = "SnapLogic"
    username: str = Field(description="Username")
    password: SecretStr = Field(description="Password")
    base_url: str = Field(
        default="https://elastic.snaplogic.com",
        description="Url to your SnapLogic instance: `https://elastic.snaplogic.com`, or similar. Used for making API calls to SnapLogic.",
    )
    org_name: str = Field(description="Organization name from SnapLogic instance")
    namespace_mapping: dict = Field(
        default={}, description="Mapping of namespaces to platform instances"
    )
    case_insensitive_namespaces: list = Field(
        default=[],
        description="List of namespaces that should be treated as case insensitive",
    )
    create_non_snaplogic_datasets: bool = Field(
        default=False,
        description="Whether to create datasets for non-SnapLogic datasets (e.g., databases, S3, etc.)",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None
