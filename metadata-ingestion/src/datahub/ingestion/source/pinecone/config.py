"""Configuration model for Pinecone source."""

from typing import Annotated, Dict, List, Optional

from pydantic import Field, PositiveInt

from datahub.configuration.common import AllowDenyPattern, Filters, TransparentSecretStr
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.agent.models import ProbeNodeKind, ProbeResult
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.pinecone.pinecone_client import PineconeClient
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulIngestionConfigBase,
    StatefulStaleMetadataRemovalConfig,
)


class PineconeConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, StatefulIngestionConfigBase
):
    """
    Configuration for Pinecone source.

    Extracts metadata from Pinecone vector database including:
    - Index configurations (dimension, metric, type)
    - Namespace information and statistics
    - Inferred schemas from vector metadata
    """

    # Authentication
    api_key: TransparentSecretStr = Field(
        description="Pinecone API key for authentication. "
        "Can be found in the Pinecone console under API Keys."
    )

    environment: Optional[str] = Field(
        default=None,
        description="Pinecone environment (for pod-based indexes). "
        "Not required for serverless indexes. Example: 'us-west1-gcp'",
    )

    # Connection
    index_host_mapping: Optional[Dict[str, str]] = Field(
        default=None,
        description="Optional manual mapping of index names to host URLs. "
        "Useful if automatic host resolution fails. "
        "Example: {'my-index': 'my-index-abc123.svc.pinecone.io'}",
    )

    # Filtering
    index_pattern: Annotated[
        AllowDenyPattern, Filters(DatasetContainerSubTypes.PINECONE_INDEX)
    ] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for indexes to filter in ingestion. "
        "Specify 'allow' patterns to include specific indexes, "
        "and 'deny' patterns to exclude indexes.",
    )

    namespace_pattern: Annotated[
        AllowDenyPattern, Filters(DatasetContainerSubTypes.PINECONE_NAMESPACE)
    ] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for namespaces to filter in ingestion. "
        "Specify 'allow' patterns to include specific namespaces, "
        "and 'deny' patterns to exclude namespaces.",
    )

    # Schema inference
    enable_schema_inference: bool = Field(
        default=True,
        description="Whether to infer schemas from vector metadata. "
        "When enabled, samples vectors from each namespace to build a schema.",
    )

    schema_sampling_size: PositiveInt = Field(
        default=100,
        description="Number of vectors to sample per namespace for schema inference. "
        "Higher values provide more accurate schemas but increase ingestion time.",
    )

    max_metadata_fields: PositiveInt = Field(
        default=100,
        description="Maximum number of metadata fields to include in the inferred schema. "
        "Limits schema size for namespaces with many metadata fields.",
    )

    # Performance
    max_workers: PositiveInt = Field(
        default=5,
        description="Maximum number of parallel workers for processing indexes and namespaces. "
        "Increase for faster ingestion of many indexes.",
    )

    # Stateful ingestion
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration for tracking processed entities "
        "and removing stale metadata.",
    )

    def get_client(self) -> PineconeClient:
        # Single home for client construction, reused by ingestion and the recipe probe.
        return PineconeClient(self)

    @classmethod
    def probe_hierarchy(cls) -> List[ProbeNodeKind]:
        # Structural only — must not connect (see ProbeCapableConfig).
        from datahub.ingestion.source.pinecone.pinecone_probe import (
            PINECONE_PROBE_HIERARCHY,
        )

        return PINECONE_PROBE_HIERARCHY

    def list_probe_children(self, parent_path: List[str], limit: int) -> ProbeResult:
        from datahub.ingestion.source.pinecone.pinecone_probe import (
            list_pinecone_children,
        )

        return list_pinecone_children(self, parent_path, limit)
