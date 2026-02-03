"""Configuration for DataHub Documents Source."""

from typing import Literal, Optional

from pydantic import Field, field_validator

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.datahub_documents.document_chunking_state_handler import (
    DocumentChunkingStatefulIngestionConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.unstructured.chunking_config import (
    ChunkingConfig,
    DataHubConnectionConfig,
    EmbeddingConfig,
)


class EventModeConfig(ConfigModel):
    """Event-driven mode configuration."""

    enabled: bool = Field(
        default=False,
        description="Enable event-driven mode (polls MCL events instead of GraphQL batch)",
    )
    consumer_id: Optional[str] = Field(
        default=None,
        description="Consumer ID for offset tracking (defaults to 'datahub-documents-{pipeline_name}')",
    )
    topics: list[str] = Field(
        default=["MetadataChangeLog_Versioned_v1"],
        description="Topics to consume for document changes",
    )
    lookback_days: Optional[int] = Field(
        default=None,
        description="Number of days to look back for events on first run (None means start from latest)",
    )
    reset_offsets: bool = Field(
        default=False,
        description="Reset consumer offsets to start from beginning",
    )
    idle_timeout_seconds: int = Field(
        default=30,
        description="Exit after this many seconds with no new events (incremental batch mode)",
    )
    poll_timeout_seconds: int = Field(
        default=2, description="Timeout for each poll request"
    )
    poll_limit: int = Field(
        default=100, description="Maximum number of events to fetch per poll"
    )


class IncrementalConfig(ConfigModel):
    """Incremental processing configuration."""

    enabled: bool = Field(
        default=True,
        description="Only process documents whose text content has changed (tracks content hash). "
        "Uses stateful ingestion when enabled. The state_file_path option is deprecated and ignored "
        "when stateful ingestion is enabled.",
    )
    state_file_path: Optional[str] = Field(
        default=None,
        description="[DEPRECATED] Path to state file. This option is ignored when stateful ingestion is enabled. "
        "State is now managed through DataHub's stateful ingestion framework.",
    )
    force_reprocess: bool = Field(
        default=False,
        description="Force reprocess all documents regardless of content hash",
    )


class DataHubDocumentsSourceConfig(
    StatefulIngestionConfigBase[DocumentChunkingStatefulIngestionConfig]
):
    """Configuration for DataHub Documents Source."""

    # DataHub connection
    datahub: DataHubConnectionConfig = Field(
        default_factory=DataHubConnectionConfig,
        description="DataHub connection configuration",
    )

    # Platform filtering (multi-platform support)
    platform_filter: Optional[list[str]] = Field(
        default=None,
        description="Filter documents by platforms. "
        "Default (None): Process all NATIVE documents (sourceType=NATIVE) regardless of platform. "
        "To include external documents from specific platforms, add them here (e.g., ['notion', 'confluence']). "
        "This will process NATIVE documents + EXTERNAL documents from the specified platforms. "
        "Use ['*'] or ['ALL'] to process all documents regardless of source type or platform.",
    )

    # Optional URN filtering
    document_urns: Optional[list[str]] = Field(
        default=None,
        description="Specific document URNs to process (if None, process all matching platforms)",
    )

    # Mode selection
    event_mode: EventModeConfig = Field(
        default_factory=EventModeConfig,
        description="Event-driven mode configuration (polls Kafka MCL events)",
    )

    # Incremental processing
    incremental: IncrementalConfig = Field(
        default_factory=IncrementalConfig,
        description="Incremental processing configuration (skip unchanged documents)",
    )

    # Chunking configuration
    chunking: ChunkingConfig = Field(
        default_factory=ChunkingConfig,
        description="Text chunking strategy configuration",
    )

    # Embedding configuration
    embedding: EmbeddingConfig = Field(
        default_factory=EmbeddingConfig,
        description="Embedding generation configuration (LiteLLM with Cohere/Bedrock)",
    )

    # Partitioning configuration
    partition_strategy: Literal["markdown"] = Field(
        default="markdown",
        description="Text partitioning strategy. Currently only 'markdown' is supported. "
        "This field is included in the document hash to trigger reprocessing if the strategy changes.",
    )

    # Processing options
    skip_empty_text: bool = Field(
        default=True, description="Skip documents with no text content"
    )
    min_text_length: int = Field(
        default=50,
        description="Minimum text length in characters to process (shorter documents are skipped)",
    )

    # Override from base class to enable stateful ingestion by default
    # This ensures stateful ingestion is always available for:
    # - Incremental mode (document hash tracking) in batch mode
    # - Event mode (offset tracking) in event mode
    stateful_ingestion: DocumentChunkingStatefulIngestionConfig = Field(
        default_factory=lambda: DocumentChunkingStatefulIngestionConfig(enabled=True),
        description="Stateful ingestion configuration. Enabled by default to support "
        "incremental mode (document hash tracking) and event mode (offset tracking).",
    )

    @field_validator("platform_filter")
    @classmethod
    def validate_platform_filter(cls, v: list[str]) -> list[str]:
        """Validate platform_filter.

        Empty list is allowed (default - processes all NATIVE documents).
        Non-empty list must contain at least one platform or wildcard.
        """
        # Empty list is valid (default behavior)
        if not v:
            return v
        # Non-empty list must have at least one element
        # (This check is redundant but kept for clarity)
        return v
