"""Configuration for DataHub Documents Source."""

from typing import Literal, Optional

from pydantic import Field, ValidationInfo, field_validator

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


class LockConfig(ConfigModel):
    """Distributed lock configuration to prevent overlapping ingestion runs.

    This source is typically scheduled on a short interval (e.g. every 15 minutes).
    A full scroll + embedding pass can take longer than the schedule interval, so two
    runs could otherwise overlap and redundantly re-embed the same documents. The lock
    uses a ``dataHubStepState`` entity as a shared lease, written synchronously to the
    primary store so concurrent runners observe it immediately.
    """

    enabled: bool = Field(
        default=True,
        description="Acquire a distributed lock before processing so overlapping "
        "scheduled runs do not duplicate scroll + indexing work. When the lock is "
        "held by another run, this run exits cleanly without processing.",
    )
    lock_id: Optional[str] = Field(
        default=None,
        description="Identifier for the lock's dataHubStepState entity. Defaults to "
        "'datahub-documents-lock-{pipeline_name}'. Runs sharing a lock_id are mutually "
        "exclusive; give unrelated pipelines distinct ids.",
    )
    lock_ttl_seconds: int = Field(
        default=1800,
        gt=0,
        description="Lease duration in seconds. The lock holder renews its lease "
        "periodically (see lock_renewal_interval_seconds) while it runs, so this only "
        "needs to exceed the renewal interval (with margin for transient write failures), "
        "NOT the total run duration. A lease not renewed within this window is treated as "
        "stale (e.g. from a crashed run) and may be taken over. After a crash, future runs "
        "are blocked for at most this long.",
    )
    lock_renewal_interval_seconds: int = Field(
        default=300,
        gt=0,
        description="How often the lock holder renews (extends) its lease while running. "
        "Must be comfortably smaller than lock_ttl_seconds. This lets a job run for hours "
        "with a short TTL: the lease stays alive as long as the run is healthy.",
    )

    @field_validator("lock_renewal_interval_seconds")
    @classmethod
    def validate_renewal_below_ttl(cls, v: int, info: ValidationInfo) -> int:
        ttl = info.data.get("lock_ttl_seconds")
        if ttl is not None and v >= ttl:
            raise ValueError(
                f"lock_renewal_interval_seconds ({v}) must be smaller than "
                f"lock_ttl_seconds ({ttl}); otherwise the lease can expire between "
                "renewals. A ratio of ~4-6x (ttl >= 4 * renewal) is recommended."
            )
        return v


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
        description="DataHub connection configuration. Only used when running standalone "
        "(e.g., CLI ingestion). In managed ingestion (deployed sources), the connection "
        "is automatically configured from the sink.",
    )

    # Platform filtering (multi-platform support)
    platform_filter: Optional[list[str]] = Field(
        default=None,
        description="Filter documents by platforms. "
        "Default (None): Process all NATIVE documents (sourceType=NATIVE) regardless of platform. "
        "EXTERNAL documents are also processed by default (see include_external_documents); "
        "set platform_filter to restrict EXTERNAL documents to specific platforms (e.g., ['notion', 'confluence']). "
        "Use ['*'] or ['ALL'] to process all documents regardless of source type or platform.",
    )

    # External document handling
    include_external_documents: bool = Field(
        default=True,
        description="Index EXTERNAL documents (sourceType=EXTERNAL), not just NATIVE ones. "
        "When platform_filter is set, EXTERNAL documents are still restricted to those "
        "platforms; when platform_filter is empty, all EXTERNAL documents are included. "
        "Set to False to restore NATIVE-only behavior.",
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
        description="Embedding generation configuration (Bedrock, Cohere, OpenAI, or Vertex AI)",
    )

    max_documents: int = Field(
        default=10000,
        ge=-1,
        description="Maximum number of documents to process per ingestion run. "
        "The job will stop and fail with an error once this limit is reached. "
        "Set to 0 or -1 to disable the limit.",
    )

    # Scroll pagination tuning (batch mode)
    scroll_batch_size: int = Field(
        default=1000,
        gt=0,
        description="Number of documents to fetch per scrollAcrossEntities page in batch mode. "
        "scrollAcrossEntities uses cursor-based pagination, so all documents are fetched across "
        "pages regardless of this value (no 10k Elasticsearch window limit). Tune for throughput "
        "vs. memory/load on GMS.",
    )
    scroll_delay_seconds: float = Field(
        default=0.0,
        ge=0,
        description="Delay in seconds between consecutive scroll page requests in batch mode. "
        "Use a small delay to reduce read load on GMS/Elasticsearch when scrolling large document sets; "
        "set to 0 to disable.",
    )
    index_delay_seconds: float = Field(
        default=0.025,
        ge=0,
        description="Delay in seconds after each document that is actually indexed (embeddings "
        "emitted). Skipped documents (unchanged, externally owned, empty, etc.) are not delayed. "
        "Defaults to 0.025s to smooth write load during bootstrap without unduly slowing large "
        "backfills (e.g. ~42 min of added pacing for 100k new documents); set to 0 to disable. "
        "Sub-second values are supported.",
    )

    # Skip EXTERNAL documents that are already semantically indexed by their own source
    skip_external_if_semantic_content_exists: bool = Field(
        default=True,
        description="For EXTERNAL documents only: before processing, check whether the document "
        "already has a semanticContent aspect for the active embedding model and, if so, skip it "
        "and leave it out of our incremental state. Some ingestion sources (e.g. Notion, "
        "Confluence) perform their own semantic indexing, so an existing aspect means that source "
        "owns the document. This source then only picks up EXTERNAL documents the source did NOT "
        "index and takes ownership of them. NATIVE documents are never skipped this way. Adds one "
        "metadata read per candidate EXTERNAL document not already in state. Incremental "
        "content-hash skipping still applies once a document is owned. Set to False to always "
        "re-embed EXTERNAL documents.",
    )

    # Locking (prevent overlapping scheduled runs)
    locking: LockConfig = Field(
        default_factory=LockConfig,
        description="Distributed lock configuration to prevent overlapping scheduled runs "
        "from duplicating scroll + indexing work.",
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
