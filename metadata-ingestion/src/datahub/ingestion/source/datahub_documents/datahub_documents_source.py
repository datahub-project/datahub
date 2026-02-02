"""DataHub Documents Source.

This source:
1. Fetches Document entities from DataHub (via GraphQL or MCL events)
2. Partitions Document.text as markdown using unstructured.io
3. Chunks text using semantic chunking strategies
4. Generates embeddings using LiteLLM (Cohere/Bedrock)
5. Emits SemanticContent aspects to DataHub

Supports both batch (GraphQL) and event-driven (Kafka MCL) modes.
"""

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, cast

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.source.datahub_documents.datahub_documents_config import (
    DataHubDocumentsSourceConfig,
)
from datahub.ingestion.source.datahub_documents.document_chunking_state_handler import (
    DocumentChunkingStateHandler,
)
from datahub.ingestion.source.datahub_documents.text_partitioner import TextPartitioner
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.unstructured.chunking_config import (
    get_semantic_search_config,
)
from datahub.ingestion.source.unstructured.event_consumer import DocumentEventConsumer
from datahub.metadata.urns import DocumentUrn
from datahub.sdk.document import Document

logger = logging.getLogger(__name__)


class DataHubDocumentsReport(StatefulIngestionReport):
    """Report for DataHub documents source."""

    num_documents_fetched: int = 0
    num_documents_processed: int = 0
    num_documents_skipped: int = 0
    num_documents_skipped_unchanged: int = 0
    num_documents_skipped_empty: int = 0
    num_chunks_created: int = 0
    num_embeddings_generated: int = 0
    processing_errors: list[str] = []

    def report_document_fetched(self) -> None:
        self.num_documents_fetched += 1

    def report_document_processed(self, num_chunks: int) -> None:
        self.num_documents_processed += 1
        self.num_chunks_created += num_chunks

    def report_document_skipped(self) -> None:
        self.num_documents_skipped += 1

    def report_document_skipped_unchanged(self) -> None:
        """Report document skipped due to unchanged content hash."""
        self.num_documents_skipped_unchanged += 1

    def report_document_skipped_empty(self) -> None:
        """Report document skipped due to empty or too-short text."""
        self.num_documents_skipped_empty += 1

    def report_embeddings_generated(self, count: int) -> None:
        self.num_embeddings_generated += count

    def report_error(self, error: str) -> None:
        self.processing_errors.append(error)


@platform_name("DataHubDocuments", id="datahub-documents")
@support_status(SupportStatus.INCUBATING)
@config_class(DataHubDocumentsSourceConfig)
class DataHubDocumentsSource(StatefulIngestionSourceBase):
    """
    This source extracts Document entities from DataHub and generates semantic embeddings.

    It supports:
    - **Batch mode**: Fetches documents via GraphQL
    - **Event-driven mode**: Processes documents in real-time from Kafka MCL events (recommended)
    - **Incremental processing**: Only reprocesses documents when content changes
    - **Smart defaults**: Auto-configures connection, chunking, and embeddings from server

    The minimal configuration requires just `config: {}` when using environment variables
    (DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN) and will automatically align with your server's
    semantic search configuration.

    **Prerequisites:** Before using this source, configure semantic search on your DataHub server.
    See the [Semantic Search Configuration Guide](../../how-to/semantic-search-configuration) for setup instructions.
    """

    def __init__(self, ctx: PipelineContext, config: DataHubDocumentsSourceConfig):
        """Initialize DataHubDocumentsSource.

        Args:
            ctx: Pipeline context
            config: Source configuration
        """
        # Cast config to base type for super().__init__ call
        # This is safe because DocumentChunkingStatefulIngestionConfig is a subclass of StatefulIngestionConfig,
        # and StatefulIngestionConfigBase uses invariant generics, so mypy requires explicit cast
        base_config = cast(StatefulIngestionConfigBase[StatefulIngestionConfig], config)
        super().__init__(base_config, ctx)
        self.config = config
        # Override report with our custom report type
        # Type annotation ensures mypy knows the correct type
        self.report: DataHubDocumentsReport = DataHubDocumentsReport()

        # Initialize state handler for stateful ingestion
        self.state_handler: Optional[DocumentChunkingStateHandler] = (
            DocumentChunkingStateHandler(
                source=self,
                config=self.config,
                pipeline_name=self.ctx.pipeline_name,
                run_id=self.ctx.run_id,
            )
            if self.state_provider.is_stateful_ingestion_configured()
            else None
        )

        # Initialize DataHub client
        graph_config = DatahubClientConfig(
            server=self.config.datahub.server,
            token=self.config.datahub.token,
        )
        self.graph = DataHubGraph(config=graph_config)

        # Load/validate embedding configuration from server
        if not self.config.embedding.allow_local_embedding_config:
            logger.info("Loading embedding configuration from DataHub server...")

            try:
                server_config = get_semantic_search_config(self.graph)

                # Check if semantic search is enabled
                if not server_config.enabled:
                    raise ValueError(
                        "Semantic search is not enabled on the DataHub server. "
                        "Cannot proceed with embedding generation. "
                        "Please enable semantic search in the server's application.yml configuration."
                    )

                # Check if user provided local config (override flow)
                if self.config.embedding.has_local_config():
                    logger.info(
                        "Validating local embedding configuration against server..."
                    )

                    # Validate local config matches server
                    self.config.embedding.validate_against_server(
                        server_config.embedding_config
                    )

                    logger.info(
                        "✓ Local embedding configuration validated successfully"
                        f"\n  Provider: {server_config.embedding_config.provider}"
                        f"\n  Model: {server_config.embedding_config.model_id}"
                    )

                else:
                    # Default flow: Load config from server
                    logger.info(
                        "No local embedding config provided - using server configuration..."
                    )

                    # Preserve any local credentials (api_key) that user may have set
                    local_api_key = self.config.embedding.api_key

                    # Replace embedding config with server config
                    from datahub.ingestion.source.unstructured.chunking_config import (
                        EmbeddingConfig,
                    )

                    self.config.embedding = EmbeddingConfig.from_server(
                        server_config.embedding_config,
                        api_key=local_api_key,
                    )

                    logger.info(
                        "✓ Loaded embedding configuration from server"
                        f"\n  Provider: {server_config.embedding_config.provider}"
                        f"\n  Model: {server_config.embedding_config.model_id}"
                        f"\n  Model Embedding Key: {server_config.embedding_config.model_embedding_key}"
                        f"\n  AWS Region: {server_config.embedding_config.aws_region or 'N/A'}"
                    )

            except Exception as e:
                # Check if this is an old server that doesn't support semantic search config API
                error_str = str(e)
                is_old_server = (
                    "does not expose semantic search configuration" in error_str
                    or "semanticSearchConfig" in error_str  # GraphQL validation error
                )

                if is_old_server and self.config.embedding.has_local_config():
                    # Backward compatibility: Old server + local config provided
                    logger.warning(
                        f"⚠️  Server does not support semantic search configuration API (likely older version). "
                        f"Falling back to local embedding configuration.\n"
                        f"  Provider: {self.config.embedding.provider}\n"
                        f"  Model: {self.config.embedding.model}\n"
                        f"  AWS Region: {self.config.embedding.aws_region or 'N/A'}\n\n"
                        f"Note: Semantic search on the server may not work if the configuration doesn't match. "
                        f"Consider upgrading to DataHub v0.14.0+ for automatic configuration sync."
                    )
                    # Continue with local config (don't raise)
                elif is_old_server:
                    # Old server but no local config - can't proceed
                    raise ValueError(
                        "Server does not support semantic search configuration API (likely older version).\n"
                        "To use this source with an older server, you must provide local embedding configuration:\n\n"
                        "  embedding:\n"
                        "    provider: bedrock  # or cohere\n"
                        "    model: cohere.embed-english-v3\n"
                        "    aws_region: us-west-2\n\n"
                        "Or upgrade your DataHub server to v0.14.0+ for automatic configuration sync."
                    ) from e
                else:
                    # Other error (semantic search disabled, validation failed, etc.)
                    logger.error(
                        f"Failed to load/validate embedding configuration from server: {e}"
                    )
                    raise ValueError(
                        f"Cannot proceed with embedding generation - server configuration failed.\n"
                        f"Error: {e}\n\n"
                        f"To bypass server validation (NOT RECOMMENDED), set:\n"
                        f"  embedding:\n"
                        f"    allow_local_embedding_config: true"
                    ) from e

        else:
            logger.warning(
                "⚠️  WARNING: Server validation is disabled (allow_local_embedding_config=true). "
                "Proceeding with local embedding configuration. "
                "This may cause semantic search to fail if configuration doesn't match the server."
            )

        # Initialize text partitioner
        self.text_partitioner = TextPartitioner()

        # Initialize embedding model name for litellm
        if self.config.embedding.provider == "bedrock":
            self.embedding_model = f"bedrock/{self.config.embedding.model}"
        else:  # cohere
            self.embedding_model = f"cohere/{self.config.embedding.model}"

        # Initialize state tracking for incremental mode
        self.document_state: dict[str, dict[str, Any]] = {}
        self.state_file_path: Optional[Path] = None

        if self.config.incremental.enabled:
            self._initialize_state_tracking()

        logger.info(
            f"Initialized DataHubDocumentsSource with platforms: {self.config.platform_filter}, "
            f"mode: {'event-driven' if self.config.event_mode.enabled else 'batch'}, "
            f"incremental: {self.config.incremental.enabled}"
        )

    def _initialize_state_tracking(self) -> None:
        """Initialize state tracking for incremental mode."""
        if self.config.incremental.state_file_path:
            self.state_file_path = Path(self.config.incremental.state_file_path)
        else:
            # Default: ~/.datahub/document_chunking_state/{pipeline_name}.json
            state_dir = Path.home() / ".datahub" / "document_chunking_state"
            state_dir.mkdir(parents=True, exist_ok=True)
            pipeline_name = self.ctx.pipeline_name or "default"
            self.state_file_path = state_dir / f"{pipeline_name}.json"

        # Load existing state
        self._load_state()
        logger.info(
            f"Incremental mode enabled, state file: {self.state_file_path}, "
            f"tracking {len(self.document_state)} documents"
        )

    def _load_state(self) -> None:
        """Load state from file."""
        if self.state_file_path and self.state_file_path.exists():
            try:
                with open(self.state_file_path) as f:
                    self.document_state = json.load(f)
                logger.debug(f"Loaded state for {len(self.document_state)} documents")
            except Exception as e:
                logger.warning(f"Failed to load state file: {e}")
                self.document_state = {}

    def _save_state(self) -> None:
        """Save state to file."""
        if self.state_file_path:
            try:
                with open(self.state_file_path, "w") as f:
                    json.dump(self.document_state, f, indent=2)
                logger.debug(f"Saved state for {len(self.document_state)} documents")
            except Exception as e:
                logger.warning(f"Failed to save state file: {e}")

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Main entry point - route to batch or event mode."""
        if self.config.event_mode.enabled:
            yield from self._process_event_mode()
        else:
            yield from self._process_batch_mode()

        # Save state after processing
        if self.config.incremental.enabled:
            self._save_state()

    def _process_batch_mode(self) -> Iterable[MetadataWorkUnit]:
        """Process documents using GraphQL search (batch mode)."""
        logger.info("Running in batch mode")

        # Fetch documents from DataHub
        documents = self._fetch_documents_graphql()

        # Process each document
        for doc in documents:
            # Check if we should process this document (incremental mode)
            if (
                self.config.incremental.enabled
                and not self.config.incremental.force_reprocess
            ):
                if not self._should_process(doc["urn"], doc.get("text", "")):
                    logger.debug(
                        f"Skipping document {doc['urn']} (unchanged content hash)"
                    )
                    self.report.report_document_skipped_unchanged()
                    continue

            # Process document and yield workunits
            yield from self._process_single_document(doc)

            # Update state after successful processing
            if self.config.incremental.enabled:
                self._update_document_state(doc["urn"], doc.get("text", ""))

    def _bootstrap_event_mode_offsets(self, consumer_id: str) -> None:
        """Bootstrap event mode by capturing current offsets BEFORE batch mode.

        This prevents race conditions: batch mode processes all documents up to the
        captured offset, and subsequent runs continue from that offset.

        Args:
            consumer_id: Consumer ID for the event consumer
        """
        if not self.state_handler or not self.state_handler.is_checkpointing_enabled():
            return

        logger.info(
            "Bootstrapping event mode: capturing current offsets BEFORE batch processing..."
        )
        # Create temporary consumer to get current offsets
        bootstrap_consumer = DocumentEventConsumer(
            graph=self.graph,
            consumer_id=consumer_id,
            topics=self.config.event_mode.topics,
            lookback_days=None,  # No lookback - just get current position
            reset_offsets=False,
            idle_timeout_seconds=5,
            poll_timeout_seconds=2,
            poll_limit=1,  # Minimal poll - we just want the offset
            state_handler=None,  # Don't need state handler for bootstrap
        )
        try:
            for topic in self.config.event_mode.topics:
                current_offset = bootstrap_consumer.get_current_offset(topic)
                if current_offset:
                    self.state_handler.update_event_offset(topic, current_offset)
                    logger.info(
                        f"Captured current offset for {topic}: {current_offset} "
                        "(before batch processing)"
                    )
                else:
                    logger.warning(
                        f"Failed to capture current offset for {topic} - "
                        "bootstrap incomplete, but batch mode will still process documents"
                    )
        except Exception as e:
            logger.warning(
                f"Error during bootstrap offset capture: {e}. "
                "Batch mode will still process documents.",
                exc_info=True,
            )
        finally:
            bootstrap_consumer.close()

    def _process_single_event(
        self, event: dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        """Process a single MCL event and yield work units if document should be processed.

        Args:
            event: MCL event dictionary with entityUrn, aspectName, aspect, etc.

        Yields:
            MetadataWorkUnit objects for the document if it should be processed, empty otherwise
        """
        entity_urn = event.get("entityUrn")
        aspect_name = event.get("aspectName")
        aspect_data = event.get("aspect")

        if not entity_urn or not aspect_name:
            logger.debug("Event missing entityUrn or aspectName, skipping")
            return

        # Filter for documentInfo aspect
        if aspect_name != "documentInfo":
            return

        # Parse aspect data (handles Avro union wrapping)
        try:
            aspect_dict = self._parse_mcl_aspect(aspect_data)
        except Exception as e:
            logger.warning(
                f"Failed to parse MCL aspect for {entity_urn}: {e}",
                exc_info=True,
            )
            return

        # Extract text from documentInfo
        contents = aspect_dict.get("contents", {})
        text = contents.get("text", "")

        if not text:
            logger.debug(f"No text content in document {entity_urn}")
            return

        # Filter by source type (NATIVE vs EXTERNAL)
        source = aspect_dict.get("source", {})
        source_type = source.get("sourceType")
        # Default to NATIVE if sourceType is not set (backward compatibility with old documents)
        if source_type is None:
            source_type = "NATIVE"

        # Determine if we should process this document based on source type and platform filter
        should_process = self._should_process_by_source_type_event(
            source_type, aspect_dict, entity_urn
        )
        if not should_process:
            return

        # Check if we should process (incremental mode)
        if (
            self.config.incremental.enabled
            and not self.config.incremental.force_reprocess
        ):
            if not self._should_process(entity_urn, text):
                logger.debug(f"Skipping document {entity_urn} (unchanged content hash)")
                self.report.report_document_skipped_unchanged()
                return

        # Build document dict
        doc = {"urn": entity_urn, "text": text}
        self.report.report_document_fetched()

        # Process document and yield work units
        yield from self._process_single_document(doc)

        # Update state
        if self.config.incremental.enabled:
            self._update_document_state(entity_urn, text)

    def _process_event_mode(self) -> Iterable[MetadataWorkUnit]:
        """Process documents using Kafka MCL events (event-driven mode).

        Automatically falls back to batch mode if:
        - State not present (first run)
        - Event API errors occur
        - Offset loading fails
        - State handler unavailable

        When falling back, bootstraps by capturing current offset BEFORE batch mode
        to prevent race conditions.
        """
        logger.info("Running in event-driven mode")

        # Set consumer_id if not provided
        consumer_id = self.config.event_mode.consumer_id or (
            f"datahub-documents-{self.ctx.pipeline_name or 'default'}"
        )

        # Check if we should fall back to batch mode before starting
        should_fallback = self._should_fallback_to_batch_mode()
        if should_fallback:
            logger.warning(
                "Event mode conditions not met, falling back to batch mode. "
                "This ensures all documents are processed even if event mode is unavailable."
            )

            # BEFORE batch mode - capture current offset to prevent race conditions
            self._bootstrap_event_mode_offsets(consumer_id)

            # NOW run batch mode - processes all documents up to the captured offset
            yield from self._process_batch_mode()

            if self.state_handler and self.state_handler.is_checkpointing_enabled():
                logger.info(
                    "Event mode bootstrapping complete. Next run will continue from captured offset."
                )
            return

        # Initialize event consumer with state handler for offset persistence
        event_consumer = DocumentEventConsumer(
            graph=self.graph,
            consumer_id=consumer_id,
            topics=self.config.event_mode.topics,
            lookback_days=self.config.event_mode.lookback_days,
            reset_offsets=self.config.event_mode.reset_offsets,
            idle_timeout_seconds=self.config.event_mode.idle_timeout_seconds,
            poll_timeout_seconds=self.config.event_mode.poll_timeout_seconds,
            poll_limit=self.config.event_mode.poll_limit,
            state_handler=self.state_handler,
        )

        try:
            # Track if we've processed any events
            events_processed = False

            # Process events
            # consume_events() already yields parsed MCL dicts with entityUrn, aspectName, etc.
            for event in event_consumer.consume_events():
                events_processed = True
                yield from self._process_single_event(event)

            # If no events were processed and no lookback window, fall back to batch
            if not events_processed and not self.config.event_mode.lookback_days:
                logger.warning(
                    "No events processed in event mode and no lookback window set. "
                    "Falling back to batch mode to ensure documents are processed."
                )
                yield from self._process_batch_mode()
                return

        except Exception as e:
            # Catch any errors during event processing and fall back to batch mode
            error_msg = str(e)
            logger.error(
                f"Error in event mode: {error_msg}. "
                "Falling back to batch mode to ensure documents are processed.",
                exc_info=True,
            )
            self.report.report_error(f"Event mode error: {error_msg}")

            # Fall back to batch mode
            yield from self._process_batch_mode()
            return

        finally:
            if event_consumer:
                event_consumer.close()

    def _parse_mcl_aspect(self, aspect_data: Any) -> dict[str, Any]:
        """Parse MCL aspect data (handles Avro union wrapping)."""
        if isinstance(aspect_data, dict):
            # Check for Avro union wrapper
            if "com.linkedin.pegasus2avro.mxe.GenericAspect" in aspect_data:
                generic_aspect = aspect_data[
                    "com.linkedin.pegasus2avro.mxe.GenericAspect"
                ]
                value_str = generic_aspect.get("value", "{}")
                return json.loads(value_str)
            return aspect_data
        elif isinstance(aspect_data, str):
            return json.loads(aspect_data)
        else:
            return {}

    def _extract_platform_from_aspect(
        self, aspect_dict: dict[str, Any]
    ) -> Optional[str]:
        """Extract platform name from documentInfo aspect."""
        # Try to get from dataPlatformInstance
        platform_instance = aspect_dict.get("dataPlatformInstance", {})
        platform_urn = platform_instance.get("platform")
        if platform_urn and isinstance(platform_urn, str):
            # Extract platform name from URN (e.g., "urn:li:dataPlatform:notion" → "notion")
            parts = platform_urn.split(":")
            if len(parts) >= 3:
                return parts[-1]
        return None

    def _should_fallback_to_batch_mode(self) -> bool:
        """Check if we should fall back to batch mode instead of event mode.

        Returns True if:
        - State handler not available (stateful ingestion not configured)
        - No offsets found and no lookback window (first run scenario)
        - Stateful ingestion disabled
        """
        # If stateful ingestion is not enabled, we can't track offsets reliably
        if not self.state_handler or not self.state_handler.is_checkpointing_enabled():
            logger.info(
                "Stateful ingestion not enabled - cannot track event offsets reliably. "
                "Falling back to batch mode."
            )
            return True

        # Check if we have any offsets stored
        # If no offsets and no lookback window, it's likely a first run
        has_offsets = False
        for topic in self.config.event_mode.topics:
            offset = self.state_handler.get_event_offset(topic)
            if offset:
                has_offsets = True
                break

        if not has_offsets and not self.config.event_mode.lookback_days:
            logger.info(
                "No event offsets found and no lookback window configured. "
                "This appears to be a first run - falling back to batch mode to process all documents."
            )
            return True

        return False

    def _fetch_platform_from_entity(self, entity_urn: str) -> Optional[str]:
        """Fetch the platform name for a given entity URN via GraphQL.

        In event mode, we only receive documentInfo aspect changes, which doesn't include
        dataPlatformInstance. We need to fetch it separately to determine the platform.

        Args:
            entity_urn: Document URN

        Returns:
            Platform name (e.g., "notion", "confluence") or None if not found
        """
        try:
            query = """
            query getDocumentPlatform($urn: String!) {
                entity(urn: $urn) {
                    ... on Document {
                        dataPlatformInstance {
                            platform {
                                urn
                            }
                        }
                    }
                }
            }
            """
            response = self.graph.execute_graphql(query, {"urn": entity_urn})
            entity = response.get("entity", {})
            platform_instance = entity.get("dataPlatformInstance", {})
            platform = platform_instance.get("platform", {})
            platform_urn = platform.get("urn")

            if platform_urn and isinstance(platform_urn, str):
                # Extract platform name from URN (e.g., "urn:li:dataPlatform:notion" → "notion")
                parts = platform_urn.split(":")
                if len(parts) >= 3:
                    return parts[-1]
            return None
        except Exception as e:
            logger.debug(f"Failed to fetch platform for {entity_urn}: {e}")
            return None

    def _should_process_by_source_type_event(
        self, source_type: Optional[str], aspect_dict: dict[str, Any], entity_urn: str
    ) -> bool:
        """Determine if document should be processed in event mode based on source type and platform filter.

        Logic:
        - NATIVE documents: Always process if platform_filter is empty, or if platform matches
        - EXTERNAL documents: Only process if their platform is in platform_filter

        Args:
            source_type: Document source type ("NATIVE" or "EXTERNAL")
            aspect_dict: Parsed documentInfo aspect dictionary
            entity_urn: Document URN for logging and fetching platform

        Returns:
            True if document should be processed, False otherwise
        """
        # Wildcard means process everything
        if self.config.platform_filter and (
            "*" in self.config.platform_filter or "ALL" in self.config.platform_filter
        ):
            return True

        # NATIVE documents: Process if platform_filter is empty (all native) or matches platform
        if source_type == "NATIVE":
            # Empty platform_filter means "all native documents"
            if not self.config.platform_filter:
                return True
            # If platform_filter is specified, check if document's platform matches
            # Native documents typically have platform="datahub" or no platform
            # Note: documentInfo doesn't contain platform, but for NATIVE we can default to "datahub"
            # or fetch it if needed. For simplicity, we'll default to "datahub" for NATIVE.
            platform_name = "datahub"  # Default for native documents
            return platform_name in self.config.platform_filter

        # EXTERNAL documents: Only process if their platform is in platform_filter
        if source_type == "EXTERNAL":
            # Empty platform_filter means "no external documents"
            if not self.config.platform_filter:
                logger.debug(
                    f"Skipping document {entity_urn} (sourceType=EXTERNAL, platform_filter is empty - only processing NATIVE documents)"
                )
                return False
            # documentInfo aspect doesn't contain platform - need to fetch it via GraphQL
            external_platform = self._fetch_platform_from_entity(entity_urn)
            if (
                external_platform is not None
                and external_platform in self.config.platform_filter
            ):
                return True
            logger.debug(
                f"Skipping document {entity_urn} (sourceType=EXTERNAL, platform={external_platform} not in platform_filter: {self.config.platform_filter})"
            )
            return False

        # Unknown source type - default to not processing
        logger.debug(
            f"Skipping document {entity_urn} (unknown sourceType: {source_type})"
        )
        return False

    def _should_process_by_source_type(
        self, entity: dict[str, Any], info: dict[str, Any]
    ) -> bool:
        """Determine if document should be processed in batch mode based on source type and platform filter.

        Logic:
        - NATIVE documents: Always process if platform_filter is empty, or if platform matches
        - EXTERNAL documents: Only process if their platform is in platform_filter

        Args:
            entity: GraphQL entity response (includes dataPlatformInstance)
            info: Document info from GraphQL (includes source)

        Returns:
            True if document should be processed, False otherwise
        """
        # Wildcard means process everything
        if self.config.platform_filter and (
            "*" in self.config.platform_filter or "ALL" in self.config.platform_filter
        ):
            return True

        # Extract source type from info
        source = info.get("source", {})
        source_type = source.get("sourceType")
        # Default to NATIVE if sourceType is not set (backward compatibility with old documents)
        if source_type is None:
            source_type = "NATIVE"

        # NATIVE documents: Process if platform_filter is empty (all native) or matches platform
        if source_type == "NATIVE":
            # Empty platform_filter means "all native documents"
            if not self.config.platform_filter:
                return True
            # If platform_filter is specified, check if document's platform matches
            # Extract platform from entity (dataPlatformInstance)
            platform_name = self._extract_platform_from_entity(entity)
            if platform_name is None:
                platform_name = "datahub"  # Default for native documents
            return platform_name in self.config.platform_filter

        # EXTERNAL documents: Only process if their platform is in platform_filter
        if source_type == "EXTERNAL":
            # Empty platform_filter means "no external documents"
            if not self.config.platform_filter:
                logger.debug(
                    f"Skipping document {entity.get('urn', 'unknown')} (sourceType=EXTERNAL, platform_filter is empty - only processing NATIVE documents)"
                )
                return False
            # Extract platform from entity (dataPlatformInstance)
            external_platform = self._extract_platform_from_entity(entity)
            if (
                external_platform is not None
                and external_platform in self.config.platform_filter
            ):
                return True
            logger.debug(
                f"Skipping document {entity.get('urn', 'unknown')} (sourceType=EXTERNAL, platform={external_platform} not in platform_filter: {self.config.platform_filter})"
            )
            return False

        # Unknown source type - default to not processing
        logger.debug(
            f"Skipping document {entity.get('urn', 'unknown')} (unknown sourceType: {source_type})"
        )
        return False

    def _extract_platform_from_entity(self, entity: dict[str, Any]) -> Optional[str]:
        """Extract platform name from GraphQL entity response."""
        # Try to get from dataPlatformInstance
        platform_instance = entity.get("dataPlatformInstance", {})
        platform = platform_instance.get("platform", {})
        platform_urn = platform.get("urn")
        if platform_urn and isinstance(platform_urn, str):
            # Extract platform name from URN (e.g., "urn:li:dataPlatform:notion" → "notion")
            parts = platform_urn.split(":")
            if len(parts) >= 3:
                return parts[-1]
        return None

    def _fetch_documents_graphql(self) -> list[dict[str, Any]]:
        """Fetch Document entities from DataHub using GraphQL."""
        query = """
        query listDocuments($input: SearchInput!) {
          search(input: $input) {
            start
            count
            total
            searchResults {
              entity {
                urn
                type
                ... on Document {
                  info {
                    contents {
                      text
                    }
                    customProperties {
                      key
                      value
                    }
                    source {
                      sourceType
                    }
                  }
                  dataPlatformInstance {
                    platform {
                      urn
                    }
                  }
                }
              }
            }
          }
        }
        """

        # Build search input with optional multi-platform filter
        search_input: dict[str, Any] = {
            "type": "DOCUMENT",
            "query": "*",
            "start": 0,
            "count": 1000,  # Fetch in batches
        }

        # Only add platform filter if specific platforms are provided
        # Empty list or wildcard ("*", "ALL") means no GraphQL filter (client-side filtering instead)
        if (
            self.config.platform_filter
            and "*" not in self.config.platform_filter
            and "ALL" not in self.config.platform_filter
        ):
            search_input["filters"] = [
                {
                    "field": "platform",
                    "values": [
                        f"urn:li:dataPlatform:{platform}"
                        for platform in self.config.platform_filter
                    ],
                }
            ]

        variables = {"input": search_input}

        try:
            response = self.graph.execute_graphql(query, variables)
            search_results = response.get("search", {}).get("searchResults", [])

            documents = []
            for result in search_results:
                entity = result.get("entity", {})
                urn = entity.get("urn")

                if not urn:
                    continue

                # Filter by specific URNs if provided
                if self.config.document_urns and urn not in self.config.document_urns:
                    continue

                # Extract text content
                info = entity.get("info", {})
                contents = info.get("contents", {})
                text = contents.get("text", "")

                # Filter by source type (NATIVE vs EXTERNAL) for batch mode
                should_process = self._should_process_by_source_type(entity, info)
                if not should_process:
                    continue

                # Skip if no text or too short
                if not text or (
                    self.config.skip_empty_text
                    and len(text) < self.config.min_text_length
                ):
                    logger.debug(
                        f"Skipping document {urn} (empty or too short: {len(text)} chars)"
                    )
                    continue

                documents.append({"urn": urn, "text": text})
                self.report.report_document_fetched()

            logger.info(
                f"Fetched {len(documents)} documents with text content from platforms: {self.config.platform_filter}"
            )
            return documents

        except Exception as e:
            logger.error(f"Failed to fetch documents from DataHub: {e}", exc_info=True)
            raise

    def _should_process(self, document_urn: str, text: str) -> bool:
        """Check if document should be processed based on content hash."""
        if (
            not self.config.incremental.enabled
            or self.config.incremental.force_reprocess
        ):
            return True

        current_hash = self._calculate_text_hash(text)

        # Use state_handler if available (proper stateful ingestion)
        if self.state_handler and self.state_handler.is_checkpointing_enabled():
            previous_hash = self.state_handler.get_document_hash(document_urn)
            return previous_hash != current_hash

        # Fall back to local document_state (simple incremental mode)
        if document_urn not in self.document_state:
            return True

        previous_hash = self.document_state[document_urn].get("content_hash")
        return previous_hash != current_hash

    def _get_processing_config_fingerprint(self) -> Dict[str, Any]:
        """Extract processing configuration that affects output artifacts.

        This fingerprint is included in the document hash to trigger reprocessing
        when configuration changes (e.g., enabling chunking, changing embedding model).

        Returns:
            Dictionary of config values that affect processing output.
        """
        # Chunking/embedding is enabled when embedding provider is configured
        embedding_enabled = self.config.embedding.provider is not None

        return {
            # Chunking affects chunk boundaries and structure
            "chunking_enabled": embedding_enabled,
            "chunking_strategy": self.config.chunking.strategy
            if embedding_enabled
            else None,
            "chunking_max_characters": self.config.chunking.max_characters
            if embedding_enabled
            else None,
            "chunking_overlap": self.config.chunking.overlap
            if embedding_enabled
            else None,
            "chunking_combine_under": self.config.chunking.combine_text_under_n_chars
            if embedding_enabled
            else None,
            # Embedding affects vector embeddings on chunks
            "embedding_enabled": embedding_enabled,
            "embedding_provider": self.config.embedding.provider
            if embedding_enabled
            else None,
            "embedding_model": self.config.embedding.model
            if embedding_enabled
            else None,
            # Partitioning affects how text is extracted
            "partition_strategy": self.config.partition_strategy,
        }

    def _calculate_text_hash(self, text: str) -> str:
        """Calculate hash of document content AND processing configuration.

        This ensures documents are reprocessed when:
        1. Content changes (text is different)
        2. Processing config changes (chunking enabled, embedding model changed, etc.)

        Args:
            text: Document text content

        Returns:
            SHA256 hash hex string
        """
        hash_input = {
            "content": text,
            "config": self._get_processing_config_fingerprint(),
        }
        # Deterministic JSON serialization
        hash_str = json.dumps(hash_input, sort_keys=True)
        return hashlib.sha256(hash_str.encode("utf-8")).hexdigest()

    def _update_document_state(self, document_urn: str, text: str) -> None:
        """Update state after processing document."""
        content_hash = self._calculate_text_hash(text)
        last_processed = datetime.utcnow().isoformat()

        # Use state_handler if available (proper stateful ingestion)
        if self.state_handler and self.state_handler.is_checkpointing_enabled():
            self.state_handler.update_document_state(
                document_urn, content_hash, last_processed
            )
        else:
            # Fall back to local document_state (simple incremental mode)
            self.document_state[document_urn] = {
                "content_hash": content_hash,
                "last_processed": last_processed,
            }

    def _process_single_document(
        self, doc: dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        """Process a single document: partition text → chunk → embed → emit SemanticContent."""
        try:
            document_urn = doc["urn"]
            text = doc.get("text", "")

            # Skip if empty or too short
            if not text or len(text) < self.config.min_text_length:
                logger.debug(
                    f"Skipping document {document_urn} (text too short: {len(text)} chars)"
                )
                self.report.report_document_skipped_empty()
                return

            # Partition text as markdown into elements
            logger.debug(f"Partitioning text for document {document_urn}")
            elements = self.text_partitioner.partition_text(text)

            if not elements:
                logger.warning(
                    f"No elements created from text for document {document_urn}"
                )
                self.report.report_document_skipped()
                return

            # Chunk the elements
            logger.debug(
                f"Chunking {len(elements)} elements for document {document_urn}"
            )
            chunks = self._chunk_elements(elements)

            if not chunks:
                logger.warning(f"No chunks created for document {document_urn}")
                self.report.report_document_skipped()
                return

            # Generate embeddings
            logger.debug(f"Generating embeddings for {len(chunks)} chunks")
            embeddings = self._generate_embeddings(chunks)

            # Emit SemanticContent aspect to DataHub
            yield from self._emit_semantic_content(document_urn, chunks, embeddings)

            self.report.report_document_processed(len(chunks))
            self.report.report_embeddings_generated(len(embeddings))

            logger.info(
                f"Successfully processed document {document_urn}: {len(chunks)} chunks, {len(embeddings)} embeddings"
            )

        except Exception as e:
            error_msg = f"Failed to process document {doc.get('urn', 'unknown')}: {e}"
            logger.error(error_msg, exc_info=True)
            self.report.report_error(error_msg)

    def _chunk_elements(self, elements: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Chunk elements using Unstructured's chunking strategies."""
        try:
            from unstructured.chunking.basic import chunk_elements as basic_chunk
            from unstructured.chunking.title import chunk_by_title
            from unstructured.staging.base import elements_from_dicts

            # Convert dict elements to Unstructured Element objects
            element_objects = elements_from_dicts(elements)

            # Apply chunking strategy
            if self.config.chunking.strategy == "by_title":
                chunks = chunk_by_title(
                    elements=element_objects,
                    max_characters=self.config.chunking.max_characters,
                    combine_text_under_n_chars=self.config.chunking.combine_text_under_n_chars,
                )
            else:  # basic
                chunks = basic_chunk(
                    elements=element_objects,
                    max_characters=self.config.chunking.max_characters,
                    overlap=self.config.chunking.overlap,
                )

            # Convert chunks back to dicts
            chunk_dicts = [chunk.to_dict() for chunk in chunks]
            logger.debug(
                f"Created {len(chunk_dicts)} chunks using {self.config.chunking.strategy} strategy"
            )
            return chunk_dicts

        except Exception as e:
            logger.error(f"Failed to chunk elements: {e}", exc_info=True)
            return []

    def _generate_embeddings(self, chunks: list[dict[str, Any]]) -> list[list[float]]:
        """Generate embeddings using litellm (supports Bedrock and Cohere)."""
        import litellm  # Lazy import to avoid ModuleNotFoundError during tests

        # Extract text from chunks
        texts = [chunk.get("text", "") for chunk in chunks]

        # Filter out empty texts
        texts = [t for t in texts if t.strip()]

        if not texts:
            return []

        try:
            # Generate embeddings in batches
            embeddings = []
            for i in range(0, len(texts), self.config.embedding.batch_size):
                batch = texts[i : i + self.config.embedding.batch_size]

                # Use litellm.embedding() which works with both Bedrock and Cohere
                response = litellm.embedding(
                    model=self.embedding_model,
                    input=batch,
                    api_key=self.config.embedding.api_key,  # Only used for Cohere
                    aws_region_name=self.config.embedding.aws_region,  # Only used for Bedrock
                )

                # Extract embeddings from response
                batch_embeddings = [data["embedding"] for data in response.data]
                embeddings.extend(batch_embeddings)
                logger.debug(f"Generated {len(batch_embeddings)} embeddings for batch")

            logger.debug(
                f"Generated {len(embeddings)} embeddings using {self.embedding_model}"
            )
            return embeddings

        except Exception as e:
            logger.error(f"Failed to generate embeddings: {e}", exc_info=True)
            raise

    def _emit_semantic_content(
        self,
        document_urn: str,
        chunks: list[dict[str, Any]],
        embeddings: list[list[float]],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit SemanticContent aspect for the document using SDK V2."""
        # Build model version string (e.g., "bedrock/cohere.embed-english-v3")
        assert self.config.embedding.model is not None
        assert self.config.embedding.provider is not None
        model_version = (
            f"{self.config.embedding.provider}/{self.config.embedding.model}"
        )

        # Get model embedding key from config (populated from server or provided locally)
        assert self.config.embedding.model_embedding_key is not None, (
            "model_embedding_key must be set"
        )
        model_key = self.config.embedding.model_embedding_key

        # Create Document entity and set semantic content using SDK V2
        doc_urn = DocumentUrn.from_string(document_urn)
        doc = Document(urn=doc_urn)

        doc.set_semantic_content(
            chunks=chunks,
            embeddings=embeddings,
            model_key=model_key,
            model_version=model_version,
            chunking_strategy=self.config.chunking.strategy,
        )

        logger.debug(
            f"Emitting SemanticContent for {document_urn} with {len(chunks)} chunks"
        )

        # Convert to work units and yield
        yield from doc.as_workunits()

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        """Cleanup resources."""
        if self.config.incremental.enabled:
            self._save_state()
        super().close()
