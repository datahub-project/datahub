"""Document Chunking and Embedding Source.

This source:
1. Fetches documents from DataHub that have stored unstructured_elements
2. Chunks them using Unstructured's semantic chunking strategies
3. Generates embeddings using LiteLLM (Cohere/Bedrock)
4. Emits SemanticContent aspects to DataHub (first-class metadata)

This provides proper semantic chunking compared to the current Java implementation
which generates a single embedding for the entire document.
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import CapabilityReport, Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.ingestion.source.unstructured.chunking_config import (
    DocumentChunkingSourceConfig,
    get_semantic_search_config,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.unstructured.chunking_config import EmbeddingConfig

logger = logging.getLogger(__name__)


@dataclass
class DocumentChunkingReport(SourceReport):
    """Report for document chunking source."""

    num_documents_fetched: int = 0
    num_documents_processed: int = 0
    num_documents_skipped: int = 0
    num_documents_skipped_unchanged: int = 0
    num_chunks_created: int = 0
    num_embeddings_generated: int = 0
    processing_errors: list[str] = field(default_factory=list)

    # Embedding statistics
    num_documents_with_embeddings: int = 0
    num_documents_without_embeddings: int = 0
    num_embedding_failures: int = 0
    embedding_failures: list[str] = field(default_factory=list)

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

    def report_embeddings_generated(self, count: int) -> None:
        self.num_embeddings_generated += count

    def report_error(self, error: str) -> None:
        self.processing_errors.append(error)

    def report_embedding_success(self) -> None:
        """Track successful embedding generation."""
        self.num_documents_with_embeddings += 1

    def report_embedding_failure(self, document_urn: str, error: str) -> None:
        """Track embedding generation failure."""
        self.num_embedding_failures += 1
        self.embedding_failures.append(f"{document_urn}: {error}")


@platform_name("DataHub")
@support_status(SupportStatus.INCUBATING)
@config_class(DocumentChunkingSourceConfig)
class DocumentChunkingSource(Source):
    """Source that chunks documents and generates embeddings."""

    def __init__(
        self,
        ctx: PipelineContext,
        config: DocumentChunkingSourceConfig,
        standalone: bool = True,
        graph: Optional[DataHubGraph] = None,
    ):
        """Initialize DocumentChunkingSource.

        Args:
            ctx: Pipeline context
            config: Chunking configuration
            standalone: If True, runs as a standalone source (fetches docs from DataHub).
                       If False, runs as a sub-component (called inline by other sources).
            graph: Optional DataHubGraph for inline mode (parent's graph connection).
        """
        super().__init__(ctx)
        self.config = config
        self.report = DocumentChunkingReport()
        self.standalone = standalone

        # Initialize DataHub client
        self.graph: Optional[DataHubGraph]
        if self.standalone:
            # Standalone mode: create our own graph client
            graph_config = DatahubClientConfig(
                server=self.config.datahub.server,
                token=self.config.datahub.token,
            )
            self.graph = DataHubGraph(config=graph_config)
        else:
            # Inline mode: use parent's graph connection if provided
            self.graph = graph

        # Auto-configure embedding using shared resolution logic
        self.embedding_model: Optional[str] = None
        self.config.embedding = DocumentChunkingSource.resolve_embedding_config(
            self.config.embedding, self.graph
        )

        # At this point, embedding config should be fully resolved
        if self.config.embedding.provider is None:
            logger.info(
                "No embedding provider configured - skipping embedding generation"
            )
        else:
            try:
                import litellm  # Lazy import to avoid ModuleNotFoundError during tests
            except ModuleNotFoundError:
                # litellm not installed - will fail later if embeddings are actually generated
                logger.debug(
                    "litellm not installed - embedding generation will fail if attempted"
                )
                litellm = None  # type: ignore

            # Initialize embedding model name for litellm
            if self.config.embedding.provider == "bedrock":
                # Prefix with bedrock/ for litellm
                assert self.config.embedding.model is not None
                self.embedding_model = f"bedrock/{self.config.embedding.model}"
                if litellm is not None:
                    litellm.set_verbose = False  # Reduce litellm logging
            elif self.config.embedding.provider == "cohere":
                # Prefix with cohere/ for litellm
                model_name = self.config.embedding.model
                assert model_name is not None
                if not model_name.startswith("cohere/"):
                    model_name = f"cohere/{model_name}"
                self.embedding_model = model_name
                if not self.config.embedding.api_key:
                    raise ValueError(
                        "Cohere API key is required when using cohere provider"
                    )
            else:
                raise ValueError(
                    f"Unsupported embedding provider: {self.config.embedding.provider}"
                )

        # Initialize state tracking for incremental mode
        self.state_file_path: Optional[Path] = None
        self.document_state: dict[str, dict[str, Any]] = {}
        if self.config.incremental_mode and not self.config.force_reprocess:
            self._initialize_state_tracking()

        logger.info(
            f"Initialized DocumentChunkingSource with chunking strategy: {self.config.chunking.strategy}"
        )
        if self.config.incremental_mode:
            logger.info(f"Incremental mode enabled, state file: {self.state_file_path}")

    def process_elements_inline(
        self, document_urn: str, elements: list[dict[str, Any]]
    ) -> Iterable[MetadataWorkUnit]:
        """Process elements inline and emit SemanticContent aspects.

        This method is used when ChunkingSource is called as a sub-component
        by other sources (e.g., NotionSource), rather than as a standalone source.

        Args:
            document_urn: URN of the document
            elements: Unstructured.io elements to chunk and embed

        Yields:
            MetadataWorkUnits containing SemanticContent aspects
        """
        if not elements:
            logger.warning(f"No elements provided for document {document_urn}")
            return

        # Chunk the elements
        chunks = self._chunk_elements(elements)
        if not chunks:
            logger.warning(f"No chunks created for document {document_urn}")
            return

        # Generate embeddings (only if configured)
        embeddings = []
        if self.embedding_model:
            try:
                embeddings = self._generate_embeddings(chunks)
                self.report.report_embedding_success()
            except Exception as e:
                # Embedding generation failed - document still ingested but no search capability
                short_error = str(e).split("\n")[0][:200]  # First line, max 200 chars

                self.report.report_embedding_failure(document_urn, short_error)

                # Report as warning so it appears in pipeline summary
                self.report.report_warning(
                    title="Embedding generation failed",
                    message="Document was ingested successfully but embedding generation failed. Semantic search will not work for this document.",
                    context=f"{document_urn}: {short_error}",
                    exc=e,
                )
                # Don't re-raise - allow document to be processed without embeddings
        else:
            logger.debug(
                f"Skipping embedding generation for {document_urn} - no embedding provider configured"
            )

        # Emit SemanticContent aspect (only if embeddings were generated)
        if embeddings:
            yield from self._emit_semantic_content(document_urn, chunks, embeddings)

        self.report.report_document_processed(len(chunks))
        self.report.report_embeddings_generated(len(embeddings))

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Fetch documents, chunk them, generate embeddings, and emit SemanticContent."""
        if not self.standalone:
            logger.warning(
                "ChunkingSource initialized as sub-component but get_workunits_internal() was called. "
                "Use process_elements_inline() instead."
            )
            return

        # Choose mode: event-driven or batch
        if self.config.event_source.enabled:
            logger.info("Running in event-driven mode (incremental batch)")
            yield from self._process_event_driven()
        else:
            logger.info("Running in batch mode (GraphQL)")
            yield from self._process_batch()

    def _process_event_driven(self) -> Iterable[MetadataWorkUnit]:
        """Process documents from MCL events (incremental batch mode)."""
        # This method is only called in standalone mode where graph is always set
        assert self.graph is not None, "Graph must be set in standalone mode"

        from datahub.ingestion.source.unstructured.event_consumer import (
            DocumentEventConsumer,
        )

        # Determine consumer ID
        consumer_id = (
            self.config.event_source.consumer_id
            or f"document-chunking-{self.ctx.pipeline_name}"
        )

        # Initialize event consumer
        event_consumer = DocumentEventConsumer(
            graph=self.graph,
            consumer_id=consumer_id,
            topics=self.config.event_source.topics,
            lookback_days=self.config.event_source.lookback_days,
            reset_offsets=self.config.event_source.reset_offsets,
            idle_timeout_seconds=self.config.event_source.idle_timeout_seconds,
            poll_timeout_seconds=self.config.event_source.poll_timeout_seconds,
            poll_limit=self.config.event_source.poll_limit,
        )

        try:
            # Consume events
            for mcl_event in event_consumer.consume_events():
                # Extract document URN and aspect data
                document_urn_raw = mcl_event.get("entityUrn")
                aspect_data = mcl_event.get("aspect")

                if not document_urn_raw or not aspect_data:
                    continue

                # Extract URN string from Avro union format
                if isinstance(document_urn_raw, dict) and "string" in document_urn_raw:
                    document_urn = document_urn_raw["string"]
                else:
                    document_urn = document_urn_raw

                # Parse aspect to extract custom properties
                try:
                    # The aspect can come in different formats from MCL events:
                    # 1. Wrapped in type name (e.g., "com.linkedin.pegasus2avro.mxe.GenericAspect")
                    #    with "value" field containing JSON string
                    # 2. Already a dict with customProperties
                    # 3. A dict with a "value" field containing JSON string
                    if isinstance(aspect_data, dict):
                        # Check if aspect is wrapped in type name (most common from Kafka/Events API)
                        if any(k.startswith("com.linkedin.") for k in aspect_data):
                            # Extract the actual aspect from the type wrapper
                            for key, value in aspect_data.items():
                                if key.startswith("com.linkedin."):
                                    # Now check if this wrapped value has a "value" field (GenericAspect)
                                    if isinstance(value, dict) and "value" in value:
                                        value_str = value.get("value", "{}")
                                        if isinstance(value_str, str):
                                            aspect_dict = json.loads(value_str)
                                        else:
                                            aspect_dict = value_str
                                    else:
                                        aspect_dict = value
                                    break
                        # Check if it has a "value" field (Avro union format)
                        elif "value" in aspect_data:
                            value_str = aspect_data.get("value", "{}")
                            if isinstance(value_str, str):
                                aspect_dict = json.loads(value_str)
                            else:
                                aspect_dict = value_str
                        else:
                            aspect_dict = aspect_data
                    else:
                        # Try to parse as JSON string
                        aspect_dict = json.loads(str(aspect_data))

                    custom_props = aspect_dict.get("customProperties", {})
                    logger.info(
                        f"Document {document_urn}: parsed aspect with keys: {list(aspect_dict.keys())}, "
                        f"customProperties keys: {list(custom_props.keys()) if custom_props else 'none'}"
                    )

                    # Check for unstructured_elements
                    if "unstructured_elements" not in custom_props:
                        logger.info(
                            f"Document {document_urn} has no unstructured_elements, skipping"
                        )
                        self.report.report_document_skipped()
                        continue

                    # Process this document and yield workunits (SemanticContent aspects)
                    doc = {
                        "urn": document_urn,
                        "custom_properties": custom_props,
                    }
                    self.report.report_document_fetched()
                    yield from self._process_single_document(doc)

                except Exception as e:
                    error_msg = f"Failed to process MCL event for {document_urn}: {e}"
                    logger.error(error_msg, exc_info=True)
                    self.report.report_error(error_msg)

        finally:
            event_consumer.close()

    def _process_batch(self) -> Iterable[MetadataWorkUnit]:
        """Process documents from GraphQL (batch mode)."""
        # Fetch documents from DataHub
        documents = self._fetch_documents()

        for doc in documents:
            # Check if document needs processing (incremental mode)
            if self.config.incremental_mode and not self.config.force_reprocess:
                if not self._should_process_document(doc):
                    logger.debug(f"Skipping document {doc['urn']} (content unchanged)")
                    self.report.report_document_skipped_unchanged()
                    continue

            # Process document and yield any workunits (SemanticContent aspects)
            yield from self._process_single_document(doc)

            # Update state after successful processing
            if self.config.incremental_mode:
                self._update_document_state(doc)

        # Save state file after processing all documents
        if self.config.incremental_mode:
            self._save_state()

    def _initialize_state_tracking(self) -> None:
        """Initialize state tracking for incremental mode."""
        if self.config.state_file_path:
            self.state_file_path = Path(self.config.state_file_path)
        else:
            # Default: ~/.datahub/chunking_state/{pipeline_name}.json
            state_dir = Path.home() / ".datahub" / "chunking_state"
            state_dir.mkdir(parents=True, exist_ok=True)
            pipeline_name = self.ctx.pipeline_name or "default"
            self.state_file_path = state_dir / f"{pipeline_name}.json"

        # Load existing state
        self._load_state()

    def _load_state(self) -> None:
        """Load state from file."""
        assert self.state_file_path is not None

        if self.state_file_path.exists():
            try:
                with open(self.state_file_path, "r") as f:
                    self.document_state = json.load(f)
                logger.info(
                    f"Loaded state for {len(self.document_state)} documents from {self.state_file_path}"
                )
            except Exception as e:
                logger.warning(f"Failed to load state file: {e}. Starting fresh.")
                self.document_state = {}
        else:
            logger.info(f"No existing state file found at {self.state_file_path}")
            self.document_state = {}

    def _save_state(self) -> None:
        """Save state to file."""
        assert self.state_file_path is not None

        try:
            with open(self.state_file_path, "w") as f:
                json.dump(self.document_state, f, indent=2)
            logger.info(
                f"Saved state for {len(self.document_state)} documents to {self.state_file_path}"
            )
        except Exception as e:
            logger.error(f"Failed to save state file: {e}")

    def _calculate_content_hash(self, doc: dict[str, Any]) -> str:
        """Get or calculate SHA256 hash of document's unstructured_elements.

        Prefers using the pre-calculated content_hash from custom properties
        (added during ingestion) to avoid re-hashing large payloads.
        Falls back to calculating hash if not present (for backwards compatibility).
        """
        # Check if content_hash was stored during ingestion
        stored_hash = doc["custom_properties"].get("content_hash")
        if stored_hash:
            return stored_hash

        # Fallback: calculate hash from unstructured_elements
        # This path handles documents ingested before content_hash was added
        elements_json = doc["custom_properties"].get("unstructured_elements", "")
        return hashlib.sha256(elements_json.encode("utf-8")).hexdigest()

    def _should_process_document(self, doc: dict[str, Any]) -> bool:
        """Check if document needs processing based on content hash."""
        doc_urn = doc["urn"]
        current_hash = self._calculate_content_hash(doc)

        # Check if we have state for this document
        if doc_urn not in self.document_state:
            logger.debug(f"Document {doc_urn} is new, will process")
            return True

        # Compare content hash
        doc_state = self.document_state[doc_urn]
        assert doc_state is not None  # We already checked doc_urn exists above
        previous_hash = doc_state.get("content_hash")
        if previous_hash != current_hash:
            prev_hash_str = previous_hash[:8] if previous_hash else "None"
            logger.debug(
                f"Document {doc_urn} content changed (hash: {prev_hash_str} -> {current_hash[:8]})"
            )
            return True

        logger.debug(f"Document {doc_urn} content unchanged (hash: {current_hash[:8]})")
        return False

    def _update_document_state(self, doc: dict[str, Any]) -> None:
        """Update state for a document after successful processing."""
        doc_urn = doc["urn"]
        content_hash = self._calculate_content_hash(doc)

        self.document_state[doc_urn] = {
            "content_hash": content_hash,
            "last_processed": datetime.utcnow().isoformat(),
        }

    def _process_single_document(
        self, doc: dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        """Process a single document: extract elements, chunk, embed, emit SemanticContent."""
        try:
            # Extract unstructured elements
            elements = self._extract_elements(doc)
            if not elements:
                logger.warning(f"No elements found for document {doc['urn']}")
                self.report.report_document_skipped()
                return

            # Chunk the elements
            chunks = self._chunk_elements(elements)
            if not chunks:
                logger.warning(f"No chunks created for document {doc['urn']}")
                self.report.report_document_skipped()
                return

            # Generate embeddings (only if configured)
            embeddings = []
            if self.embedding_model:
                try:
                    embeddings = self._generate_embeddings(chunks)
                    self.report.report_embedding_success()
                except Exception as e:
                    # Embedding generation failed - document still processed but no search capability
                    short_error = str(e).split("\n")[0][
                        :200
                    ]  # First line, max 200 chars

                    self.report.report_embedding_failure(doc["urn"], short_error)

                    # Report as warning so it appears in pipeline summary
                    self.report.report_warning(
                        title="Embedding generation failed",
                        message="Document was ingested successfully but embedding generation failed. Semantic search will not work for this document.",
                        context=f"{doc['urn']}: {short_error}",
                        exc=e,
                    )
                    # Don't re-raise - allow document to be processed without embeddings
            else:
                logger.debug(
                    f"Skipping embedding generation for {doc['urn']} - no embedding provider configured"
                )

            # Emit SemanticContent aspect to DataHub (only if embeddings were generated)
            if embeddings:
                yield from self._emit_semantic_content(doc["urn"], chunks, embeddings)

            self.report.report_document_processed(len(chunks))
            self.report.report_embeddings_generated(len(embeddings))

        except Exception as e:
            error_msg = f"Failed to process document {doc.get('urn', 'unknown')}: {e}"
            logger.error(error_msg, exc_info=True)
            self.report.report_error(error_msg)

    def _fetch_documents(self) -> list[dict[str, Any]]:
        """Fetch documents from DataHub using GraphQL."""
        # This method is only called in standalone mode where graph is always set
        assert self.graph is not None, "Graph must be set in standalone mode"

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
                    customProperties {
                      key
                      value
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

        # Build search input
        search_input: dict[str, Any] = {
            "type": "DOCUMENT",
            "query": "*",
            "start": 0,
            "count": 1000,  # Fetch in batches
        }

        # Add platform filter if specified
        if self.config.platform_filter:
            search_input["filters"] = [
                {
                    "field": "platform",
                    "values": [f"urn:li:dataPlatform:{self.config.platform_filter}"],
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

                # Extract custom properties
                custom_props = {}
                info = entity.get("info", {})
                if info and "customProperties" in info:
                    for prop in info["customProperties"]:
                        custom_props[prop["key"]] = prop["value"]

                # Check if document has unstructured_elements
                if "unstructured_elements" not in custom_props:
                    logger.debug(
                        f"Document {urn} has no unstructured_elements, skipping"
                    )
                    continue

                documents.append({"urn": urn, "custom_properties": custom_props})
                self.report.report_document_fetched()

            logger.info(
                f"Fetched {len(documents)} documents with unstructured_elements"
            )
            return documents

        except Exception as e:
            logger.error(f"Failed to fetch documents from DataHub: {e}", exc_info=True)
            raise

    def _extract_elements(self, doc: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract and parse unstructured_elements from custom properties."""
        elements_json = doc["custom_properties"].get("unstructured_elements")
        if not elements_json:
            return []

        try:
            elements = json.loads(elements_json)
            logger.debug(f"Extracted {len(elements)} elements from {doc['urn']}")
            return elements
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse elements JSON for {doc['urn']}: {e}")
            return []

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
            logger.info(
                f"Created {len(chunk_dicts)} chunks using {self.config.chunking.strategy} strategy"
            )
            return chunk_dicts

        except Exception as e:
            logger.error(f"Failed to chunk elements: {e}", exc_info=True)
            return []

    def _generate_embeddings(self, chunks: list[dict[str, Any]]) -> list[list[float]]:
        """Generate embeddings using litellm (supports Bedrock and Cohere)."""
        try:
            import litellm
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(
                "litellm is required for embedding generation. "
                "Install with: pip install 'acryl-datahub[unstructured-embedding]'"
            ) from e

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

            logger.info(
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
        """Emit SemanticContent aspect for the document."""
        from datahub.metadata.schema_classes import (
            EmbeddingChunkClass,
            EmbeddingModelDataClass,
            SemanticContentClass,
        )

        # Build model version string (e.g., "bedrock/cohere.embed-english-v3")
        model_version = (
            f"{self.config.embedding.provider}/{self.config.embedding.model}"
        )

        # Build embedding chunks
        embedding_chunks = []
        current_offset = 0

        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings, strict=False)):
            chunk_text = chunk.get("text", "")
            chunk_length = len(chunk_text)

            embedding_chunk = EmbeddingChunkClass(
                position=i,
                vector=embedding,
                characterOffset=current_offset,
                characterLength=chunk_length,
                tokenCount=None,  # Optional field - not calculated since it's unused
                text=chunk_text,
            )
            embedding_chunks.append(embedding_chunk)

            current_offset += chunk_length

        # Build embedding model data
        embedding_model_data = EmbeddingModelDataClass(
            modelVersion=model_version,
            generatedAt=int(datetime.utcnow().timestamp() * 1000),  # milliseconds
            chunkingStrategy=self.config.chunking.strategy,
            totalChunks=len(chunks),
            totalTokens=sum(c.tokenCount or 0 for c in embedding_chunks),
            chunks=embedding_chunks,
        )

        # Build SemanticContent aspect
        # Map key should be model identifier (e.g., cohere_embed_v3)
        assert self.config.embedding.model is not None
        if "embed-english-v3" in self.config.embedding.model:
            model_key = "cohere_embed_v3"
        else:
            model_key = self.config.embedding.model.replace("-", "_").replace(".", "_")

        semantic_content = SemanticContentClass(
            embeddings={model_key: embedding_model_data}
        )

        # Create MetadataWorkUnit
        mcp = MetadataChangeProposalWrapper(
            entityUrn=document_urn,
            aspect=semantic_content,
        )

        workunit = MetadataWorkUnit(id=f"{document_urn}-semanticContent", mcp=mcp)

        logger.info(
            f"Emitting SemanticContent for {document_urn} with {len(chunks)} chunks"
        )

        yield workunit

    @staticmethod
    def resolve_embedding_config(
        embedding_config: "EmbeddingConfig",
        graph: Optional[DataHubGraph] = None,
    ) -> "EmbeddingConfig":
        """Resolve embedding configuration using server-first, then defaults logic.

        This is the shared logic used by both actual ingestion (__init__) and
        test_connection to ensure consistent behavior.

        Resolution order:
        1. If explicit local config provided: return it (optionally validate vs server)
        2. If not: try to load from DataHub server
        3. If server unreachable or not configured: use defaults

        Args:
            embedding_config: Initial embedding configuration from recipe
            graph: Optional DataHubGraph for querying server config

        Returns:
            Resolved EmbeddingConfig ready for use

        Raises:
            ValueError: If local config validation fails (unless allow_local_embedding_config=true)
        """
        from datahub.ingestion.source.unstructured.chunking_config import (
            EmbeddingConfig,
        )

        # Check if user provided explicit embedding configuration
        if embedding_config.has_local_config():
            # Explicit config provided - validate against server if available
            if not embedding_config.allow_local_embedding_config and graph:
                logger.info("Loading embedding configuration from DataHub server...")

                try:
                    server_config = get_semantic_search_config(graph)

                    # Check if semantic search is enabled
                    if not server_config.enabled:
                        raise ValueError(
                            "Semantic search is not enabled on the DataHub server. "
                            "Cannot proceed with embedding generation. "
                            "Please enable semantic search in the server's application.yml configuration."
                        )

                    # Validate local config matches server
                    logger.info(
                        "Validating local embedding configuration against server..."
                    )

                    embedding_config.validate_against_server(
                        server_config.embedding_config
                    )

                    logger.info(
                        "✓ Local embedding configuration validated successfully"
                        f"\n  Provider: {server_config.embedding_config.provider}"
                        f"\n  Model: {server_config.embedding_config.model_id}"
                    )

                except Exception as e:
                    # Check if this is an old server that doesn't support semantic search config API
                    is_old_server = (
                        "does not expose semantic search configuration" in str(e)
                    )

                    if is_old_server:
                        # Backward compatibility: Old server + local config provided
                        logger.warning(
                            f"⚠️  Server does not support semantic search configuration API (likely older version). "
                            f"Falling back to local embedding configuration.\n"
                            f"  Provider: {embedding_config.provider}\n"
                            f"  Model: {embedding_config.model}\n"
                            f"  AWS Region: {embedding_config.aws_region or 'N/A'}\n\n"
                            f"Note: Semantic search on the server may not work if the configuration doesn't match. "
                            f"Consider upgrading to DataHub v0.14.0+ for automatic configuration sync."
                        )
                        # Continue with local config (don't raise)
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

            return embedding_config

        # No explicit config - try server first, then defaults
        if graph:
            try:
                logger.info(
                    "No embedding provider configured - loading from DataHub server..."
                )
                server_config = get_semantic_search_config(graph)

                if (
                    server_config
                    and server_config.enabled
                    and server_config.embedding_config
                ):
                    logger.info(
                        f"Loaded embedding config from server: {server_config.embedding_config.provider} / {server_config.embedding_config.model_id}"
                    )
                    # Preserve any local credentials (api_key) that user may have set
                    local_api_key = embedding_config.api_key
                    resolved = EmbeddingConfig.from_server(
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
                    return resolved
                else:
                    # Server doesn't have semantic search enabled - use defaults
                    logger.info(
                        "Semantic search not enabled on server - using default client-side embedding config (Bedrock/Cohere)"
                    )
                    default_config = EmbeddingConfig.get_default_config()
                    logger.info(
                        "✓ Using default embedding configuration"
                        f"\n  Provider: {default_config.provider}"
                        f"\n  Model: {default_config.model}"
                        f"\n  AWS Region: {default_config.aws_region}"
                    )
                    return default_config

            except Exception as e:
                logger.warning(
                    f"Failed to load embedding config from server: {e}. Using default client-side embedding config."
                )
                default_config = EmbeddingConfig.get_default_config()
                logger.info(
                    "✓ Using default embedding configuration"
                    f"\n  Provider: {default_config.provider}"
                    f"\n  Model: {default_config.model}"
                    f"\n  AWS Region: {default_config.aws_region}"
                )
                return default_config
        else:
            # No graph available - use defaults
            logger.info(
                "No DataHub server connection available - using default client-side embedding config"
            )
            default_config = EmbeddingConfig.get_default_config()
            logger.info(
                "✓ Using default embedding configuration"
                f"\n  Provider: {default_config.provider}"
                f"\n  Model: {default_config.model}"
                f"\n  AWS Region: {default_config.aws_region}"
            )
            return default_config

    @staticmethod
    def test_embedding_capability(
        embedding_config: "EmbeddingConfig",
    ) -> CapabilityReport:
        """Test if embedding generation will work with the provided configuration.

        This tests the actual embedding API connection by generating a test embedding.
        Used by sources (like Notion) to validate semantic search capability during test_connection.

        Args:
            embedding_config: The embedding configuration to test

        Returns:
            CapabilityReport indicating if embedding generation is capable
        """
        # Check if embedding config is provided and enabled
        if not embedding_config or not embedding_config.provider:
            return CapabilityReport(
                capable=False,
                failure_reason="Embedding configuration not provided",
                mitigation_message="Configure embedding provider (bedrock or cohere) to enable semantic search. "
                "See https://datahubproject.io/docs/semantic-search/",
            )

        try:
            # Try to import litellm
            try:
                import litellm
            except ModuleNotFoundError:
                return CapabilityReport(
                    capable=False,
                    failure_reason="litellm package not installed",
                    mitigation_message="Install litellm: pip install 'acryl-datahub[unstructured-embedding]'",
                )

            # Determine embedding model name for litellm
            if embedding_config.provider == "bedrock":
                if not embedding_config.model:
                    return CapabilityReport(
                        capable=False,
                        failure_reason="Bedrock model not specified in embedding config",
                        mitigation_message="Set embedding.model to a valid Bedrock model (e.g., 'amazon.titan-embed-text-v1')",
                    )
                embedding_model = f"bedrock/{embedding_config.model}"
                litellm.set_verbose = False

            elif embedding_config.provider == "cohere":
                if not embedding_config.model:
                    return CapabilityReport(
                        capable=False,
                        failure_reason="Cohere model not specified in embedding config",
                        mitigation_message="Set embedding.model to a valid Cohere model (e.g., 'embed-english-v3.0')",
                    )

                model_name = embedding_config.model
                if not model_name.startswith("cohere/"):
                    model_name = f"cohere/{model_name}"
                embedding_model = model_name

                if not embedding_config.api_key:
                    return CapabilityReport(
                        capable=False,
                        failure_reason="Cohere API key not provided",
                        mitigation_message="Set embedding.api_key to your Cohere API key. "
                        "Get one at https://dashboard.cohere.com/api-keys",
                    )
            else:
                return CapabilityReport(
                    capable=False,
                    failure_reason=f"Unsupported embedding provider: {embedding_config.provider}",
                    mitigation_message="Supported providers: 'bedrock', 'cohere'",
                )

            # Test embedding generation with a simple text
            test_text = "DataHub semantic search test"

            try:
                response = litellm.embedding(
                    model=embedding_model,
                    input=[test_text],
                    api_key=embedding_config.api_key,
                    aws_region_name=embedding_config.aws_region,
                )

                # Verify we got an embedding back
                if not response or not response.data or len(response.data) == 0:
                    return CapabilityReport(
                        capable=False,
                        failure_reason="Embedding API returned empty response",
                        mitigation_message="Check your embedding configuration and API credentials",
                    )

                embedding = response.data[0]["embedding"]
                embedding_dim = len(embedding)

                # Success! Embedding generation works
                return CapabilityReport(
                    capable=True,
                    mitigation_message=f"Semantic search enabled: {embedding_config.provider}/{embedding_config.model} "
                    f"(dimension: {embedding_dim}). Documents will be searchable in DataHub.",
                )

            except Exception as e:
                # Parse error message for common issues
                error_str = str(e)
                error_class = e.__class__.__name__

                # Provide specific guidance for common errors
                if "AuthFailure" in error_str or "InvalidClientTokenId" in error_str:
                    mitigation = (
                        "AWS credentials not configured or invalid. "
                        "Ensure AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY are set, "
                        "or IAM role is properly configured."
                    )
                elif "invalid_api_key" in error_str or "Unauthorized" in error_str:
                    mitigation = (
                        "Invalid API key. "
                        "Check your Cohere API key at https://dashboard.cohere.com/api-keys"
                    )
                elif "ValidationException" in error_str:
                    mitigation = (
                        "Invalid model or parameters. "
                        f"Check that model '{embedding_config.model}' is valid for {embedding_config.provider}."
                    )
                elif "AccessDeniedException" in error_str:
                    mitigation = (
                        "AWS IAM permissions missing. "
                        "Ensure your AWS credentials have 'bedrock:InvokeModel' permission."
                    )
                elif (
                    "rate_limit" in error_str.lower() or "throttl" in error_str.lower()
                ):
                    mitigation = "Rate limit exceeded. Try again in a few moments or increase your API quota."
                else:
                    mitigation = f"Check your {embedding_config.provider} configuration and credentials."

                return CapabilityReport(
                    capable=False,
                    failure_reason=f"Embedding generation failed: {error_class}: {error_str[:200]}",
                    mitigation_message=mitigation,
                )

        except Exception as e:
            return CapabilityReport(
                capable=False,
                failure_reason=f"Unexpected error testing embeddings: {e}",
                mitigation_message="Check logs for more details",
            )

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        """Cleanup resources."""
        pass
