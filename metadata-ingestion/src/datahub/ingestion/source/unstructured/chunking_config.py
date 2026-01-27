"""Configuration for Document Chunking and Embedding Source."""

from typing import Any, Literal, Optional

from pydantic import Field, field_validator

from datahub.configuration.common import ConfigModel


class ServerEmbeddingConfig(ConfigModel):
    """Embedding configuration fetched from DataHub server via AppConfig API."""

    provider: str
    model_id: str
    model_embedding_key: str
    aws_region: Optional[str] = None


class ServerSemanticSearchConfig(ConfigModel):
    """Semantic search configuration fetched from DataHub server via AppConfig API."""

    enabled: bool
    enabled_entities: list[str]
    embedding_config: ServerEmbeddingConfig


class ChunkingConfig(ConfigModel):
    """Chunking strategy configuration."""

    strategy: Literal["basic", "by_title"] = Field(
        default="by_title", description="Chunking strategy to use"
    )
    max_characters: int = Field(default=500, description="Maximum characters per chunk")
    overlap: int = Field(default=0, description="Character overlap between chunks")
    combine_text_under_n_chars: int = Field(
        default=100, description="Combine chunks smaller than this size"
    )


class EmbeddingConfig(ConfigModel):
    """Embedding generation configuration.

    Default behavior: Fetches configuration from DataHub server automatically.
    Override behavior: Validates local config against server when explicitly set.
    """

    # Core configuration (Optional - loaded from server if not set)
    provider: Optional[Literal["bedrock", "cohere"]] = Field(
        default=None,
        description="Embedding provider (bedrock uses AWS, cohere uses API key). If not set, loads from server.",
    )
    model: Optional[str] = Field(
        default=None,
        description="Model name. If not set, loads from server.",
    )
    model_embedding_key: Optional[str] = Field(
        default=None,
        description="Storage key for embeddings (e.g., 'cohere_embed_v3'). Required if overriding server config. If not set, loads from server.",
    )
    aws_region: Optional[str] = Field(
        default=None,
        description="AWS region for Bedrock. If not set, loads from server.",
    )

    # Credentials (can be provided locally even when using server config)
    api_key: Optional[str] = Field(
        default=None,
        description="API key for Cohere (not needed for Bedrock with IAM roles)",
    )

    # Processing configuration (not from server)
    batch_size: int = Field(
        default=25, description="Batch size for embedding API calls"
    )
    input_type: Optional[str] = Field(
        default="search_document",
        description="Input type for Cohere embeddings",
    )

    # Break-glass override flag
    allow_local_embedding_config: bool = Field(
        default=False,
        description="BREAK-GLASS: Allow local config without server validation. NOT RECOMMENDED - may break semantic search.",
    )

    # Internal: cached server config after loading/validation
    _server_config: Optional[ServerEmbeddingConfig] = None

    @field_validator("model_embedding_key")
    @classmethod
    def validate_model_embedding_key(cls, v: Optional[str]) -> Optional[str]:
        """Validate model_embedding_key is Elasticsearch-compatible (no dots)."""
        if v is not None and "." in v:
            raise ValueError(
                f"model_embedding_key '{v}' contains '.' which is not allowed in Elasticsearch field names. "
                f"Use underscores instead (e.g., 'cohere_embed_v3' not 'cohere.embed.v3')"
            )
        return v

    def has_local_config(self) -> bool:
        """Check if user provided explicit local configuration."""
        return (
            self.provider is not None
            or self.model is not None
            or self.model_embedding_key is not None
        )

    @classmethod
    def from_server(
        cls, server_config: ServerEmbeddingConfig, api_key: Optional[str] = None
    ) -> "EmbeddingConfig":
        """Create EmbeddingConfig from server configuration.

        Args:
            server_config: Server's embedding configuration
            api_key: Optional local API key for Cohere (dev environments)

        Returns:
            EmbeddingConfig populated with server values
        """
        # Normalize provider from server format to local format
        provider = cls._normalize_provider_from_server(server_config.provider)

        config = cls(
            provider=provider,
            model=server_config.model_id,
            model_embedding_key=server_config.model_embedding_key,
            aws_region=server_config.aws_region,
            api_key=api_key,
        )
        # Set private field after construction
        config._server_config = server_config
        return config

    def validate_against_server(self, server_config: ServerEmbeddingConfig) -> None:
        """Validate local config against server config (for override flow).

        Checks:
        - Provider matches (bedrock/cohere)
        - Model identifier matches exactly
        - Model embedding key matches exactly
        - AWS region matches (for Bedrock only)

        Raises:
            ValueError: If validation fails with detailed error message
        """
        errors = []

        # Normalize providers for comparison
        assert self.provider is not None, "provider must be set when validating"
        assert self.model is not None, "model must be set when validating"
        assert self.model_embedding_key is not None, (
            "model_embedding_key must be set when overriding server config"
        )

        local_provider = self._normalize_provider(self.provider)
        server_provider = self._normalize_provider(server_config.provider)

        if local_provider != server_provider:
            errors.append(
                f"Provider mismatch: local='{self.provider}', server='{server_config.provider}'"
            )

        if self.model != server_config.model_id:
            errors.append(
                f"Model mismatch: local='{self.model}', server='{server_config.model_id}'"
            )

        if self.model_embedding_key != server_config.model_embedding_key:
            errors.append(
                f"Model embedding key mismatch: local='{self.model_embedding_key}', server='{server_config.model_embedding_key}'"
            )

        if local_provider == "bedrock" and self.aws_region != server_config.aws_region:
            errors.append(
                f"AWS region mismatch: local='{self.aws_region}', server='{server_config.aws_region}'"
            )

        if errors:
            # Build detailed error message with server config and fix suggestions
            error_list = "\n  - ".join(errors)
            raise ValueError(
                f"Embedding configuration mismatch with DataHub server:\n  - {error_list}\n\n"
                f"Server configuration:\n"
                f"  Provider: {server_config.provider}\n"
                f"  Model: {server_config.model_id}\n"
                f"  Model Embedding Key: {server_config.model_embedding_key}\n"
                f"  AWS Region: {server_config.aws_region or 'N/A'}\n\n"
                f"To fix this issue:\n"
                f"  1. Update your local config to match the server configuration above, OR\n"
                f"  2. Update the server's semantic search configuration in application.yml, OR\n"
                f"  3. Set 'allow_local_embedding_config: true' (NOT RECOMMENDED - will break semantic search)"
            )

        self._server_config = server_config

    @staticmethod
    def _normalize_provider(provider: str) -> str:
        """Normalize provider name for comparison: 'aws-bedrock' → 'bedrock'."""
        provider_lower = provider.lower()
        if "bedrock" in provider_lower:
            return "bedrock"
        if "cohere" in provider_lower:
            return "cohere"
        return provider_lower

    @staticmethod
    def _normalize_provider_from_server(
        server_provider: str,
    ) -> Literal["bedrock", "cohere"]:  # type: ignore
        """Convert server provider format to local config format."""
        normalized = EmbeddingConfig._normalize_provider(server_provider)
        if normalized == "bedrock":
            return "bedrock"
        elif normalized == "cohere":
            return "cohere"
        else:
            raise ValueError(f"Unsupported provider from server: {server_provider}")

    @staticmethod
    def get_default_config() -> "EmbeddingConfig":
        """Get default client-side embedding configuration.

        Returns sensible defaults for AWS Bedrock with Cohere embeddings.
        Uses allow_local_embedding_config=True as a "break glass" to bypass
        server validation.

        Returns:
            EmbeddingConfig with Bedrock/Cohere defaults
        """
        return EmbeddingConfig(
            provider="bedrock",
            model="cohere.embed-english-v3",
            model_embedding_key="cohere_embed_v3",
            aws_region="us-west-2",
            allow_local_embedding_config=True,  # Break glass: skip server validation
        )


class DataHubConnectionConfig(ConfigModel):
    """DataHub connection configuration."""

    server: str = Field(
        default="http://localhost:8080", description="DataHub GMS server URL"
    )
    token: Optional[str] = Field(
        default=None, description="DataHub API token for authentication"
    )


class EventSourceConfig(ConfigModel):
    """Event-driven mode configuration."""

    enabled: bool = Field(
        default=False,
        description="Enable event-driven mode (polls MCL events instead of GraphQL)",
    )
    consumer_id: Optional[str] = Field(
        default=None,
        description="Consumer ID for offset tracking (defaults to 'document-chunking-{pipeline_name}')",
    )
    topics: list[str] = Field(
        default=["MetadataChangeLog_Versioned_v1"],
        description="Topics to consume (MCL topics for document changes)",
    )
    lookback_days: Optional[int] = Field(
        default=None,
        description="Number of days to look back for events on first run",
    )
    reset_offsets: bool = Field(
        default=False, description="Reset consumer offsets to start from beginning"
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


class DocumentChunkingSourceConfig(ConfigModel):
    """Configuration for Document Chunking and Embedding Source."""

    # DataHub connection
    datahub: DataHubConnectionConfig = Field(
        default_factory=DataHubConnectionConfig, description="DataHub connection config"
    )

    # Chunking configuration
    chunking: ChunkingConfig = Field(
        default_factory=ChunkingConfig, description="Chunking configuration"
    )

    # Embedding configuration
    embedding: EmbeddingConfig = Field(
        default_factory=EmbeddingConfig, description="Embedding configuration"
    )

    # Event source configuration
    event_source: EventSourceConfig = Field(
        default_factory=EventSourceConfig,
        description="Event-driven mode configuration (incremental batch)",
    )

    # Filtering (for batch mode)
    document_urns: Optional[list[str]] = Field(
        default=None,
        description="Specific document URNs to process in batch mode (if None, process all documents)",
    )
    platform_filter: Optional[str] = Field(
        default=None,
        description="Filter documents by platform in batch mode (e.g., 'notion')",
    )

    # Processing options
    batch_size: int = Field(
        default=10, description="Number of documents to process in parallel"
    )
    skip_if_embeddings_exist: bool = Field(
        default=True,
        description="Skip documents that already have embeddings for this model",
    )

    # Incremental mode configuration
    incremental_mode: bool = Field(
        default=False,
        description="Enable incremental processing - only re-process documents whose content has changed",
    )
    state_file_path: Optional[str] = Field(
        default=None,
        description="Path to state file for tracking document content hashes (defaults to ~/.datahub/chunking_state/{pipeline_name}.json)",
    )
    force_reprocess: bool = Field(
        default=False,
        description="Force reprocess all documents regardless of content hash (overrides incremental_mode)",
    )


def get_processing_config_fingerprint(
    chunking: ChunkingConfig, embedding: EmbeddingConfig
) -> dict[str, Any]:
    """Extract processing configuration that affects output artifacts.

    This fingerprint is included in document hashes to trigger reprocessing
    when configuration changes (e.g., changing chunking strategy, embedding model).

    The fingerprint includes only configuration values that affect the SemanticContent
    aspect output (chunks and embeddings), not processing parameters like batch_size
    or credentials.

    Args:
        chunking: Chunking configuration
        embedding: Embedding configuration

    Returns:
        Dictionary of config values that affect processing output.
    """
    # Embedding is enabled when provider is configured
    embedding_enabled = embedding.provider is not None

    return {
        # Chunking affects chunk boundaries and structure
        "chunking_strategy": chunking.strategy if embedding_enabled else None,
        "chunking_max_characters": chunking.max_characters
        if embedding_enabled
        else None,
        "chunking_overlap": chunking.overlap if embedding_enabled else None,
        "chunking_combine_text_under_n_chars": (
            chunking.combine_text_under_n_chars if embedding_enabled else None
        ),
        # Embedding affects vector embeddings on chunks
        "embedding_provider": embedding.provider if embedding_enabled else None,
        "embedding_model": embedding.model if embedding_enabled else None,
        "embedding_model_key": (
            embedding.model_embedding_key if embedding_enabled else None
        ),
    }


def get_semantic_search_config(graph: Any) -> ServerSemanticSearchConfig:
    """Fetch server semantic search configuration via AppConfig query.

    Args:
        graph: DataHubGraph instance

    Returns:
        ServerSemanticSearchConfig with embedding configuration from server

    Raises:
        GraphError: If query fails or server doesn't support semantic search config
    """
    from datahub.configuration.common import GraphError

    query = """
        query getSemanticSearchConfig {
          appConfig {
            semanticSearchConfig {
              enabled
              enabledEntities
              embeddingConfig {
                provider
                modelId
                modelEmbeddingKey
                awsProviderConfig {
                  region
                }
              }
            }
          }
        }
    """

    response = graph.execute_graphql(
        query=query, operation_name="getSemanticSearchConfig"
    )

    semantic_search_config = response.get("appConfig", {}).get("semanticSearchConfig")

    if not semantic_search_config:
        raise GraphError(
            "Server does not expose semantic search configuration. "
            "Ensure DataHub server version supports semantic search config API (v0.14.0+)."
        )

    # Parse into ServerSemanticSearchConfig model
    embedding_config_dict = semantic_search_config["embeddingConfig"]

    # Extract AWS region from nested awsProviderConfig
    aws_region = None
    if embedding_config_dict.get("awsProviderConfig"):
        aws_region = embedding_config_dict["awsProviderConfig"].get("region")

    server_embedding_config = ServerEmbeddingConfig(
        provider=embedding_config_dict["provider"],
        model_id=embedding_config_dict["modelId"],
        model_embedding_key=embedding_config_dict["modelEmbeddingKey"],
        aws_region=aws_region,
    )

    return ServerSemanticSearchConfig(
        enabled=semantic_search_config["enabled"],
        enabled_entities=semantic_search_config["enabledEntities"],
        embedding_config=server_embedding_config,
    )
