"""NotionSource - Extract documents from Notion using Unstructured.io."""

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Type, Union

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.notion.notion_config import NotionSourceConfig
from datahub.ingestion.source.notion.notion_report import NotionSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.unstructured.document_builder import (
    DocumentEntityBuilder,
)

logger = logging.getLogger(__name__)


@platform_name("Notion")
@config_class(NotionSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class NotionSource(StatefulIngestionSourceBase, TestableSource):
    platform = "notion"  # Required for stateful ingestion checkpoint job_id
    """
    Extract documents, pages, and databases from Notion as DataHub Document entities.

    This source connects to the Notion API to ingest workspace content including pages,
    databases, and their hierarchical relationships. It uses Unstructured.io for robust
    text extraction and partitioning, with optional support for embeddings and semantic search.

    ## Capabilities

    This source can extract the following from Notion:

    - **Pages and Databases**: All accessible pages and databases in your workspace
    - **Hierarchical Structure**: Parent-child relationships between pages
    - **Rich Content**: Text, tables, code blocks, and other Notion content types
    - **Metadata**: Page titles, creation/modification timestamps, and URLs
    - **Embeddings**: Optional vector embeddings for semantic search (via Cohere or AWS Bedrock)
    - **Document Chunks**: Configurable chunking strategies for downstream processing

    ## Prerequisites

    ### 1. Create a Notion Integration

    To use this source, you must create a Notion internal integration:

    1. Go to [Notion Integrations](https://www.notion.so/my-integrations)
    2. Click **"+ New integration"**
    3. Give your integration a name (e.g., "DataHub Connector")
    4. Select the workspace where you want to extract content
    5. Under **Capabilities**, ensure these permissions are enabled:
       - **Read content**: Required for accessing page and database content
       - **Read comments**: Optional, for extracting comment metadata
    6. Click **Submit** to create the integration
    7. Copy the **Internal Integration Token** (starts with `secret_...`)

    ### 2. Share Pages with Integration

    Notion integrations only have access to pages explicitly shared with them:

    1. Navigate to the page or database you want to ingest
    2. Click the **"..."** menu (top right)
    3. Select **"Add connections"** or **"Connect to"**
    4. Search for your integration name and select it
    5. The integration now has access to this page and all its children (if `recursive: true`)

    **Important**: For workspace-wide ingestion, share the integration with your top-level pages.

    ### 3. Find Page/Database IDs

    Page and database IDs are required in the configuration:

    - **Page URL format**: `https://www.notion.so/Page-Title-{PAGE_ID}`
    - **Database URL format**: `https://www.notion.so/{DATABASE_ID}?v=...`

    The ID is the 32-character hex string in the URL (with or without hyphens).

    Example IDs:
    - `abc123def456abc123def456abc123de` (32 chars, no hyphens)
    - `abc123de-f456-abcd-ef12-3456abc123de` (standard UUID format)

    ## Configuration

    ### Minimal Configuration

    ```yaml
    source:
      type: notion
      config:
        # Required: Notion API credentials
        api_key: "secret_abc123..."  # From integration settings

        # Optional: Specify page_ids and/or database_ids to ingest
        # If both are empty, all accessible pages and databases will be auto-discovered
        page_ids:
          - "abc123def456abc123def456abc123de"  # Top-level page

        # Optional: Recursively fetch child pages (default: true)
        recursive: true
    ```

    ### Advanced Configuration

    ```yaml
    source:
      type: notion
      config:
        api_key: "secret_abc123..."

        # Ingest multiple pages and databases
        page_ids:
          - "abc123def456abc123def456abc123de"
          - "def456abc123def456abc123def456ab"
        database_ids:
          - "123456abcdef123456abcdef123456ab"

        # Text processing configuration
        processing:
          partition:
            strategy: "auto"  # auto, fast, ocr_only, hi_res
            partition_by_api: false  # Use Unstructured API (requires api_key)
            api_key: "${UNSTRUCTURED_API_KEY}"  # If partition_by_api: true
          parallelism:
            num_processes: 2  # Parallel processing workers

        # Embedding configuration (optional)
        embedding:
          provider: "cohere"  # Supported: cohere, bedrock
          model: "embed-english-v3.0"  # Cohere embedding model
          api_key: "${COHERE_API_KEY}"  # Required for Cohere
          # For AWS Bedrock:
          # provider: "bedrock"
          # model: "amazon.titan-embed-text-v1"
          # aws_region: "us-east-1"

        # Chunking configuration
        chunking:
          strategy: "by_title"  # Supported: by_title, basic
          max_characters: 500  # Maximum characters per chunk
          overlap: 0  # Character overlap between chunks
          combine_text_under_n_chars: 100  # Combine chunks smaller than this

        # Hierarchy configuration
        hierarchy:
          enabled: true  # Preserve parent-child relationships
          parent_strategy: "notion"  # Use Notion's native hierarchy

        # Filtering configuration
        filtering:
          skip_empty_documents: true
          min_text_length: 50  # Skip pages with < 50 chars

        # Advanced options
        advanced:
          work_dir: "/tmp/notion_ingestion"  # Temporary processing directory
          preserve_outputs: false  # Keep processed files for debugging
          continue_on_failure: true  # Continue on individual page errors
          raise_on_error: false  # Don't fail entire run on errors
    ```

    ## Key Features

    ### 1. Hierarchical Document Structure

    When `hierarchy.enabled: true`, the source preserves Notion's page hierarchy:
    - Parent pages are linked via the `parentDocument` relationship
    - Enables browsing documents in their original workspace structure
    - Supports unlimited nesting levels

    ### 2. Text Extraction Strategies

    Multiple partitioning strategies via Unstructured.io:
    - **auto** (default): Automatically detects best strategy per document
    - **fast**: Fastest processing, basic text extraction
    - **ocr_only**: Uses OCR for image-heavy pages
    - **hi_res**: Highest quality, processes layouts and tables

    ### 3. Embedding Generation

    Optional semantic search support:
    - **Supported providers**: Cohere (API key), AWS Bedrock (IAM roles)
    - **Chunking strategies**: by_title, basic
    - **Configurable chunk size**: Optimize for your embedding model (in characters)
    - **Automatic deduplication**: Prevents duplicate chunk embeddings

    ### 4. Stateful Ingestion

    Supports smart incremental updates via stateful ingestion:
    - **Content Change Detection**: Only reprocesses documents when content or embeddings config changes
    - **Deletion Detection**: Automatically removes stale entities from DataHub
    - **Recursive Discovery**: Start from root pages/databases, automatically discovers and ingests child pages
    - **State Persistence**: Maintains processing state between runs to skip unchanged documents

    ## Common Use Cases

    ### 1. Workspace-wide Documentation Search

    ```yaml
    # Ingest entire workspace documentation
    page_ids:
      - "workspace_root_page_id"  # Top-level page shared with integration
    recursive: true
    embedding:
      provider: "cohere"
      model: "embed-english-v3.0"
      api_key: "${COHERE_API_KEY}"
    ```

    ### 2. Specific Database Ingestion

    ```yaml
    # Ingest a specific Notion database (e.g., "Product Requirements")
    database_ids:
      - "product_requirements_db_id"
    recursive: false  # Only database entries, not child pages
    ```

    ### 3. Multi-workspace Setup

    ```yaml
    # Ingest from multiple workspaces (requires multiple integrations)
    page_ids:
      - "workspace_1_page_id"
      - "workspace_2_page_id"
    ```

    ## Limitations and Considerations

    ### Notion API Limits

    - **Rate Limits**: Notion enforces rate limits (3 requests/second for paid, 1/second for free)
    - **Access Scope**: Integration only sees explicitly shared pages
    - **Content Types**: Some Notion blocks may not extract perfectly (e.g., complex embeds)

    ### Performance Considerations

    - **Large Workspaces**: First run may take significant time for large workspaces
    - **Embedding Generation**: Adds processing time proportional to content volume
    - **API Costs**: Unstructured API and embedding providers may incur costs

    ### Document Updates

    With stateful ingestion enabled (default):
    - **Incremental Updates**: Only reprocesses documents when content or config changes (via content hash comparison)
    - **Deletion Detection**: Automatically removes documents from DataHub when deleted from Notion
    - **State Tracking**: Maintains state between runs to skip unchanged documents

    ## Troubleshooting

    ### Common Issues

    **"Integration not found" or "Unauthorized" errors:**
    - Verify the `api_key` is correct (should start with `secret_`)
    - Ensure pages are shared with the integration
    - Check that the integration has "Read content" capability

    **Empty or missing content:**
    - Verify pages contain text (empty pages are skipped by default with `skip_empty_documents: true`)
    - Check `min_text_length` filter setting
    - Ensure `recursive: true` if expecting child pages

    **Slow ingestion:**
    - Increase `processing.parallelism.num_processes` (default: 2)
    - Consider using `partition_by_api: false` for local processing
    - Filter specific pages instead of entire workspace

    **Embedding generation failures:**
    - Verify provider API key is correct
    - Check provider-specific rate limits
    - Ensure embedding model name is valid for your provider

    ## Related Documentation

    - [Notion API Documentation](https://developers.notion.com/)
    - [Unstructured.io Documentation](https://docs.unstructured.io/)
    - [Cohere Embeddings API](https://docs.cohere.com/reference/embed)
    - [AWS Bedrock Embeddings](https://docs.aws.amazon.com/bedrock/latest/userguide/embeddings.html)
    - [DataHub Document Ingestion](https://datahubproject.io/docs/generated/ingestion/sources/)

    ## Support

    For questions and support, please reach out on the [DataHub Slack](https://datahubspace.slack.com/) in the #troubleshoot channel.
    """

    config: NotionSourceConfig
    report: NotionSourceReport

    def __init__(self, config: NotionSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report = NotionSourceReport()

        # Initialize shared builders
        self.document_builder = DocumentEntityBuilder(
            config=config.document_mapping,
            source_type="notion",
        )

        # Initialize chunking/embedding sub-component
        from datahub.ingestion.source.unstructured.chunking_config import (
            DataHubConnectionConfig,
            DocumentChunkingSourceConfig,
        )
        from datahub.ingestion.source.unstructured.chunking_source import (
            DocumentChunkingSource,
        )

        # Create chunking config (embedding will be auto-configured by DocumentChunkingSource)
        chunking_config = DocumentChunkingSourceConfig(
            datahub=DataHubConnectionConfig(),  # Not used in inline mode
            chunking=config.chunking,
            embedding=config.embedding,
        )

        # Pass graph to DocumentChunkingSource so it can load server config
        self.chunking_source = DocumentChunkingSource(
            ctx=ctx,
            config=chunking_config,
            standalone=False,  # Run as sub-component
            graph=ctx.graph,  # Pass graph for server config loading
        )

        # Notion parent metadata tracking
        self.notion_parent_metadata: Dict[str, Any] = {}

        # Initialize stateful ingestion handler for stale entity removal
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, ctx
        )

        # Initialize document state tracking for content-based change detection
        self.document_state: Dict[str, Dict[str, Any]] = {}
        self.state_file_path: Optional[Path] = None

        if self.config.stateful_ingestion and self.config.stateful_ingestion.enabled:
            self._initialize_state_tracking()

    def _patch_notion_client_for_is_locked(self) -> None:
        """Monkeypatch unstructured-ingest's Page class to support is_locked field.

        TEMPORARY FIX: The Notion API now returns is_locked field in page responses,
        but the Page class in unstructured-ingest 0.7.2 (which wraps notion-client 2.7.0)
        doesn't support it. This causes Page.__init__() to fail with "unexpected keyword argument 'is_locked'".

        This monkeypatch wraps the Page.__init__ to ignore is_locked until we upgrade.

        Fixed upstream in notion-client: https://github.com/ramnes/notion-sdk-py/pull/286
        Remove this once unstructured-ingest upgrades to notion-client >= 2.8.0
        """
        try:
            from unstructured_ingest.processes.connectors.notion.types.page import (
                Page,
            )

            original_init = Page.__init__

            def patched_init(self, **kwargs):
                # Remove is_locked if present (Notion API 2025 addition)
                kwargs.pop("is_locked", None)
                original_init(self, **kwargs)

            Page.__init__ = patched_init
            logger.info(
                "Applied monkeypatch to unstructured-ingest Page class for is_locked field support"
            )
        except ImportError:
            # unstructured-ingest not installed (shouldn't happen in notion source, but be defensive)
            logger.warning(
                "unstructured-ingest Page class not found - skipping is_locked monkeypatch. "
                "This may cause issues if Notion API returns is_locked field."
            )
        except Exception as e:
            # Don't fail ingestion if monkeypatch fails
            logger.warning(
                f"Failed to apply is_locked monkeypatch: {e}. "
                "May encounter issues with is_locked field."
            )

    @staticmethod
    def _monkeypatch_database_property_description():
        """Monkeypatch unstructured-ingest database property classes to support 'description' field.

        The Notion API now returns a 'description' field for all database properties, but
        unstructured-ingest 0.7.2 doesn't support it. This causes errors like:
        "Date.__init__() got an unexpected keyword argument 'description'"

        This patches all database property classes to filter out the description field.
        """
        try:
            # Import database property classes directly
            from unstructured_ingest.processes.connectors.notion.types.database_properties.checkbox import (
                Checkbox,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.created_by import (
                CreatedBy,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.created_time import (
                CreatedTime,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.date import (
                Date,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.email import (
                Email,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.files import (
                Files,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.formula import (
                Formula,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.last_edited_by import (
                LastEditedBy,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.last_edited_time import (
                LastEditedTime,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.multiselect import (
                MultiSelect,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.number import (
                Number,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.people import (
                People,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.phone_number import (
                PhoneNumber,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.relation import (
                Relation,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.rich_text import (
                RichText,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.rollup import (
                Rollup,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.select import (
                Select,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.status import (
                Status,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.title import (
                Title,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.unique_id import (
                UniqueID,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.url import (
                URL,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties.verification import (
                Verification,
            )

            # List of all property classes that need patching
            property_classes = [
                Checkbox,
                CreatedBy,
                CreatedTime,
                Date,
                Email,
                Files,
                Formula,
                LastEditedBy,
                LastEditedTime,
                MultiSelect,
                Number,
                People,
                PhoneNumber,
                Relation,
                RichText,
                Rollup,
                Select,
                Status,
                Title,
                UniqueID,
                URL,
                Verification,
            ]

            # Patch each class
            def make_patched_from_dict(original_method: Any) -> Any:
                """Factory to create patched from_dict method with proper closure."""

                def patched_from_dict(cls: Type[Any], data: dict) -> Any:
                    # Remove description field if present
                    data_copy = data.copy()
                    data_copy.pop("description", None)
                    return original_method(data_copy)

                # Make it a classmethod for monkey-patching
                return classmethod(patched_from_dict)  # type: ignore[arg-type]

            for prop_class in property_classes:
                original_from_dict = prop_class.from_dict
                prop_class.from_dict = make_patched_from_dict(original_from_dict)

            logger.info(
                f"Applied monkeypatch to {len(property_classes)} database property classes for description field support"
            )
        except ImportError as e:
            logger.warning(
                f"unstructured-ingest database property classes not found - skipping description monkeypatch: {e}. "
                "This may cause issues if Notion API returns description field."
            )
        except Exception as e:
            logger.warning(
                f"Failed to apply description monkeypatch: {e}. "
                "May encounter issues with description field."
            )

    @staticmethod
    def _monkeypatch_database_title_extraction():
        """Monkeypatch unstructured-ingest to include database title in HTML output.

        The extract_database_html function retrieves the database object but doesn't
        include the title in the HTML. This causes documents to have generic IDs as titles
        instead of friendly names like "Employee Directory".

        This patch prepends the database title as an H1 element before the table.
        """
        try:
            from htmlBuilder.tags import H1, Div
            from unstructured_ingest.processes.connectors.notion import helpers

            original_extract = helpers.extract_database_html

            def patched_extract_database_html(
                client: Any, database_id: str, logger: Any
            ) -> Any:
                # Call original function to get the table HTML
                result = original_extract(client, database_id, logger)

                # Retrieve database to get title
                database = client.databases.retrieve(database_id=database_id)

                # Extract title text - handle both dict and object responses
                title_text = None
                try:
                    # Try dict access first (newer API versions)
                    if isinstance(database, dict):
                        title_parts = database.get("title", [])
                    else:
                        # Try object access (older API versions)
                        title_parts = getattr(database, "title", [])

                    if title_parts and len(title_parts) > 0:
                        # Handle both dict and object for title parts
                        first_part = title_parts[0]
                        if isinstance(first_part, dict):
                            title_text = first_part.get("plain_text", "")
                        else:
                            title_text = getattr(first_part, "plain_text", "")
                except Exception as e:
                    logger.warning(f"Failed to extract database title: {e}")

                # Prepend title as H1 if we have it
                if title_text and result.html:
                    title_element = H1([], title_text)
                    # Wrap title + table in a div
                    result.html = Div([], [title_element, result.html])

                return result

            helpers.extract_database_html = patched_extract_database_html

            logger.info(
                "Applied monkeypatch to extract_database_html for database title extraction"
            )
        except ImportError as e:
            logger.warning(
                f"unstructured-ingest helpers not found - skipping database title monkeypatch: {e}."
            )
        except Exception as e:
            logger.warning(
                f"Failed to apply database title monkeypatch: {e}. "
                "Database titles may not be extracted."
            )

    @staticmethod
    def _monkeypatch_databases_endpoint_query():
        """Monkeypatch unstructured-ingest DatabasesEndpoint to fix super().query() AttributeError.

        TEMPORARY FIX: In some environments (particularly with older notion-client versions),
        calling super().query() in DatabasesEndpoint.iterate_query() fails with:
        AttributeError: 'super' object has no attribute 'query'

        This happens because unstructured-ingest[notion]==0.7.2 doesn't pin a specific
        notion-client version, so different environments can get incompatible versions.

        This patches iterate_query to call the parent's request method directly instead
        of relying on super().query(), which may not exist in all notion-client versions.
        """
        try:
            from typing import Any, Generator, List

            from unstructured_ingest.processes.connectors.notion.client import (
                DatabasesEndpoint,
            )
            from unstructured_ingest.processes.connectors.notion.types.database_properties import (
                map_cells,
            )
            from unstructured_ingest.processes.connectors.notion.types.page import (
                Page,
            )

            def patched_iterate_query(
                self: Any, database_id: str, **kwargs: Any
            ) -> Generator[List[Page], None, None]:
                """Patched version that avoids super().query() call."""
                try:
                    # Import pick from notion_client to build request body
                    from notion_client.api_endpoints import pick
                except ImportError:
                    # Fallback if pick is not available - just use kwargs directly
                    def pick(base: Dict[Any, Any], *keys: str) -> Dict[Any, Any]:
                        return {k: v for k, v in base.items() if k in keys}

                next_cursor = None
                while True:
                    # Build request body (what super().query() would do)
                    body = pick(kwargs, "filter", "sorts", "start_cursor", "page_size")
                    if next_cursor:
                        body["start_cursor"] = next_cursor

                    # Call parent.request directly instead of super().query()
                    # This is what DatabasesEndpoint.query() does in the parent class
                    response: dict = self.parent.request(
                        path=f"databases/{database_id}/query",
                        method="POST",
                        body=body,
                        auth=kwargs.get("auth"),
                    )

                    pages = [
                        Page.from_dict(data=p) for p in response.pop("results", [])
                    ]
                    for p in pages:
                        p.properties = map_cells(p.properties)
                    yield pages

                    next_cursor = response.get("next_cursor")
                    if not response.get("has_more") or not next_cursor:
                        return

            DatabasesEndpoint.iterate_query = patched_iterate_query
            logger.info(
                "Applied monkeypatch to unstructured-ingest DatabasesEndpoint for super().query() compatibility"
            )
        except ImportError as e:
            logger.warning(
                f"unstructured-ingest DatabasesEndpoint not found - skipping query monkeypatch: {e}."
            )
        except Exception as e:
            logger.warning(
                f"Failed to apply DatabasesEndpoint query monkeypatch: {e}. "
                "May encounter 'super' object has no attribute 'query' errors."
            )

    def _initialize_state_tracking(self) -> None:
        """Initialize state tracking for content-based change detection.

        Stores document content hashes and processing config to detect when
        documents need reprocessing due to content or config changes.
        """
        # Determine state file path
        if (
            self.config.stateful_ingestion is not None
            and hasattr(self.config.stateful_ingestion, "state_file_path")
            and self.config.stateful_ingestion.state_file_path
        ):
            self.state_file_path = Path(self.config.stateful_ingestion.state_file_path)
        else:
            # Default: ~/.datahub/notion_state/{pipeline_name}.json
            state_dir = Path.home() / ".datahub" / "notion_state"
            state_dir.mkdir(parents=True, exist_ok=True)
            pipeline_name = self.ctx.pipeline_name or "default"
            self.state_file_path = state_dir / f"{pipeline_name}.json"

        # Load existing state
        self._load_state()
        logger.info(
            f"Content-based change detection enabled, state file: {self.state_file_path}, "
            f"tracking {len(self.document_state)} documents"
        )

    def _load_state(self) -> None:
        """Load document state from file."""
        if self.state_file_path and self.state_file_path.exists():
            try:
                with open(self.state_file_path) as f:
                    self.document_state = json.load(f)
                logger.debug(f"Loaded state for {len(self.document_state)} documents")
            except Exception as e:
                logger.warning(f"Failed to load state file: {e}")
                self.document_state = {}

    def _save_state(self) -> None:
        """Save document state to file."""
        if self.state_file_path:
            try:
                with open(self.state_file_path, "w") as f:
                    json.dump(self.document_state, f, indent=2)
                logger.debug(f"Saved state for {len(self.document_state)} documents")
            except Exception as e:
                logger.warning(f"Failed to save state file: {e}")

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
            # Embedding affects vector embeddings on chunks
            "embedding_enabled": embedding_enabled,
            "embedding_provider": self.config.embedding.provider
            if embedding_enabled
            else None,
            "embedding_model": self.config.embedding.model
            if embedding_enabled
            else None,
            # Partitioning affects how text is extracted
            "partition_strategy": self.config.processing.partition.strategy,
            # Hierarchy affects parent relationships
            "hierarchy_enabled": self.config.hierarchy.enabled,
        }

    def _calculate_document_hash(self, text: str, page_id: str) -> str:
        """Calculate hash of document content AND processing configuration.

        This ensures documents are reprocessed when:
        1. Content changes (text is different)
        2. Processing config changes (chunking enabled, embedding model changed, etc.)

        Args:
            text: Document text content
            page_id: Notion page ID (for logging)

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

    def _should_process_document(
        self, document_urn: str, text: str, page_id: str
    ) -> bool:
        """Check if document should be processed based on content and config hash.

        Args:
            document_urn: DataHub document URN
            text: Document text content
            page_id: Notion page ID (for logging)

        Returns:
            True if document should be processed, False to skip
        """
        if (
            not self.config.stateful_ingestion
            or not self.config.stateful_ingestion.enabled
        ):
            return True

        # Force reprocess overrides incremental mode
        if (
            hasattr(self.config.stateful_ingestion, "incremental")
            and hasattr(self.config.stateful_ingestion.incremental, "force_reprocess")
            and self.config.stateful_ingestion.incremental.force_reprocess
        ):
            logger.debug(f"Force reprocess enabled, processing {page_id}")
            return True

        # Calculate current hash (content + config)
        current_hash = self._calculate_document_hash(text, page_id)

        # Check if we've seen this document before
        if document_urn not in self.document_state:
            logger.debug(f"New document {page_id}, will process")
            return True

        # Compare hashes
        previous_hash = self.document_state[document_urn].get("content_hash")
        if previous_hash != current_hash:
            logger.debug(
                f"Document {page_id} changed (content or config), will reprocess"
            )
            return True

        logger.debug(f"Document {page_id} unchanged, skipping")
        return False

    def _update_document_state(
        self, document_urn: str, text: str, page_id: str
    ) -> None:
        """Update state after successfully processing document.

        Args:
            document_urn: DataHub document URN
            text: Document text content
            page_id: Notion page ID (for logging)
        """
        from datetime import datetime

        self.document_state[document_urn] = {
            "content_hash": self._calculate_document_hash(text, page_id),
            "last_processed": datetime.utcnow().isoformat(),
            "page_id": page_id,
        }

    @staticmethod
    def _test_semantic_search_capability(
        config: NotionSourceConfig,
    ) -> CapabilityReport:
        """Test semantic search capability using the same logic as ingestion.

        Uses DocumentChunkingSource.resolve_embedding_config() to ensure
        test_connection mirrors actual ingestion behavior exactly:
        1. If explicit embedding config provided: validate it
        2. If not: try to load from DataHub server
        3. If server doesn't have config or is unreachable: use defaults

        Args:
            config: Full Notion source configuration

        Returns:
            CapabilityReport for semantic search capability
        """
        from datahub.ingestion.graph.client import DataHubGraph
        from datahub.ingestion.graph.config import DatahubClientConfig
        from datahub.ingestion.source.unstructured.chunking_source import (
            DocumentChunkingSource,
        )

        try:
            # Create graph connection to query server (if datahub config provided)
            graph = None
            if config.datahub and config.datahub.server:
                graph_config = DatahubClientConfig(
                    server=config.datahub.server,
                    token=config.datahub.token,
                )
                graph = DataHubGraph(config=graph_config)

            # Use shared resolution logic (server → defaults)
            resolved_config = DocumentChunkingSource.resolve_embedding_config(
                config.embedding, graph
            )

            # Test the resolved configuration
            return DocumentChunkingSource.test_embedding_capability(resolved_config)

        except ValueError as e:
            # Config validation failed (e.g., semantic search not enabled on server)
            return CapabilityReport(
                capable=False,
                failure_reason=str(e),
                mitigation_message="Fix the embedding configuration or enable semantic search on DataHub server.",
            )
        except Exception as e:
            # Unexpected error during config resolution
            return CapabilityReport(
                capable=False,
                failure_reason=f"Failed to resolve embedding configuration: {e}",
                mitigation_message="Check DataHub server connection and embedding configuration.",
            )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Test connection to Notion API.

        Validates:
        1. Config is parseable
        2. API key is valid
        3. Can connect to Notion API
        4. If page_ids/database_ids provided, validates they are accessible
        5. Semantic search capability (if explicitly configured)
        """
        try:
            config = NotionSourceConfig.parse_obj(config_dict)
        except Exception as e:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Failed to parse config: {e}",
            )

        try:
            from notion_client import Client

            # Test basic connectivity by calling the search endpoint
            client = Client(auth=config.api_key.get_secret_value())

            # Call search with page_size=1 to validate API key
            try:
                response: dict = client.search(page_size=1)  # type: ignore[assignment]
                basic_connectivity = CapabilityReport(
                    capable=True,
                )
            except Exception as e:
                return TestConnectionReport(
                    basic_connectivity=CapabilityReport(
                        capable=False,
                        failure_reason=f"Failed to connect to Notion API: {e.__class__.__name__}: {e}",
                        mitigation_message="Check that your API key is valid and has not expired. "
                        "You can create a new integration at https://www.notion.so/my-integrations",
                    )
                )

            # If specific pages/databases are provided, validate access
            if config.page_ids or config.database_ids:
                inaccessible = []

                for page_id in config.page_ids:
                    try:
                        client.pages.retrieve(page_id)
                    except Exception as e:
                        inaccessible.append(f"page {page_id}: {e.__class__.__name__}")

                for db_id in config.database_ids:
                    try:
                        client.databases.retrieve(db_id)
                    except Exception as e:
                        inaccessible.append(f"database {db_id}: {e.__class__.__name__}")

                if inaccessible:
                    return TestConnectionReport(
                        basic_connectivity=basic_connectivity,
                        capability_report={
                            "Page/Database Access": CapabilityReport(
                                capable=False,
                                failure_reason=f"Cannot access: {', '.join(inaccessible[:3])}{'...' if len(inaccessible) > 3 else ''}",
                                mitigation_message="Make sure the pages and databases are shared with your integration. "
                                "Open the page in Notion → Click '...' → 'Add connections' → Choose your integration.",
                            )
                        },
                    )

                # All pages/databases accessible
                capability_report: Dict[
                    Union[SourceCapability, str], CapabilityReport
                ] = {
                    "Page/Database Access": CapabilityReport(capable=True),
                    "Semantic Search": NotionSource._test_semantic_search_capability(
                        config
                    ),
                }

                return TestConnectionReport(
                    basic_connectivity=basic_connectivity,
                    capability_report=capability_report,
                )

            # No specific pages provided - just basic connectivity succeeded
            total_accessible = len(response.get("results", []))

            capability_report = {
                "Auto-Discovery": CapabilityReport(
                    capable=True,
                    mitigation_message=f"Integration can access {total_accessible}+ pages/databases. "
                    "Auto-discovery will find all accessible content.",
                ),
                "Semantic Search": NotionSource._test_semantic_search_capability(
                    config
                ),
            }

            return TestConnectionReport(
                basic_connectivity=basic_connectivity,
                capability_report=capability_report,
            )

        except Exception as e:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Unexpected error during connection test: {e}",
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "NotionSource":
        config = NotionSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        """Register workunit processors for stateful ingestion.

        The stale entity removal handler will automatically track all emitted
        document URNs and generate deletion workunits for any that disappeared.
        """
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Main method to generate work units."""
        try:
            # Run Unstructured.io pipeline
            output_dir = self._run_unstructured_pipeline()

            # Process output files
            # The stale_entity_removal_handler.workunit_processor registered in
            # get_workunit_processors() will automatically wrap this stream to track URNs
            yield from self._process_unstructured_output(output_dir)

            # Save state after successful processing
            if (
                self.config.stateful_ingestion
                and self.config.stateful_ingestion.enabled
            ):
                self._save_state()
                logger.info(f"Saved state for {len(self.document_state)} documents")

        except Exception as e:
            logger.error(f"Failed to run Unstructured pipeline: {e}", exc_info=True)
            self.report.report_failure(str(e))
            if self.config.advanced.raise_on_error:
                raise

    def _validate_page_accessibility(
        self, page_ids: list[str], database_ids: list[str]
    ) -> None:
        """Validate that provided page_ids and database_ids are accessible to the integration.

        Raises:
            ValueError: If any provided page or database is not accessible
        """
        from notion_client import Client

        if not page_ids and not database_ids:
            return  # Nothing to validate

        logger.info("Validating accessibility of provided pages and databases...")

        client = Client(auth=self.config.api_key.get_secret_value())
        inaccessible_pages = []
        inaccessible_databases = []

        # Validate pages
        for page_id in page_ids:
            try:
                client.pages.retrieve(page_id)
                logger.debug(f"Validated access to page: {page_id}")
            except Exception as e:
                inaccessible_pages.append((page_id, str(e)))
                logger.warning(
                    f"Page {page_id} is not accessible: {e.__class__.__name__}: {e}"
                )

        # Validate databases
        for db_id in database_ids:
            try:
                client.databases.retrieve(db_id)
                logger.debug(f"Validated access to database: {db_id}")
            except Exception as e:
                inaccessible_databases.append((db_id, str(e)))
                logger.warning(
                    f"Database {db_id} is not accessible: {e.__class__.__name__}: {e}"
                )

        # Raise error if any pages or databases are inaccessible
        if inaccessible_pages or inaccessible_databases:
            error_msg_parts = []

            if inaccessible_pages:
                page_ids_str = ", ".join([pid for pid, _ in inaccessible_pages])
                error_msg_parts.append(
                    f"The following page IDs are not accessible: {page_ids_str}"
                )

            if inaccessible_databases:
                db_ids_str = ", ".join([did for did, _ in inaccessible_databases])
                error_msg_parts.append(
                    f"The following database IDs are not accessible: {db_ids_str}"
                )

            error_msg_parts.append(
                "\n\nMake sure these pages/databases are shared with your Notion integration. "
                "To share a page or database with your integration:\n"
                "1. Open the page in Notion\n"
                "2. Click the '...' menu in the top right\n"
                "3. Select 'Add connections'\n"
                "4. Choose your integration"
            )

            raise ValueError("\n\n".join(error_msg_parts))

        logger.info(
            f"Successfully validated access to {len(page_ids)} pages and {len(database_ids)} databases"
        )

    def _discover_all_accessible_pages(self) -> tuple[list[str], list[str]]:
        """Discover all pages and databases accessible to the integration.

        Uses the Notion API search endpoint without a query parameter to find all
        pages and databases that have been shared with the integration.

        Returns:
            Tuple of (page_ids, database_ids) discovered
        """
        from notion_client import Client

        logger.info(
            "No page_ids or database_ids provided - discovering all accessible pages and databases..."
        )

        client = Client(auth=self.config.api_key.get_secret_value())

        discovered_page_ids = []
        discovered_database_ids = []
        start_cursor = None
        total_pages = 0
        total_databases = 0

        try:
            while True:
                # Call search without query to get all accessible content
                search_params = {"page_size": 100}
                if start_cursor:
                    search_params["start_cursor"] = start_cursor

                response: dict = client.search(**search_params)  # type: ignore[assignment]

                # Process results
                for result in response.get("results", []):
                    object_type = result.get("object")
                    result_id = result.get("id")

                    if not result_id:
                        continue

                    # Separate pages from databases
                    if object_type == "page":
                        discovered_page_ids.append(result_id)
                        total_pages += 1
                        logger.debug(
                            f"Discovered page: {result.get('properties', {}).get('title', {}).get('title', [{}])[0].get('plain_text', result_id)}"
                        )
                    elif object_type == "database":
                        discovered_database_ids.append(result_id)
                        total_databases += 1
                        logger.debug(
                            f"Discovered database: {result.get('title', [{}])[0].get('plain_text', result_id)}"
                        )

                # Check for more results
                if not response.get("has_more", False):
                    break

                start_cursor = response.get("next_cursor")

        except Exception as e:
            logger.error(f"Failed to discover accessible pages: {e}", exc_info=True)
            raise

        logger.info(
            f"Discovery complete: found {total_pages} pages and {total_databases} databases"
        )

        return discovered_page_ids, discovered_database_ids

    def _run_unstructured_pipeline(self) -> Path:
        """Run the Unstructured.io Notion pipeline and return output directory."""
        logger.info("Running Unstructured.io Notion pipeline...")

        # Apply monkeypatches for Notion API compatibility
        # Do this here (not in __init__) because modules are only imported when unstructured-ingest loads
        self._patch_notion_client_for_is_locked()
        self._monkeypatch_database_property_description()
        self._monkeypatch_database_title_extraction()
        self._monkeypatch_databases_endpoint_query()

        # Auto-discover pages if none provided
        page_ids = self.config.page_ids
        database_ids = self.config.database_ids

        if not page_ids and not database_ids:
            discovered_pages, discovered_databases = (
                self._discover_all_accessible_pages()
            )
            page_ids = discovered_pages
            database_ids = discovered_databases

            if not page_ids and not database_ids:
                logger.warning(
                    "No pages or databases discovered. Make sure pages are shared with your integration."
                )
        else:
            # Validate that provided pages/databases are accessible
            self._validate_page_accessibility(page_ids, database_ids)

        if not page_ids and database_ids:
            logger.info(f"Using configured database_ids: {len(database_ids)} databases")
        elif page_ids and not database_ids:
            logger.info(f"Using configured page_ids: {len(page_ids)} pages")
        else:
            logger.info(
                f"Using configured page_ids and database_ids: {len(page_ids)} pages, {len(database_ids)} databases"
            )

        # Create work directory
        work_dir = Path(self.config.advanced.work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)

        # Create output directory
        output_dir = work_dir / "output"
        output_dir.mkdir(exist_ok=True)

        try:
            from unstructured_ingest.interfaces.processor import ProcessorConfig
            from unstructured_ingest.pipeline.pipeline import Pipeline
            from unstructured_ingest.processes.connectors.local import (
                LocalUploaderConfig,
            )
            from unstructured_ingest.processes.connectors.notion.connector import (
                NotionAccessConfig,
                NotionConnectionConfig,
                NotionDownloaderConfig,
                NotionIndexerConfig,
            )
            from unstructured_ingest.processes.partitioner import PartitionerConfig

            # Configure processor
            processor_config = ProcessorConfig(
                work_dir=str(work_dir),
                output_dir=str(output_dir),
                num_processes=self.config.processing.parallelism.num_processes,
                verbose=self.config.advanced.raise_on_error,
                reprocess=False,
                raise_on_error=False,
            )

            # Configure Notion indexer (use discovered IDs if auto-discovery was triggered)
            indexer_config = NotionIndexerConfig(
                page_ids=page_ids,
                database_ids=database_ids,
                recursive=self.config.recursive,
            )

            # Configure Notion connection
            access_config = NotionAccessConfig(
                notion_api_key=self.config.api_key.get_secret_value()
            )
            connection_config = NotionConnectionConfig(access_config=access_config)

            # Configure downloader
            downloader_config = NotionDownloaderConfig()

            # Configure partitioner
            partitioner_config = PartitionerConfig(
                strategy=self.config.processing.partition.strategy,
                partition_by_api=self.config.processing.partition.partition_by_api,
                api_key=self.config.processing.partition.api_key,
                ocr_languages=self.config.processing.partition.ocr_languages,
                additional_partition_args=self.config.processing.partition.additional_args,
            )

            # Configure uploader (destination)
            uploader_config = LocalUploaderConfig(output_dir=str(output_dir))

            # Create pipeline
            pipeline = Pipeline.from_configs(
                context=processor_config,
                indexer_config=indexer_config,
                downloader_config=downloader_config,
                source_connection_config=connection_config,
                partitioner_config=partitioner_config,
                uploader_config=uploader_config,
            )

            # Run pipeline
            pipeline.run()

            logger.info(f"Unstructured pipeline completed. Output in {output_dir}")
            return output_dir

        except Exception as e:
            logger.error(f"Unstructured pipeline failed: {e}", exc_info=True)

            # Check if we have any partial output to process
            if output_dir.exists():
                output_files = list(output_dir.glob("*.json"))
                if output_files:
                    logger.warning(
                        f"Pipeline failed but found {len(output_files)} successfully processed files. "
                        f"Continuing with partial results."
                    )
                    return output_dir

            # No partial output, re-raise the exception
            raise

    def _load_notion_parent_metadata(self, work_dir: Path) -> Dict[str, Any]:
        """Load parent metadata from Unstructured's indexer FileData files.

        Returns a mapping of page_id -> additional_metadata for parent extraction.
        """
        parent_metadata_map: Dict[str, Any] = {}
        indexer_dir = work_dir / "indexer"

        if not indexer_dir.exists():
            return parent_metadata_map

        for file_data_file in indexer_dir.glob("*.json"):
            try:
                with open(file_data_file, "r") as f:
                    file_data = json.load(f)

                # Extract page_id from record_locator
                record_locator = file_data.get("metadata", {}).get("record_locator", {})
                page_id = record_locator.get("page_id") or record_locator.get(
                    "database_id"
                )

                if page_id:
                    additional_metadata = file_data.get("additional_metadata", {})

                    # Copy Notion API timestamps from FileData metadata
                    file_metadata = file_data.get("metadata", {})
                    if "date_created" in file_metadata:
                        additional_metadata["date_created"] = file_metadata[
                            "date_created"
                        ]
                    if "date_modified" in file_metadata:
                        additional_metadata["date_modified"] = file_metadata[
                            "date_modified"
                        ]

                    parent_metadata_map[page_id] = additional_metadata

            except Exception as e:
                logger.warning(f"Failed to load FileData from {file_data_file}: {e}")

        logger.info(
            f"Loaded parent metadata for {len(parent_metadata_map)} Notion pages"
        )
        return parent_metadata_map

    def _process_unstructured_output(
        self, output_dir: Path
    ) -> Iterable[MetadataWorkUnit]:
        """Process Unstructured output JSON files and generate work units."""
        # Load Notion parent metadata (needed for hierarchy + timestamps)
        work_dir = output_dir.parent
        self.notion_parent_metadata = self._load_notion_parent_metadata(work_dir)

        parent_page_ids: Set[str] = set()
        if self.config.hierarchy.enabled:
            # Collect all parent page IDs from the metadata to exempt them from filtering
            for _page_id, additional_metadata in self.notion_parent_metadata.items():
                parent_info = additional_metadata.get("parent")
                if parent_info and isinstance(parent_info, dict):
                    parent_page_id = parent_info.get("page_id")
                    if parent_page_id:
                        parent_page_ids.add(parent_page_id)

            logger.info(
                f"Identified {len(parent_page_ids)} parent pages to exempt from text length filtering"
            )

        # Find all JSON output files
        json_files = list(output_dir.glob("**/*.json"))
        logger.info(f"Found {len(json_files)} output files to process")

        # First pass: determine which pages will actually be ingested
        ingested_page_ids: Set[str] = set()
        files_to_process = []
        for json_file in json_files:
            self.report.report_file_scanned()

            try:
                # Load JSON output (unstructured outputs a list of elements)
                with open(json_file, "r", encoding="utf-8") as f:
                    elements = json.load(f)

                # Validate it's a list
                if not isinstance(elements, list):
                    raise ValueError(
                        f"Expected JSON array of elements, got {type(elements)}"
                    )

                # Extract common metadata from first element
                metadata = elements[0].get("metadata", {}) if elements else {}

                # Wrap in expected format
                data = {"elements": elements, "metadata": metadata}

                # Check if we should skip this file
                if self._should_skip_file(data, parent_page_ids):
                    continue

                # Track this page as being ingested
                data_source = metadata.get("data_source", {})
                record_locator = data_source.get("record_locator", {})
                page_id = record_locator.get("page_id")
                if page_id:
                    ingested_page_ids.add(page_id)

                # Save for second pass
                files_to_process.append(data)

            except Exception as e:
                error_msg = f"Failed to process {json_file}: {e}"
                logger.error(error_msg, exc_info=True)
                self.report.report_file_failed(error_msg)

                if not self.config.advanced.continue_on_failure:
                    raise

        logger.info(
            f"Will ingest {len(ingested_page_ids)} pages after filtering. "
            f"Only parent references within this set will be emitted."
        )

        # Second pass: create documents with validated parent references
        for data in files_to_process:
            try:
                # Create document entity, passing ingested page IDs for parent validation
                yield from self._create_document_entity(data, ingested_page_ids)

            except Exception as e:
                metadata = data.get("metadata", {})
                filename = (
                    metadata.get("filename", "unknown")
                    if isinstance(metadata, dict)
                    else "unknown"
                )
                error_msg = f"Failed to create document entity for {filename}: {e}"
                logger.error(error_msg, exc_info=True)
                self.report.report_file_failed(error_msg)

                if not self.config.advanced.continue_on_failure:
                    raise

    def _should_skip_file(
        self, data: dict, parent_page_ids: Optional[Set[str]] = None
    ) -> bool:
        """Check if file should be skipped based on filtering config."""
        metadata = data.get("metadata", {})
        elements = data.get("elements", [])

        # Extract page_id for this document
        data_source = metadata.get("data_source", {})
        record_locator = data_source.get("record_locator", {})
        current_page_id = record_locator.get("page_id")

        # Check for Notion page ID prefix filtering
        # If page_ids were specified, only keep docs with matching prefixes
        if self.config.page_ids:
            # Extract page_id from metadata
            data_source = metadata.get("data_source", {})
            record_locator = data_source.get("record_locator", {})
            page_id = record_locator.get("page_id")
            database_id = record_locator.get("database_id")

            # Check if this document matches any of the page_id prefixes
            # Normalize IDs by removing hyphens for comparison (Notion returns both formats)
            should_keep = False
            for configured_page_id in self.config.page_ids:
                # Normalize configured ID (remove hyphens) and take first 13 chars
                normalized_prefix = configured_page_id.replace("-", "")[:13]

                # Keep if page_id or database_id starts with the prefix (after normalization)
                if page_id:
                    normalized_page_id = page_id.replace("-", "")
                    if normalized_page_id.startswith(normalized_prefix):
                        should_keep = True
                        break
                if database_id:
                    normalized_database_id = database_id.replace("-", "")
                    if normalized_database_id.startswith(normalized_prefix):
                        should_keep = True
                        break

            if not should_keep:
                self.report.report_file_skipped(
                    metadata.get("filename", "unknown"),
                    "Notion page_id/database_id doesn't match configured page_ids prefix",
                )
                return True

        # Check for empty documents
        if self.config.filtering.skip_empty_documents:
            if not elements:
                self.report.report_file_skipped(
                    metadata.get("filename", "unknown"), "No elements extracted"
                )
                return True

        # Check text length (exempt parent pages when hierarchy is enabled)
        is_parent_page = (
            parent_page_ids is not None
            and current_page_id is not None
            and current_page_id in parent_page_ids
        )

        total_text_length = sum(len(elem.get("text", "")) for elem in elements)
        if total_text_length < self.config.filtering.min_text_length:
            if is_parent_page:
                logger.info(
                    f"Skipping text length filter for parent page {current_page_id} "
                    f"(has {total_text_length} chars, below min {self.config.filtering.min_text_length})"
                )
            else:
                self.report.report_file_skipped(
                    metadata.get("filename", "unknown"),
                    f"Text too short: {total_text_length} chars",
                )
                return True

        # Check file size if specified
        file_size = metadata.get("file_size")
        if file_size:
            if (
                self.config.filtering.min_file_size
                and file_size < self.config.filtering.min_file_size
            ):
                self.report.report_file_skipped(
                    metadata.get("filename", "unknown"),
                    f"File too small: {file_size} bytes",
                )
                return True

            if (
                self.config.filtering.max_file_size
                and file_size > self.config.filtering.max_file_size
            ):
                self.report.report_file_skipped(
                    metadata.get("filename", "unknown"),
                    f"File too large: {file_size} bytes",
                )
                return True

        return False

    def _extract_last_modified_time(
        self, metadata: Dict[str, Any]
    ) -> Optional[datetime]:
        """Extract last_edited_time from Notion API (stored as date_modified in FileData)."""
        data_source = metadata.get("data_source", {})
        record_locator = data_source.get("record_locator", {})
        page_id_str = record_locator.get("page_id")

        if not page_id_str:
            return None

        additional_metadata = self.notion_parent_metadata.get(page_id_str, {})
        date_modified = additional_metadata.get("date_modified")

        if not date_modified:
            return None

        try:
            # Notion API format: "2026-01-17T08:19:00.000Z"
            return datetime.fromisoformat(date_modified.replace("Z", "+00:00"))
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to parse date_modified '{date_modified}': {e}")
            return None

    def _extract_created_time(self, metadata: Dict[str, Any]) -> Optional[datetime]:
        """Extract created_time from Notion API (stored as date_created in FileData)."""
        data_source = metadata.get("data_source", {})
        record_locator = data_source.get("record_locator", {})
        page_id_str = record_locator.get("page_id")

        if not page_id_str:
            return None

        additional_metadata = self.notion_parent_metadata.get(page_id_str, {})
        date_created = additional_metadata.get("date_created")

        if not date_created:
            return None

        try:
            # Notion API format: "2025-12-04T18:35:00.000Z"
            return datetime.fromisoformat(date_created.replace("Z", "+00:00"))
        except (ValueError, AttributeError) as e:
            logger.warning(f"Failed to parse date_created '{date_created}': {e}")
            return None

    def _extract_notion_url(self, metadata: Dict[str, Any]) -> Optional[str]:
        """Extract Notion page/database URL from additional_metadata or construct from ID."""
        data_source = metadata.get("data_source", {})
        record_locator = data_source.get("record_locator", {})

        if not isinstance(record_locator, dict):
            return None

        # Try to get page_id first
        page_id_str = record_locator.get("page_id")
        if page_id_str:
            # Get additional_metadata from our preloaded map
            additional_metadata = self.notion_parent_metadata.get(page_id_str, {})
            url = additional_metadata.get("url")
            if url:
                return url
            # If URL not in metadata, construct it from page_id
            # Format: https://www.notion.so/{id-without-dashes}
            clean_id = page_id_str.replace("-", "")
            return f"https://www.notion.so/{clean_id}"

        # Try database_id if page_id not found
        database_id_str = record_locator.get("database_id")
        if database_id_str:
            # Construct URL from database_id
            # Format: https://www.notion.so/{id-without-dashes}
            clean_id = database_id_str.replace("-", "")
            return f"https://www.notion.so/{clean_id}"

        return None

    def _extract_notion_parent_urn(
        self,
        elements: List[dict],
        metadata: Dict[str, Any],
        ingested_page_ids: Optional[Set[str]] = None,
    ) -> Optional[str]:
        """Extract parent page URN from Notion metadata.

        Only returns a parent URN if the parent page is being ingested.
        This prevents broken references to pages outside the ingestion scope.

        Notion's additional_metadata contains a 'parent' field with parent page/database info.
        """
        data_source = metadata.get("data_source", {})
        record_locator = data_source.get("record_locator", {})

        if not isinstance(record_locator, dict):
            return None

        # Get current page_id
        page_id_str = record_locator.get("page_id")
        if not page_id_str:
            return None

        # Get additional_metadata from our preloaded map
        additional_metadata = self.notion_parent_metadata.get(page_id_str, {})
        parent_info = additional_metadata.get("parent")

        if not parent_info or not isinstance(parent_info, dict):
            return None

        # Extract parent page_id from Notion's parent structure
        # Notion parent can be: {"type": "page_id", "page_id": "abc-123"}
        # or: {"type": "database_id", "database_id": "def-456"}
        # or: {"type": "workspace", "workspace": true}
        parent_type = parent_info.get("type")
        parent_id = None

        if parent_type == "page_id":
            parent_id = parent_info.get("page_id")
        elif parent_type == "database_id":
            parent_id = parent_info.get("database_id")
        # Skip workspace parents (root level pages)

        if not parent_id:
            return None

        # Only emit parent URN if the parent page is being ingested
        if ingested_page_ids is not None and parent_id not in ingested_page_ids:
            logger.info(
                f"Skipping parent reference for page {page_id_str}: "
                f"parent {parent_id} is not being ingested (outside scope)"
            )
            return None

        # Generate parent URN using same ID pattern as documents
        parent_doc_id = self.document_builder.id_generator.generate_id(
            filename=f"{parent_id}.html",
            directory="",
            metadata={"source_record_locator": f"page_id={parent_id}"},
        )

        # Construct URN
        parent_urn = f"urn:li:document:{parent_doc_id}"
        return parent_urn

    def _create_document_entity(
        self, data: dict, ingested_page_ids: Optional[Set[str]] = None
    ) -> Iterable[MetadataWorkUnit]:
        """Create a Document entity from Unstructured output."""
        elements = data.get("elements", [])
        metadata = data.get("metadata", {})

        # Extract page ID for change detection
        data_source = metadata.get("data_source", {})
        record_locator = data_source.get("record_locator", {})
        page_id = record_locator.get("page_id", "unknown")

        # Extract text content for hashing
        text_content = " ".join(elem.get("text", "") for elem in elements)

        # Build document URN early (needed for state tracking)
        # Use the same logic as document_builder to ensure consistency
        doc_id = f"notion.{page_id}"
        document_urn = f"urn:li:document:{doc_id}"

        # Check if we should process this document (content/config changed)
        if not self._should_process_document(document_urn, text_content, page_id):
            logger.info(f"Skipping unchanged document: {page_id}")
            return  # Skip processing, document hasn't changed

        # Custom properties are no longer extracted - important metadata like
        # last_modified now goes in proper DocumentInfo fields instead of customProperties
        custom_properties: Dict[str, str] = {}

        # Remove local file system paths that aren't useful for Notion
        # These are local cache paths from unstructured-ingest, not Notion URLs
        custom_properties.pop("source_directory", None)
        custom_properties.pop("source_filename", None)

        # Add processing metadata
        custom_properties["ingestion_source"] = "notion"
        custom_properties["processing_strategy"] = (
            self.config.processing.partition.strategy
        )

        # Extract Notion URL from additional_metadata
        notion_url = self._extract_notion_url(metadata)
        if notion_url:
            # Inject URL into data_source for DocumentSource aspect
            if "data_source" not in metadata:
                metadata["data_source"] = {}
            metadata["data_source"]["url"] = notion_url

        # Determine parent URN
        parent_urn = None
        if self.config.hierarchy.enabled:
            parent_urn = self._extract_notion_parent_urn(
                elements, metadata, ingested_page_ids
            )

        # Extract Notion API timestamps
        created_time = self._extract_created_time(metadata)
        last_modified_time = self._extract_last_modified_time(metadata)

        # Build document entity
        doc = self.document_builder.build_document_entity(
            elements=elements,
            metadata=metadata,
            custom_properties=custom_properties,
            parent_urn=parent_urn,
            created_time=created_time,
            last_modified_time=last_modified_time,
        )

        # Add dataPlatformInstance aspect
        from datahub.metadata.schema_classes import DataPlatformInstanceClass

        platform_instance = DataPlatformInstanceClass(
            platform="urn:li:dataPlatform:notion"
        )
        doc._set_aspect(platform_instance)

        # Get document URN for chunking/embedding (convert to string)
        document_urn = str(doc.urn)

        # Generate work units from Document entity
        for wu in doc.as_workunits():
            yield wu

        # Generate embeddings inline using ChunkingSource
        try:
            yield from self.chunking_source.process_elements_inline(
                document_urn=document_urn, elements=elements
            )
        except Exception as e:
            logger.warning(
                f"Failed to generate embeddings for {document_urn}: {e}. "
                f"Document will be ingested without embeddings.",
                exc_info=True,
            )

        # Update report
        file_type = metadata.get("filetype", "unknown")
        text_bytes = sum(len(elem.get("text", "")) for elem in elements)
        self.report.report_file_processed(file_type, len(elements), text_bytes)
        self.report.report_document_created(is_folder=False)
        self.report.report_partitioning_strategy(
            self.config.processing.partition.strategy
        )

        # Update document state after successful processing
        if self.config.stateful_ingestion and self.config.stateful_ingestion.enabled:
            self._update_document_state(document_urn, text_content, page_id)

    def get_report(self) -> NotionSourceReport:
        # Copy embedding statistics from chunking source report
        if self.chunking_source:
            chunking_report = self.chunking_source.report
            self.report.num_documents_with_embeddings = (
                chunking_report.num_documents_with_embeddings
            )
            self.report.num_embedding_failures = chunking_report.num_embedding_failures
            # Extend LossyList with items from the regular list
            for failure in chunking_report.embedding_failures:
                self.report.embedding_failures.append(failure)

        return self.report

    def close(self) -> None:
        """Cleanup resources."""
        # Clean up work directory if not preserving outputs
        if not self.config.advanced.preserve_outputs:
            work_dir = Path(self.config.advanced.work_dir)
            if work_dir.exists():
                import shutil

                try:
                    shutil.rmtree(work_dir)
                    logger.info(f"Cleaned up work directory: {work_dir}")
                except Exception as e:
                    logger.warning(f"Failed to clean up work directory: {e}")

        # Call parent close() to prepare and commit stateful ingestion state
        super().close()
