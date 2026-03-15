"""
Confluence source for DataHub ingestion.

This source connector extracts pages and spaces from Atlassian Confluence (Cloud and Data Center)
and ingests them as Document entities in DataHub with support for semantic search.
"""

import hashlib
import logging
import time
from typing import Any, Dict, Iterable, List, Optional, Set, Union

from atlassian import Confluence

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
from datahub.ingestion.source.confluence.confluence_config import ConfluenceSourceConfig
from datahub.ingestion.source.confluence.confluence_hierarchy import (
    ConfluenceHierarchyExtractor,
)
from datahub.ingestion.source.confluence.confluence_report import ConfluenceSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.unstructured.document_builder import (
    DocumentEntityBuilder,
)
from datahub.metadata.schema_classes import (
    DataPlatformInfoClass,
    DataPlatformInstanceClass,
    DocumentStateClass,
    PlatformTypeClass,
)
from datahub.sdk.document import Document

logger = logging.getLogger(__name__)


@platform_name("Confluence")
@config_class(ConfluenceSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class ConfluenceSource(StatefulIngestionSourceBase, TestableSource):
    platform = "confluence"  # Required for stateful ingestion checkpoint job_id
    """
    Ingests Confluence pages and spaces as DataHub Document entities.

    ## Capabilities

    - **Metadata Extraction**: Extracts pages, spaces, and hierarchical relationships
    - **Semantic Search**: Generates embeddings for semantic search capabilities
    - **Stateful Ingestion**: Supports incremental updates based on content changes
    - **Parent-Child Relationships**: Preserves page hierarchy within spaces
    - **Cloud & Data Center**: Supports both Confluence Cloud and Data Center/Server

    ## Prerequisites

    ### Confluence Cloud Setup

    1. **Generate API Token**:
       - Go to https://id.atlassian.com/manage-profile/security/api-tokens
       - Create a new API token
       - Save the token securely

    2. **Grant Permissions**:
       - User must have read access to spaces you want to ingest
       - Recommended: Use a dedicated service account

    ### Confluence Data Center Setup

    1. **Generate Personal Access Token**:
       - Navigate to: User Profile > Settings > Personal Access Tokens
       - Create a new token with read permissions
       - Save the token securely

    2. **Grant Permissions**:
       - User must have read access to spaces you want to ingest
       - Check space permissions in Space Settings > Permissions

    ## Configuration

    ### Minimal Configuration (Cloud)

    ```yaml
    source:
      type: confluence
      config:
        # Confluence Cloud
        url: "https://your-domain.atlassian.net/wiki"
        username: "user@company.com"
        api_token: "${CONFLUENCE_API_TOKEN}"

        # Simple unified URLs - mix spaces and pages freely!
        urls:
          - "https://your-domain.atlassian.net/wiki/spaces/TEAM"  # Full space
          - "https://your-domain.atlassian.net/wiki/spaces/DOCS/pages/123456/Parent"  # Page tree
          - "TEAM"  # Space key also works
          - "789012"  # Page ID also works

        # Control recursive child page crawling (applies to page URLs)
        recursive: true  # Default: crawl all child pages
    ```

    ### Minimal Configuration (Data Center)

    ```yaml
    source:
      type: confluence
      config:
        url: "https://confluence.your-company.com"
        cloud: false
        personal_access_token: "${CONFLUENCE_PAT}"
        urls:
          - "PUBLIC"  # Space key
          - "https://confluence.your-company.com/pages/viewpage.action?pageId=123456"  # Page URL
    ```

    ### Advanced Configuration with Semantic Search

    ```yaml
    source:
      type: confluence
      config:
        url: "https://your-domain.atlassian.net/wiki"
        username: "your-email@example.com"
        api_token: "${CONFLUENCE_API_TOKEN}"

        # Mix spaces and page trees in one simple list
        urls:
          - "https://your-domain.atlassian.net/wiki/spaces/PUBLIC"  # All pages from PUBLIC space
          - "https://your-domain.atlassian.net/wiki/spaces/TEAM/pages/123456/Engineering"  # Just this page tree
          - "https://confluence.company.com/pages/viewpage.action?pageId=789012"  # Data Center format

        recursive: true  # Crawl children from page URLs
        max_pages_per_space: 1000

        # Enable chunking for semantic search
        chunking:
          enabled: true
          strategy: "by_title"
          chunk_size: 500
          chunk_overlap: 50

        # Enable embeddings
        embedding:
          enabled: true
          provider: "openai"
          model: "text-embedding-3-small"
          api_key: "${OPENAI_API_KEY}"

        # Content filtering
        filtering:
          min_text_length: 100
          include_empty_docs: false

        # Stateful ingestion
        stateful_ingestion:
          enabled: true
          remove_stale_metadata: true
    ```

    ## Key Features

    ### Hierarchical Relationships

    The connector automatically preserves the page hierarchy:
    - Space → Page → Child Page → Grandchild Page
    - Parent relationships are extracted from Confluence's ancestor metadata

    ### Stateful Ingestion

    Supports incremental updates:
    - Tracks all ingested document URNs in DataHub
    - Automatically removes stale entities when pages are deleted
    - DataHub handles deduplication of unchanged content

    ### Semantic Search

    When chunking and embeddings are enabled:
    - Documents are split into chunks based on configured strategy
    - Embeddings are generated for each chunk
    - Enables semantic search in DataHub UI

    ## Common Use Cases

    ### 1. Ingest Specific Spaces

    ```yaml
    config:
      urls:
        - "ENGINEERING"
        - "PRODUCT"
        - "DESIGN"
    ```

    ### 2. Ingest Specific Page Trees

    ```yaml
    config:
      urls:
        - "https://domain.atlassian.net/wiki/spaces/DOCS/pages/123456/API-Documentation"
        - "https://domain.atlassian.net/wiki/spaces/TEAM/pages/789012/Onboarding"
      recursive: true  # Include all child pages
    ```

    ### 3. Mix Spaces and Pages

    ```yaml
    config:
      urls:
        - "PUBLIC"  # Full space
        - "https://domain.atlassian.net/wiki/spaces/TEAM/pages/123456/Engineering"  # Just this page tree
    ```

    ### 4. Ingest All Accessible Spaces

    ```yaml
    config:
      # Omit urls field to auto-discover
      max_spaces: 100  # Limit for safety
    ```

    ### 5. Enable Semantic Search

    ```yaml
    config:
      chunking:
        enabled: true
        strategy: "by_title"
      embedding:
        enabled: true
        provider: "openai"
    ```

    ### 6. Incremental Updates

    ```yaml
    config:
      stateful_ingestion:
        enabled: true
        remove_stale_metadata: true
    ```

    ## Troubleshooting

    ### Authentication Errors

    - **Cloud**: Verify username/email and API token are correct
    - **Data Center**: Verify PAT is valid and has read permissions
    - Test connection: Use `datahub check source-connection -c config.yml`

    ### Rate Limiting

    If hitting Confluence API rate limits:
    ```yaml
    config:
      max_pages_per_space: 500  # Reduce batch size
      advanced:
        max_workers: 1  # Reduce parallelism
    ```

    ### Large Spaces Timeout

    For spaces with 10,000+ pages:
    ```yaml
    config:
      max_pages_per_space: 5000  # Set reasonable limit
      advanced:
        timeout_seconds: 600  # Increase timeout
    ```

    ### Permission Errors

    - Verify service account has read access to all target spaces
    - Check Space Settings > Permissions in Confluence
    - Review failed_spaces in ingestion report

    ## Notes

    - **Page IDs**: Uses Confluence's numeric page IDs for URN generation
    - **Attachments**: Not supported in v1 (pages only)
    - **Archived Content**: Archived pages are not ingested by default
    - **Rate Limits**: Respects Confluence Cloud rate limits with backoff
    """

    platform = "confluence"

    config: ConfluenceSourceConfig
    report: ConfluenceSourceReport

    def __init__(self, config: ConfluenceSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report = ConfluenceSourceReport()

        # Initialize Confluence API client
        self.confluence_client = self._create_confluence_client()

        # Initialize document builder
        self.document_builder = DocumentEntityBuilder(
            config=config.document_mapping,
            source_type="confluence",
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
            max_documents=config.max_documents,
        )

        # Pass graph to DocumentChunkingSource so it can load server config
        self.chunking_source = DocumentChunkingSource(
            ctx=ctx,
            config=chunking_config,
            standalone=False,  # Run as sub-component
            graph=ctx.graph,  # Pass graph for server config loading
        )

        # Initialize stateful ingestion handler for stale entity removal
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, ctx
        )

    def _get_instance_id(self) -> str:
        """
        Get instance identifier for this Confluence installation.

        Returns unique identifier distinguishing between multiple Confluence instances:
        - Explicit: Uses platform_instance config if provided (human-readable)
        - Auto: Generates deterministic hash from base URL (guaranteed unique)

        Hash-based approach ensures URN uniqueness across all Confluence installations
        without requiring manual configuration or differentiating Cloud vs Data Center.
        """
        if self.config.platform_instance:
            return self.config.platform_instance

        if self.config.url:
            # Generate deterministic hash from base URL
            # First 8 chars provide ~4 billion unique values (collision probability negligible)
            url_hash = hashlib.sha256(self.config.url.encode()).hexdigest()[:8]
            return url_hash

        return "default"

    def _create_confluence_client(self) -> Confluence:
        """Create and configure Confluence API client."""
        if self.config.cloud:
            # Cloud authentication: username + API token
            if not self.config.username or not self.config.api_token:
                raise ValueError("Confluence Cloud requires username and api_token")

            return Confluence(
                url=self.config.url,
                username=self.config.username,
                password=self.config.api_token.get_secret_value(),
                cloud=True,
            )
        else:
            # Data Center authentication: Personal Access Token
            if not self.config.personal_access_token:
                raise ValueError(
                    "Confluence Data Center requires personal_access_token"
                )

            return Confluence(
                url=self.config.url,
                token=self.config.personal_access_token.get_secret_value(),
                cloud=False,
            )

    def _is_space_allowed(self, space_key: str) -> bool:
        """
        Check if space is allowed by spaces.allow/spaces.deny lists.

        Args:
            space_key: Confluence space key

        Returns:
            True if space should be ingested, False otherwise
        """
        # If spaces.allow is specified, space must be in the list
        if self.config._parsed_space_allow is not None:
            if space_key not in self.config._parsed_space_allow:
                return False

        # If spaces.deny is specified, space must NOT be in the list
        if self.config._parsed_space_deny is not None:
            if space_key in self.config._parsed_space_deny:
                return False

        return True

    def _is_page_allowed(self, page_id: str) -> bool:
        """
        Check if page is allowed by pages.allow/pages.deny lists.

        Args:
            page_id: Confluence page ID

        Returns:
            True if page should be ingested, False otherwise
        """
        # If pages.allow is specified, page must be in the list
        if self.config._parsed_page_allow is not None:
            if page_id not in self.config._parsed_page_allow:
                return False

        # If pages.deny is specified, page must NOT be in the list
        if self.config._parsed_page_deny is not None:
            if page_id in self.config._parsed_page_deny:
                return False

        return True

    def _get_spaces(self) -> List[str]:
        """
        Get list of space keys with allow/deny filtering.

        Returns:
            List of filtered Confluence space keys
        """
        # If spaces.allow is specified, use those spaces directly
        if self.config._parsed_space_allow is not None:
            logger.info(
                f"Using {len(self.config._parsed_space_allow)} spaces from spaces.allow"
            )
            spaces = list(self.config._parsed_space_allow)
        else:
            # Auto-discover spaces
            logger.info("Auto-discovering Confluence spaces...")
            try:
                spaces_response = self.confluence_client.get_all_spaces(
                    start=0,
                    limit=self.config.max_spaces,
                    expand="homepage,description",
                )

                if not spaces_response or "results" not in spaces_response:
                    logger.warning("No spaces found in Confluence")
                    return []

                spaces = [
                    space["key"]
                    for space in spaces_response["results"]
                    if "key" in space
                ]

                logger.info(f"Discovered {len(spaces)} spaces")

            except Exception as e:
                logger.error(f"Failed to discover spaces: {e}")
                self.report.report_failure("space_discovery", str(e))
                return []

        # Apply spaces.deny filter
        if self.config._parsed_space_deny is not None:
            original_count = len(spaces)
            spaces = [s for s in spaces if s not in self.config._parsed_space_deny]
            filtered_count = original_count - len(spaces)
            if filtered_count > 0:
                logger.info(f"Filtered out {filtered_count} spaces via spaces.deny")

        return spaces

    def _get_pages_recursively(
        self, page_id: str, visited: Optional[Set[str]] = None
    ) -> Iterable[Dict[str, Any]]:
        """
        Recursively fetch a page and all its child pages.

        Args:
            page_id: Starting page ID
            visited: Set of already-visited page IDs (to prevent cycles)

        Yields:
            Page metadata dictionaries
        """
        if visited is None:
            visited = set()

        # Prevent infinite loops from circular references
        if page_id in visited:
            logger.warning(f"Page {page_id} already visited, skipping to prevent cycle")
            return

        visited.add(page_id)

        try:
            # Fetch the page itself with full metadata
            page = self.confluence_client.get_page_by_id(
                page_id,
                expand="ancestors,version,space,body.storage",
            )

            if not page:
                logger.warning(f"Page {page_id} not found or not accessible")
                return

            logger.debug(f"Fetched page: {page_id} - {page.get('title', 'Unknown')}")
            self.report.report_page_scanned()
            yield page

            # If recursive mode enabled, fetch child pages
            if self.config.recursive:
                try:
                    # Get direct child pages (convert generator to list for length check)
                    children_response = list(
                        self.confluence_client.get_child_pages(page_id)
                    )

                    # The response is a list of page objects
                    if children_response:
                        child_count = len(children_response)
                        logger.debug(
                            f"Found {child_count} child pages for page {page_id}"
                        )

                        # Recursively process each child
                        for child_page in children_response:
                            child_id = str(child_page.get("id", ""))
                            if child_id:
                                yield from self._get_pages_recursively(
                                    child_id, visited
                                )

                except Exception as e:
                    logger.warning(
                        f"Failed to fetch child pages for page {page_id}: {e}"
                    )
                    # Continue processing even if child fetching fails

        except Exception as e:
            logger.error(f"Failed to fetch page {page_id}: {e}")
            self.report.report_failure(f"page_{page_id}", str(e))

            if not self.config.advanced.continue_on_failure:
                raise

    def _get_pages_in_space(self, space_key: str) -> Iterable[Dict[str, Any]]:
        """
        Get all pages in a Confluence space.

        Args:
            space_key: Confluence space key

        Yields:
            Page metadata dictionaries
        """
        start = 0
        limit = 100  # API pagination size
        total_pages = 0

        try:
            while total_pages < self.config.max_pages_per_space:
                # Get page content for the space
                response = self.confluence_client.get_all_pages_from_space(
                    space=space_key,
                    start=start,
                    limit=min(limit, self.config.max_pages_per_space - total_pages),
                    expand="ancestors,version,space,body.storage",
                )

                if not response:
                    break

                pages = (
                    response
                    if isinstance(response, list)
                    else response.get("results", [])
                )

                if not pages:
                    break

                for page in pages:
                    self.report.report_page_scanned()
                    yield page
                    total_pages += 1

                # Check if there are more pages
                if isinstance(response, dict):
                    links = response.get("_links", {})
                    if not links.get("next"):
                        break

                start += limit

                if total_pages >= self.config.max_pages_per_space:
                    logger.warning(
                        f"Reached max_pages_per_space limit ({self.config.max_pages_per_space}) for space {space_key}"
                    )
                    break

        except Exception as e:
            logger.error(f"Failed to fetch pages from space {space_key}: {e}")
            self.report.report_space_failed(space_key, str(e))

    def _get_pages_from_spaces(self, space_keys: List[str]) -> Iterable[Dict[str, Any]]:
        """
        Get all pages from specified spaces.

        Args:
            space_keys: List of space keys to fetch pages from

        Yields:
            Page metadata dictionaries
        """
        for space_key in space_keys:
            try:
                logger.info(f"Fetching pages from space: {space_key}")
                yield from self._get_pages_in_space(space_key)
            except Exception as e:
                logger.error(f"Failed to process space '{space_key}': {e}")
                self.report.report_failure(f"space_{space_key}", str(e))

                if not self.config.advanced.continue_on_failure:
                    raise

    def _get_pages_from_page_allow(self) -> Iterable[Dict[str, Any]]:
        """
        Get pages specified in pages.allow list.

        Yields:
            Page metadata dictionaries
        """
        if not self.config._parsed_page_allow:
            return

        logger.info(
            f"Processing {len(self.config._parsed_page_allow)} page(s) from pages.allow"
        )

        visited_pages: Set[str] = set()

        for page_id in self.config._parsed_page_allow:
            try:
                logger.info(
                    f"Fetching page tree from page: {page_id} (recursive={self.config.recursive})"
                )
                if self.config.recursive:
                    # Recursive crawl from this page
                    yield from self._get_pages_recursively(page_id, visited_pages)
                else:
                    # Just fetch this page (non-recursive)
                    page = self.confluence_client.get_page_by_id(
                        page_id,
                        expand="ancestors,version,space,body.storage",
                    )
                    if page:
                        visited_pages.add(page_id)
                        self.report.report_page_scanned()
                        yield page
                    else:
                        logger.warning(f"Page {page_id} not found or not accessible")

            except Exception as e:
                logger.error(f"Failed to process page '{page_id}': {e}")
                self.report.report_failure(f"page_{page_id}", str(e))

                if not self.config.advanced.continue_on_failure:
                    raise

    def _extract_text_from_page(self, page: Dict[str, Any]) -> str:
        """
        Extract text content from Confluence page.

        Args:
            page: Page metadata from Confluence API

        Returns:
            Extracted text content
        """
        text_parts = []

        # Extract title
        title = page.get("title", "")
        if title:
            text_parts.append(title)

        # Extract body content
        body = page.get("body", {})
        if isinstance(body, dict):
            storage = body.get("storage", {})
            if isinstance(storage, dict):
                value = storage.get("value", "")
                if value:
                    # Simple HTML tag removal (unstructured will do proper parsing)
                    import re

                    clean_text = re.sub(r"<[^>]+>", " ", value)
                    clean_text = re.sub(r"\s+", " ", clean_text).strip()
                    text_parts.append(clean_text)

        return "\n\n".join(text_parts)

    def _build_page_urn(self, page_id: str) -> str:
        """
        Build DataHub document URN for a Confluence page with instance identifier.

        Args:
            page_id: Confluence page ID

        Returns:
            DataHub document URN with format: urn:li:document:confluence-{instance_id}-{page_id}
        """
        instance_id = self._get_instance_id()
        return f"urn:li:document:{self.platform}-{instance_id}-{page_id}"

    def _extract_parent_urn(
        self, page: Dict[str, Any], ingested_page_ids: Set[str]
    ) -> Optional[str]:
        """
        Extract and validate parent URN for a page.

        Args:
            page: Page metadata from Confluence API
            ingested_page_ids: Set of page IDs being ingested

        Returns:
            Parent URN if valid, None otherwise
        """
        parent_id = ConfluenceHierarchyExtractor.extract_parent_id(page)
        if not parent_id:
            return None

        # Only emit parent relationship if parent is being ingested
        if parent_id not in ingested_page_ids:
            logger.info(
                f"Parent page {parent_id} not in ingestion scope (have: {sorted(list(ingested_page_ids))}), skipping parent relationship"
            )
            return None

        logger.info(
            f"Parent page {parent_id} is being ingested, including parent relationship"
        )
        instance_id = self._get_instance_id()
        return ConfluenceHierarchyExtractor.build_parent_urn(
            parent_id, platform=self.platform, instance_id=instance_id
        )

    def _create_document_entity(
        self,
        page: Dict[str, Any],
        ingested_page_ids: Set[str],
        parent_page_ids: Set[str],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Create DataHub document entity from Confluence page.

        Args:
            page: Page metadata from Confluence API
            ingested_page_ids: Set of page IDs being ingested
            parent_page_ids: Set of page IDs that have children (are parents)

        Yields:
            MetadataWorkUnit for document entity
        """
        start_time = time.time()

        page_id = str(page.get("id", ""))
        if not page_id:
            logger.warning("Page missing ID, skipping")
            return

        # Apply pages.deny filter
        if not self._is_page_allowed(page_id):
            logger.info(f"Page {page_id} filtered by pages.deny, skipping")
            self.report.report_page_skipped(page_id, "Filtered by pages.deny")
            return

        # Extract text content
        text = self._extract_text_from_page(page)

        # Check minimum text length
        if len(text) < self.config.filtering.min_text_length:
            # Exempt parent pages from text length filter
            if page_id not in parent_page_ids:
                self.report.report_page_skipped(
                    page_id,
                    f"Text length {len(text)} < minimum {self.config.filtering.min_text_length}",
                )
                return

        # Extract metadata
        title = (
            ConfluenceHierarchyExtractor.extract_page_title(page) or f"Page {page_id}"
        )
        url = ConfluenceHierarchyExtractor.extract_page_url(page)
        space_key = ConfluenceHierarchyExtractor.extract_space_key(page)

        # Build full URL if needed
        if url and not url.startswith("http"):
            base_url = self.config.url.rstrip("/")
            url = f"{base_url}{url}"

        # Extract parent URN
        parent_urn = self._extract_parent_urn(page, ingested_page_ids)

        # Build custom properties
        custom_properties = {
            "space_key": space_key or "",
            "page_id": page_id,
        }

        # Get timestamps if available
        version = page.get("version", {})
        created_time = None
        last_modified_time = None
        if isinstance(version, dict):
            # Confluence uses "when" field for timestamps
            when = version.get("when")
            if when:
                try:
                    from dateutil import parser as date_parser

                    last_modified_time = date_parser.parse(when)
                except Exception:
                    pass

        # Create Document using SDK (use confluence-{instance_id}-{page_id} format for uniqueness)
        instance_id = self._get_instance_id()
        doc_id = f"confluence-{instance_id}-{page_id}"

        # Construct proper external URL - fallback to base URL + page ID if webui not available
        if not url:
            # Remove trailing /wiki if present, then add the page path
            base_url = self.config.url.rstrip("/")
            if base_url.endswith("/wiki"):
                base_url = base_url[:-5]
            url = f"{base_url}/wiki/pages/{page_id}"

        # Debug logging
        logger.debug(
            f"Creating document: doc_id={doc_id}, parent_urn={parent_urn}, url={url}"
        )

        doc = Document.create_external_document(
            id=doc_id,
            title=title,
            platform=self.platform,
            external_url=url,
            external_id=page_id,
            text=text,
            status=DocumentStateClass.PUBLISHED,
            custom_properties=custom_properties,
            parent_document=parent_urn,
            created_time=created_time,
            last_modified_time=last_modified_time,
        )

        # Add dataPlatformInstance aspect for UI features (e.g., "View in Confluence" button)
        # TODO: This should be added to the Document SDK as an official parameter
        # instead of using the backdoor _set_aspect() method
        if self.config.url:
            # Extract instance identifier from URL (e.g., "datahub-integration-testing" from the domain)
            from urllib.parse import urlparse

            parsed_url = urlparse(self.config.url)
            # Use the domain as the instance identifier
            instance_id = (
                parsed_url.netloc.split(".")[0]
                if parsed_url.netloc
                else self.config.url
            )

            platform_instance = DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=make_dataplatform_instance_urn(self.platform, instance_id),
            )
            doc._set_aspect(platform_instance)

        # Add BrowsePathsV2 for hierarchical navigation
        browse_path_v2 = ConfluenceHierarchyExtractor.build_browse_path_v2(
            page_metadata=page,
            platform=self.platform,
            instance_id=self._get_instance_id(),
            ingested_page_ids=ingested_page_ids,
        )
        if browse_path_v2:
            doc._set_aspect(browse_path_v2)

        # Emit all workunits from the Document
        for workunit in doc.as_workunits():
            yield workunit

        # Generate embeddings inline using ChunkingSource
        # Convert text to elements format expected by chunking source
        elements = [
            {
                "type": "NarrativeText",
                "text": text,
                "metadata": {
                    "page_id": page_id,
                    "title": title,
                    "url": url,
                },
            }
        ]

        # Get document URN for chunking/embedding
        document_urn = f"urn:li:document:{doc_id}"

        try:
            yield from self.chunking_source.process_elements_inline(
                document_urn=document_urn, elements=elements
            )
        except RuntimeError as e:
            if self.chunking_source.report.num_documents_limit_reached:
                self.report.num_documents_limit_reached = True
                raise
            logger.warning(
                f"Failed to generate embeddings for {document_urn}: {e}. "
                f"Document will be ingested without embeddings.",
                exc_info=True,
            )
        except Exception as e:
            logger.warning(
                f"Failed to generate embeddings for {document_urn}: {e}. "
                f"Document will be ingested without embeddings.",
                exc_info=True,
            )

        # Update metrics
        processing_time = time.time() - start_time
        self.report.report_page_processed(page_id, processing_time)
        self.report.report_text_extracted(len(text))

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Main ingestion loop.

        Yields:
            MetadataWorkUnit for all entities
        """
        # Emit platform metadata with Confluence logo
        platform_urn = make_data_platform_urn(self.platform)
        platform_info = DataPlatformInfoClass(
            name=self.platform,
            type=PlatformTypeClass.OTHERS,
            datasetNameDelimiter=".",
            displayName="Confluence",
            logoUrl="https://cdn.worldvectorlogo.com/logos/confluence-1.svg",
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=platform_urn,
            aspect=platform_info,
        ).as_workunit()

        # Track all page IDs being ingested for parent validation
        all_pages: List[Dict[str, Any]] = []

        # Determine ingestion strategy based on configuration
        if self.config._parsed_page_allow is not None:
            # Page-specific ingestion: fetch only specified pages
            logger.info("Using pages.allow - fetching specific pages only")
            try:
                pages = list(self._get_pages_from_page_allow())
                all_pages.extend(pages)
                logger.info(f"Collected {len(pages)} pages from pages.allow")
            except Exception as e:
                logger.error(f"Failed to collect pages from pages.allow: {e}")
                if not self.config.advanced.continue_on_failure:
                    raise
        else:
            # Space-based ingestion: discover/use spaces, then fetch all their pages
            discovered_spaces = self._get_spaces()

            if not discovered_spaces:
                logger.warning("No spaces to process")
                return

            logger.info(f"Processing {len(discovered_spaces)} space(s)")

            try:
                pages = list(self._get_pages_from_spaces(discovered_spaces))
                all_pages.extend(pages)
                logger.info(f"Collected {len(pages)} pages from spaces")
            except Exception as e:
                logger.error(f"Failed to collect pages from spaces: {e}")
                if not self.config.advanced.continue_on_failure:
                    raise

        # Deduplicate pages by ID (in case page appears multiple times)
        unique_pages_map = {
            str(page.get("id")): page for page in all_pages if page.get("id")
        }
        all_pages = list(unique_pages_map.values())

        logger.info(f"Total unique pages collected: {len(all_pages)}")

        if not all_pages:
            logger.warning("No pages collected for ingestion")
            return

        # Build set of ingested page IDs
        ingested_page_ids = {
            str(page.get("id", "")) for page in all_pages if page.get("id")
        }
        logger.info(
            f"Ingesting {len(ingested_page_ids)} pages: {sorted(list(ingested_page_ids))}"
        )

        # Build set of parent page IDs (pages that have children)
        parent_page_ids: Set[str] = set()
        for page in all_pages:
            ancestors = page.get("ancestors", [])
            if ancestors:
                # The last ancestor is the immediate parent
                parent_id = str(ancestors[-1].get("id", ""))
                if parent_id:
                    parent_page_ids.add(parent_id)

        # Second pass: create document entities
        for page in all_pages:
            try:
                yield from self._create_document_entity(
                    page, ingested_page_ids, parent_page_ids
                )

            except Exception as e:
                page_id = str(page.get("id", "unknown"))
                space_key = (
                    ConfluenceHierarchyExtractor.extract_space_key(page) or "unknown"
                )
                logger.error(f"Failed to create entity for page {page_id}: {e}")
                self.report.report_page_failed(page_id, space_key, str(e))

                if (
                    not self.config.advanced.continue_on_failure
                    or self.chunking_source.report.num_documents_limit_reached
                ):
                    raise

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        """Register workunit processors for stateful ingestion.

        The stale entity removal handler will automatically track all emitted
        document URNs and generate deletion workunits for any that disappeared.
        """
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    @staticmethod
    def _test_semantic_search_capability(
        config: ConfluenceSourceConfig,
    ) -> CapabilityReport:
        """Test semantic search capability using the same logic as ingestion.

        Uses DocumentChunkingSource.resolve_embedding_config() to ensure
        test_connection mirrors actual ingestion behavior exactly:
        1. If explicit embedding config provided: validate it
        2. If not: try to load from DataHub server
        3. If server doesn't have config or is unreachable: use defaults

        Args:
            config: Full Confluence source configuration

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
            if hasattr(config, "datahub") and config.datahub and config.datahub.server:
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
    def test_connection(config_dict: dict) -> TestConnectionReport:  # noqa: C901
        """Test connection to Confluence API.

        Validates:
        1. Config is parseable
        2. Authentication credentials are valid
        3. Can connect to Confluence API
        4. If spaces provided, validates they are accessible
        5. Auto-discovers spaces and reports count
        6. Semantic search capability (from server config)
        """
        try:
            config = ConfluenceSourceConfig.model_validate(config_dict)
        except Exception as e:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Failed to parse config: {e}",
            )

        try:
            # Create Confluence client
            if config.cloud:
                if not config.username or not config.api_token:
                    return TestConnectionReport(
                        basic_connectivity=CapabilityReport(
                            capable=False,
                            failure_reason="Cloud authentication requires username and api_token",
                            mitigation_message="Provide both username and api_token for Confluence Cloud. "
                            "Generate API token at: https://id.atlassian.com/manage-profile/security/api-tokens",
                        )
                    )

                client = Confluence(
                    url=config.url,
                    username=config.username,
                    password=config.api_token.get_secret_value(),
                    cloud=True,
                )
            else:
                if not config.personal_access_token:
                    return TestConnectionReport(
                        basic_connectivity=CapabilityReport(
                            capable=False,
                            failure_reason="Data Center authentication requires personal_access_token",
                            mitigation_message="Generate a Personal Access Token from: User Profile > Settings > Personal Access Tokens",
                        )
                    )

                client = Confluence(
                    url=config.url,
                    token=config.personal_access_token.get_secret_value(),
                    cloud=False,
                )

            # Test basic connectivity
            try:
                client.get_all_spaces(start=0, limit=1)
                basic_connectivity = CapabilityReport(capable=True)
            except Exception as e:
                return TestConnectionReport(
                    basic_connectivity=CapabilityReport(
                        capable=False,
                        failure_reason=f"Failed to connect to Confluence: {e.__class__.__name__}: {e}",
                        mitigation_message="Check that your credentials are correct and you have access to Confluence. "
                        "Verify the URL is correct (include /wiki for Cloud).",
                    )
                )

            # If specific spaces/pages are configured, validate access
            if config._parsed_space_allow or config._parsed_page_allow:
                inaccessible_items = []
                accessible_pages = 0
                num_spaces = 0
                num_pages = 0

                # Test spaces.allow
                if config._parsed_space_allow:
                    for space_key in config._parsed_space_allow[:5]:  # Test first 5
                        try:
                            num_spaces += 1
                            space = client.get_space(space_key)
                            # Try to count pages in space
                            try:
                                pages = client.get_all_pages_from_space(
                                    space_key, start=0, limit=10
                                )
                                if pages:
                                    accessible_pages += len(pages)
                            except Exception:
                                pass  # Page counting is optional
                        except Exception as e:
                            inaccessible_items.append(
                                f"space {space_key}: {e.__class__.__name__}"
                            )

                # Test pages.allow
                if config._parsed_page_allow:
                    for page_id in config._parsed_page_allow[:5]:  # Test first 5
                        try:
                            num_pages += 1
                            page = client.get_page_by_id(page_id)
                            if page:
                                accessible_pages += 1
                        except Exception as e:
                            inaccessible_items.append(
                                f"page {page_id}: {e.__class__.__name__}"
                            )

                if inaccessible_items:
                    return TestConnectionReport(
                        basic_connectivity=basic_connectivity,
                        capability_report={
                            "Space/Page Access": CapabilityReport(
                                capable=False,
                                failure_reason=f"Cannot access: {', '.join(inaccessible_items[:3])}{'...' if len(inaccessible_items) > 3 else ''}",
                                mitigation_message="Verify your account has permission to access these Confluence spaces/pages. "
                                "Check Space Settings > Permissions in Confluence.",
                            )
                        },
                    )

                # All configured items accessible
                total_items = num_spaces + num_pages
                capability_report: Dict[
                    Union[SourceCapability, str], CapabilityReport
                ] = {
                    "Space/Page Access": CapabilityReport(
                        capable=True,
                        mitigation_message=f"Successfully validated access to {total_items} configured item(s) "
                        f"({num_spaces} space(s), {num_pages} page(s)). "
                        f"Found ~{accessible_pages}+ accessible pages.",
                    ),
                    "Semantic Search": ConfluenceSource._test_semantic_search_capability(
                        config
                    ),
                }

                return TestConnectionReport(
                    basic_connectivity=basic_connectivity,
                    capability_report=capability_report,
                )

            # No specific spaces configured - perform auto-discovery
            try:
                all_spaces = client.get_all_spaces(
                    start=0,
                    limit=min(config.max_spaces, 100),  # Cap at 100 for test
                    expand="homepage,description",
                )

                if not all_spaces or "results" not in all_spaces:
                    return TestConnectionReport(
                        basic_connectivity=basic_connectivity,
                        capability_report={
                            "Auto-Discovery": CapabilityReport(
                                capable=False,
                                failure_reason="No spaces found",
                                mitigation_message="Your account may not have access to any Confluence spaces. "
                                "Contact your Confluence administrator to grant space access.",
                            )
                        },
                    )

                num_spaces = len(all_spaces["results"])

                # Sample a few spaces to estimate page counts
                total_pages_estimate = 0
                for space in all_spaces["results"][:3]:  # Sample first 3 spaces
                    try:
                        space_key = space.get("key")
                        if space_key:
                            pages = client.get_all_pages_from_space(
                                space_key, start=0, limit=10
                            )
                            if pages:
                                total_pages_estimate += len(pages)
                    except Exception:
                        pass  # Page counting is optional

                capability_report = {
                    "Auto-Discovery": CapabilityReport(
                        capable=True,
                        mitigation_message=f"Discovered {num_spaces} accessible space(s) "
                        f"{'(limited by max_spaces config)' if num_spaces >= config.max_spaces else ''}. "
                        f"Found ~{total_pages_estimate}+ pages in sampled spaces. "
                        f"Auto-discovery will ingest all accessible content.",
                    ),
                    "Semantic Search": ConfluenceSource._test_semantic_search_capability(
                        config
                    ),
                }

                return TestConnectionReport(
                    basic_connectivity=basic_connectivity,
                    capability_report=capability_report,
                )

            except Exception as e:
                return TestConnectionReport(
                    basic_connectivity=basic_connectivity,
                    capability_report={
                        "Auto-Discovery": CapabilityReport(
                            capable=False,
                            failure_reason=f"Failed to discover spaces: {e}",
                            mitigation_message="Check that your account has permission to list Confluence spaces.",
                        )
                    },
                )

        except Exception as e:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Unexpected error during connection test: {e}",
            )

    def get_report(self) -> ConfluenceSourceReport:
        """Get ingestion report."""
        # Copy statistics from chunking source report
        if self.chunking_source:
            chunking_report = self.chunking_source.report
            self.report.num_documents_with_embeddings = (
                chunking_report.num_documents_with_embeddings
            )
            self.report.num_embedding_failures = chunking_report.num_embedding_failures
            # Extend LossyList with items from the regular list
            for failure in chunking_report.embedding_failures:
                self.report.embedding_failures.append(failure)
            self.report.num_documents_limit_reached = (
                chunking_report.num_documents_limit_reached
            )

        return self.report

    def close(self) -> None:
        """Clean up resources."""
        super().close()
