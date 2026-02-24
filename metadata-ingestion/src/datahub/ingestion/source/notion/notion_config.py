"""Configuration models for NotionSource."""

from typing import List

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.unstructured.chunking_config import (
    ChunkingConfig,
    DataHubConnectionConfig,
    EmbeddingConfig,
)
from datahub.ingestion.source.unstructured.config import (
    AdvancedConfig,
    DocumentMappingConfig,
    FilteringConfig,
    HierarchyConfig,
    ProcessingConfig,
)


class NotionSourceConfig(
    StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig], ConfigModel
):
    """
    Notion ingestion configuration.

    This source extracts documents from Notion pages and databases
    using the Notion API and Unstructured.io text extraction.
    """

    # Notion-specific fields (flattened to top level)
    api_key: SecretStr = Field(
        description="Notion internal integration token. "
        "Create one at https://www.notion.so/my-integrations"
    )

    page_ids: List[str] = Field(
        default_factory=list,
        description="List of Notion page IDs to ingest. "
        "IDs can be found in page URLs: https://www.notion.so/Page-Title-{PAGE_ID}. "
        "If both page_ids and database_ids are empty, the source will automatically "
        "discover and ingest ALL pages and databases accessible to the integration.",
    )

    database_ids: List[str] = Field(
        default_factory=list,
        description="List of Notion database IDs to ingest. "
        "If both page_ids and database_ids are empty, the source will automatically "
        "discover and ingest ALL pages and databases accessible to the integration. "
        "IDs can be found in database URLs: https://www.notion.so/{DATABASE_ID}",
    )

    recursive: bool = Field(
        default=True,
        description="Recursively fetch child pages. "
        "When true, ingests all descendant pages of specified pages/databases.",
    )

    # Shared configuration sections (from unstructured/config.py)
    processing: ProcessingConfig = Field(
        default_factory=ProcessingConfig,
        description="Text extraction and partitioning configuration",
    )

    document_mapping: DocumentMappingConfig = Field(
        default_factory=DocumentMappingConfig,
        description="Document entity mapping configuration (ID generation, title extraction)",
    )

    hierarchy: HierarchyConfig = Field(
        default_factory=HierarchyConfig,
        description="Parent-child relationship configuration",
    )

    filtering: FilteringConfig = Field(
        default_factory=FilteringConfig,
        description="Document filtering configuration",
    )

    datahub: DataHubConnectionConfig = Field(
        default_factory=DataHubConnectionConfig,
        description="DataHub connection configuration (for querying server-side embedding config)",
    )

    chunking: ChunkingConfig = Field(
        default_factory=ChunkingConfig,
        description="Chunking strategy configuration (for embeddings)",
    )

    embedding: EmbeddingConfig = Field(
        default_factory=EmbeddingConfig,
        description="Embedding generation configuration (LiteLLM with Cohere/Bedrock)",
    )

    advanced: AdvancedConfig = Field(
        default_factory=AdvancedConfig,
        description="Advanced configuration options (work directory, error handling)",
    )

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, v: SecretStr) -> SecretStr:
        """Validate API key is not empty."""
        if not v or not v.get_secret_value().strip():
            raise ValueError("api_key is required and cannot be empty")
        return v

    @field_validator("page_ids")
    @classmethod
    def validate_page_ids(cls, v: List[str]) -> List[str]:
        """Validate and normalize page IDs, supporting both raw IDs and URLs."""
        normalized = []
        for item in v:
            item = item.strip()

            # Check if it's a URL (contains notion.so)
            if "notion.so" in item.lower():
                # Extract ID from URL
                # URL formats:
                # - https://www.notion.so/workspace/Page-Title-{PAGE_ID}
                # - https://www.notion.so/Page-Title-{PAGE_ID}?param=value
                # - https://notion.so/{PAGE_ID}

                # Remove query parameters and anchors
                base_url = item.split("?")[0].split("#")[0]

                # Get the last path segment
                path_parts = base_url.rstrip("/").split("/")
                last_segment = path_parts[-1]

                # If the last segment contains dashes, the ID is after the last dash
                # Otherwise, the whole segment might be the ID
                if "-" in last_segment:
                    potential_id = last_segment.split("-")[-1]
                else:
                    potential_id = last_segment

                extracted_id = potential_id.replace("-", "").lower()

                # Validate the extracted ID
                if len(extracted_id) != 32:
                    raise ValueError(
                        f"Could not extract valid Notion ID from URL: {item}. "
                        f"Expected 32-character hex ID, got {len(extracted_id)} characters."
                    )

                try:
                    int(extracted_id, 16)
                except ValueError:
                    raise ValueError(
                        f"Extracted ID from URL contains non-hexadecimal characters: {item}"
                    ) from None

                # Format with dashes for consistency (Notion format: 8-4-4-4-12)
                formatted_id = f"{extracted_id[:8]}-{extracted_id[8:12]}-{extracted_id[12:16]}-{extracted_id[16:20]}-{extracted_id[20:]}"
                normalized.append(formatted_id)

            else:
                # Raw ID provided - normalize and validate
                clean_id = item.replace("-", "")
                if len(clean_id) != 32:
                    raise ValueError(
                        f"Invalid Notion page ID format: {item}. "
                        f"Expected 32 hex characters (with or without hyphens)"
                    )
                try:
                    int(clean_id, 16)
                except ValueError:
                    raise ValueError(
                        f"Invalid Notion page ID format: {item}. "
                        f"Must contain only hexadecimal characters"
                    ) from None

                # Format with dashes for consistency
                formatted_id = f"{clean_id[:8]}-{clean_id[8:12]}-{clean_id[12:16]}-{clean_id[16:20]}-{clean_id[20:]}"
                normalized.append(formatted_id)

        return normalized

    @field_validator("database_ids")
    @classmethod
    def validate_database_ids(cls, v: List[str]) -> List[str]:
        """Validate and normalize database IDs, supporting both raw IDs and URLs."""
        normalized = []
        for item in v:
            item = item.strip()

            # Check if it's a URL (contains notion.so)
            if "notion.so" in item.lower():
                # Extract ID from URL (same logic as page_ids)
                base_url = item.split("?")[0].split("#")[0]
                path_parts = base_url.rstrip("/").split("/")
                last_segment = path_parts[-1]

                if "-" in last_segment:
                    potential_id = last_segment.split("-")[-1]
                else:
                    potential_id = last_segment

                extracted_id = potential_id.replace("-", "").lower()

                if len(extracted_id) != 32:
                    raise ValueError(
                        f"Could not extract valid Notion ID from URL: {item}. "
                        f"Expected 32-character hex ID, got {len(extracted_id)} characters."
                    )

                try:
                    int(extracted_id, 16)
                except ValueError:
                    raise ValueError(
                        f"Extracted ID from URL contains non-hexadecimal characters: {item}"
                    ) from None

                formatted_id = f"{extracted_id[:8]}-{extracted_id[8:12]}-{extracted_id[12:16]}-{extracted_id[16:20]}-{extracted_id[20:]}"
                normalized.append(formatted_id)

            else:
                # Raw ID provided - normalize and validate
                clean_id = item.replace("-", "")
                if len(clean_id) != 32:
                    raise ValueError(
                        f"Invalid Notion database ID format: {item}. "
                        f"Expected 32 hex characters (with or without hyphens)"
                    )
                try:
                    int(clean_id, 16)
                except ValueError:
                    raise ValueError(
                        f"Invalid Notion database ID format: {item}. "
                        f"Must contain only hexadecimal characters"
                    ) from None

                formatted_id = f"{clean_id[:8]}-{clean_id[8:12]}-{clean_id[12:16]}-{clean_id[16:20]}-{clean_id[20:]}"
                normalized.append(formatted_id)

        return normalized

    # Removed validator - auto-discovery is now supported when both page_ids and database_ids are empty

    @model_validator(mode="after")
    def set_hierarchy_defaults(self) -> "NotionSourceConfig":
        """Set default hierarchy strategy to 'notion' for Notion source."""
        if self.hierarchy.enabled and self.hierarchy.parent_strategy == "folder":
            # Override default 'folder' strategy to 'notion' for Notion source
            self.hierarchy.parent_strategy = "notion"
        return self

    @model_validator(mode="after")
    def set_document_id_pattern(self) -> "NotionSourceConfig":
        """Set document ID pattern to use only Notion page ID, not local cache path.

        The default pattern {source_type}-{directory}-{basename} includes the local
        cache directory path, which is unstable and not portable. For Notion, we only
        want to use the page ID (basename) to create stable URNs like:
        urn:li:document:notion-2bffc6a6-4277-8024-97c9-d0f26faa4480
        """
        if self.document_mapping.id_pattern == "{source_type}-{directory}-{basename}":
            # Override default pattern to exclude directory (local cache path)
            self.document_mapping.id_pattern = "{source_type}-{basename}"
        return self
