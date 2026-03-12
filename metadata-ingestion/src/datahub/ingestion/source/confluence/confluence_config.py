"""Configuration classes for Confluence source."""

import re
from typing import List, Optional
from urllib.parse import urlparse

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
    EmbeddingConfig,
)
from datahub.ingestion.source.unstructured.config import (
    AdvancedConfig,
    DocumentMappingConfig,
    FilteringConfig,
    HierarchyConfig,
    ProcessingConfig,
)


class SpaceFilterConfig(ConfigModel):
    """Configuration for filtering Confluence spaces."""

    allow: Optional[List[str]] = Field(
        default=None,
        description=(
            "List of Confluence spaces to include in ingestion. "
            "By default, all accessible spaces are discovered. "
            "Specify space keys or URLs to limit ingestion to specific spaces.\n\n"
            "Examples:\n"
            "  - Space keys: ['ENGINEERING', 'PRODUCT', 'DESIGN']\n"
            "  - Space URLs: ['https://domain.atlassian.net/wiki/spaces/TEAM']\n"
            "  - Mixed: ['ENGINEERING', 'https://domain.atlassian.net/wiki/spaces/PRODUCT']\n\n"
            "If specified, only these spaces will be ingested. "
            "Use deny to exclude specific spaces from discovery."
        ),
    )

    deny: Optional[List[str]] = Field(
        default=None,
        description=(
            "List of Confluence spaces to exclude from ingestion. "
            "Applies after allow filtering.\n\n"
            "Examples:\n"
            "  - Exclude personal spaces: ['~user1', '~user2']\n"
            "  - Exclude specific spaces: ['ARCHIVE', 'OLD_DOCS']\n"
            "  - Space URLs: ['https://domain.atlassian.net/wiki/spaces/TEST']\n\n"
            "Useful for excluding personal spaces or archived content."
        ),
    )


class PageFilterConfig(ConfigModel):
    """Configuration for filtering Confluence pages."""

    allow: Optional[List[str]] = Field(
        default=None,
        description=(
            "List of specific Confluence pages to include in ingestion. "
            "By default, all pages in discovered spaces are included. "
            "Specify page IDs or URLs to limit ingestion to specific pages and their children.\n\n"
            "Examples:\n"
            "  - Page IDs: ['123456', '789012']\n"
            "  - Page URLs: ['https://domain.atlassian.net/wiki/spaces/ENG/pages/123456/API-Docs']\n\n"
            "When specified, only these page trees will be ingested (if recursive=true). "
            "This allows focusing on specific documentation sections."
        ),
    )

    deny: Optional[List[str]] = Field(
        default=None,
        description=(
            "List of specific Confluence pages to exclude from ingestion. "
            "Applies after allow filtering.\n\n"
            "Examples:\n"
            "  - Exclude specific pages: ['123456', '789012']\n"
            "  - Page URLs: ['https://domain.atlassian.net/wiki/spaces/ENG/pages/999999/Draft']\n\n"
            "Useful for excluding specific pages within otherwise included spaces."
        ),
    )


class ConfluenceAuthConfig(ConfigModel):
    """Authentication configuration for Confluence."""

    username: Optional[str] = Field(
        default=None,
        description="Username for Confluence Cloud authentication (required for Cloud).",
    )
    api_token: Optional[SecretStr] = Field(
        default=None,
        description="API token for Confluence Cloud authentication (required for Cloud).",
    )
    personal_access_token: Optional[SecretStr] = Field(
        default=None,
        description="Personal Access Token for Confluence Data Center authentication.",
    )

    @model_validator(mode="after")
    def validate_auth_config(self) -> "ConfluenceAuthConfig":
        """Ensure exactly one authentication method is provided."""
        cloud_auth = bool(self.username and self.api_token)
        datacenter_auth = bool(self.personal_access_token)

        if not cloud_auth and not datacenter_auth:
            raise ValueError(
                "Either provide username + api_token (for Cloud) or personal_access_token (for Data Center)"
            )

        if cloud_auth and datacenter_auth:
            raise ValueError(
                "Cannot provide both Cloud authentication (username + api_token) and "
                "Data Center authentication (personal_access_token)"
            )

        if self.username and not self.api_token:
            raise ValueError("Username provided but api_token is missing")

        if self.api_token and not self.username:
            raise ValueError("API token provided but username is missing")

        return self


class ConfluenceSourceConfig(
    StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig], ConfigModel
):
    """Configuration for Confluence source connector."""

    # Confluence connection
    url: str = Field(
        description="Base URL of your Confluence instance. "
        "Examples: 'https://your-domain.atlassian.net/wiki' (Cloud) or "
        "'https://confluence.your-company.com' (Data Center)"
    )

    platform_instance: Optional[str] = Field(
        default=None,
        description="Optional human-readable identifier for this Confluence instance "
        "(e.g., 'mycompany-prod', 'team-a-confluence'). "
        "If not provided, automatically generated by hashing the base URL, which guarantees "
        "global uniqueness across all Confluence installations (both Cloud and Data Center). "
        "Use explicit values for more readable URNs, but auto-generated hashes are "
        "perfectly fine and require no manual configuration.",
    )

    cloud: bool = Field(
        default=True,
        description="Whether this is a Confluence Cloud instance (True) or Data Center/Server (False).",
    )

    # Authentication
    username: Optional[str] = Field(
        default=None,
        description="Username for Confluence Cloud authentication (required for Cloud).",
    )
    api_token: Optional[SecretStr] = Field(
        default=None,
        description="API token for Confluence Cloud authentication. "
        "Generate at: https://id.atlassian.com/manage-profile/security/api-tokens",
    )
    personal_access_token: Optional[SecretStr] = Field(
        default=None,
        description="Personal Access Token for Confluence Data Center authentication. "
        "Generate from: User Profile > Settings > Personal Access Tokens",
    )

    # Space filtering
    spaces: SpaceFilterConfig = Field(
        default_factory=SpaceFilterConfig,
        description="Configuration for filtering Confluence spaces.",
    )

    # Page filtering
    pages: PageFilterConfig = Field(
        default_factory=PageFilterConfig,
        description="Configuration for filtering Confluence pages.",
    )

    # Internal field to store parsed allow/deny lists
    _parsed_space_allow: Optional[List[str]] = None
    _parsed_space_deny: Optional[List[str]] = None
    _parsed_page_allow: Optional[List[str]] = None
    _parsed_page_deny: Optional[List[str]] = None

    max_spaces: int = Field(
        default=100,
        description="Maximum number of spaces to ingest when auto-discovering (applies when urls is not set).",
    )

    # Page configuration
    max_pages_per_space: int = Field(
        default=1000,
        description="Maximum number of pages to ingest per space.",
    )
    recursive: bool = Field(
        default=True,
        description="Whether to recursively fetch child pages (applies to page URLs only).",
    )

    # Content processing
    processing: ProcessingConfig = Field(
        default_factory=ProcessingConfig,
        description="Document processing configuration (partitioning strategy, OCR, etc.).",
    )

    # Document mapping
    document_mapping: DocumentMappingConfig = Field(
        default_factory=DocumentMappingConfig,
        description="Configuration for mapping Confluence pages to DataHub documents.",
    )

    # Hierarchy
    hierarchy: HierarchyConfig = Field(
        default_factory=HierarchyConfig,
        description="Parent-child relationship configuration.",
    )

    # Filtering
    filtering: FilteringConfig = Field(
        default_factory=FilteringConfig,
        description="Filtering options for document content.",
    )

    # Chunking and embeddings for semantic search
    chunking: ChunkingConfig = Field(
        default_factory=ChunkingConfig,
        description="Configuration for document chunking (required for embeddings).",
    )
    embedding: EmbeddingConfig = Field(
        default_factory=EmbeddingConfig,
        description="Configuration for generating vector embeddings for semantic search.",
    )

    max_documents: int = Field(
        default=10000,
        ge=-1,
        description="Maximum number of documents to process per ingestion run. "
        "The job will stop and fail with an error once this limit is reached. "
        "Set to 0 or -1 to disable the limit.",
    )

    # Advanced options
    advanced: AdvancedConfig = Field(
        default_factory=AdvancedConfig,
        description="Advanced ingestion options.",
    )

    @field_validator("platform_instance")
    @classmethod
    def validate_platform_instance(cls, v: Optional[str]) -> Optional[str]:
        """Validate platform_instance contains only safe characters."""
        if v is not None:
            if not v:
                raise ValueError("platform_instance cannot be empty string")
            if not re.match(r"^[a-zA-Z0-9_-]+$", v):
                raise ValueError(
                    f"platform_instance '{v}' must contain only alphanumeric characters, hyphens, and underscores"
                )
        return v

    @field_validator("url")
    @classmethod
    def normalize_url(cls, v: str) -> str:
        """
        Normalize Confluence URL by stripping common suffixes that users might accidentally include.

        Users often copy URLs directly from their browser which include paths like /home, /spaces, etc.
        We automatically strip these to get the base URL.
        """
        # Strip trailing slashes first
        v = v.rstrip("/")

        # Common suffixes to remove (in order of specificity)
        common_suffixes = [
            "/wiki/home",
            "/wiki/spaces",
            "/wiki/pages",
            "/home",
            "/spaces",
            "/pages",
        ]

        for suffix in common_suffixes:
            if v.endswith(suffix):
                v = v[: -len(suffix)]
                break

        # Ensure we have at least the /wiki part for Cloud instances
        # Cloud URLs should be: https://domain.atlassian.net/wiki
        if ".atlassian.net" in v and not v.endswith("/wiki"):
            v = v + "/wiki"

        return v

    @staticmethod
    def _parse_space_identifier(space_input: str) -> str:
        """
        Extract space key from space identifier (URL or key).

        Args:
            space_input: Space key or URL

        Returns:
            Space key (e.g., "TEAM", "~user1")
        """
        space_input = space_input.strip()

        # Validate not empty after stripping
        if not space_input:
            raise ValueError("Space identifier cannot be empty or whitespace-only")

        # If it's a URL, extract the space key
        if space_input.startswith(("http://", "https://")):
            parsed = urlparse(space_input)
            space_match = re.search(r"/spaces/([^/]+)", parsed.path)
            if space_match:
                return space_match.group(1)
            raise ValueError(
                f"Could not extract space key from URL: {space_input}. "
                "Expected format: https://domain.atlassian.net/wiki/spaces/KEY"
            )

        # Otherwise, it's a space key
        return space_input

    @staticmethod
    def _parse_page_identifier(page_input: str) -> str:
        """
        Extract page ID from page identifier (URL or ID).

        Args:
            page_input: Page ID or URL

        Returns:
            Page ID (e.g., "123456")
        """
        page_input = page_input.strip()

        # Validate not empty after stripping
        if not page_input:
            raise ValueError("Page identifier cannot be empty or whitespace-only")

        # If it's a URL, extract the page ID
        if page_input.startswith(("http://", "https://")):
            parsed = urlparse(page_input)

            # Check Cloud format: /pages/123456
            page_match = re.search(r"/pages/(\d+)", parsed.path)
            if page_match:
                return page_match.group(1)

            # Check Data Center format: ?pageId=123456
            from urllib.parse import parse_qs

            query_params = parse_qs(parsed.query)
            page_id_list = query_params.get("pageId", [])
            if page_id_list:
                return page_id_list[0]

            raise ValueError(
                f"Could not extract page ID from URL: {page_input}. "
                "Expected format: https://domain.atlassian.net/wiki/spaces/SPACE/pages/123456/Title "
                "or https://confluence.company.com/pages/viewpage.action?pageId=123456"
            )

        # Otherwise, it's a page ID
        if not page_input.isdigit():
            raise ValueError(
                f"Page ID must be numeric: {page_input}. "
                "Use pages.allow for page IDs, spaces.allow for space keys."
            )

        return page_input

    @model_validator(mode="after")
    def parse_allow_deny_lists(self) -> "ConfluenceSourceConfig":
        """Parse and normalize all allow/deny lists."""
        # Parse spaces.allow
        if self.spaces.allow is not None:
            if not isinstance(self.spaces.allow, list) or not self.spaces.allow:
                raise ValueError("spaces.allow must be a non-empty list")
            self._parsed_space_allow = [
                self._parse_space_identifier(s) for s in self.spaces.allow
            ]

        # Parse spaces.deny
        if self.spaces.deny is not None:
            if not isinstance(self.spaces.deny, list) or not self.spaces.deny:
                raise ValueError("spaces.deny must be a non-empty list")
            self._parsed_space_deny = [
                self._parse_space_identifier(s) for s in self.spaces.deny
            ]

        # Parse pages.allow
        if self.pages.allow is not None:
            if not isinstance(self.pages.allow, list) or not self.pages.allow:
                raise ValueError("pages.allow must be a non-empty list")
            self._parsed_page_allow = [
                self._parse_page_identifier(p) for p in self.pages.allow
            ]

        # Parse pages.deny
        if self.pages.deny is not None:
            if not isinstance(self.pages.deny, list) or not self.pages.deny:
                raise ValueError("pages.deny must be a non-empty list")
            self._parsed_page_deny = [
                self._parse_page_identifier(p) for p in self.pages.deny
            ]

        return self

    @model_validator(mode="after")
    def validate_auth_config(self) -> "ConfluenceSourceConfig":
        """Ensure authentication method matches cloud setting."""
        cloud_auth = bool(self.username and self.api_token)
        datacenter_auth = bool(self.personal_access_token)

        if not cloud_auth and not datacenter_auth:
            raise ValueError(
                "Either provide username + api_token (for Cloud) or personal_access_token (for Data Center)"
            )

        if cloud_auth and datacenter_auth:
            raise ValueError(
                "Cannot provide both Cloud authentication (username + api_token) and "
                "Data Center authentication (personal_access_token)"
            )

        # Validate auth method matches cloud setting
        if self.cloud and not cloud_auth:
            raise ValueError(
                "Confluence Cloud (cloud: true) requires username and api_token. "
                "For Data Center, set cloud: false and use personal_access_token instead."
            )

        if not self.cloud and not datacenter_auth:
            raise ValueError(
                "Confluence Data Center (cloud: false) requires personal_access_token. "
                "For Cloud, set cloud: true and use username + api_token instead."
            )

        if self.username and not self.api_token:
            raise ValueError("Username provided but api_token is missing")

        if self.api_token and not self.username:
            raise ValueError("API token provided but username is missing")

        return self

    @model_validator(mode="after")
    def set_hierarchy_defaults(self) -> "ConfluenceSourceConfig":
        """Set default hierarchy strategy to 'confluence'."""
        if self.hierarchy.parent_strategy == "folder":
            self.hierarchy.parent_strategy = "confluence"
        return self

    @model_validator(mode="after")
    def set_document_id_pattern(self) -> "ConfluenceSourceConfig":
        """Set document ID pattern to exclude directory component."""
        # Override default pattern to exclude directory
        if self.document_mapping.id_pattern == "{source_type}-{directory}-{basename}":
            self.document_mapping.id_pattern = "{source_type}-{basename}"
        return self
