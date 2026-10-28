import re
from typing import List, Literal, Optional

from pydantic import Field, field_validator, model_validator

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    TransparentSecretStr,
)
from datahub.configuration.source_common import DatasetSourceConfigMixin
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

# Supported file extensions for data_lake mode ingestion.
# Schema inference is attempted for structured formats; PDF files are tracked
# as Dataset entities with properties but no schema metadata.
SUPPORTED_FILE_TYPES = frozenset(
    ["csv", "tsv", "json", "jsonl", "parquet", "avro", "xlsx", "pdf"]
)


class SharePointAuthConfig(ConfigModel):
    """Authentication configuration for SharePoint via Microsoft Graph API.

    Supports service principal (app-only) authentication using either a
    client secret or a certificate. Use a service principal with the
    ``Sites.Read.All`` and ``Files.Read.All`` Microsoft Graph API permissions
    (application, not delegated).
    """

    tenant_id: str = Field(
        description="Azure Active Directory tenant ID (GUID or domain name)."
    )
    client_id: str = Field(
        description="Application (client) ID of the registered Azure AD app."
    )
    client_secret: Optional[TransparentSecretStr] = Field(
        default=None,
        description=(
            "Client secret for the registered Azure AD application. "
            "Required when certificate_path is not provided."
        ),
    )
    certificate_path: Optional[str] = Field(
        default=None,
        description=(
            "Absolute path to a PEM- or PFX-encoded certificate file for "
            "certificate-based authentication. Required when client_secret "
            "is not provided."
        ),
    )
    certificate_password: Optional[TransparentSecretStr] = Field(
        default=None,
        description="Password for the certificate file, if the certificate is encrypted.",
    )

    @model_validator(mode="after")
    def validate_credentials(self) -> "SharePointAuthConfig":
        has_secret = self.client_secret is not None
        has_cert = self.certificate_path is not None

        if not has_secret and not has_cert:
            raise ValueError(
                "Either client_secret or certificate_path must be provided "
                "for SharePoint authentication."
            )
        if has_secret and has_cert:
            raise ValueError(
                "Provide either client_secret or certificate_path, not both."
            )
        return self


class SharePointSiteConfig(ConfigModel):
    """Scoping configuration: which SharePoint sites and libraries to ingest."""

    hostname: str = Field(
        description=(
            "SharePoint Online hostname, e.g. 'myorg.sharepoint.com'. "
            "This is the root hostname under which all sites are discovered."
        )
    )
    site_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Allow/deny pattern applied to site relative paths "
            "(e.g. '/sites/Engineering', '/teams/DataTeam'). "
            "By default all accessible sites are included."
        ),
    )
    library_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Allow/deny pattern applied to document library names "
            "(e.g. 'Documents', 'Shared Documents'). "
            "By default all document libraries are included."
        ),
    )
    skip_personal_sites: bool = Field(
        default=True,
        description=(
            "When True (the default), personal OneDrive sites are excluded from "
            "ingestion. Personal sites have paths starting with '/personal/' "
            "(e.g. '/personal/jane_doe_contoso_com'). Disable this to also ingest "
            "individual OneDrive for Business libraries."
        ),
    )

    @field_validator("hostname")
    @classmethod
    def normalize_hostname(cls, v: str) -> str:
        v = v.strip()
        for prefix in ("https://", "http://"):
            if v.startswith(prefix):
                v = v[len(prefix) :]
        return v.rstrip("/")


class SharePointSourceConfig(
    StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig],
    DatasetSourceConfigMixin,
):
    """Configuration for the SharePoint source connector.

    SharePoint can be ingested in two modes:

    * ``data_lake`` — treats SharePoint Document Libraries as a structured
      file store, similar to the S3/ABS/GCS connectors. Files are ingested as
      Dataset entities with schema inference.

    * ``document`` — treats SharePoint Pages (and optionally Office files) as
      knowledge documents, similar to the Confluence/Notion connectors. Pages
      are ingested as Document entities with optional semantic-search support.
    """

    auth: SharePointAuthConfig = Field(
        description="Authentication configuration for Microsoft Graph API."
    )
    site: SharePointSiteConfig = Field(
        description="Scoping configuration: hostname, site filter, library filter."
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description=(
            "Optional human-readable identifier for this SharePoint tenant "
            "(e.g. 'myorg-sharepoint'). Used to disambiguate multiple tenants. "
            "Must contain only alphanumeric characters, hyphens, and underscores."
        ),
    )
    mode: Literal["data_lake", "document", "both"] = Field(
        default="data_lake",
        description=(
            "Ingestion mode. Use 'data_lake' to ingest structured files as Datasets; "
            "use 'document' to ingest SharePoint pages and Office files as Documents; "
            "use 'both' to run both in a single pipeline."
        ),
    )
    file_types: List[str] = Field(
        default=list(SUPPORTED_FILE_TYPES),
        description=(
            "List of file extensions to include during data_lake ingestion. "
            f"Supported values: {sorted(SUPPORTED_FILE_TYPES)}."
        ),
    )
    max_rows: int = Field(
        default=100,
        gt=0,
        description="Maximum number of rows to read when inferring schema from CSV/TSV/JSON/Excel files.",
    )
    max_files: int = Field(
        default=10000,
        gt=0,
        description="Maximum number of files to ingest per run in data_lake mode.",
    )
    ingest_pages: bool = Field(
        default=True,
        description="Whether to ingest SharePoint modern pages as Document entities.",
    )
    ingest_files: bool = Field(
        default=True,
        description=(
            "Whether to ingest Office files (Word, PDF, etc.) found in document "
            "libraries as Document entities. Requires the 'unstructured' extra."
        ),
    )
    max_documents: int = Field(
        default=10000,
        ge=-1,
        description=(
            "Maximum number of documents to process per run in document mode. "
            "Set to 0 or -1 to disable the limit."
        ),
    )
    processing: ProcessingConfig = Field(
        default_factory=ProcessingConfig,
        description="Document processing configuration (partitioning strategy, OCR, etc.).",
    )
    document_mapping: DocumentMappingConfig = Field(
        default_factory=DocumentMappingConfig,
        description="Configuration for mapping SharePoint pages to DataHub documents.",
    )
    hierarchy: HierarchyConfig = Field(
        default_factory=HierarchyConfig,
        description="Parent-child relationship configuration.",
    )
    filtering: FilteringConfig = Field(
        default_factory=FilteringConfig,
        description="Filtering options for document content.",
    )
    chunking: ChunkingConfig = Field(
        default_factory=ChunkingConfig,
        description="Configuration for document chunking (required for embeddings).",
    )
    embedding: EmbeddingConfig = Field(
        default_factory=EmbeddingConfig,
        description="Configuration for generating vector embeddings for semantic search.",
    )
    advanced: AdvancedConfig = Field(
        default_factory=AdvancedConfig,
        description="Advanced ingestion options.",
    )

    @field_validator("platform_instance")
    @classmethod
    def validate_platform_instance(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            if not v:
                raise ValueError("platform_instance cannot be an empty string.")
            if not re.match(r"^[a-zA-Z0-9_-]+$", v):
                raise ValueError(
                    f"platform_instance '{v}' must contain only alphanumeric "
                    "characters, hyphens, and underscores."
                )
        return v

    @field_validator("file_types", mode="before")
    @classmethod
    def validate_file_types(cls, v: List[str]) -> List[str]:
        unsupported = set(v) - SUPPORTED_FILE_TYPES
        if unsupported:
            raise ValueError(
                f"Unsupported file_types: {sorted(unsupported)}. "
                f"Supported types are: {sorted(SUPPORTED_FILE_TYPES)}."
            )
        return v

    def uses_data_lake(self) -> bool:
        return self.mode in ("data_lake", "both")

    def uses_document(self) -> bool:
        return self.mode in ("document", "both")

    @model_validator(mode="after")
    def validate_mode_config(self) -> "SharePointSourceConfig":
        if self.uses_data_lake() and not self.file_types:
            raise ValueError(
                "file_types cannot be empty when mode is 'data_lake' or 'both'."
            )
        return self

    @model_validator(mode="after")
    def set_document_id_pattern(self) -> "SharePointSourceConfig":
        if self.document_mapping.id_pattern == "{source_type}-{directory}-{basename}":
            self.document_mapping.id_pattern = "{source_type}-{basename}"
        return self
