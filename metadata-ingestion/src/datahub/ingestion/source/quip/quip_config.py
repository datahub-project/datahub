import re
from typing import List, Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
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
)


class QuipSourceConfig(
    StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig], ConfigModel
):
    access_token: SecretStr = Field(
        description="Quip Personal Access Token (or OAuth access token). "
        "Generate a personal token at <base_url>/dev/token.",
    )

    base_url: str = Field(
        default="https://platform.quip.com",
        description="Base URL of the Quip Automation API. Use "
        "'https://platform.quip.com' for quip.com, or your enterprise endpoint "
        "(e.g. 'https://platform.quip-amazon.com').",
    )

    platform_instance: Optional[str] = Field(
        default=None,
        description="Optional human-readable identifier for this Quip site "
        "(e.g. 'mycompany'). If not provided, a deterministic hash of base_url is "
        "used so that URNs are globally unique without manual configuration.",
    )

    folder_ids: List[str] = Field(
        default_factory=list,
        description="Quip folder IDs to ingest (each folder is crawled recursively "
        "when recursive=true). If neither folder_ids nor thread_ids is set, the "
        "authenticated user's accessible folders (private, desktop, shared, group) "
        "are auto-discovered.",
    )

    thread_ids: List[str] = Field(
        default_factory=list,
        description="Specific Quip thread (document) IDs to ingest in addition to "
        "any folders. Useful for ingesting individual documents.",
    )

    recursive: bool = Field(
        default=True,
        description="Whether to recursively crawl sub-folders of the configured/"
        "discovered folders.",
    )

    thread_types: Optional[List[str]] = Field(
        default=None,
        description="Optional allow-list of Quip thread types to ingest "
        "(e.g. ['document', 'spreadsheet', 'slides', 'chat']). By default all "
        "thread types are ingested.",
    )

    max_threads: int = Field(
        default=10000,
        ge=-1,
        description="Maximum number of threads (documents) to ingest per run. "
        "Set to 0 or -1 to disable the limit.",
    )

    document_import_mode: DocumentImportMode = Field(
        default=DocumentImportMode.EXTERNAL,
        description="NATIVE imports editable documents in DataHub. EXTERNAL imports "
        "read-only references that link back to Quip.",
    )

    parent_document_urn: Optional[str] = Field(
        default=None,
        description="Optional parent document URN for top-level imported folders.",
    )

    document_mapping: DocumentMappingConfig = Field(
        default_factory=DocumentMappingConfig,
        description="Configuration for mapping Quip threads to DataHub documents.",
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
    max_documents: int = Field(
        default=10000,
        ge=-1,
        description="Maximum number of documents to embed per run. Set to 0 or -1 "
        "to disable the limit.",
    )
    advanced: AdvancedConfig = Field(
        default_factory=AdvancedConfig,
        description="Advanced ingestion options.",
    )

    @field_validator("access_token")
    @classmethod
    def validate_access_token(cls, v: SecretStr) -> SecretStr:
        if not v or not v.get_secret_value().strip():
            raise ValueError("access_token must not be empty")
        return v

    @field_validator("base_url")
    @classmethod
    def normalize_base_url(cls, v: str) -> str:
        v = v.strip().rstrip("/")
        if not v.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return v

    @field_validator("parent_document_urn")
    @classmethod
    def validate_parent_document_urn(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.startswith("urn:li:document:"):
            raise ValueError(
                "parent_document_urn must be a document URN (urn:li:document:...)"
            )
        return v

    @field_validator("platform_instance")
    @classmethod
    def validate_platform_instance(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            if not v:
                raise ValueError("platform_instance cannot be empty string")
            if not re.match(r"^[a-zA-Z0-9_-]+$", v):
                raise ValueError(
                    f"platform_instance '{v}' must contain only alphanumeric "
                    "characters, hyphens, and underscores"
                )
        return v

    @model_validator(mode="after")
    def apply_document_import_mode(self) -> "QuipSourceConfig":
        self.document_mapping.source.type = self.document_import_mode.value
        if self.parent_document_urn:
            self.hierarchy.folder_mapping.root_parent = self.parent_document_urn
        return self

    @model_validator(mode="after")
    def set_document_id_pattern(self) -> "QuipSourceConfig":
        # URNs must be stable across runs, so drop the directory component that the
        # default pattern would otherwise embed. Only rewrite the default pattern,
        # never a user-customized one.
        default_id_pattern = DocumentMappingConfig.model_fields["id_pattern"].default
        if self.document_mapping.id_pattern == default_id_pattern:
            self.document_mapping.id_pattern = "{source_type}-{basename}"
        return self
