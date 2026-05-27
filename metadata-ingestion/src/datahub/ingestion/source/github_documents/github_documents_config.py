"""Configuration for the github-documents ingestion source."""

from typing import List, Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from datahub.configuration.common import ConfigModel
from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
from datahub.ingestion.source.github_documents.github_api import parse_repo_identifier
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.unstructured.config import (
    DocumentMappingConfig,
    HierarchyConfig,
)


class GitHubDocumentsSourceConfig(
    StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig], ConfigModel
):
    """
    Configuration for ingesting markdown and text documents from a GitHub repository.
    """

    github_token: SecretStr = Field(
        description="GitHub access token (PAT or GitHub App installation token)."
    )
    repository: str = Field(
        description=(
            "Repository to ingest, as 'owner/repo' or 'https://github.com/owner/repo'."
        )
    )
    branch: str = Field(
        default="main",
        description="Branch to read files from.",
    )
    path_prefix: str = Field(
        default="",
        description="Only ingest files under this path (e.g. 'docs').",
    )
    file_extensions: List[str] = Field(
        default_factory=lambda: [".md", ".txt"],
        description="File extensions to include (include the leading dot).",
    )
    parent_document_urn: Optional[str] = Field(
        default=None,
        description=(
            "Optional parent document URN. Top-level imported items are nested beneath "
            "this document while preserving the GitHub folder hierarchy."
        ),
    )
    document_import_mode: DocumentImportMode = Field(
        default=DocumentImportMode.NATIVE,
        description=(
            "NATIVE imports editable documents in DataHub. EXTERNAL imports read-only "
            "references that link back to GitHub."
        ),
    )
    show_in_global_context: bool = Field(
        default=True,
        description="Whether imported documents appear in global search and navigation.",
    )

    document_mapping: DocumentMappingConfig = Field(
        default_factory=DocumentMappingConfig,
        description="Document entity mapping configuration.",
    )
    hierarchy: HierarchyConfig = Field(
        default_factory=HierarchyConfig,
        description="Parent-child relationship configuration.",
    )

    @field_validator("github_token")
    @classmethod
    def validate_github_token(cls, value: SecretStr) -> SecretStr:
        if not value or not value.get_secret_value().strip():
            raise ValueError("github_token is required and cannot be empty")
        return value

    @field_validator("repository")
    @classmethod
    def validate_repository(cls, value: str) -> str:
        return parse_repo_identifier(value)

    @field_validator("file_extensions")
    @classmethod
    def normalize_extensions(cls, extensions: List[str]) -> List[str]:
        if not extensions:
            return [".md", ".txt"]
        normalized = []
        for ext in extensions:
            ext = ext.strip().lower()
            if not ext:
                continue
            if not ext.startswith("."):
                ext = f".{ext}"
            normalized.append(ext)
        return normalized or [".md", ".txt"]

    @model_validator(mode="after")
    def apply_document_mapping_defaults(self) -> "GitHubDocumentsSourceConfig":
        if self.document_mapping.id_pattern == "{source_type}-{directory}-{basename}":
            self.document_mapping.id_pattern = "{source_type}-{basename}"

        if self.document_import_mode == DocumentImportMode.NATIVE:
            self.document_mapping.source.type = "NATIVE"
        else:
            self.document_mapping.source.type = "EXTERNAL"

        if self.parent_document_urn:
            self.hierarchy.folder_mapping.root_parent = self.parent_document_urn
        return self
