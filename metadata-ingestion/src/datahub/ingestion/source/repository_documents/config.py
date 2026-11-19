from pathlib import PurePosixPath
from typing import List, Optional

from pydantic import Field, field_validator

from datahub.configuration.source_common import PlatformInstanceConfigMixin
from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
from datahub.ingestion.source.repository_documents.paths import (
    validate_platform_instance,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class RepositoryDocumentsSourceConfigBase(
    StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig],
    PlatformInstanceConfigMixin,
):
    """Settings shared by every git-host documents source."""

    repository: str = Field(description="Repository to ingest.")
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
    max_files: int = Field(
        default=500,
        ge=1,
        description=(
            "Maximum number of matching files to import per run. Additional matches are skipped "
            "with a report warning."
        ),
    )
    create_repo_root_document: bool = Field(
        default=True,
        description=(
            "When True, create a folder document named after the repository and nest "
            "imported files beneath it."
        ),
    )
    parent_document_urn: Optional[str] = Field(
        default=None,
        description=(
            "Optional parent document URN. Top-level imported items are nested beneath "
            "this document while preserving the repository folder hierarchy."
        ),
    )
    document_import_mode: DocumentImportMode = Field(
        default=DocumentImportMode.NATIVE,
        description=(
            "NATIVE imports editable documents in DataHub. EXTERNAL imports read-only "
            "references that link back to the repository."
        ),
    )
    show_in_global_context: bool = Field(
        default=True,
        description="Whether imported documents appear in global search and navigation.",
    )
    enable_okf: bool = Field(
        default=False,
        description=(
            "Opt in to importing OKF concepts inside discovered or configured bundles. "
            "Requires a server that supports the okfProperties aspect; enabling "
            "this on an unsupported server fails the run. Disabled by default "
            "to import every file verbatim without OKF metadata."
        ),
    )
    okf_bundle_roots: List[str] = Field(
        default_factory=list,
        description=(
            "Additional bundle roots relative to the repository, for bundles without "
            "a versioned index.md. Use an empty string for the repository root. "
            "Directories with an index.md declaring okf_version are also discovered."
        ),
    )
    include_repository_in_browse_path: bool = Field(
        default=True,
        description=(
            "Include the repository name as a top-level entry in the browse path. "
            "When a repository root document is created, that document already "
            "provides the repository entry; this adds it for configurations that "
            "skip the root document or nest under a configured parent document."
        ),
    )
    include_organization_in_browse_path: bool = Field(
        default=False,
        description=(
            "Include the repository owner (GitHub organization, GitLab group "
            "path) as the top-most entry in the browse path, above the "
            "repository name."
        ),
    )

    @field_validator("platform_instance")
    @classmethod
    def _validate_platform_instance(cls, value: Optional[str]) -> Optional[str]:
        # The instance is part of every source id and of the
        # dataPlatformInstance URN; see validate_platform_instance.
        return validate_platform_instance(value)

    @field_validator("okf_bundle_roots")
    @classmethod
    def normalize_bundle_roots(cls, roots: List[str]) -> List[str]:
        normalized: List[str] = []
        for root in roots:
            path = PurePosixPath(root)
            if path.is_absolute() or ".." in path.parts or "\\" in root:
                raise ValueError(
                    "OKF bundle roots must be repository-relative POSIX paths"
                )
            canonical = "" if str(path) == "." else str(path)
            if canonical not in normalized:
                normalized.append(canonical)
        return normalized

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
