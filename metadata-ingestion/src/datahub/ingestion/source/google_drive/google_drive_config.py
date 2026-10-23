from typing import Annotated, List, Optional, Sequence

from pydantic import Field, field_validator, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.ingestion.agent.verdicts import Verdict, VerdictContext
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.ingestion.source.common.subtypes import DocumentSubTypes
from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
from datahub.ingestion.source.google_drive.constants import (
    DOCUMENT_URN_PREFIX,
    DRIVE_ID_RE,
    DRIVE_URL_FOLDER_ID_RE,
    DRIVE_URL_QUERY_ID_RE,
    KIND_GOOGLE_DOC,
    PLATFORM_INSTANCE_RE,
)
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


def parse_drive_id(value: str) -> str:
    """Extract a Drive folder or shared-drive ID from a bare ID or a Drive URL."""
    candidate = value.strip()
    for pattern in (DRIVE_URL_FOLDER_ID_RE, DRIVE_URL_QUERY_ID_RE):
        match = pattern.search(candidate)
        if match:
            candidate = match.group(1)
            break
    if DRIVE_ID_RE.match(candidate):
        return candidate
    raise ValueError(
        f"'{value}' is not a Google Drive folder ID, shared drive ID, or folder URL"
    )


class GoogleDriveSourceConfig(
    StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig], ConfigModel
):
    credential: Optional[GCPCredential] = Field(
        default=None,
        description="Service account key used to call the Google Drive API. "
        "If omitted, Application Default Credentials are used.",
    )
    delegated_user: Optional[str] = Field(
        default=None,
        description="Email of a Google Workspace user to impersonate via domain-wide "
        "delegation. Without it, the service account only sees files that are "
        "explicitly shared with its client_email. Requires `credential`.",
    )
    root_folder_ids: List[str] = Field(
        description="Google Drive folder IDs, shared drive IDs, or Drive folder URLs to "
        "ingest. Google Docs under each root are ingested, recursively by default.",
    )
    recursive: bool = Field(
        default=True,
        description="Whether to walk subfolders. When false, only Google Docs directly "
        "inside each root are ingested and no subfolder documents are emitted.",
    )
    folder_pattern: Annotated[AllowDenyPattern, Filters(DocumentSubTypes.FOLDER)] = (
        Field(
            default=AllowDenyPattern.allow_all(),
            description="Regex patterns matched against folder paths such as "
            "'Root/Engineering/Archive' (the root folder's name comes first). A denied "
            "folder is skipped together with everything beneath it. Configured roots are "
            "never filtered.",
        )
    )
    document_pattern: Annotated[AllowDenyPattern, Filters(KIND_GOOGLE_DOC)] = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns matched against Google Doc titles.",
    )
    platform_instance: Optional[str] = Field(
        default=None,
        description="Optional identifier for this Google Workspace, attached to every "
        "ingested document. Document URNs do not depend on it because Drive file IDs "
        "are globally unique.",
    )
    document_import_mode: DocumentImportMode = Field(
        default=DocumentImportMode.EXTERNAL,
        description="NATIVE imports editable documents in DataHub. EXTERNAL imports "
        "read-only references that link back to Google Drive.",
    )
    parent_document_urn: Optional[str] = Field(
        default=None,
        description="Optional URN of an existing DataHub document to place the imported "
        "folder tree under.",
    )
    create_root_documents: bool = Field(
        default=True,
        description="Emit a folder document for each configured root. When false, items "
        "directly inside a root attach to `parent_document_urn` (or become top-level "
        "documents).",
    )
    extract_owners: bool = Field(
        default=True,
        description="Map Google Drive file owners to DataHub owners (users keyed by "
        "email address). Files in shared drives have no owners.",
    )
    skip_empty_documents: bool = Field(
        default=True,
        description="Skip Google Docs whose exported text is empty.",
    )
    min_text_length: int = Field(
        default=0,
        ge=0,
        description="Skip Google Docs whose exported text is shorter than this many "
        "characters. 0 keeps every non-empty document.",
    )
    max_text_length: int = Field(
        default=1_000_000,
        ge=0,
        description="Skip Google Docs whose exported text (after images are replaced "
        "with placeholders) is longer than this many characters. A skipped document "
        "that was ingested earlier is soft-deleted by stale metadata removal. "
        "0 disables the limit.",
    )
    chunking: ChunkingConfig = Field(
        default_factory=ChunkingConfig,
        description="Configuration for document chunking (required for embeddings).",
    )
    embedding: EmbeddingConfig = Field(
        default_factory=EmbeddingConfig,
        description="Configuration for generating vector embeddings for semantic search. "
        "By default the configuration is loaded from the DataHub server.",
    )
    max_documents: int = Field(
        default=10000,
        ge=-1,
        description="Maximum number of documents to embed per ingestion run. The run "
        "fails once this limit is reached. Set to 0 or -1 to disable the limit.",
    )
    continue_on_failure: bool = Field(
        default=True,
        description="Whether to keep ingesting other documents when one document fails. "
        "Set to false to abort the run on the first per-document error.",
    )

    @field_validator("root_folder_ids")
    @classmethod
    def normalize_root_folder_ids(cls, value: List[str]) -> List[str]:
        if not value:
            raise ValueError("root_folder_ids must contain at least one entry")
        normalized: List[str] = []
        for entry in value:
            drive_id = parse_drive_id(entry)
            if drive_id not in normalized:
                normalized.append(drive_id)
        return normalized

    @field_validator("platform_instance")
    @classmethod
    def validate_platform_instance(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and not PLATFORM_INSTANCE_RE.match(value):
            raise ValueError(
                f"platform_instance '{value}' must contain only alphanumeric "
                "characters, hyphens, and underscores"
            )
        return value

    @field_validator("parent_document_urn")
    @classmethod
    def validate_parent_document_urn(cls, value: Optional[str]) -> Optional[str]:
        if value is not None and not value.startswith(DOCUMENT_URN_PREFIX):
            raise ValueError(
                f"parent_document_urn must be a document URN starting with "
                f"'{DOCUMENT_URN_PREFIX}', got '{value}'"
            )
        return value

    @model_validator(mode="after")
    def validate_delegation_requires_credential(self) -> "GoogleDriveSourceConfig":
        if self.delegated_user and self.credential is None:
            raise ValueError(
                "delegated_user requires a service-account `credential`; "
                "Application Default Credentials cannot impersonate users"
            )
        return self

    @model_validator(mode="after")
    def validate_text_length_range(self) -> "GoogleDriveSourceConfig":
        # An inverted range would skip every document and, through stale removal,
        # soft-delete everything already ingested.
        if self.max_text_length and self.min_text_length > self.max_text_length:
            raise ValueError(
                f"min_text_length ({self.min_text_length}) must not exceed "
                f"max_text_length ({self.max_text_length})"
            )
        return self

    @classmethod
    def probe_provider_class(cls) -> type:
        # Lazy, so ingestion never loads the probe module.
        from datahub.ingestion.source.google_drive.google_drive_probe import (
            GoogleDriveMetadataProbe,
        )

        return GoogleDriveMetadataProbe

    def probe_verdict_override(self, ctx: VerdictContext) -> Optional[Verdict]:
        # Lazy, so ingestion never loads the probe module.
        from datahub.ingestion.source.google_drive.google_drive_probe import (
            probe_verdict,
        )

        return probe_verdict(self, ctx)

    def probe_ancestor_kinds(self, kind: str) -> Optional[Sequence[str]]:
        # Folders and Docs are judged on their whole folder chain by
        # probe_verdict_override, from the listing's own facts, so the framework is
        # not asked to judge a --parent separately.
        return ()
