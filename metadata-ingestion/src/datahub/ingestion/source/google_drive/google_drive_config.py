"""Configuration for the Google Drive / Google Docs ingestion source."""

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
    FilteringConfig,
)


class GoogleDriveAuthConfig(ConfigModel):
    """Authentication configuration for Google Drive API.

    Supports three authentication methods (in priority order):
    1. Service Account JSON key file (``service_account_key_file``)
    2. Service Account JSON string (``service_account_key_json``)
    3. Application Default Credentials / Workload Identity (automatic)
    """

    service_account_key_file: Optional[str] = Field(
        default=None,
        description=(
            "Path to a Google service account JSON key file. "
            "The service account must have the Google Drive API enabled "
            "and read access to the files/folders you want to ingest."
        ),
    )

    service_account_key_json: Optional[SecretStr] = Field(
        default=None,
        description=(
            "Service account JSON key as a string (alternative to a file path). "
            "Useful when injecting credentials via environment variables or secrets managers."
        ),
    )

    @model_validator(mode="after")
    def _check_auth(self) -> "GoogleDriveAuthConfig":
        if self.service_account_key_file and self.service_account_key_json:
            raise ValueError(
                "Provide either service_account_key_file or service_account_key_json, not both."
            )
        return self


class GoogleDriveSourceConfig(
    StatefulIngestionConfigBase[StatefulStaleMetadataRemovalConfig], ConfigModel
):
    """
    Google Drive ingestion source configuration.

    Ingests Google Docs (and optionally Google Slides / Google Sheets) from
    Google Drive as DataHub Document entities with full-text markdown content.

    ## Authentication

    The source supports three authentication modes:

    ### Service Account (recommended for production)

    Create a service account at https://console.cloud.google.com/iam-admin/serviceaccounts,
    enable the **Google Drive API** for your project, and grant the service account
    access to the folders you want to ingest by sharing them with its email address.

    ```yaml
    source:
      type: google-drive
      config:
        credentials:
          service_account_key_file: "/path/to/service-account.json"
    ```

    ### Application Default Credentials

    If you're running on Google Cloud (GKE, Cloud Run, Compute Engine), or have
    run ``gcloud auth application-default login`` locally, the source will pick up
    credentials automatically — no explicit credential config needed.

    ## Configuration Examples

    ### Minimal — ingest all accessible Google Docs

    ```yaml
    source:
      type: google-drive
      config:
        credentials:
          service_account_key_file: "/secrets/sa-key.json"

    sink:
      type: datahub-rest
      config:
        server: "http://localhost:8080"
    ```

    ### Specific folders

    ```yaml
    source:
      type: google-drive
      config:
        credentials:
          service_account_key_file: "/secrets/sa-key.json"
        folder_ids:
          - "1A2B3C4D5E6F7G8H9I0J"  # Engineering Docs
          - "9Z8Y7X6W5V4U3T2S1R0Q"  # Product Specs

    sink:
      type: datahub-rest
      config:
        server: "http://localhost:8080"
    ```

    ### Include Slides and generate embeddings

    ```yaml
    source:
      type: google-drive
      config:
        credentials:
          service_account_key_file: "/secrets/sa-key.json"
        include_slides: true
        embedding:
          provider: cohere
          cohere:
            api_key: "${COHERE_API_KEY}"
            model: embed-english-v3.0

    sink:
      type: datahub-rest
      config:
        server: "http://localhost:8080"
    ```
    """

    # ---------------------------------------------------------------------------
    # Authentication
    # ---------------------------------------------------------------------------
    credentials: GoogleDriveAuthConfig = Field(
        default_factory=GoogleDriveAuthConfig,
        description="Authentication configuration for the Google Drive API.",
    )

    # ---------------------------------------------------------------------------
    # Scope / content selection
    # ---------------------------------------------------------------------------
    folder_ids: List[str] = Field(
        default_factory=list,
        description=(
            "List of Google Drive folder IDs to ingest (recursively). "
            "You can find a folder's ID in its URL: "
            "``https://drive.google.com/drive/folders/{FOLDER_ID}``. "
            "If empty, all Google Docs accessible to the authenticated account "
            "are ingested (subject to the Drive API ``files.list`` scope)."
        ),
    )

    include_docs: bool = Field(
        default=True,
        description="Ingest Google Docs (MIME type ``application/vnd.google-apps.document``).",
    )

    include_slides: bool = Field(
        default=False,
        description=(
            "Ingest Google Slides presentations "
            "(MIME type ``application/vnd.google-apps.presentation``). "
            "Content is exported as plain text (one block per slide)."
        ),
    )

    include_sheets: bool = Field(
        default=False,
        description=(
            "Ingest Google Sheets "
            "(MIME type ``application/vnd.google-apps.spreadsheet``). "
            "Content is exported as CSV-formatted text."
        ),
    )

    ingest_folders: bool = Field(
        default=True,
        description=(
            "Materialise Google Drive folders as DataHub Document entities so "
            "that files nested inside them preserve their parent-child hierarchy "
            "in the DataHub UI."
        ),
    )

    recursive: bool = Field(
        default=True,
        description=(
            "Recursively discover files inside sub-folders of ``folder_ids``. "
            "Has no effect when ``folder_ids`` is empty (all-Drive mode already "
            "returns every accessible file)."
        ),
    )

    # ---------------------------------------------------------------------------
    # Document import mode
    # ---------------------------------------------------------------------------
    document_import_mode: DocumentImportMode = Field(
        default=DocumentImportMode.EXTERNAL,
        description=(
            "``NATIVE`` imports editable documents directly into DataHub. "
            "``EXTERNAL`` imports read-only references that link back to "
            "the original Google Drive file."
        ),
    )

    platform_instance: Optional[str] = Field(
        default=None,
        description=(
            "Optional DataHub platform instance identifier. "
            "Use this when you have multiple Google Drive ingestions and need "
            "to distinguish them (e.g. ``my-org-drive``)."
        ),
    )

    parent_document_urn: Optional[str] = Field(
        default=None,
        description="Optional parent document URN to attach top-level documents to.",
    )

    # ---------------------------------------------------------------------------
    # Filtering
    # ---------------------------------------------------------------------------
    filtering: FilteringConfig = Field(
        default_factory=FilteringConfig,
        description="Document filtering configuration (e.g. minimum text length).",
    )

    # ---------------------------------------------------------------------------
    # Chunking / embedding (optional — enables semantic search)
    # ---------------------------------------------------------------------------
    chunking: ChunkingConfig = Field(
        default_factory=ChunkingConfig,
        description="Text chunking configuration for semantic search.",
    )

    embedding: Optional[EmbeddingConfig] = Field(
        default=None,
        description=(
            "Embedding provider configuration. When set, the source will "
            "generate vector embeddings for each document and emit them as "
            "SemanticContent aspects, enabling semantic search in DataHub."
        ),
    )

    max_documents: Optional[int] = Field(
        default=None,
        description="Maximum number of documents to ingest per run (useful for testing).",
    )

    # ---------------------------------------------------------------------------
    # Advanced
    # ---------------------------------------------------------------------------
    advanced: AdvancedConfig = Field(
        default_factory=AdvancedConfig,
        description="Advanced ingestion settings (e.g. continue_on_failure).",
    )

    requests_per_minute: int = Field(
        default=60,
        description=(
            "Maximum Google Drive API requests per minute. "
            "The default matches the standard user rate limit."
        ),
    )

    @field_validator("folder_ids", mode="before")
    @classmethod
    def _strip_folder_urls(cls, v: List[str]) -> List[str]:
        """Accept full Drive folder URLs and extract only the ID."""
        import re

        cleaned = []
        for item in v:
            # https://drive.google.com/drive/folders/{ID}  (optional ?... suffix)
            m = re.search(r"/folders/([a-zA-Z0-9_-]+)", item)
            if m:
                cleaned.append(m.group(1))
            else:
                cleaned.append(item)
        return cleaned
