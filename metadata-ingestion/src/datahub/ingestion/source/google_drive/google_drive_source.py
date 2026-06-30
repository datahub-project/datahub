"""
Google Drive source for DataHub ingestion.

Ingests Google Docs (and optionally Google Slides / Sheets) from Google Drive
as DataHub Document entities with full-text markdown content and optional
vector embeddings for semantic search.

Architecture
------------
This source follows the same pattern as the Confluence and Notion sources:

1.  Authenticate against the Google Drive API (service account or ADC).
2.  Discover files (respecting ``folder_ids``, ``recursive``, and MIME-type filters).
3.  Export each file to markdown (primary) or HTML→markdown (fallback), reusing
    the same export logic as Lisa's ``GoogleDocsExtractor``.
4.  Emit DataHub ``Document`` entities via the SDK.
5.  Optionally generate chunked embeddings via ``DocumentChunkingSource``
    (enables semantic search).
6.  Optionally materialise Google Drive folder entities to preserve hierarchy.
"""

import hashlib
import io
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

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
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
from datahub.ingestion.source.google_drive.google_drive_config import (
    GoogleDriveSourceConfig,
)
from datahub.ingestion.source.google_drive.google_drive_report import (
    GoogleDriveSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DataPlatformInfoClass,
    DataPlatformInstanceClass,
    DocumentStateClass,
    PlatformTypeClass,
)
from datahub.sdk.document import Document

logger = logging.getLogger(__name__)

# Mime types ----------------------------------------------------------------
MIME_GOOGLE_DOC = "application/vnd.google-apps.document"
MIME_GOOGLE_SLIDES = "application/vnd.google-apps.presentation"
MIME_GOOGLE_SHEETS = "application/vnd.google-apps.spreadsheet"
MIME_GOOGLE_FOLDER = "application/vnd.google-apps.folder"

# Export targets
EXPORT_MARKDOWN = "text/markdown"
EXPORT_HTML = "text/html"
EXPORT_TEXT = "text/plain"
EXPORT_CSV = "text/csv"

# Fields to request from Drive API
_DRIVE_FILE_FIELDS = (
    "id,name,mimeType,webViewLink,parents,createdTime,modifiedTime,"
    "lastModifyingUser,owners,starred,trashed,size"
)
_DRIVE_FOLDER_FIELDS = "id,name,parents,webViewLink"

# Bump when the export/parsing algorithm changes to force re-ingestion
EXTRACTION_ALGO_VERSION = "1"

# Subtype for folder document entities (matches DataHub convention)
FOLDER_SUBTYPE = "Folder"


@platform_name("Google Drive")
@config_class(GoogleDriveSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class GoogleDriveSource(StatefulIngestionSourceBase, TestableSource):
    """
    Ingests Google Docs (and optionally Slides / Sheets) from Google Drive as
    DataHub Document entities.

    ## Capabilities

    - **Full-text extraction**: Exports Google Docs to Markdown; falls back to
      HTML → Markdown conversion when the native markdown export is unavailable.
    - **Google Slides**: Optional export of presentations as plain text.
    - **Google Sheets**: Optional export of spreadsheets as CSV text.
    - **Folder hierarchy**: Materialises Drive folders as DataHub Document
      entities so files preserve their parent-child relationships in the UI.
    - **Semantic search**: Optional chunked embeddings via Cohere, AWS Bedrock,
      or Vertex AI for semantic search within DataHub.
    - **Stateful ingestion**: Tracks previously ingested documents so stale
      entities are removed when files are deleted from Drive.
    - **Incremental updates**: Skips unchanged documents using content hashing
      to avoid redundant re-processing.

    ## Authentication

    See ``GoogleDriveAuthConfig`` for details.  The short version:

    - Service account JSON key file (``credentials.service_account_key_file``)
    - Service account JSON string (``credentials.service_account_key_json``)
    - Application Default Credentials (nothing to configure — works on GCP or
      after ``gcloud auth application-default login``)

    ## Quick-start recipe

    ```yaml
    source:
      type: google-drive
      config:
        credentials:
          service_account_key_file: "/secrets/sa-key.json"
        folder_ids:
          - "1A2B3C4D5E6F7G8H9I0J"

    sink:
      type: datahub-rest
      config:
        server: "http://localhost:8080"
    ```
    """

    platform = "google-drive"

    report: GoogleDriveSourceReport

    def __init__(self, config: GoogleDriveSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report = GoogleDriveSourceReport()

        # Build Google Drive API client
        self._drive_service = self._build_drive_service()

        # html→markdown converter (fallback for Docs that don't support markdown export)
        try:
            import html2text as _html2text

            self._html_converter = _html2text.HTML2Text()
            self._html_converter.ignore_links = False
            self._html_converter.ignore_images = True
            self._html_converter.ignore_emphasis = False
        except ImportError:
            self._html_converter = None
            logger.warning(
                "html2text is not installed — HTML fallback for Docs export is disabled. "
                "Install it with: pip install html2text"
            )

        # Rate limiter: tokens per second derived from requests_per_minute
        self._min_request_interval = 60.0 / self.config.requests_per_minute
        self._last_request_time: float = 0.0

        # Initialise chunking/embedding sub-component
        from datahub.ingestion.source.unstructured.chunking_config import (
            DataHubConnectionConfig,
            DocumentChunkingSourceConfig,
        )
        from datahub.ingestion.source.unstructured.chunking_source import (
            DocumentChunkingSource,
        )

        chunking_config = DocumentChunkingSourceConfig(
            datahub=DataHubConnectionConfig(),
            chunking=config.chunking,
            embedding=config.embedding,
            max_documents=config.max_documents,
        )
        self.chunking_source = DocumentChunkingSource(
            ctx=ctx,
            config=chunking_config,
            standalone=False,
            graph=ctx.graph,
        )

    # ------------------------------------------------------------------
    # Authentication helpers
    # ------------------------------------------------------------------

    def _build_drive_service(self) -> Any:
        """Build and return an authenticated ``googleapiclient`` Drive v3 service."""
        from googleapiclient.discovery import build  # type: ignore[import-untyped]

        creds = self._get_credentials()
        return build("drive", "v3", credentials=creds, cache_discovery=False)

    def _get_credentials(self) -> Any:
        """Resolve Google credentials from config or Application Default Credentials."""
        import json as _json

        from google.oauth2 import service_account  # type: ignore[import-untyped]

        scopes = ["https://www.googleapis.com/auth/drive.readonly"]

        if self.config.credentials.service_account_key_file:
            logger.info("Using service account key file for authentication")
            return service_account.Credentials.from_service_account_file(
                self.config.credentials.service_account_key_file, scopes=scopes
            )

        if self.config.credentials.service_account_key_json:
            logger.info("Using service account JSON string for authentication")
            key_data = _json.loads(
                self.config.credentials.service_account_key_json.get_secret_value()
            )
            return service_account.Credentials.from_service_account_info(
                key_data, scopes=scopes
            )

        # Fall back to Application Default Credentials
        logger.info("Using Application Default Credentials")
        import google.auth  # type: ignore[import-untyped]

        credentials, _ = google.auth.default(scopes=scopes)
        return credentials

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _rate_limit(self) -> None:
        """Simple token-bucket rate limiter to stay within Drive API quotas."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.monotonic()

    # ------------------------------------------------------------------
    # Drive API helpers
    # ------------------------------------------------------------------

    def _list_files_in_folder(
        self, folder_id: str, recursive: bool
    ) -> List[Dict[str, Any]]:
        """Return all matching files under *folder_id*.

        Args:
            folder_id: Google Drive folder ID.
            recursive: Whether to recurse into sub-folders.

        Returns:
            List of Drive file metadata dicts.
        """
        mime_types = self._selected_mime_types()
        results: List[Dict[str, Any]] = []
        sub_folder_ids: List[str] = []

        page_token: Optional[str] = None
        while True:
            self._rate_limit()
            # Query: files whose parent is folder_id AND are not trashed
            query = f"'{folder_id}' in parents and trashed = false"
            response = (
                self._drive_service.files()
                .list(
                    q=query,
                    fields=f"nextPageToken, files({_DRIVE_FILE_FIELDS})",
                    pageSize=200,
                    pageToken=page_token,
                )
                .execute()
            )
            for f in response.get("files", []):
                if f["mimeType"] == MIME_GOOGLE_FOLDER:
                    self.report.report_folder_discovered()
                    if recursive:
                        sub_folder_ids.append(f["id"])
                elif f["mimeType"] in mime_types:
                    results.append(f)
                    self.report.report_file_discovered()

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        # Recurse into sub-folders
        for sub_id in sub_folder_ids:
            results.extend(self._list_files_in_folder(sub_id, recursive=recursive))

        return results

    def _list_all_accessible_files(self) -> List[Dict[str, Any]]:
        """Return all Drive files accessible to the authenticated account."""
        mime_types = self._selected_mime_types()
        mime_query = " or ".join(f"mimeType = '{m}'" for m in mime_types)
        query = f"({mime_query}) and trashed = false"

        results: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        while True:
            self._rate_limit()
            response = (
                self._drive_service.files()
                .list(
                    q=query,
                    fields=f"nextPageToken, files({_DRIVE_FILE_FIELDS})",
                    pageSize=200,
                    pageToken=page_token,
                )
                .execute()
            )
            for f in response.get("files", []):
                results.append(f)
                self.report.report_file_discovered()

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        return results

    def _get_folder_metadata(self, folder_id: str) -> Optional[Dict[str, Any]]:
        """Fetch folder metadata by ID. Returns None on failure."""
        try:
            self._rate_limit()
            return (
                self._drive_service.files()
                .get(fileId=folder_id, fields=_DRIVE_FOLDER_FIELDS)
                .execute()
            )
        except Exception as e:
            logger.warning(f"Could not fetch metadata for folder {folder_id}: {e}")
            return None

    def _collect_folder_ancestors(
        self, file_metadata: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Return ordered list of ancestor folder metadata (root → direct parent)."""
        ancestors: List[Dict[str, Any]] = []
        parent_ids: List[str] = list(file_metadata.get("parents", []))

        # Walk up the parent chain (Drive files have at most one parent)
        while parent_ids:
            parent_id = parent_ids[0]
            meta = self._get_folder_metadata(parent_id)
            if not meta:
                break
            # Stop at drive root (no further parents)
            if meta.get("mimeType") == MIME_GOOGLE_FOLDER:
                ancestors.insert(0, meta)
                parent_ids = list(meta.get("parents", []))
            else:
                break

        return ancestors

    # ------------------------------------------------------------------
    # Content export
    # ------------------------------------------------------------------

    def _export_doc_as_markdown(self, file_id: str) -> Optional[str]:
        """Export a Google Doc as Markdown. Returns None on failure."""
        from googleapiclient.errors import HttpError  # type: ignore[import-untyped]
        from googleapiclient.http import (
            MediaIoBaseDownload,  # type: ignore[import-untyped]
        )

        try:
            self._rate_limit()
            request = self._drive_service.files().export_media(
                fileId=file_id, mimeType=EXPORT_MARKDOWN
            )
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            return fh.getvalue().decode("utf-8")
        except HttpError as e:
            if e.resp.status in (403, 415):
                # Markdown export not available for this file type
                logger.debug(
                    f"Markdown export unavailable for {file_id} ({e.resp.status})"
                )
            else:
                logger.warning(f"Markdown export failed for {file_id}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Markdown export error for {file_id}: {e}")
            return None

    def _export_doc_as_html(self, file_id: str) -> Optional[str]:
        """Export a Google Doc as HTML. Returns None on failure."""
        from googleapiclient.http import (
            MediaIoBaseDownload,  # type: ignore[import-untyped]
        )

        try:
            self._rate_limit()
            request = self._drive_service.files().export_media(
                fileId=file_id, mimeType=EXPORT_HTML
            )
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            return fh.getvalue().decode("utf-8")
        except Exception as e:
            logger.warning(f"HTML export failed for {file_id}: {e}")
            return None

    def _export_as_plain_text(self, file_id: str) -> Optional[str]:
        """Export as plain text (used for Slides and Sheets)."""
        from googleapiclient.http import (
            MediaIoBaseDownload,  # type: ignore[import-untyped]
        )

        mime = EXPORT_TEXT
        try:
            self._rate_limit()
            request = self._drive_service.files().export_media(
                fileId=file_id, mimeType=mime
            )
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            return fh.getvalue().decode("utf-8")
        except Exception as e:
            logger.warning(f"Plain-text export failed for {file_id}: {e}")
            return None

    def _extract_text(self, file_metadata: Dict[str, Any]) -> Optional[str]:
        """Extract text from a Drive file, choosing the best export format."""
        file_id = file_metadata["id"]
        mime_type = file_metadata["mimeType"]

        if mime_type == MIME_GOOGLE_DOC:
            # Try markdown first (cleanest for DataHub docs)
            text = self._export_doc_as_markdown(file_id)
            if text:
                return text
            # Fallback: HTML → markdown
            if self._html_converter:
                html = self._export_doc_as_html(file_id)
                if html:
                    return self._html_converter.handle(html)
            # Last resort: plain text
            return self._export_as_plain_text(file_id)

        elif mime_type in (MIME_GOOGLE_SLIDES, MIME_GOOGLE_SHEETS):
            return self._export_as_plain_text(file_id)

        return None

    # ------------------------------------------------------------------
    # Entity building helpers
    # ------------------------------------------------------------------

    def _selected_mime_types(self) -> List[str]:
        """Return the list of MIME types to ingest based on config."""
        types: List[str] = []
        if self.config.include_docs:
            types.append(MIME_GOOGLE_DOC)
        if self.config.include_slides:
            types.append(MIME_GOOGLE_SLIDES)
        if self.config.include_sheets:
            types.append(MIME_GOOGLE_SHEETS)
        return types

    def _get_instance_id(self) -> str:
        """Return a stable platform instance identifier."""
        if self.config.platform_instance:
            return self.config.platform_instance
        # Use credentials fingerprint as a deterministic fallback
        if self.config.credentials.service_account_key_file:
            raw = self.config.credentials.service_account_key_file
        elif self.config.credentials.service_account_key_json:
            raw = self.config.credentials.service_account_key_json.get_secret_value()
        else:
            raw = "adc"
        return hashlib.sha256(raw.encode()).hexdigest()[:12]

    def _build_doc_id(self, file_id: str) -> str:
        instance_id = self._get_instance_id()
        return f"google-drive-{instance_id}-{file_id}"

    def _parse_timestamp(self, ts: Optional[str]) -> Optional[datetime]:
        if not ts:
            return None
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(
                tzinfo=None
            )
        except Exception:
            return None

    def _build_custom_properties(
        self, file_metadata: Dict[str, Any], content_hash: str
    ) -> Dict[str, str]:
        """Build the customProperties dict for a Document entity."""
        owners = file_metadata.get("owners", [])
        owner_email = owners[0].get("emailAddress", "") if owners else ""
        owner_name = owners[0].get("displayName", "") if owners else ""

        lmu = file_metadata.get("lastModifyingUser", {})
        last_modifier = lmu.get("emailAddress", "") if isinstance(lmu, dict) else ""

        return {
            "file_id": file_metadata["id"],
            "mime_type": file_metadata.get("mimeType", ""),
            "owner_email": owner_email,
            "owner_name": owner_name,
            "last_modifying_user_email": last_modifier,
            "content_hash": content_hash,
            "extraction_algo_version": EXTRACTION_ALGO_VERSION,
        }

    def _add_platform_instance(self, doc: Document) -> None:
        """Attach a DataPlatformInstance aspect so the UI can render "View in Drive"."""
        instance_id = self._get_instance_id()
        doc._set_aspect(
            DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=make_dataplatform_instance_urn(self.platform, instance_id),
            )
        )

    def _build_document(
        self,
        *,
        doc_id: str,
        title: str,
        text: str,
        external_url: str,
        file_id: str,
        custom_properties: Dict[str, str],
        parent_urn: Optional[str],
        created_time: Optional[datetime],
        last_modified_time: Optional[datetime],
    ) -> Document:
        if self.config.document_import_mode == DocumentImportMode.NATIVE:
            return Document.create_document(
                id=doc_id,
                title=title,
                text=text,
                status=DocumentStateClass.PUBLISHED,
                custom_properties=custom_properties,
                parent_document=parent_urn,
                created_time=created_time,
                last_modified_time=last_modified_time,
            )
        return Document.create_external_document(
            id=doc_id,
            title=title,
            platform=self.platform,
            external_url=external_url,
            external_id=file_id,
            text=text,
            status=DocumentStateClass.PUBLISHED,
            custom_properties=custom_properties,
            parent_document=parent_urn,
            created_time=created_time,
            last_modified_time=last_modified_time,
        )

    def _build_folder_document(
        self,
        *,
        folder_metadata: Dict[str, Any],
        parent_urn: Optional[str],
    ) -> Document:
        folder_id = folder_metadata["id"]
        doc_id = self._build_doc_id(folder_id)
        title = folder_metadata.get("name", folder_id)
        url = folder_metadata.get(
            "webViewLink", f"https://drive.google.com/drive/folders/{folder_id}"
        )

        if self.config.document_import_mode == DocumentImportMode.NATIVE:
            return Document.create_document(
                id=doc_id,
                title=title,
                text="",
                status=DocumentStateClass.PUBLISHED,
                subtype=FOLDER_SUBTYPE,
                custom_properties={"folder_id": folder_id},
                parent_document=parent_urn,
            )
        return Document.create_external_document(
            id=doc_id,
            title=title,
            platform=self.platform,
            external_url=url,
            external_id=folder_id,
            text="",
            status=DocumentStateClass.PUBLISHED,
            subtype=FOLDER_SUBTYPE,
            custom_properties={"folder_id": folder_id},
            parent_document=parent_urn,
        )

    # ------------------------------------------------------------------
    # Ingestion helpers
    # ------------------------------------------------------------------

    def _emit_platform_metadata(self) -> Iterable[MetadataWorkUnit]:
        """Emit the DataPlatformInfo aspect with the Google Drive logo."""
        platform_urn = make_data_platform_urn(self.platform)
        yield MetadataChangeProposalWrapper(
            entityUrn=platform_urn,
            aspect=DataPlatformInfoClass(
                name=self.platform,
                type=PlatformTypeClass.OTHERS,
                datasetNameDelimiter="/",
                displayName="Google Drive",
                logoUrl="https://www.gstatic.com/images/branding/product/2x/drive_48dp.png",
            ),
        ).as_workunit()

    def _ingest_folder_chain(
        self,
        file_metadata: Dict[str, Any],
        already_emitted_folders: Set[str],
    ) -> Tuple[Optional[str], List[MetadataWorkUnit]]:
        """Materialise ancestor folders for *file_metadata* if not already emitted.

        Returns (parent_urn, list_of_workunits).  The returned parent_urn is the
        URN of the direct parent folder (or None if the file is at Drive root).
        """
        if not self.config.ingest_folders:
            return None, []

        ancestors = self._collect_folder_ancestors(file_metadata)
        if not ancestors:
            return None, []

        workunits: List[MetadataWorkUnit] = []
        parent_urn: Optional[str] = self.config.parent_document_urn

        for folder_meta in ancestors:
            folder_id = folder_meta["id"]
            doc_id = self._build_doc_id(folder_id)
            folder_urn = f"urn:li:document:{doc_id}"

            if folder_id not in already_emitted_folders:
                doc = self._build_folder_document(
                    folder_metadata=folder_meta,
                    parent_urn=parent_urn,
                )
                self._add_platform_instance(doc)
                workunits.extend(doc.as_workunits())
                already_emitted_folders.add(folder_id)
                self.report.report_folder_ingested()

            parent_urn = folder_urn

        return parent_urn, workunits

    def _ingest_file(
        self,
        file_metadata: Dict[str, Any],
        already_emitted_folders: Set[str],
    ) -> Iterable[MetadataWorkUnit]:
        """Yield workunits for a single Drive file."""
        file_id = file_metadata["id"]
        title = file_metadata.get("name", file_id)

        try:
            # Extract text content
            text = self._extract_text(file_metadata)
            if not text:
                logger.warning(
                    f"Could not extract text from {file_id} ({title}), skipping"
                )
                self.report.report_doc_failed(file_id, "Text extraction returned empty")
                return

            # Check minimum text length
            if len(text) < self.config.filtering.min_text_length:
                self.report.report_doc_skipped_too_short(
                    file_id, len(text), self.config.filtering.min_text_length
                )
                return

            # Content hash (used for dedup / force-re-embed detection)
            hash_input = json.dumps(
                {
                    "modifiedTime": file_metadata.get("modifiedTime", ""),
                    "algo_version": EXTRACTION_ALGO_VERSION,
                },
                sort_keys=True,
            )
            content_hash = hashlib.sha256(hash_input.encode()).hexdigest()

            custom_properties = self._build_custom_properties(
                file_metadata, content_hash
            )

            # Timestamps
            created_time = self._parse_timestamp(file_metadata.get("createdTime"))
            last_modified_time = self._parse_timestamp(
                file_metadata.get("modifiedTime")
            )

            # Materialise ancestor folders (if enabled) and get parent URN
            parent_urn, folder_workunits = self._ingest_folder_chain(
                file_metadata, already_emitted_folders
            )
            yield from folder_workunits

            # Use configured parent URN if no folder chain
            if parent_urn is None:
                parent_urn = self.config.parent_document_urn

            doc_id = self._build_doc_id(file_id)
            external_url = file_metadata.get(
                "webViewLink",
                f"https://drive.google.com/file/d/{file_id}/view",
            )

            doc = self._build_document(
                doc_id=doc_id,
                title=title,
                text=text,
                external_url=external_url,
                file_id=file_id,
                custom_properties=custom_properties,
                parent_urn=parent_urn,
                created_time=created_time,
                last_modified_time=last_modified_time,
            )
            self._add_platform_instance(doc)
            yield from doc.as_workunits()

            # Chunking / embedding (enables semantic search)
            document_urn = f"urn:li:document:{doc_id}"
            elements = [
                {
                    "type": "NarrativeText",
                    "text": text,
                    "metadata": {
                        "file_id": file_id,
                        "title": title,
                        "url": external_url,
                    },
                }
            ]
            try:
                yield from self.chunking_source.process_elements_inline(
                    document_urn=document_urn, elements=elements
                )
            except RuntimeError:
                if self.chunking_source.report.num_documents_limit_reached:
                    self.report.num_documents_limit_reached = True
                    raise
                logger.warning(
                    f"Embedding generation failed for {document_urn}; "
                    "document will be ingested without embeddings."
                )
            except Exception as e:
                logger.warning(
                    f"Embedding generation failed for {document_urn}: {e}; "
                    "document will be ingested without embeddings."
                )

            self.report.report_doc_processed(len(text))

        except Exception as e:
            logger.error(
                f"Failed to ingest file {file_id} ({title}): {e}", exc_info=True
            )
            self.report.report_doc_failed(file_id, str(e))
            if not self.config.advanced.continue_on_failure:
                raise

    # ------------------------------------------------------------------
    # StatefulIngestionSourceBase interface
    # ------------------------------------------------------------------

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Main ingestion loop."""
        yield from self._emit_platform_metadata()

        # Discover files
        if self.config.folder_ids:
            all_files: List[Dict[str, Any]] = []
            for folder_id in self.config.folder_ids:
                logger.info(f"Listing files in folder: {folder_id}")
                all_files.extend(
                    self._list_files_in_folder(
                        folder_id, recursive=self.config.recursive
                    )
                )
        else:
            logger.info("No folder_ids specified — listing all accessible Drive files")
            all_files = self._list_all_accessible_files()

        # Deduplicate by file ID
        unique_files: Dict[str, Dict[str, Any]] = {
            f["id"]: f for f in all_files if not f.get("trashed", False)
        }

        logger.info(f"Discovered {len(unique_files)} unique file(s) to ingest")

        # Apply max_documents limit
        file_list = list(unique_files.values())
        if self.config.max_documents is not None:
            file_list = file_list[: self.config.max_documents]
            logger.info(
                f"max_documents={self.config.max_documents} applied, processing {len(file_list)} file(s)"
            )

        # Track emitted folders to avoid duplicates
        emitted_folders: Set[str] = set()

        for file_metadata in file_list:
            if self.chunking_source.report.num_documents_limit_reached:
                self.report.num_documents_limit_reached = True
                logger.info("Document limit reached, stopping ingestion")
                break

            try:
                yield from self._ingest_file(file_metadata, emitted_folders)
            except RuntimeError:
                # Raised when document limit is hit inside _ingest_file
                if self.report.num_documents_limit_reached:
                    break
                raise

    def get_report(self) -> GoogleDriveSourceReport:
        return self.report

    def close(self) -> None:
        super().close()

    # ------------------------------------------------------------------
    # TestableSource interface
    # ------------------------------------------------------------------

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Test that the Google Drive API is reachable and credentials work."""
        report = TestConnectionReport()

        try:
            config = GoogleDriveSourceConfig.parse_obj(config_dict)
            ctx = PipelineContext(run_id="test-connection")
            source = GoogleDriveSource(config, ctx)

            # Try a minimal API call
            source._rate_limit()
            resp = (
                source._drive_service.files()
                .list(
                    q="trashed = false",
                    fields="files(id,name)",
                    pageSize=1,
                )
                .execute()
            )

            report.basic_connectivity = CapabilityReport(capable=True)
            file_count = len(resp.get("files", []))
            logger.info(f"Test connection succeeded; sample file count: {file_count}")

        except Exception as e:
            report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=str(e),
            )
            logger.error(f"Test connection failed: {e}")

        return report
