import hashlib
import json
import logging
from datetime import datetime
from functools import partial
from typing import Dict, Iterable, List, Optional, Type, Union

from googleapiclient.discovery import Resource

from datahub.emitter.mce_builder import (
    get_sys_time,
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
from datahub.ingestion.api.workunit_processor import WorkunitProcessor
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.common.subtypes import DocumentSubTypes
from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
from datahub.ingestion.source.google_drive.constants import (
    ASPECT_DOCUMENT_INFO,
    ASPECT_OWNERSHIP,
    ASPECT_STATUS,
    CAPABILITY_SEMANTIC_SEARCH,
    CHANGE_DETECTION_BATCH_SIZE,
    DOCUMENT_ENTITY_TYPE,
    DRIVE_SCOPES,
    ELEMENT_TYPE_NARRATIVE_TEXT,
    EXCESS_BLANK_LINES_RE,
    EXTRACTION_ALGO_VERSION,
    FAILURE_NOT_FOUND,
    FAILURE_TOO_LARGE,
    IMAGE_PLACEHOLDER,
    IMAGE_PLACEHOLDER_WITH_ALT,
    INLINE_DATA_IMAGE_RE,
    MIME_FOLDER,
    OWNERSHIP_ATTRIBUTION_ACTOR,
    PLATFORM,
    PROGRESS_LOG_INTERVAL,
    PROP_CONTENT_HASH,
    PROP_DRIVE_ID,
    PROP_EXTRACTION_ALGO_VERSION,
    PROP_FILE_ID,
    PROP_FOLDER_PATH,
    PROP_INGEST_FINGERPRINT,
    PROP_LAST_MODIFYING_USER,
    PROP_MIME_TYPE,
    PROP_PARENT_ID,
    PROP_SIZE_BYTES,
    PROP_VERSION,
    REFERENCE_DATA_IMAGE_RE,
    REFERENCE_IMAGE_USE_RE,
    STAGE_DISCOVERY,
    STAGE_DOCUMENTS,
    STAGE_FOLDERS,
)
from datahub.ingestion.source.google_drive.google_drive_client import (
    GoogleDriveClient,
)
from datahub.ingestion.source.google_drive.google_drive_config import (
    GoogleDriveSourceConfig,
)
from datahub.ingestion.source.google_drive.google_drive_hierarchy import (
    build_browse_path_v2,
    make_document_id,
    make_document_urn,
    resolve_parent_urn,
)
from datahub.ingestion.source.google_drive.google_drive_report import (
    GoogleDriveSourceReport,
)
from datahub.ingestion.source.google_drive.google_drive_selection import (
    MAX_TEXT_LENGTH,
    MIN_TEXT_LENGTH,
    SKIP_EMPTY_DOCUMENTS,
    exported_text_verdict,
    root_document_verdict,
)
from datahub.ingestion.source.google_drive.models import (
    DocumentRef,
    DriveFile,
    DriveTree,
    EmbedResult,
    ExportResult,
    FolderNode,
    StoredDocument,
    UnchangedCheck,
)
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    auto_stale_entity_removal,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.unstructured.chunking_config import (
    DataHubConnectionConfig,
    DocumentChunkingSourceConfig,
)
from datahub.ingestion.source.unstructured.chunking_source import (
    DocumentChunkingSource,
    SkipMarkerReadError,
    compute_source_text_sha256,
)
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DocumentInfoClass,
    DocumentStateClass,
    MetadataAttributionClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk.document import Document
from datahub.specific.aspect_helpers.ownership import HasOwnershipPatch
from datahub.utilities.lossy_collections import LossyList

logger = logging.getLogger(__name__)


def image_placeholder(alt: str) -> str:
    alt = alt.strip()
    return IMAGE_PLACEHOLDER_WITH_ALT.format(alt=alt) if alt else IMAGE_PLACEHOLDER


def clean_markdown(text: str) -> str:
    """Normalise exported markdown before it is stored and embedded.

    Any change here must bump EXTRACTION_ALGO_VERSION so already-ingested documents
    are re-exported with the new cleaning.
    """
    text = INLINE_DATA_IMAGE_RE.sub(lambda m: image_placeholder(m.group(1)), text)
    data_refs = {m.group(1) for m in REFERENCE_DATA_IMAGE_RE.finditer(text)}
    text = REFERENCE_DATA_IMAGE_RE.sub("", text)
    if data_refs:
        text = REFERENCE_IMAGE_USE_RE.sub(
            lambda m: (
                image_placeholder(m.group(1)) if m.group(2) in data_refs else m.group(0)
            ),
            text,
        )
    text = text.replace("\r\n", "\n")
    text = EXCESS_BLANK_LINES_RE.sub("\n\n", text)
    return text.strip()


class DocumentExportError(Exception):
    """Both exports failed for a reason that may be transient."""


class DocumentOwnershipPatchBuilder(HasOwnershipPatch):
    """Ownership patches for document entities, which have no SDK patch builder."""


def to_stored_document(
    info: object, status: object, ownership: object, attribution_source: str
) -> StoredDocument:
    connector_owners: List[str] = []
    if isinstance(ownership, OwnershipClass):
        connector_owners = [
            owner.owner
            for owner in ownership.owners
            if owner.attribution is not None
            and owner.attribution.source == attribution_source
        ]
    return StoredDocument(
        info=info
        if isinstance(info, DocumentInfoClass) and info.customProperties
        else None,
        removed=isinstance(status, StatusClass) and status.removed,
        connector_owners=connector_owners,
    )


@platform_name("Google Drive", id=PLATFORM)
@config_class(GoogleDriveSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.OWNERSHIP, "Enabled by default via `extract_owners`")
@capability(
    SourceCapability.PROBE,
    "Check what a recipe would ingest before running it, with `datahub recipe probe`",
)
class GoogleDriveSource(StatefulIngestionSourceBase, TestableSource):
    """Ingest Google Docs from Google Drive folders and shared drives as DataHub
    Document entities, preserving the folder hierarchy."""

    platform = PLATFORM

    def __init__(
        self,
        config: GoogleDriveSourceConfig,
        ctx: PipelineContext,
        drive_service: Optional[Resource] = None,
    ):
        # The base class is typed for StatefulIngestionConfigBase[StatefulIngestionConfig]
        # and its generic parameter is invariant, so the more specific config is rejected.
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report: GoogleDriveSourceReport = GoogleDriveSourceReport()
        self.client = GoogleDriveClient(config, self.report, service=drive_service)
        self.chunking_source = DocumentChunkingSource(
            ctx=ctx,
            config=DocumentChunkingSourceConfig(
                datahub=DataHubConnectionConfig(),
                chunking=config.chunking,
                embedding=config.embedding,
                max_documents=config.max_documents,
            ),
            standalone=False,
            graph=ctx.graph,
        )
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            state_provider=self.state_provider,
            report=self.report,
            config=self.config,
            state_type_class=GenericCheckpointState,
            pipeline_name=ctx.pipeline_name,
            run_id=ctx.run_id,
            platform=self.platform,
        )
        self._ingest_fingerprint = self._compute_ingest_fingerprint()
        instance = config.platform_instance
        # Owners this connector writes carry this source, so later runs can update or
        # remove them without touching owners added in DataHub.
        self._owner_attribution_source = (
            make_dataplatform_instance_urn(PLATFORM, instance)
            if instance
            else make_data_platform_urn(PLATFORM)
        )
        # An item found unchanged, kept so that a run which writes nothing can still
        # emit one workunit (see _emit_presence_if_idle).
        self._unchanged_urn: Optional[str] = None

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "GoogleDriveSource":
        config = GoogleDriveSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_excluded_workunit_processors(
        self,
    ) -> List[Union[str, Type[WorkunitProcessor]]]:
        # The manual handler below lets unchanged documents that are skipped without
        # emitting be registered as still present, so they are not soft-deleted.
        return [AutoStaleEntityRemovalProcessor]

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            partial(auto_stale_entity_removal, self.stale_entity_removal_handler),
        ]

    def _compute_ingest_fingerprint(self) -> str:
        """Hash of everything besides the Drive file itself that shapes the output.

        Stored on each document; a mismatch forces a re-export, so config changes such
        as switching the import mode take effect without manual intervention.
        """
        payload = json.dumps(
            {
                "algo": EXTRACTION_ALGO_VERSION,
                "import_mode": str(self.config.document_import_mode),
                "parent_document_urn": self.config.parent_document_urn,
                "create_root_documents": self.config.create_root_documents,
                "extract_owners": self.config.extract_owners,
                "platform_instance": self.config.platform_instance,
                # These decide whether a document is kept at all, so a tightened
                # setting must re-evaluate documents that look unchanged.
                "skip_empty_documents": self.config.skip_empty_documents,
                "min_text_length": self.config.min_text_length,
                "max_text_length": self.config.max_text_length,
                # A run without an embedding provider still records the Drive
                # version, so enabling or switching the model must re-embed.
                "embedding_model": self.chunking_source.embedding_model,
                "embedding_key": self.chunking_source.get_model_embedding_key(),
                "chunking": self.config.chunking.model_dump(mode="json"),
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        with self.report.new_stage(STAGE_DISCOVERY):
            self._log_authenticated_user()
            roots = self._resolve_roots()
            if not roots:
                return
            tree = self.client.walk(roots)
            # An unparseable Doc may still exist in DataHub; keep it rather than
            # letting stale removal soft-delete it.
            for file_id in tree.unparseable_document_ids:
                self.stale_entity_removal_handler.add_entity_to_state(
                    DOCUMENT_ENTITY_TYPE, make_document_urn(file_id)
                )
            logger.info(
                "Discovered %d folders and %d Google Docs under %d root(s)",
                len(tree.folders),
                len(tree.documents),
                len(roots),
            )

        with self.report.new_stage(STAGE_FOLDERS):
            folders = [
                folder
                for folder in tree.folders.values()
                if not folder.is_root or root_document_verdict(self.config).included
            ]
            for start in range(0, len(folders), CHANGE_DETECTION_BATCH_SIZE):
                folder_page = folders[start : start + CHANGE_DETECTION_BATCH_SIZE]
                stored_folders = self._load_stored_documents(
                    [folder.id for folder in folder_page if folder.version]
                )
                for folder in folder_page:
                    yield from self._emit_folder_safely(
                        folder, tree, stored_folders.get(make_document_urn(folder.id))
                    )

        with self.report.new_stage(STAGE_DOCUMENTS):
            total = len(tree.documents)
            processed = 0
            for start in range(0, total, CHANGE_DETECTION_BATCH_SIZE):
                page = tree.documents[start : start + CHANGE_DETECTION_BATCH_SIZE]
                stored = self._load_stored_documents(
                    [ref.file.id for ref in page if ref.file.version]
                )
                for ref in page:
                    yield from self._process_document_safely(
                        ref, tree, stored.get(make_document_urn(ref.file.id))
                    )
                    processed += 1
                    if processed % PROGRESS_LOG_INTERVAL == 0 or processed == total:
                        logger.info("Processed %d of %d Google Docs", processed, total)
            self._merge_chunking_logs()
            yield from self._emit_presence_if_idle()

    def _emit_presence_if_idle(self) -> Iterable[MetadataWorkUnit]:
        """Emit one no-op status write when every item was skipped as unchanged.

        Stale-entity removal treats a run that produced no workunits as a failed
        discovery and keeps the previous state, so a quiet run would never remove
        Docs deleted from Drive. Re-asserting that one present item is not removed
        changes nothing in DataHub, and a run that discovers nothing still produces
        nothing, so that fail-safe keeps working.
        """
        if self.report.events_produced == 0 and self._unchanged_urn is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=self._unchanged_urn, aspect=StatusClass(removed=False)
            ).as_workunit()

    def _record_unchanged(self, urn: str) -> None:
        self.stale_entity_removal_handler.add_entity_to_state(DOCUMENT_ENTITY_TYPE, urn)
        if self._unchanged_urn is None:
            self._unchanged_urn = urn

    def _emit_folder_safely(
        self, folder: FolderNode, tree: DriveTree, stored: Optional[StoredDocument]
    ) -> Iterable[MetadataWorkUnit]:
        try:
            yield from self._emit_folder(folder, tree, stored)
        except Exception as e:
            self.report.folders_failed += 1
            # Keep the folder in state: its documents still point to it as parent.
            self.stale_entity_removal_handler.add_entity_to_state(
                DOCUMENT_ENTITY_TYPE, make_document_urn(folder.id)
            )
            self.report.warning(
                title="Failed to ingest Drive folder",
                message="A folder document could not be built; documents beneath "
                "it are still ingested.",
                context=f"folder_id={folder.id}, path={folder.path}",
                exc=e,
            )

    def _process_document_safely(
        self, ref: DocumentRef, tree: DriveTree, stored: Optional[StoredDocument]
    ) -> Iterable[MetadataWorkUnit]:
        try:
            yield from self._process_document(ref, tree, stored)
        except Exception as e:
            if self.report.num_documents_limit_reached:
                self.report.failure(
                    title="Document limit reached",
                    message="The run stopped after max_documents documents were "
                    "embedded. Raise max_documents, or set it to 0, to ingest the rest.",
                    context=f"max_documents={self.config.max_documents}",
                    exc=e,
                )
                raise
            self.report.documents_failed += 1
            self.report.failed_documents.append(f"{ref.file.id} ({ref.file.name})")
            # The failure may be transient; keep what DataHub already has rather than
            # letting stale removal soft-delete the document.
            self.stale_entity_removal_handler.add_entity_to_state(
                DOCUMENT_ENTITY_TYPE, make_document_urn(ref.file.id)
            )
            self.report.warning(
                title="Failed to ingest Google Doc",
                message="A Google Doc could not be ingested; the copy already in "
                "DataHub, if any, was kept.",
                context=f"file_id={ref.file.id}, title={ref.file.name}",
                exc=e,
            )
            if not self.config.continue_on_failure:
                raise

    def _log_authenticated_user(self) -> None:
        try:
            email = self.client.get_authenticated_email()
            logger.info("Authenticated to Google Drive as %s", email)
        except Exception as e:
            logger.debug("Could not determine authenticated Drive user: %s", e)

    def _resolve_roots(self) -> List[FolderNode]:
        roots: List[FolderNode] = []
        for root_id in self.config.root_folder_ids:
            self.report.roots_scanned += 1
            try:
                roots.append(self.client.get_root(root_id))
            except Exception as e:
                self.report.roots_failed += 1
                self.report.failure(
                    title="Drive root not accessible",
                    message="A configured root folder could not be read. Share it with "
                    "the service account's client_email, or set delegated_user to a "
                    "user who can access it.",
                    context=f"root_folder_id={root_id}",
                    exc=e,
                )
        return roots

    def _emit_folder(
        self, folder: FolderNode, tree: DriveTree, stored: Optional[StoredDocument]
    ) -> Iterable[MetadataWorkUnit]:
        urn = make_document_urn(folder.id)
        check = self._check_unchanged(
            stored,
            version=folder.version,
            title=folder.title,
            parent_id=folder.parent_id or "",
            folder_path=folder.path,
        )
        if check.unchanged:
            self.report.folders_skipped_unchanged += 1
            self._record_unchanged(urn)
            if check.soft_deleted:
                yield from self._resurrect(urn)
            return

        parent_folder = tree.folders.get(folder.parent_id) if folder.parent_id else None
        parent_urn = resolve_parent_urn(
            parent_folder,
            self.config.create_root_documents,
            self.config.parent_document_urn,
        )
        custom_properties = {
            PROP_FILE_ID: folder.id,
            PROP_MIME_TYPE: MIME_FOLDER,
            PROP_FOLDER_PATH: folder.path,
            PROP_PARENT_ID: folder.parent_id or "",
            PROP_VERSION: folder.version or "",
            PROP_INGEST_FINGERPRINT: self._ingest_fingerprint,
        }
        if folder.drive_id:
            custom_properties[PROP_DRIVE_ID] = folder.drive_id

        doc = self._build_document(
            file_id=folder.id,
            title=folder.title,
            text="",
            external_url=folder.web_view_link or "",
            parent_urn=parent_urn,
            custom_properties=custom_properties,
            subtype=DocumentSubTypes.FOLDER,
            created_time=folder.created_time,
            last_modified_time=folder.modified_time,
        )
        self._attach_common_aspects(doc, parent_folder, tree)
        self.report.folders_emitted += 1
        yield from doc.as_workunits()

    def _process_document(
        self, ref: DocumentRef, tree: DriveTree, stored: Optional[StoredDocument]
    ) -> Iterable[MetadataWorkUnit]:
        file = ref.file
        urn = make_document_urn(file.id)
        folder = tree.folders[ref.folder_id]

        check = self._check_unchanged(
            stored,
            version=file.version,
            title=file.name,
            parent_id=folder.id,
            folder_path=folder.path,
        )
        if check.unchanged:
            self.report.documents_skipped_unchanged += 1
            self._record_unchanged(urn)
            if check.soft_deleted:
                yield from self._resurrect(urn)
            return

        result = self.client.export_markdown(file.id)
        if result.used_html_fallback:
            self.report.documents_html_fallback += 1
        if result.text is None:
            self._handle_export_failure(file, result)
            return

        text = clean_markdown(result.text)
        text_verdict = exported_text_verdict(self.config, len(text))
        if text_verdict.excluded_by in (SKIP_EMPTY_DOCUMENTS, MIN_TEXT_LENGTH):
            self.report.documents_skipped_empty += 1
            return
        if text_verdict.excluded_by == MAX_TEXT_LENGTH:
            self.report.documents_skipped_too_long += 1
            # Not registered in state, so a previously ingested copy is soft-deleted.
            self.report.warning(
                title="Google Doc exceeds max_text_length",
                message="The document's exported text is longer than max_text_length "
                "and was skipped.",
                context=f"file_id={file.id}, title={file.name}, length={len(text)}",
            )
            return

        text_sha256 = compute_source_text_sha256(text)
        embedded = self._embed(urn, file, text, text_sha256)

        custom_properties = self._document_properties(
            file, folder, text_sha256, embedding_ok=embedded.succeeded
        )
        doc = self._build_document(
            file_id=file.id,
            title=file.name,
            text=text,
            external_url=file.web_view_link or "",
            parent_urn=resolve_parent_urn(
                folder,
                self.config.create_root_documents,
                self.config.parent_document_urn,
            ),
            custom_properties=custom_properties,
            created_time=file.created_time,
            last_modified_time=file.modified_time,
        )
        self._attach_common_aspects(doc, folder, tree)
        self.report.documents_emitted += 1
        yield from doc.as_workunits()
        yield from self._ownership_workunits(urn, file, stored)
        yield from embedded.workunits
        if embedded.limit_error is not None:
            self.report.num_documents_limit_reached = True
            raise embedded.limit_error

    def _handle_export_failure(self, file: DriveFile, result: ExportResult) -> None:
        self.report.documents_export_failed += 1
        if result.failure_reason == FAILURE_TOO_LARGE:
            self.report.warning(
                title="Google Doc too large to export",
                message="Google Drive refuses to export documents over 10 MB; "
                "the document was skipped.",
                context=f"file_id={file.id}, title={file.name}",
            )
        elif result.failure_reason == FAILURE_NOT_FOUND:
            self.report.warning(
                title="Google Doc no longer exists",
                message="The document was removed from Drive after it was listed "
                "and was skipped.",
                context=f"file_id={file.id}, title={file.name}",
            )
        else:
            # Possibly transient: fail the document so its stored copy is kept and
            # continue_on_failure decides whether the run goes on.
            raise DocumentExportError(
                f"reason={result.failure_reason}, error={result.error}"
            )

    def _embed(
        self, urn: str, file: DriveFile, text: str, text_sha256: str
    ) -> EmbedResult:
        """Run chunking/embedding before the document is emitted.

        The outcome is recorded on the document (see _document_properties) so that a
        failed embedding is retried on the next run instead of being skipped as
        unchanged.
        """
        workunits: List[MetadataWorkUnit] = []
        elements = [
            {
                "type": ELEMENT_TYPE_NARRATIVE_TEXT,
                "text": text,
                "metadata": {
                    "file_id": file.id,
                    "title": file.name,
                    "url": file.web_view_link or "",
                },
            }
        ]
        try:
            for wu in self.chunking_source.process_elements_inline(
                document_urn=urn,
                elements=elements,
                source_text_sha256=text_sha256,
            ):
                workunits.append(wu)
        except SkipMarkerReadError as e:
            # The stored embedding state could not be read; retry next run.
            self._report_embedding_deferred(file, e)
            return EmbedResult(workunits=workunits, succeeded=False)
        except RuntimeError as e:
            if self.chunking_source.report.num_documents_limit_reached:
                return EmbedResult(workunits=workunits, succeeded=True, limit_error=e)
            self._report_embedding_deferred(file, e)
            return EmbedResult(workunits=workunits, succeeded=False)
        except Exception as e:
            self._report_embedding_deferred(file, e)
            return EmbedResult(workunits=workunits, succeeded=False)
        return EmbedResult(workunits=workunits, succeeded=True)

    def _report_embedding_deferred(self, file: DriveFile, exc: Exception) -> None:
        self.report.documents_embedding_deferred += 1
        self.report.warning(
            title="Embedding generation deferred",
            message="The document was ingested without embeddings; they will be "
            "retried on the next run.",
            context=f"file_id={file.id}, title={file.name}",
            exc=exc,
        )

    def _load_stored_documents(self, file_ids: List[str]) -> Dict[str, StoredDocument]:
        """Read back what DataHub holds for a page of Drive items, keyed by URN.

        Reads go to the entity store rather than search: a document soft-deleted by
        the previous run may not be indexed as removed yet, and missing that would
        leave a restored document deleted. Documents DataHub has never seen are
        absent from the result.
        """
        graph = self.ctx.graph
        urns = [make_document_urn(file_id) for file_id in file_ids]
        if graph is None or not urns:
            return {}
        try:
            entities = graph.get_entities(
                DOCUMENT_ENTITY_TYPE,
                urns,
                aspects=[ASPECT_DOCUMENT_INFO, ASPECT_STATUS, ASPECT_OWNERSHIP],
            )
        except Exception as e:
            self.report.change_detection_batch_fallbacks += 1
            self.report.warning(
                title="Batched change detection lookup failed",
                message="Stored documents were read from DataHub one at a time "
                "instead, which is slower.",
                context=f"documents={len(urns)}",
                exc=e,
            )
            return self._load_stored_documents_individually(graph, urns)
        self.report.change_detection_batch_reads += 1
        stored: Dict[str, StoredDocument] = {}
        for urn, aspects in entities.items():
            info = aspects.get(ASPECT_DOCUMENT_INFO)
            status = aspects.get(ASPECT_STATUS)
            ownership = aspects.get(ASPECT_OWNERSHIP)
            stored[urn] = to_stored_document(
                info[0] if info else None,
                status[0] if status else None,
                ownership[0] if ownership else None,
                self._owner_attribution_source,
            )
        return stored

    def _load_stored_documents_individually(
        self, graph: DataHubGraph, urns: List[str]
    ) -> Dict[str, StoredDocument]:
        stored: Dict[str, StoredDocument] = {}
        for urn in urns:
            try:
                aspects = graph.get_entity_semityped(
                    urn, aspects=[ASPECT_DOCUMENT_INFO, ASPECT_STATUS, ASPECT_OWNERSHIP]
                )
            except Exception as e:
                self.report.change_detection_lookup_failures += 1
                self.report.warning(
                    title="Change detection lookup failed",
                    message="The stored document could not be read from DataHub, so "
                    "it was exported again instead of being checked for changes.",
                    context=f"urn={urn}",
                    exc=e,
                )
                continue
            stored[urn] = to_stored_document(
                aspects.get(ASPECT_DOCUMENT_INFO),
                aspects.get(ASPECT_STATUS),
                aspects.get(ASPECT_OWNERSHIP),
                self._owner_attribution_source,
            )
        return stored

    def _check_unchanged(
        self,
        stored: Optional[StoredDocument],
        version: Optional[str],
        title: str,
        parent_id: str,
        folder_path: str,
    ) -> UnchangedCheck:
        if stored is None or stored.info is None or not version:
            return UnchangedCheck(unchanged=False, soft_deleted=False)
        info = stored.info
        props = info.customProperties
        unchanged = (
            props.get(PROP_VERSION) == version
            and props.get(PROP_INGEST_FINGERPRINT) == self._ingest_fingerprint
            and props.get(PROP_PARENT_ID) == parent_id
            # Renaming or moving an ancestor changes the path but not the item.
            and props.get(PROP_FOLDER_PATH) == folder_path
            and info.title == title
        )
        return UnchangedCheck(unchanged=unchanged, soft_deleted=stored.removed)

    def _resurrect(self, urn: str) -> Iterable[MetadataWorkUnit]:
        self.report.documents_resurrected += 1
        yield MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=StatusClass(removed=False)
        ).as_workunit()

    def _document_properties(
        self,
        file: DriveFile,
        folder: FolderNode,
        text_sha256: str,
        embedding_ok: bool,
    ) -> Dict[str, str]:
        properties = {
            PROP_FILE_ID: file.id,
            PROP_MIME_TYPE: file.mime_type,
            PROP_FOLDER_PATH: folder.path,
            PROP_PARENT_ID: folder.id,
            # An empty version makes the next run treat the document as changed, which
            # retries embeddings that failed during this run.
            PROP_VERSION: (file.version or "") if embedding_ok else "",
            PROP_INGEST_FINGERPRINT: self._ingest_fingerprint,
            PROP_CONTENT_HASH: text_sha256,
            PROP_EXTRACTION_ALGO_VERSION: EXTRACTION_ALGO_VERSION,
        }
        if file.drive_id:
            properties[PROP_DRIVE_ID] = file.drive_id
        if file.size:
            properties[PROP_SIZE_BYTES] = file.size
        if file.last_modifying_user and file.last_modifying_user.email_address:
            properties[PROP_LAST_MODIFYING_USER] = (
                file.last_modifying_user.email_address
            )
        return properties

    def _owner_urns(self, file: DriveFile) -> List[str]:
        if not self.config.extract_owners:
            return []
        owners: List[str] = []
        for owner in file.owners:
            if owner.email_address:
                urn = str(CorpUserUrn(owner.email_address))
                if urn not in owners:
                    owners.append(urn)
        return owners

    def _ownership_workunits(
        self, urn: str, file: DriveFile, stored: Optional[StoredDocument]
    ) -> Iterable[MetadataWorkUnit]:
        """Patch the Drive owners in rather than replacing the ownership aspect.

        Owners added in DataHub must survive a re-ingest, so each Drive owner is
        attributed to this connector, and only attributed owners that Drive no longer
        lists are removed.
        """
        current = self._owner_urns(file)
        previous = stored.connector_owners if stored else []
        removed = [owner for owner in previous if owner not in current]
        if not current and not removed:
            return
        patch = DocumentOwnershipPatchBuilder(urn)
        for owner in removed:
            patch.remove_owner(owner, attribution_source=self._owner_attribution_source)
        attributed_at = (
            int(file.modified_time.timestamp() * 1000)
            if file.modified_time
            else get_sys_time()
        )
        for owner in current:
            patch.add_owner(
                OwnerClass(
                    owner=owner,
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                    attribution=MetadataAttributionClass(
                        time=attributed_at,
                        actor=OWNERSHIP_ATTRIBUTION_ACTOR,
                        source=self._owner_attribution_source,
                    ),
                )
            )
        for mcp in patch.build():
            yield MetadataWorkUnit(id=f"{urn}-{ASPECT_OWNERSHIP}", mcp_raw=mcp)

    def _build_document(
        self,
        file_id: str,
        title: str,
        text: str,
        external_url: str,
        parent_urn: Optional[str],
        custom_properties: Dict[str, str],
        subtype: Optional[str] = None,
        created_time: Optional[datetime] = None,
        last_modified_time: Optional[datetime] = None,
    ) -> Document:
        if self.config.document_import_mode == DocumentImportMode.NATIVE:
            return Document.create_document(
                id=make_document_id(file_id),
                title=title,
                text=text,
                status=DocumentStateClass.PUBLISHED,
                subtype=subtype,
                parent_document=parent_urn,
                custom_properties=custom_properties,
                created_time=created_time,
                last_modified_time=last_modified_time,
            )
        return Document.create_external_document(
            id=make_document_id(file_id),
            title=title,
            platform=PLATFORM,
            external_url=external_url,
            external_id=file_id,
            text=text,
            status=DocumentStateClass.PUBLISHED,
            subtype=subtype,
            parent_document=parent_urn,
            custom_properties=custom_properties,
            created_time=created_time,
            last_modified_time=last_modified_time,
        )

    def _attach_common_aspects(
        self, doc: Document, parent_folder: Optional[FolderNode], tree: DriveTree
    ) -> None:
        instance = self.config.platform_instance
        doc._set_aspect(
            DataPlatformInstanceClass(
                platform=make_data_platform_urn(PLATFORM),
                instance=make_dataplatform_instance_urn(PLATFORM, instance)
                if instance
                else None,
            )
        )
        try:
            browse_path = build_browse_path_v2(
                parent_folder,
                tree.folders,
                self.config.create_root_documents,
                self.config.parent_document_urn,
            )
            if browse_path:
                doc._set_aspect(browse_path)
        except Exception as e:
            self.report.warning(
                title="Browse path generation failed",
                message="The document was emitted without a browse path.",
                context=f"urn={doc.urn}",
                exc=e,
            )
        # Drive is the source of truth: an item that exists again in Drive must not
        # stay soft-deleted from an earlier run.
        doc._set_aspect(StatusClass(removed=False))

    def get_report(self) -> GoogleDriveSourceReport:
        chunking_report = self.chunking_source.report
        self.report.num_documents_with_embeddings = (
            chunking_report.num_documents_with_embeddings
        )
        self.report.num_embedding_failures = chunking_report.num_embedding_failures
        self.report.embedding_failures = LossyList()
        for failure in chunking_report.embedding_failures:
            self.report.embedding_failures.append(failure)
        self.report.num_documents_truncated_oversized = (
            chunking_report.num_documents_truncated_oversized
        )
        self.report.num_documents_dropped_oversized = (
            chunking_report.num_documents_dropped_oversized
        )
        return self.report

    def _merge_chunking_logs(self) -> None:
        """Copy warnings and failures raised while embedding into this report.

        The chunking source logs to its own report, which the pipeline never prints;
        without this, truncated or dropped embeddings would be invisible.
        """
        chunking_report = self.chunking_source.report
        for entry in chunking_report.warnings:
            for context in entry.context or [None]:
                self.report.warning(
                    title=entry.title,
                    message=entry.message,
                    context=context,
                    log=False,
                )
        for entry in chunking_report.failures:
            for context in entry.context or [None]:
                self.report.failure(
                    title=entry.title,
                    message=entry.message,
                    context=context,
                    log=False,
                )

    @staticmethod
    def _test_semantic_search_capability(
        config: GoogleDriveSourceConfig,
    ) -> CapabilityReport:
        try:
            resolved = DocumentChunkingSource.resolve_embedding_config(
                config.embedding, None
            )
            return DocumentChunkingSource.test_embedding_capability(resolved)
        except ValueError as e:
            return CapabilityReport(
                capable=False,
                failure_reason=str(e),
                mitigation_message="Fix the embedding configuration or enable "
                "semantic search on the DataHub server.",
            )
        except Exception as e:
            return CapabilityReport(
                capable=False,
                failure_reason=f"Failed to resolve embedding configuration: {e}",
                mitigation_message="Check the embedding configuration.",
            )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        try:
            config = GoogleDriveSourceConfig.model_validate(config_dict)
        except Exception as e:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Failed to parse config: {e}",
            )

        try:
            client = GoogleDriveClient(config, GoogleDriveSourceReport())
            client.get_authenticated_email()
        except Exception as e:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False,
                    failure_reason=f"Failed to authenticate to Google Drive: {e}",
                    mitigation_message="Verify the service account key, that the "
                    "Google Drive API is enabled in its GCP project, and (when using "
                    "delegated_user) that domain-wide delegation grants the "
                    f"{DRIVE_SCOPES[0]} scope.",
                )
            )

        inaccessible: List[str] = []
        for root_id in config.root_folder_ids:
            try:
                client.get_root(root_id)
            except Exception as e:
                inaccessible.append(f"{root_id} ({e.__class__.__name__})")

        capability_report: Dict[Union[SourceCapability, str], CapabilityReport] = {
            CAPABILITY_SEMANTIC_SEARCH: GoogleDriveSource._test_semantic_search_capability(
                config
            )
        }
        if inaccessible:
            share_with = (
                config.credential.client_email
                if config.credential
                else "the authenticated account"
            )
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False,
                    failure_reason="Cannot access root folder(s): "
                    + ", ".join(inaccessible),
                    mitigation_message=f"Share these folders with {share_with}, "
                    "or set delegated_user to a user who can access them.",
                ),
                capability_report=capability_report,
            )
        return TestConnectionReport(
            basic_connectivity=CapabilityReport(capable=True),
            capability_report=capability_report,
        )
