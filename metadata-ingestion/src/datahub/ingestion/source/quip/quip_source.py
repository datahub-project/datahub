import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Set, Union

from pydantic import BaseModel, Field

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_document_urn,
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
from datahub.ingestion.source.quip.quip_client import (
    QuipClient,
    QuipClientError,
    QuipFolder,
)
from datahub.ingestion.source.quip.quip_config import QuipSourceConfig
from datahub.ingestion.source.quip.quip_html import quip_html_to_markdown
from datahub.ingestion.source.quip.quip_report import QuipSourceReport
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.unstructured.chunking_config import (
    DataHubConnectionConfig,
    DocumentChunkingSourceConfig,
)
from datahub.ingestion.source.unstructured.chunking_source import (
    DocumentChunkingSource,
)
from datahub.metadata.schema_classes import (
    DataPlatformInfoClass,
    DataPlatformInstanceClass,
    DocumentStateClass,
    PlatformTypeClass,
)
from datahub.sdk.document import Document

logger = logging.getLogger(__name__)

# Bump when the thread extraction algorithm changes (HTML->Markdown conversion,
# text normalization) to force re-ingestion of all threads on the next run.
EXTRACTION_ALGO_VERSION = "1"

QUIP_LOGO_URL = "assets/platforms/quiplogo.png"


class CrawlResult(BaseModel):
    """Folder tree discovered while crawling Quip."""

    # folder_id -> folder
    folders: Dict[str, QuipFolder] = Field(default_factory=dict)
    # folder_id -> parent folder_id (None for root folders)
    folder_parents: Dict[str, Optional[str]] = Field(default_factory=dict)
    # thread_id -> shallowest containing folder_id (None for unparented threads)
    thread_parents: Dict[str, Optional[str]] = Field(default_factory=dict)


@platform_name("Quip")
@config_class(QuipSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class QuipSource(StatefulIngestionSourceBase, TestableSource):
    """Ingests Quip folders and threads as DataHub Document entities."""

    platform = "quip"  # Required for stateful ingestion checkpoint job_id

    config: QuipSourceConfig
    report: QuipSourceReport

    def __init__(self, config: QuipSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report = QuipSourceReport()

        self.client = QuipClient(
            access_token=config.access_token.get_secret_value(),
            base_url=config.base_url,
        )

        # Chunking/embedding sub-component shared with the Notion/Confluence
        # connectors; runs inline rather than as a standalone source.
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

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "QuipSource":
        config = QuipSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _get_instance_id(self) -> str:
        if self.config.platform_instance:
            return self.config.platform_instance
        return hashlib.sha256(self.config.base_url.encode()).hexdigest()[:8]

    def _thread_doc_id(self, thread_id: str) -> str:
        return f"{self.platform}-{self._get_instance_id()}-{thread_id}"

    def _folder_doc_id(self, folder_id: str) -> str:
        return f"{self.platform}-{self._get_instance_id()}-folder-{folder_id}"

    def _discover_root_folder_ids(self) -> List[str]:
        try:
            user = self.client.get_authenticated_user()
        except QuipClientError as e:
            self.report.report_failure("user", f"Failed to fetch current user: {e}")
            return []
        root_ids = user.root_folder_ids
        logger.info(f"Auto-discovered {len(root_ids)} root Quip folder(s)")
        return root_ids

    def _crawl(self) -> CrawlResult:
        crawl = CrawlResult()

        root_folder_ids = self.config.folder_ids or (
            self._discover_root_folder_ids() if not self.config.thread_ids else []
        )

        max_depth = self.config.hierarchy.folder_mapping.max_depth
        for folder_id in root_folder_ids:
            self._crawl_folder(
                folder_id,
                parent_folder_id=None,
                depth=0,
                max_depth=max_depth,
                crawl=crawl,
            )

        for thread_id in self.config.thread_ids:
            crawl.thread_parents.setdefault(thread_id, None)

        return crawl

    def _crawl_folder(
        self,
        folder_id: str,
        parent_folder_id: Optional[str],
        depth: int,
        max_depth: int,
        crawl: CrawlResult,
    ) -> None:
        if folder_id in crawl.folders:
            return

        # Once the thread budget is exhausted no further threads will be recorded,
        # so descending into the rest of the tree only wastes get_folder API calls.
        if self._thread_limit_reached(len(crawl.thread_parents)):
            return

        try:
            folder = self.client.get_folder(folder_id)
        except QuipClientError as e:
            self.report.report_warning(folder_id, f"Failed to fetch folder: {e}")
            return

        crawl.folders[folder_id] = folder
        crawl.folder_parents[folder_id] = parent_folder_id
        self.report.report_folder_scanned()

        # Assign this folder's own threads before descending so a thread that also
        # appears in a sub-folder is parented to the shallower folder. Quip folders
        # behave like tags (a thread can live in many), but DataHub's parentDocument
        # is single-valued, so we keep the first (shallowest) folder seen.
        for child in folder.children:
            if child.thread_id and child.thread_id not in crawl.thread_parents:
                if self._thread_limit_reached(len(crawl.thread_parents)):
                    break
                crawl.thread_parents[child.thread_id] = folder_id

        if self.config.recursive and depth + 1 <= max_depth:
            for child in folder.children:
                if child.folder_id:
                    self._crawl_folder(
                        child.folder_id,
                        parent_folder_id=folder_id,
                        depth=depth + 1,
                        max_depth=max_depth,
                        crawl=crawl,
                    )

    def _thread_limit_reached(self, current_count: int) -> bool:
        if self.config.max_threads is None or self.config.max_threads <= 0:
            return False
        return current_count >= self.config.max_threads

    def _folder_parent_urn(
        self, folder_id: str, folder_parents: Dict[str, Optional[str]]
    ) -> Optional[str]:
        if not self.config.hierarchy.enabled:
            return None
        parent_folder_id = folder_parents.get(folder_id)
        if parent_folder_id and parent_folder_id in folder_parents:
            return make_document_urn(self._folder_doc_id(parent_folder_id))
        return self.config.hierarchy.folder_mapping.root_parent

    def _create_folder_document(
        self,
        folder_id: str,
        folder: QuipFolder,
        folder_parents: Dict[str, Optional[str]],
    ) -> Iterable[MetadataWorkUnit]:
        title = folder.folder.title or f"Folder {folder_id}"
        parent_urn = self._folder_parent_urn(folder_id, folder_parents)

        # Folders have no stable external content URL in Quip, so they are always
        # modeled as NATIVE documents regardless of document_import_mode (which
        # governs threads only).
        doc = Document.create_document(
            id=self._folder_doc_id(folder_id),
            title=title,
            text="",
            status=DocumentStateClass.PUBLISHED,
            subtype="Folder",
            parent_document=parent_urn,
            custom_properties={"folder_id": folder_id, "quip_type": "folder"},
            created_time=_usec_to_datetime(folder.folder.created_usec),
            last_modified_time=_usec_to_datetime(folder.folder.updated_usec),
        )
        self._set_platform_instance(doc)
        yield from doc.as_workunits()
        self.report.report_folder_document_created()

    def _thread_parent_urn(
        self,
        parent_folder_id: Optional[str],
        in_scope_folders: Set[str],
    ) -> Optional[str]:
        if not self.config.hierarchy.enabled:
            return None
        if (
            parent_folder_id
            and self.config.hierarchy.folder_mapping.create_parent_docs
            and parent_folder_id in in_scope_folders
        ):
            return make_document_urn(self._folder_doc_id(parent_folder_id))
        return self.config.hierarchy.folder_mapping.root_parent

    def _create_thread_document(
        self,
        thread_id: str,
        parent_folder_id: Optional[str],
        in_scope_folders: Set[str],
    ) -> Iterable[MetadataWorkUnit]:
        start_time = time.time()
        self.report.report_thread_scanned()

        try:
            response = self.client.get_thread(thread_id)
        except QuipClientError as e:
            self.report.report_thread_failed(thread_id, str(e))
            if not self.config.advanced.continue_on_failure:
                raise
            return

        thread = response.thread
        thread_type = (thread.type or "document").lower()

        if self.config.thread_types and thread_type not in self.config.thread_types:
            self.report.report_thread_skipped(
                thread_id, f"Thread type '{thread_type}' not in thread_types"
            )
            return

        title = thread.title or f"Quip thread {thread_id}"
        html = response.html or ""
        text = quip_html_to_markdown(html)

        if len(text) < self.config.filtering.min_text_length:
            self.report.report_thread_skipped(
                thread_id,
                f"Text length {len(text)} < minimum {self.config.filtering.min_text_length}",
            )
            return

        url = thread.link or f"{self.config.base_url}/{thread_id}"
        parent_urn = self._thread_parent_urn(parent_folder_id, in_scope_folders)

        content_hash = hashlib.sha256(
            json.dumps(
                {"html": html, "algo_version": EXTRACTION_ALGO_VERSION}, sort_keys=True
            ).encode("utf-8")
        ).hexdigest()

        doc_id = self._thread_doc_id(thread_id)
        doc = self._build_thread_document(
            doc_id=doc_id,
            title=title,
            text=text,
            url=url,
            thread_id=thread_id,
            subtype=thread_type.capitalize(),
            parent_urn=parent_urn,
            custom_properties={
                "thread_id": thread_id,
                "quip_type": thread_type,
                "author_id": thread.author_id or "",
                "content_hash": content_hash,
                "extraction_algo_version": EXTRACTION_ALGO_VERSION,
            },
            created_time=_usec_to_datetime(thread.created_usec),
            last_modified_time=_usec_to_datetime(thread.updated_usec),
        )
        self._set_platform_instance(doc)
        yield from doc.as_workunits()

        document_urn = make_document_urn(doc_id)
        try:
            yield from self.chunking_source.process_elements_inline(
                document_urn=document_urn,
                elements=[
                    {
                        "type": "NarrativeText",
                        "text": text,
                        "metadata": {
                            "thread_id": thread_id,
                            "title": title,
                            "url": url,
                        },
                    }
                ],
            )
        except RuntimeError:
            if self.chunking_source.report.num_documents_limit_reached:
                self.report.num_documents_limit_reached = True
                raise
            logger.warning(
                f"Failed to generate embeddings for {document_urn}; "
                "document ingested without embeddings.",
                exc_info=True,
            )
        except Exception:
            logger.warning(
                f"Failed to generate embeddings for {document_urn}; "
                "document ingested without embeddings.",
                exc_info=True,
            )

        self.report.report_thread_processed()
        self.report.report_text_extracted(len(text))
        logger.debug(
            "Processed thread %s in %.2fs", thread_id, time.time() - start_time
        )

    def _build_thread_document(
        self,
        *,
        doc_id: str,
        title: str,
        text: str,
        url: str,
        thread_id: str,
        subtype: str,
        parent_urn: Optional[str],
        custom_properties: Dict[str, str],
        created_time: Optional[datetime],
        last_modified_time: Optional[datetime],
    ) -> Document:
        if self.config.document_import_mode == DocumentImportMode.NATIVE:
            return Document.create_document(
                id=doc_id,
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
            id=doc_id,
            title=title,
            platform=self.platform,
            external_url=url,
            external_id=thread_id,
            text=text,
            status=DocumentStateClass.PUBLISHED,
            subtype=subtype,
            parent_document=parent_urn,
            custom_properties=custom_properties,
            created_time=created_time,
            last_modified_time=last_modified_time,
        )

    def _set_platform_instance(self, doc: Document) -> None:
        # Use the low-level _set_aspect (mirrors the Confluence connector) until the
        # Document SDK exposes dataPlatformInstance as a first-class parameter.
        doc._set_aspect(
            DataPlatformInstanceClass(
                platform=make_data_platform_urn(self.platform),
                instance=make_dataplatform_instance_urn(
                    self.platform, self._get_instance_id()
                ),
            )
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=make_data_platform_urn(self.platform),
            aspect=DataPlatformInfoClass(
                name=self.platform,
                type=PlatformTypeClass.OTHERS,
                datasetNameDelimiter="/",
                displayName="Quip",
                logoUrl=QUIP_LOGO_URL,
            ),
        ).as_workunit()

        crawl = self._crawl()
        in_scope_folders = set(crawl.folders.keys())

        if not crawl.folders and not crawl.thread_parents:
            logger.warning("No Quip folders or threads found to ingest")
            return

        # Emit folder documents first so thread parents resolve.
        if self.config.hierarchy.folder_mapping.create_parent_docs:
            for folder_id, folder in crawl.folders.items():
                try:
                    yield from self._create_folder_document(
                        folder_id, folder, crawl.folder_parents
                    )
                except Exception as e:
                    self.report.report_warning(
                        folder_id, f"Failed to create folder document: {e}"
                    )
                    if not self.config.advanced.continue_on_failure:
                        raise

        for thread_id, parent_folder_id in crawl.thread_parents.items():
            try:
                yield from self._create_thread_document(
                    thread_id, parent_folder_id, in_scope_folders
                )
            except Exception as e:
                self.report.report_thread_failed(thread_id, str(e))
                if (
                    not self.config.advanced.continue_on_failure
                    or self.chunking_source.report.num_documents_limit_reached
                ):
                    raise

    def get_report(self) -> QuipSourceReport:
        chunking_report = self.chunking_source.report
        self.report.num_documents_with_embeddings = (
            chunking_report.num_documents_with_embeddings
        )
        self.report.num_embedding_failures = chunking_report.num_embedding_failures
        for failure in chunking_report.embedding_failures:
            self.report.embedding_failures.append(failure)
        self.report.num_documents_limit_reached = (
            chunking_report.num_documents_limit_reached
        )
        return self.report

    @staticmethod
    def _test_semantic_search_capability(
        config: QuipSourceConfig,
    ) -> CapabilityReport:
        try:
            # No graph available in the static connection test; resolve_embedding_config
            # falls back to defaults when the server can't be queried.
            resolved_config = DocumentChunkingSource.resolve_embedding_config(
                config.embedding, None
            )
            return DocumentChunkingSource.test_embedding_capability(resolved_config)
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
                mitigation_message="Check DataHub server connection and embedding "
                "configuration.",
            )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        try:
            config = QuipSourceConfig.model_validate(config_dict)
        except Exception as e:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Failed to parse config: {e}",
            )

        client = QuipClient(
            access_token=config.access_token.get_secret_value(),
            base_url=config.base_url,
        )

        try:
            user = client.get_authenticated_user()
        except Exception as e:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False,
                    failure_reason=f"Failed to connect to Quip: {e}",
                    mitigation_message="Verify the access_token is valid and base_url "
                    "points to your Quip Automation API endpoint.",
                )
            )

        capability_report: Dict[Union[SourceCapability, str], CapabilityReport] = {}

        inaccessible: List[str] = []
        for folder_id in config.folder_ids[:5]:
            try:
                client.get_folder(folder_id)
            except Exception as e:
                inaccessible.append(f"folder {folder_id}: {e.__class__.__name__}")
        for thread_id in config.thread_ids[:5]:
            try:
                client.get_thread(thread_id)
            except Exception as e:
                inaccessible.append(f"thread {thread_id}: {e.__class__.__name__}")

        if inaccessible:
            capability_report["Folder/Thread Access"] = CapabilityReport(
                capable=False,
                failure_reason=f"Cannot access: {', '.join(inaccessible[:3])}",
                mitigation_message="Verify the token's user has access to the "
                "configured folders/threads.",
            )
        else:
            num_roots = len(config.folder_ids) or len(user.root_folder_ids)
            capability_report["Folder/Thread Access"] = CapabilityReport(
                capable=True,
                mitigation_message=f"Authenticated as '{user.name or 'unknown'}'; "
                f"{num_roots} accessible root folder(s).",
            )

        capability_report["Semantic Search"] = (
            QuipSource._test_semantic_search_capability(config)
        )

        return TestConnectionReport(
            basic_connectivity=CapabilityReport(capable=True),
            capability_report=capability_report,
        )


def _usec_to_datetime(usec: Optional[int]) -> Optional[datetime]:
    if not usec:
        return None
    try:
        return datetime.fromtimestamp(int(usec) / 1_000_000, tz=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None
