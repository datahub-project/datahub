import logging
import time
from typing import Dict, Generator, Iterable, List, Optional, Set

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
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.embeddings import ServerEmbeddingRunError
from datahub.ingestion.source.confluence.client import (
    ConfluenceClient,
    create_atlassian_client,
)
from datahub.ingestion.source.confluence.config import ConfluenceSourceConfig
from datahub.ingestion.source.confluence.connection import (
    run_connection_test,
)
from datahub.ingestion.source.confluence.crawler import ConfluenceCrawler
from datahub.ingestion.source.confluence.documents import (
    ConfluenceDocumentBuilder,
)
from datahub.ingestion.source.confluence.hierarchy import (
    FolderNode,
    collect_folder_nodes,
)
from datahub.ingestion.source.confluence.models import ConfluencePage
from datahub.ingestion.source.confluence.report import ConfluenceSourceReport
from datahub.ingestion.source.confluence.storage_format import (
    storage_to_markdown,
    strip_html_tags,
)
from datahub.ingestion.source.confluence.users import ConfluenceUserResolver
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
from datahub.utilities.lossy_collections import LossyList

logger = logging.getLogger(__name__)


@platform_name("Confluence")
@config_class(ConfluenceSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class ConfluenceSource(StatefulIngestionSourceBase, TestableSource):
    """Ingests Confluence Cloud and Data Center pages, and Cloud folders, as
    Document entities, keeping the page hierarchy and generating embeddings
    for semantic search."""

    platform = "confluence"  # Required for stateful ingestion checkpoint job_id
    config: ConfluenceSourceConfig
    report: ConfluenceSourceReport

    def __init__(self, config: ConfluenceSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report = ConfluenceSourceReport()

        self.confluence_client = create_atlassian_client(config)
        self.client = ConfluenceClient(self.confluence_client, self.report)
        self.crawler = ConfluenceCrawler(self.client, config, self.report)
        self.documents = ConfluenceDocumentBuilder(config)
        self.user_resolver = ConfluenceUserResolver(self.client, self.report)

        self.chunking_source = DocumentChunkingSource(
            ctx=ctx,
            config=DocumentChunkingSourceConfig(
                datahub=DataHubConnectionConfig(),  # Not used in inline mode
                chunking=config.chunking,
                embedding=config.embedding,
                max_documents=config.max_documents,
            ),
            standalone=False,
            graph=ctx.graph,  # Loads the embedding config from the server
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # Pages are collected before any is emitted: a page links to its parent
        # only when the parent is ingested too, which needs the full set.
        pages = list(self.crawler.iter_pages())
        if not pages:
            logger.warning("No pages collected for ingestion")
            return
        logger.info(f"Collected {len(pages)} pages")

        ingested_ids = {page.id for page in pages}
        # Sparse parent pages are kept even when below min_text_length, since
        # they anchor the hierarchy.
        parent_ids = {page.parent_id for page in pages if page.parent_id}

        folders: Dict[str, FolderNode] = {}
        if self.config.ingest_folders:
            try:
                folders = collect_folder_nodes(pages)
            except Exception as e:
                # Folders only reconstruct the hierarchy; failing to find them
                # must not stop the pages from being ingested.
                self.report.report_folder_failed("<discovery>", str(e))
            ingested_ids |= set(folders)

        # Folders first, so the parents exist before the pages that reference them.
        for folder in folders.values():
            try:
                workunits = self.documents.folder_document(
                    folder, ingested_ids
                ).as_workunits()
            except Exception as e:
                # Folders only reconstruct the hierarchy; one that cannot be
                # built must not stop the pages from being ingested.
                self.report.report_folder_failed(folder.id, str(e))
                continue
            yield from workunits
            self.report.folders_ingested += 1

        for page in pages:
            try:
                yield from self._page_workunits(page, ingested_ids, parent_ids)
            except ServerEmbeddingRunError:
                raise
            except Exception as e:
                self.report.report_page_failed(page.id, page.space.key or "", str(e))
                if (
                    not self.config.advanced.continue_on_failure
                    or self.chunking_source.report.num_documents_limit_reached
                ):
                    raise

    def _page_workunits(
        self, page: ConfluencePage, ingested_ids: Set[str], parent_ids: Set[str]
    ) -> Iterable[MetadataWorkUnit]:
        start_time = time.time()

        if self._is_page_denied(page.id):
            self.report.report_page_skipped(page.id, "Filtered by pages.deny")
            return

        text = self._page_text(page)
        skip_reason = self._skip_reason(text)
        if skip_reason and page.id not in parent_ids:
            self.report.report_page_skipped(page.id, skip_reason)
            return

        yield from self.documents.page_document(page, text, ingested_ids).as_workunits()

        if not (yield from self._embed(page, text)):
            return
        self.report.report_page_processed(page.id, time.time() - start_time)
        self.report.report_text_extracted(len(text))

    def _is_page_denied(self, page_id: str) -> bool:
        """pages.allow seeds the crawl, so only pages.deny is checked here.
        Re-checking pages.allow would drop the child pages a recursive crawl
        found under the seeds."""
        deny = self.config._parsed_page_deny
        return deny is not None and page_id in deny

    def _skip_reason(self, text: str) -> Optional[str]:
        # Empty pages are dropped even when min_text_length is 0, so an empty
        # string is never embedded.
        if self.config.filtering.skip_empty_documents and not text:
            return "No text content"
        if len(text) < self.config.filtering.min_text_length:
            return (
                f"Text length {len(text)} < minimum "
                f"{self.config.filtering.min_text_length}"
            )
        return None

    def _page_text(self, page: ConfluencePage) -> str:
        parts: List[str] = [page.title] if page.title else []
        if page.storage:
            parts.append(self._storage_to_text(page))
        return "\n\n".join(parts)

    def _storage_to_text(self, page: ConfluencePage) -> str:
        try:
            return storage_to_markdown(page.storage, user_resolver=self.user_resolver)
        except Exception as e:
            self.report.warning(
                title="Page converted as plain text",
                message="Markdown conversion failed, so the page's formatting, "
                "links and mentions were dropped from its text.",
                context=page.id,
                exc=e,
            )
            return strip_html_tags(page.storage)

    def _embed(
        self, page: ConfluencePage, text: str
    ) -> Generator[MetadataWorkUnit, None, bool]:
        """Chunk and embed the page text. Returns False when the page must be
        retried next run rather than counted as processed."""
        document_urn = self.documents.page_urn(page.id)
        elements = [
            {
                "type": "NarrativeText",
                "text": text,
                "metadata": {
                    "page_id": page.id,
                    "title": page.title,
                    "url": self.documents.page_url(page),
                },
            }
        ]
        try:
            yield from self.chunking_source.process_elements_inline(
                document_urn=document_urn,
                elements=elements,
                # Hash the exact text placed on DocumentInfo, so the embeddings'
                # sourceTextSha256 byte-matches the server's resolvedTextSha256.
                source_text_sha256=compute_source_text_sha256(text),
            )
        except SkipMarkerReadError as e:
            # The skip marker was not written, so the page must be retried next
            # run rather than treated like an embedding failure.
            logger.warning(f"Skip marker deferred for {document_urn}: {e}")
            return False
        except Exception as e:
            if (
                isinstance(e, ServerEmbeddingRunError)
                or self.chunking_source.report.num_documents_limit_reached
            ):
                self.report.num_documents_limit_reached = (
                    self.chunking_source.report.num_documents_limit_reached
                )
                raise
            logger.warning(
                f"Failed to generate embeddings for {document_urn}: {e}. "
                "Document will be ingested without embeddings.",
                exc_info=True,
            )
        return True

    @staticmethod
    def _test_semantic_search_capability(
        config: ConfluenceSourceConfig,
        graph: Optional[DataHubGraph] = None,
    ) -> CapabilityReport:
        """Check embeddings the way ingestion resolves them: explicit config,
        else the server's config, else defaults."""
        return DocumentChunkingSource.test_semantic_search_capability(
            config.embedding, datahub=config.datahub, graph=graph
        )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        return run_connection_test(
            config_dict, ConfluenceSource._test_semantic_search_capability
        )

    def get_report(self) -> ConfluenceSourceReport:
        # get_report is called more than once per run, so copy the chunking
        # stats rather than appending to them.
        chunking_report = self.chunking_source.report
        self.report.num_documents_with_embeddings = (
            chunking_report.num_documents_with_embeddings
        )
        self.report.num_embedding_failures = chunking_report.num_embedding_failures
        self.report.embedding_failures = LossyList()
        self.report.embedding_failures.extend(chunking_report.embedding_failures)
        self.report.num_documents_limit_reached = (
            chunking_report.num_documents_limit_reached
        )
        return self.report

    def close(self) -> None:
        try:
            self.chunking_source.close()
        finally:
            super().close()
