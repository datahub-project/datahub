import io
import logging
import re
import time
from typing import Iterable, List, Optional

from unstructured.partition.auto import partition  # type: ignore[import]

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_dataset_to_container
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
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
)
from datahub.ingestion.source.data_lake_common.data_lake_utils import ContainerWUCreator
from datahub.ingestion.source.sharepoint.sharepoint_client import (
    SharePointClient,
    SharePointClientError,
    SharePointDrive,
    SharePointItem,
    SharePointPage,
    SharePointSite,
)
from datahub.ingestion.source.sharepoint.sharepoint_config import SharePointSourceConfig
from datahub.ingestion.source.sharepoint.sharepoint_report import SharePointSourceReport
from datahub.ingestion.source.sharepoint.sharepoint_schema import (
    SCHEMA_INFERRABLE_EXTENSIONS,
    infer_schema,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.unstructured.chunking_config import (
    DataHubConnectionConfig,
    DocumentChunkingSourceConfig,
)
from datahub.ingestion.source.unstructured.chunking_source import DocumentChunkingSource
from datahub.ingestion.source.unstructured.document_builder import (
    DocumentEntityBuilder,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DataPlatformInfoClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DocumentStateClass,
    OperationClass,
    OperationTypeClass,
    OtherSchemaClass,
    PlatformTypeClass,
    _Aspect,
)
from datahub.sdk.document import Document

logger = logging.getLogger(__name__)


# File extensions ingested as Document entities in document mode.
_DOCUMENT_FILE_EXTENSIONS = frozenset(
    ["docx", "doc", "pdf", "pptx", "ppt", "txt", "md"]
)

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(html: str) -> str:
    """Return plain text by removing HTML tags and collapsing whitespace."""
    text = _HTML_TAG_RE.sub(" ", html)
    return " ".join(text.split())


@platform_name("SharePoint")
@config_class(SharePointSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.TEST_CONNECTION, "Validates Graph API credentials")
@capability(
    SourceCapability.DELETION_DETECTION, "Enabled by default via stateful ingestion"
)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.CONTAINERS,
    "Site, Document Library, and Folder containers in data_lake mode",
)
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Schema inference for CSV, TSV, JSON, Parquet, Avro, and Excel in data_lake mode",
)
class SharePointSource(StatefulIngestionSourceBase, TestableSource):
    """Ingest SharePoint Online content into DataHub.

    ## Prerequisites

    1. Register an Azure AD application in the Azure Portal.
    2. Grant it the following **application** (not delegated) Microsoft Graph
       API permissions and have an admin grant consent:
       - ``Sites.Read.All``
       - ``Files.Read.All``
       - (For document mode with pages) ``Pages.Read.All``
    3. Note the tenant ID, client ID, and either create a client secret or
       upload a certificate.

    ## Configuration Examples

    ### Data lake mode (CSV/Parquet files as Datasets)

    ```yaml
    source:
      type: sharepoint
      config:
        auth:
          tenant_id: "<tenant-id>"
          client_id: "<app-client-id>"
          client_secret: "<client-secret>"
        site:
          hostname: "myorg.sharepoint.com"
          site_pattern:
            allow: ["/sites/DataEngineering", "/sites/Analytics"]
          library_pattern:
            deny: ["^Site Assets$", "^Style Library$"]
        mode: data_lake
        file_types: [csv, parquet, xlsx]
        env: PROD
    ```

    ### Document mode (Pages and files as Documents)

    ```yaml
    source:
      type: sharepoint
      config:
        auth:
          tenant_id: "<tenant-id>"
          client_id: "<app-client-id>"
          client_secret: "<client-secret>"
        site:
          hostname: "myorg.sharepoint.com"
        mode: document
        ingest_pages: true
        ingest_files: false
        chunking:
          enabled: true
          strategy: by_title
        embedding:
          enabled: true
          provider: openai
          api_key: "${OPENAI_API_KEY}"
        stateful_ingestion:
          enabled: true
          remove_stale_metadata: true
    ```

    ### Both modes (Datasets and Documents in one pipeline)

    Use ``mode: both`` to emit Datasets for structured files and Documents for
    pages/Office files in a single pipeline run.

    ```yaml
    source:
      type: sharepoint
      config:
        auth:
          tenant_id: "<tenant-id>"
          client_id: "<app-client-id>"
          client_secret: "<client-secret>"
        site:
          hostname: "myorg.sharepoint.com"
        mode: both
        file_types: [csv, parquet, xlsx]
    ```
    """

    platform = "sharepoint"

    config: SharePointSourceConfig
    report: SharePointSourceReport

    def __init__(self, config: SharePointSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)  # type: ignore[arg-type]
        self.config = config
        self.report = SharePointSourceReport()
        self.client = SharePointClient(auth=config.auth, hostname=config.site.hostname)

        if config.uses_document():
            self.document_builder = DocumentEntityBuilder(
                config=config.document_mapping,
                source_type="sharepoint",
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
        else:
            self.document_builder = None  # type: ignore[assignment]
            self.chunking_source = None  # type: ignore[assignment]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SharePointSource":
        config = SharePointSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self._emit_platform_info()
        if self.config.uses_data_lake():
            yield from self._get_data_lake_workunits()
        if self.config.uses_document():
            yield from self._get_document_workunits()

    def get_report(self) -> SharePointSourceReport:
        if self.chunking_source is not None:
            self.report.num_documents_with_embeddings = (
                self.chunking_source.report.num_documents_with_embeddings
            )
            self.report.num_embedding_failures = (
                self.chunking_source.report.num_embedding_failures
            )
            self.report.embedding_failures = (
                self.chunking_source.report.embedding_failures  # type: ignore[assignment]
            )
        return self.report

    def _emit_platform_info(self) -> Iterable[MetadataWorkUnit]:
        platform_urn = make_data_platform_urn(self.platform)
        yield MetadataChangeProposalWrapper(
            entityUrn=platform_urn,
            aspect=DataPlatformInfoClass(
                name=self.platform,
                type=PlatformTypeClass.FILE_SYSTEM,
                datasetNameDelimiter="/",
                displayName="SharePoint",
                logoUrl="https://upload.wikimedia.org/wikipedia/commons/e/e1/Microsoft_Office_SharePoint_%282019%E2%80%93present%29.svg",
            ),
        ).as_workunit()

    def _iter_allowed_sites(self) -> Iterable[SharePointSite]:
        for site in self.client.list_sites():
            self.report.report_site_scanned()
            if self.config.site.skip_personal_sites and site.path.lower().startswith(
                "/personal/"
            ):
                logger.debug(
                    f"Site '{site.path}' is a personal OneDrive site; "
                    "skipping (set skip_personal_sites: false to include)."
                )
                continue
            if not self.config.site.site_pattern.allowed(site.path):
                logger.debug(f"Site '{site.path}' excluded by site_pattern; skipping.")
                continue
            yield site

    def _iter_allowed_drives(self, site: SharePointSite) -> Iterable[SharePointDrive]:
        for drive in self.client.list_drives(site.id):
            if not drive.is_document_library:
                continue
            self.report.report_library_scanned()
            if not self.config.site.library_pattern.allowed(drive.name):
                logger.debug(
                    f"Library '{drive.name}' in site '{site.path}' "
                    "excluded by library_pattern; skipping."
                )
                continue
            yield drive

    def _get_data_lake_workunits(self) -> Iterable[MetadataWorkUnit]:
        allowed_extensions = frozenset(self.config.file_types)
        wu_creator = ContainerWUCreator(
            self.platform,
            self.config.platform_instance,
            self.config.env,
        )

        for site in self._iter_allowed_sites():
            try:
                yield from self._iter_site_data_lake(
                    site, allowed_extensions, wu_creator
                )
                self.report.report_site_processed()
            except Exception as exc:
                logger.error(
                    f"Failed to process site '{site.path}': {exc}", exc_info=True
                )
                self.report.report_site_failed(site.path, str(exc))
                if not self.config.advanced.continue_on_failure:
                    raise

    def _iter_site_data_lake(
        self,
        site: SharePointSite,
        allowed_extensions: frozenset,
        wu_creator: ContainerWUCreator,
    ) -> Iterable[MetadataWorkUnit]:
        for drive in self._iter_allowed_drives(site):
            for item in self.client.list_items_recursive(site.id, drive.id, drive.name):
                if self.report.files_processed >= self.config.max_files:
                    logger.info(
                        f"Reached max_files limit ({self.config.max_files}); "
                        "stopping data_lake ingestion."
                    )
                    return

                if item.extension not in allowed_extensions:
                    self.report.report_file_skipped(
                        item.name, f"Extension '{item.extension}' not in file_types"
                    )
                    continue

                self.report.report_file_scanned(item.size)
                try:
                    yield from self._ingest_file(site, drive, item, wu_creator)
                    self.report.report_file_processed()
                except Exception as exc:
                    path = item.full_path(drive.name)
                    logger.error(
                        f"Failed to ingest file '{path}': {exc}", exc_info=True
                    )
                    self.report.report_file_failed(path, str(exc))
                    if not self.config.advanced.continue_on_failure:
                        raise
            self.report.report_library_processed()

    def _ingest_file(
        self,
        site: SharePointSite,
        drive: SharePointDrive,
        item: SharePointItem,
        wu_creator: ContainerWUCreator,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit Dataset MCPs for a single SharePoint file."""
        logical_path = self._build_dataset_path(site, drive, item)
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=logical_path,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        aspects: List[Optional[_Aspect]] = []

        if self.config.platform_instance:
            aspects.append(
                DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                )
            )

        aspects.append(
            DatasetPropertiesClass(
                name=item.name,
                description="",
                externalUrl=item.web_url,
                customProperties={
                    "site": site.display_name,
                    "library": drive.name,
                    "size_bytes": str(item.size),
                    "mime_type": item.mime_type or "",
                    "last_modified": item.last_modified.isoformat(),
                    "created": item.created.isoformat(),
                },
            )
        )

        if (
            item.size > 0
            and item.download_url
            and item.extension.lower() in SCHEMA_INFERRABLE_EXTENSIONS
        ):
            schema = self._try_infer_schema(item)
            if schema is not None:
                data_platform_urn = make_data_platform_urn(self.platform)
                aspects.append(
                    SchemaMetadata(
                        schemaName=item.name,
                        platform=data_platform_urn,
                        version=0,
                        hash="",
                        fields=schema,
                        platformSchema=OtherSchemaClass(rawSchema=""),
                    )
                )
            else:
                self.report.files_without_schema += 1
        elif item.size == 0:
            logger.debug(f"Skipping schema inference for empty file '{item.name}'.")

        aspects.append(
            OperationClass(
                timestampMillis=int(time.time() * 1000),
                lastUpdatedTimestamp=int(item.last_modified.timestamp() * 1000),
                operationType=OperationTypeClass.UPDATE,
            )
        )

        for mcp in MetadataChangeProposalWrapper.construct_many(
            entityUrn=dataset_urn,
            aspects=aspects,
        ):
            yield mcp.as_workunit()

        yield from self._emit_container_hierarchy(
            site, drive, item, dataset_urn, wu_creator
        )

    def _try_infer_schema(self, item: SharePointItem) -> Optional[list]:
        try:
            assert item.download_url is not None
            file_bytes = self.client.download_file_bytes(item.download_url)
            return infer_schema(item, file_bytes, self.config.max_rows)
        except Exception as exc:
            logger.warning(
                f"Schema inference failed for '{item.name}': {exc}", exc_info=True
            )
            return None

    def _build_dataset_path(
        self,
        site: SharePointSite,
        drive: SharePointDrive,
        item: SharePointItem,
    ) -> str:
        parts = [
            self.config.site.hostname,
            site.path.strip("/"),
            drive.name,
        ]
        if item.parent_path:
            parts.append(item.parent_path.strip("/"))
        parts.append(item.name)
        return "/".join(p for p in parts if p)

    def _emit_container_hierarchy(
        self,
        site: SharePointSite,
        drive: SharePointDrive,
        item: SharePointItem,
        dataset_urn: str,
        wu_creator: ContainerWUCreator,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit deduplicated container entities for Site → Library → Folder… → Dataset."""
        site_abs_path = f"{self.config.site.hostname}{site.path}"
        site_key = wu_creator.gen_folder_key(site_abs_path)
        yield from wu_creator.create_emit_containers(
            container_key=site_key,
            name=site.display_name or site.name,
            sub_types=[DatasetContainerSubTypes.SHAREPOINT_SITE],
        )

        library_abs_path = f"{site_abs_path}/{drive.name}"
        library_key = wu_creator.gen_folder_key(library_abs_path)
        yield from wu_creator.create_emit_containers(
            container_key=library_key,
            name=drive.name,
            sub_types=[DatasetContainerSubTypes.SHAREPOINT_DOCUMENT_LIBRARY],
            parent_container_key=site_key,
        )

        parent_key = library_key
        if item.parent_path:
            cumulative = library_abs_path
            for segment in item.parent_path.strip("/").split("/"):
                if not segment:
                    continue
                cumulative = f"{cumulative}/{segment}"
                folder_key = wu_creator.gen_folder_key(cumulative)
                yield from wu_creator.create_emit_containers(
                    container_key=folder_key,
                    name=segment,
                    sub_types=[DatasetContainerSubTypes.FOLDER],
                    parent_container_key=parent_key,
                )
                parent_key = folder_key

        yield from add_dataset_to_container(parent_key, dataset_urn)

    def _get_document_workunits(self) -> Iterable[MetadataWorkUnit]:
        for site in self._iter_allowed_sites():
            try:
                if self.config.ingest_pages:
                    yield from self._iter_site_pages(site)
                if self.config.ingest_files:
                    yield from self._iter_site_doc_files(site)
                self.report.report_site_processed()
            except RuntimeError:
                raise
            except Exception as exc:
                logger.error(
                    f"Failed to process site '{site.path}': {exc}", exc_info=True
                )
                self.report.report_site_failed(site.path, str(exc))
                if not self.config.advanced.continue_on_failure:
                    raise

    def _check_document_limit(self) -> None:
        limit = self.config.max_documents
        docs_so_far = self.report.pages_processed + self.report.doc_files_processed
        if limit > 0 and docs_so_far >= limit:
            self.report.num_documents_limit_reached = True
            raise RuntimeError(
                f"Document limit of {limit} reached. "
                "Increase max_documents or set it to -1 to disable."
            )

    def _iter_site_pages(self, site: SharePointSite) -> Iterable[MetadataWorkUnit]:
        for page in self.client.list_pages(site.id):
            self._check_document_limit()
            self.report.report_page_scanned()
            try:
                yield from self._ingest_page(site, page)
                self.report.report_page_processed()
            except Exception as exc:
                logger.error(
                    f"Failed to ingest page '{page.id}' in site '{site.path}': {exc}",
                    exc_info=True,
                )
                self.report.report_page_failed(page.id, site.path, str(exc))
                if not self.config.advanced.continue_on_failure:
                    raise

    def _ingest_page(
        self, site: SharePointSite, page: SharePointPage
    ) -> Iterable[MetadataWorkUnit]:
        html = self.client.get_page_html(site.id, page.id)
        text = _strip_html(html) if html else page.title
        self.report.report_text_extracted(len(text))

        instance_id = self.config.platform_instance or self.config.site.hostname
        doc_id = f"sharepoint-{instance_id}-{page.id}"
        document_urn = f"urn:li:document:{doc_id}"

        doc = Document.create_external_document(
            id=doc_id,
            title=page.title,
            platform=self.platform,
            external_url=page.web_url,
            external_id=page.id,
            text=text,
            status=DocumentStateClass.PUBLISHED,
            custom_properties={
                "site": site.display_name or site.name,
                "site_path": site.path,
                "content_type": "page",
            },
            created_time=page.created,
            last_modified_time=page.last_modified,
        )

        if self.config.platform_instance:
            doc._set_aspect(
                DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                )
            )

        doc._set_aspect(self._build_page_browse_path(site))

        for wu in doc.as_workunits():
            yield wu

        yield from self._emit_page_embeddings(document_urn, page.title, text)

    def _build_page_browse_path(self, site: SharePointSite) -> BrowsePathsV2Class:
        return BrowsePathsV2Class(
            path=[
                BrowsePathEntryClass(id=self.config.site.hostname),
                BrowsePathEntryClass(id=site.path.strip("/") or site.display_name),
                BrowsePathEntryClass(id="Pages"),
            ]
        )

    def _emit_page_embeddings(
        self, document_urn: str, title: str, text: str
    ) -> Iterable[MetadataWorkUnit]:
        if self.chunking_source is None:
            return
        elements = [
            {
                "type": "NarrativeText",
                "text": text,
                "metadata": {"title": title},
            }
        ]
        try:
            yield from self.chunking_source.process_elements_inline(
                document_urn=document_urn,
                elements=elements,
            )
        except RuntimeError as exc:
            if self.chunking_source.report.num_documents_limit_reached:
                self.report.num_documents_limit_reached = True
                raise
            logger.warning(
                f"Embedding generation failed for '{document_urn}': {exc}. "
                "Document will be ingested without embeddings.",
                exc_info=True,
            )
        except Exception as exc:
            logger.warning(
                f"Embedding generation failed for '{document_urn}': {exc}. "
                "Document will be ingested without embeddings.",
                exc_info=True,
            )

    def _iter_site_doc_files(self, site: SharePointSite) -> Iterable[MetadataWorkUnit]:
        for drive in self._iter_allowed_drives(site):
            for item in self.client.list_items_recursive(site.id, drive.id, drive.name):
                self._check_document_limit()

                if item.extension not in _DOCUMENT_FILE_EXTENSIONS:
                    continue
                if not item.download_url:
                    continue

                self.report.doc_files_scanned += 1
                try:
                    yield from self._ingest_doc_file(site, drive, item, partition)
                    self.report.doc_files_processed += 1
                except Exception as exc:
                    path = item.full_path(drive.name)
                    logger.error(
                        f"Failed to ingest doc file '{path}': {exc}", exc_info=True
                    )
                    self.report.doc_files_failed += 1
                    if not self.config.advanced.continue_on_failure:
                        raise

    def _ingest_doc_file(
        self,
        site: SharePointSite,
        drive: SharePointDrive,
        item: SharePointItem,
        partition_fn: object,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit Document MCPs for a single Office file via unstructured."""
        file_bytes = self.client.download_file_bytes(item.download_url)  # type: ignore[arg-type]
        file_obj = io.BytesIO(file_bytes)

        try:
            elements = partition_fn(file=file_obj, content_type=item.mime_type)  # type: ignore[operator]
        except Exception as exc:
            logger.warning(
                f"Unstructured partitioning failed for '{item.name}': {exc}",
                exc_info=True,
            )
            return

        element_dicts = [
            {"type": type(e).__name__, "text": str(e), "metadata": {}} for e in elements
        ]

        instance_id = self.config.platform_instance or self.config.site.hostname
        doc_id = f"sharepoint-{instance_id}-file-{item.id}"
        document_urn = f"urn:li:document:{doc_id}"

        doc = self.document_builder.build_document_entity(
            elements=element_dicts,
            metadata={
                "url": item.web_url,
                "record_locator": {"item_id": item.id},
            },
            custom_properties={
                "site": site.display_name or site.name,
                "library": drive.name,
                "file_name": item.name,
                "content_type": "file",
            },
            parent_urn=None,
            related_assets=None,
            created_time=item.created,
            last_modified_time=item.last_modified,
        )

        if self.config.platform_instance:
            doc._set_aspect(
                DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                )
            )

        for wu in doc.as_workunits():
            yield wu

        text = " ".join(str(e.get("text", "")) for e in element_dicts)
        self.report.report_text_extracted(len(text))
        yield from self._emit_page_embeddings(document_urn, item.name, text)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Validate SharePoint Graph API credentials."""
        try:
            config = SharePointSourceConfig.model_validate(config_dict)
        except Exception as exc:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Failed to parse config: {exc}",
            )

        client = SharePointClient(auth=config.auth, hostname=config.site.hostname)

        try:
            client.test_connectivity()
            basic_connectivity = CapabilityReport(capable=True)
        except SharePointClientError as exc:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False,
                    failure_reason=str(exc),
                    mitigation_message=(
                        "Verify tenant_id, client_id, and credentials are correct. "
                        "Ensure the app has Sites.Read.All and Files.Read.All "
                        "Microsoft Graph API permissions with admin consent."
                    ),
                )
            )
        except Exception as exc:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False,
                    failure_reason=f"Unexpected error: {exc}",
                    mitigation_message=(
                        "Check that the 'msal' Python package is installed and "
                        "network connectivity to graph.microsoft.com is available."
                    ),
                )
            )

        capability_report: dict = {}

        try:
            sites = list(client.list_sites())
            capability_report["Site Discovery"] = CapabilityReport(
                capable=True,
                failure_reason=f"Found {len(sites)} site(s) matching hostname.",
            )
        except SharePointClientError as exc:
            capability_report["Site Discovery"] = CapabilityReport(
                capable=False,
                failure_reason=str(exc),
                mitigation_message="Verify Sites.Read.All permission is granted.",
            )

        return TestConnectionReport(
            basic_connectivity=basic_connectivity,
            capability_report=capability_report,
        )
