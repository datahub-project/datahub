import hashlib
import json
from datetime import datetime
from typing import Dict, List, Optional, Set, Union
from urllib.parse import urlparse

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.ingestion.source.common.subtypes import DocumentSubTypes
from datahub.ingestion.source.confluence.config import (
    ConfluenceSourceConfig,
)
from datahub.ingestion.source.confluence.constants import (
    EXTRACTION_ALGO_VERSION,
)
from datahub.ingestion.source.confluence.hierarchy import (
    PLATFORM,
    FolderNode,
    build_browse_path,
    document_id,
    document_urn,
)
from datahub.ingestion.source.confluence.models import (
    ConfluenceAncestor,
    ConfluencePage,
    ConfluenceSpaceRef,
)
from datahub.ingestion.source.documents.document_import_mode import DocumentImportMode
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    DataPlatformInstanceClass,
    DocumentStateClass,
)
from datahub.sdk.document import Document


def instance_id_for(config: ConfluenceSourceConfig) -> str:
    """The id that keeps document URNs unique across Confluence sites: the
    configured platform instance, else a stable hash of the site URL."""
    if config.platform_instance:
        return config.platform_instance
    return hashlib.sha256(config.url.encode()).hexdigest()[:8]


def content_hash(page: ConfluencePage) -> str:
    """Hash of the raw body and the extraction version. The inline chunking
    step re-embeds a page when this changes, so bumping EXTRACTION_ALGO_VERSION
    re-embeds every page. The separate datahub-documents source hashes the
    extracted text instead, so it only re-embeds pages whose text changed."""
    payload = json.dumps(
        {"body": page.storage, "algo_version": EXTRACTION_ALGO_VERSION},
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class ConfluenceDocumentBuilder:
    """Turns pages and folders into Document entities: URNs, links back to
    Confluence, parent links, browse paths and the platform instance."""

    def __init__(self, config: ConfluenceSourceConfig):
        self.config = config
        self.instance_id = instance_id_for(config)

    def page_urn(self, page_id: str) -> str:
        return document_urn(self.instance_id, page_id)

    def page_url(self, page: ConfluencePage) -> str:
        if page.webui:
            # webui is relative to the site's context path (/wiki on Cloud).
            return (
                page.webui
                if page.webui.startswith("http")
                else f"{self.config.url}{page.webui}"
            )
        return f"{self.config.url}/pages/viewpage.action?pageId={page.id}"

    def folder_url(self, folder: FolderNode) -> str:
        # Folders only exist on Cloud, whose pages live under /wiki. The config
        # validator only adds /wiki for *.atlassian.net, so a site on a custom
        # domain may be configured without it.
        wiki = self.config.url.removesuffix("/wiki") + "/wiki"
        if folder.space.key:
            return f"{wiki}/spaces/{folder.space.key}/folder/{folder.id}"
        return f"{wiki}/folder/{folder.id}"

    def page_document(
        self, page: ConfluencePage, text: str, ingested_ids: Set[str]
    ) -> Document:
        return self._document(
            content_id=page.id,
            title=page.title or f"Page {page.id}",
            text=text,
            url=self.page_url(page),
            parent_id=page.parent_id,
            ingested_ids=ingested_ids,
            custom_properties={
                "space_key": page.space.key or "",
                "page_id": page.id,
                "content_hash": content_hash(page),
                "extraction_algo_version": EXTRACTION_ALGO_VERSION,
            },
            created_time=page.created,
            last_modified_time=page.last_modified,
            extra_aspects=self._context_aspects(
                page.space, page.ancestors, ingested_ids
            ),
        )

    def folder_document(self, folder: FolderNode, ingested_ids: Set[str]) -> Document:
        return self._document(
            content_id=folder.id,
            title=folder.title,
            text="",
            url=self.folder_url(folder),
            parent_id=folder.parent_id,
            ingested_ids=ingested_ids,
            custom_properties={
                "space_key": folder.space.key or "",
                "folder_id": folder.id,
            },
            subtype=DocumentSubTypes.FOLDER,
            extra_aspects=self._context_aspects(
                folder.space, folder.ancestor_prefix, ingested_ids
            ),
        )

    def _parent_urn(
        self, parent_id: Optional[str], ingested_ids: Set[str]
    ) -> Optional[str]:
        # A parent outside this run has no document to link to; such pages,
        # like root pages, go under parent_document_urn when it is set.
        if parent_id and parent_id in ingested_ids:
            return document_urn(self.instance_id, parent_id)
        return self.config.parent_document_urn

    def _document(
        self,
        content_id: str,
        title: str,
        text: str,
        url: str,
        parent_id: Optional[str],
        ingested_ids: Set[str],
        custom_properties: Dict[str, str],
        extra_aspects: List[Union[DataPlatformInstanceClass, BrowsePathsV2Class]],
        created_time: Optional[datetime] = None,
        last_modified_time: Optional[datetime] = None,
        subtype: Optional[str] = None,
    ) -> Document:
        doc_id = document_id(self.instance_id, content_id)
        parent_urn = self._parent_urn(parent_id, ingested_ids)
        if self.config.document_import_mode == DocumentImportMode.NATIVE:
            return Document.create_document(
                id=doc_id,
                title=title,
                text=text,
                status=DocumentStateClass.PUBLISHED,
                subtype=subtype,
                custom_properties=custom_properties,
                parent_document=parent_urn,
                created_time=created_time,
                last_modified_time=last_modified_time,
                extra_aspects=extra_aspects,
            )
        return Document.create_external_document(
            id=doc_id,
            title=title,
            platform=PLATFORM,
            external_url=url,
            external_id=content_id,
            text=text,
            status=DocumentStateClass.PUBLISHED,
            subtype=subtype,
            custom_properties=custom_properties,
            parent_document=parent_urn,
            created_time=created_time,
            last_modified_time=last_modified_time,
            extra_aspects=extra_aspects,
        )

    def _context_aspects(
        self,
        space: ConfluenceSpaceRef,
        ancestors: List[ConfluenceAncestor],
        ingested_ids: Set[str],
    ) -> List[Union[DataPlatformInstanceClass, BrowsePathsV2Class]]:
        # The platform instance lets the UI render the "View in Confluence" link.
        aspects: List[Union[DataPlatformInstanceClass, BrowsePathsV2Class]] = [
            DataPlatformInstanceClass(
                platform=make_data_platform_urn(PLATFORM),
                instance=make_dataplatform_instance_urn(
                    PLATFORM, self._platform_instance()
                ),
            )
        ]
        browse_path = build_browse_path(
            space, ancestors, self.instance_id, ingested_ids
        )
        if browse_path:
            aspects.append(browse_path)
        return aspects

    def _platform_instance(self) -> str:
        # The configured instance, matching the URNs. Without one, the site
        # subdomain, which is what earlier versions always used.
        if self.config.platform_instance:
            return self.config.platform_instance
        netloc = urlparse(self.config.url).netloc
        return netloc.split(".")[0] if netloc else self.config.url
