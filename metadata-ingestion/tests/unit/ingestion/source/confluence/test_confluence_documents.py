from typing import Any, Dict, Set

import pytest

from datahub.ingestion.source.confluence.documents import (
    ConfluenceDocumentBuilder,
)
from datahub.ingestion.source.confluence.hierarchy import FolderNode
from datahub.ingestion.source.confluence.models import (
    ConfluencePage,
    ConfluenceSpaceRef,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DocumentInfoClass,
)
from datahub.sdk.document import Document
from tests.unit.conftest import aspects_by_urn, aspects_of_type
from tests.unit.ingestion.source.confluence.conftest import make_config, make_page

PARENT_URN = "urn:li:document:handbook"


def _aspects(doc: Document) -> list:
    (aspects,) = aspects_by_urn(doc.as_workunits()).values()
    return aspects


def _info(
    builder: ConfluenceDocumentBuilder, page: Dict[str, Any], ingested: Set[str]
) -> DocumentInfoClass:
    doc = builder.page_document(ConfluencePage.model_validate(page), "text", ingested)
    (info,) = aspects_of_type(_aspects(doc), DocumentInfoClass)
    return info


def test_parent_document_urn_holds_pages_without_an_ingested_parent() -> None:
    builder = ConfluenceDocumentBuilder(make_config(parent_document_urn=PARENT_URN))
    root = make_page("1")
    orphan = make_page("2", ancestors=[{"id": "99", "title": "Not ingested"}])
    child = make_page("3", ancestors=[{"id": "1", "title": "Page 1"}])
    ingested = {"1", "2", "3"}

    parents = [
        _info(builder, page, ingested).parentDocument for page in (root, orphan, child)
    ]

    assert [p.document if p else None for p in parents] == [
        PARENT_URN,
        PARENT_URN,
        builder.page_urn("1"),
    ]


def test_platform_instance_aspect_matches_the_configured_instance() -> None:
    builder = ConfluenceDocumentBuilder(make_config(platform_instance="prod-wiki"))
    doc = builder.page_document(
        ConfluencePage.model_validate(make_page("1")), "t", {"1"}
    )

    (instance,) = aspects_of_type(_aspects(doc), DataPlatformInstanceClass)

    assert instance.instance is not None
    assert instance.instance.endswith(",prod-wiki)")
    assert builder.page_urn("1") == "urn:li:document:confluence-prod-wiki-1"


@pytest.mark.parametrize(
    "url",
    [
        "https://acme.atlassian.net/wiki",
        "https://wiki.acme.com",
        "https://wiki.acme.com/wiki",
    ],
)
def test_folder_links_point_under_wiki(url: str) -> None:
    # Only *.atlassian.net gets /wiki added by the config, but Cloud folders
    # always live under it.
    builder = ConfluenceDocumentBuilder(make_config(url=url))
    folder = FolderNode(
        id="f1", title="Area", parent_id=None, space=ConfluenceSpaceRef(key="TEAM")
    )

    assert builder.folder_url(folder) == (
        f"{url.removesuffix('/wiki')}/wiki/spaces/TEAM/folder/f1"
    )
