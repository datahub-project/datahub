"""Unit tests for Notion hierarchy extraction."""

from datahub.ingestion.source.notion.notion_hierarchy import NotionHierarchyExtractor
from datahub.metadata.schema_classes import BrowsePathsV2Class


def _urn(page_id: str) -> str:
    return f"urn:li:document:notion-{page_id}"


def _title(page_id: str) -> str:
    titles = {
        "root": "Engineering",
        "mid": "Backend",
        "parent": "API",
    }
    return titles.get(page_id, page_id)


def test_extract_parent_id_page_parent() -> None:
    metadata = {"parent": {"type": "page_id", "page_id": "parent-1"}}
    assert NotionHierarchyExtractor.extract_parent_id(metadata) == "parent-1"


def test_extract_parent_id_database_parent() -> None:
    metadata = {"parent": {"type": "database_id", "database_id": "db-1"}}
    assert NotionHierarchyExtractor.extract_parent_id(metadata) == "db-1"


def test_extract_parent_id_workspace_is_root() -> None:
    metadata = {"parent": {"type": "workspace", "workspace": True}}
    assert NotionHierarchyExtractor.extract_parent_id(metadata) is None


def test_extract_parent_id_missing_or_invalid() -> None:
    assert NotionHierarchyExtractor.extract_parent_id(None) is None
    assert NotionHierarchyExtractor.extract_parent_id({}) is None
    assert NotionHierarchyExtractor.extract_parent_id({"parent": "nope"}) is None


def test_extract_ancestor_chain_multi_level() -> None:
    parent_metadata = {
        "child": {"parent": {"type": "page_id", "page_id": "parent"}},
        "parent": {"parent": {"type": "page_id", "page_id": "mid"}},
        "mid": {"parent": {"type": "page_id", "page_id": "root"}},
        "root": {"parent": {"type": "workspace", "workspace": True}},
    }

    chain = NotionHierarchyExtractor.extract_ancestor_chain("child", parent_metadata)
    # Root first, immediate parent last; the page itself is excluded.
    assert chain == ["root", "mid", "parent"]


def test_extract_ancestor_chain_root_page() -> None:
    parent_metadata = {
        "root": {"parent": {"type": "workspace", "workspace": True}},
    }
    assert (
        NotionHierarchyExtractor.extract_ancestor_chain("root", parent_metadata) == []
    )


def test_extract_ancestor_chain_stops_on_missing_parent() -> None:
    # "mid" is referenced as a parent but absent from the map.
    parent_metadata = {
        "child": {"parent": {"type": "page_id", "page_id": "mid"}},
    }
    chain = NotionHierarchyExtractor.extract_ancestor_chain("child", parent_metadata)
    assert chain == ["mid"]


def test_extract_ancestor_chain_is_cycle_safe() -> None:
    # Malformed data with a parent cycle should not loop forever.
    parent_metadata = {
        "a": {"parent": {"type": "page_id", "page_id": "b"}},
        "b": {"parent": {"type": "page_id", "page_id": "a"}},
    }
    chain = NotionHierarchyExtractor.extract_ancestor_chain("a", parent_metadata)
    assert chain == ["b"]


def test_build_browse_path_v2_root_page_returns_none() -> None:
    parent_metadata = {
        "root": {"parent": {"type": "workspace", "workspace": True}},
    }
    browse_path = NotionHierarchyExtractor.build_browse_path_v2(
        page_id="root",
        parent_metadata=parent_metadata,
        urn_builder=_urn,
        title_resolver=_title,
        ingested_page_ids={"root"},
    )
    assert browse_path is None


def test_build_browse_path_v2_with_ancestors() -> None:
    parent_metadata = {
        "child": {"parent": {"type": "page_id", "page_id": "parent"}},
        "parent": {"parent": {"type": "page_id", "page_id": "mid"}},
        "mid": {"parent": {"type": "page_id", "page_id": "root"}},
        "root": {"parent": {"type": "workspace", "workspace": True}},
    }
    ingested = {"child", "parent", "mid", "root"}

    browse_path = NotionHierarchyExtractor.build_browse_path_v2(
        page_id="child",
        parent_metadata=parent_metadata,
        urn_builder=_urn,
        title_resolver=_title,
        ingested_page_ids=ingested,
    )

    assert browse_path is not None
    assert isinstance(browse_path, BrowsePathsV2Class)
    assert [e.id for e in browse_path.path] == ["Engineering", "Backend", "API"]
    assert [e.urn for e in browse_path.path] == [
        _urn("root"),
        _urn("mid"),
        _urn("parent"),
    ]


def test_build_browse_path_v2_ancestor_not_ingested_is_omitted() -> None:
    parent_metadata = {
        "child": {"parent": {"type": "page_id", "page_id": "parent"}},
        "parent": {"parent": {"type": "page_id", "page_id": "root"}},
        "root": {"parent": {"type": "workspace", "workspace": True}},
    }
    # Only the child is in scope; ancestors outside the run are not in the path.
    browse_path = NotionHierarchyExtractor.build_browse_path_v2(
        page_id="child",
        parent_metadata=parent_metadata,
        urn_builder=_urn,
        title_resolver=_title,
        ingested_page_ids={"child"},
    )

    assert browse_path is None


def test_build_browse_path_v2_empty_page_id_returns_none() -> None:
    """Missing page id should yield no browse path rather than raising."""
    assert (
        NotionHierarchyExtractor.build_browse_path_v2(
            page_id="",
            parent_metadata={},
            urn_builder=_urn,
            title_resolver=_title,
        )
        is None
    )


def test_build_browse_path_v2_no_metadata_map_returns_none() -> None:
    """A page absent from the metadata map has no ancestors -> no browse path."""
    assert (
        NotionHierarchyExtractor.build_browse_path_v2(
            page_id="orphan",
            parent_metadata={},
            urn_builder=_urn,
            title_resolver=_title,
        )
        is None
    )


def test_build_browse_path_v2_without_ingested_set_includes_all_ancestors() -> None:
    """When the ingested set is omitted, all resolved ancestors are included."""
    parent_metadata = {
        "child": {"parent": {"type": "page_id", "page_id": "root"}},
        "root": {"parent": {"type": "workspace", "workspace": True}},
    }
    browse_path = NotionHierarchyExtractor.build_browse_path_v2(
        page_id="child",
        parent_metadata=parent_metadata,
        urn_builder=_urn,
        title_resolver=_title,
    )

    assert browse_path is not None
    assert [e.id for e in browse_path.path] == ["Engineering"]
    assert browse_path.path[0].urn == _urn("root")


def test_build_browse_path_v2_explicit_root_has_no_ancestors() -> None:
    """A page explicitly scoped via page_ids is a browse-path root."""
    parent_metadata = {
        "scoped-page": {"parent": {"type": "page_id", "page_id": "outside-parent"}},
        "outside-parent": {"parent": {"type": "workspace", "workspace": True}},
    }
    browse_path = NotionHierarchyExtractor.build_browse_path_v2(
        page_id="scoped-page",
        parent_metadata=parent_metadata,
        urn_builder=_urn,
        title_resolver=_title,
        ingested_page_ids={"scoped-page"},
        browse_path_root_ids={"scoped-page"},
    )

    assert browse_path is None


def test_build_browse_path_v2_explicit_root_anchors_descendants() -> None:
    parent_metadata = {
        "child": {"parent": {"type": "page_id", "page_id": "scoped-page"}},
        "scoped-page": {"parent": {"type": "page_id", "page_id": "outside-parent"}},
        "outside-parent": {"parent": {"type": "workspace", "workspace": True}},
    }
    browse_path = NotionHierarchyExtractor.build_browse_path_v2(
        page_id="child",
        parent_metadata=parent_metadata,
        urn_builder=_urn,
        title_resolver=_title,
        ingested_page_ids={"scoped-page", "child"},
        browse_path_root_ids={"scoped-page"},
    )

    assert browse_path is not None
    assert [e.id for e in browse_path.path] == ["scoped-page"]
    assert browse_path.path[0].urn == _urn("scoped-page")
