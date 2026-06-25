"""Unit tests for Confluence hierarchy extraction."""

from datahub.ingestion.source.confluence.confluence_hierarchy import (
    ConfluenceHierarchyExtractor,
)
from datahub.metadata.schema_classes import BrowsePathsV2Class


def test_extract_parent_id_with_parent() -> None:
    """Test extracting parent ID when page has ancestors."""
    page_metadata = {
        "id": "12345",
        "ancestors": [
            {"id": "11111", "title": "Grandparent"},
            {"id": "22222", "title": "Parent"},
        ],
    }

    parent_id = ConfluenceHierarchyExtractor.extract_parent_id(page_metadata)
    assert parent_id == "22222"


def test_extract_parent_id_no_parent() -> None:
    """Test extracting parent ID when page has no ancestors (root page)."""
    page_metadata = {"id": "12345", "ancestors": []}

    parent_id = ConfluenceHierarchyExtractor.extract_parent_id(page_metadata)
    assert parent_id is None


def test_extract_parent_id_missing_ancestors() -> None:
    """Test extracting parent ID when ancestors field is missing."""
    page_metadata = {
        "id": "12345",
    }

    parent_id = ConfluenceHierarchyExtractor.extract_parent_id(page_metadata)
    assert parent_id is None


def test_extract_parent_id_invalid_ancestors() -> None:
    """Test extracting parent ID when ancestors is not a list."""
    page_metadata = {"id": "12345", "ancestors": "invalid"}

    parent_id = ConfluenceHierarchyExtractor.extract_parent_id(page_metadata)
    assert parent_id is None


def test_extract_parent_id_empty_metadata() -> None:
    """Test extracting parent ID from empty metadata."""
    parent_id = ConfluenceHierarchyExtractor.extract_parent_id({})
    assert parent_id is None


def test_extract_parent_id_none_metadata() -> None:
    """Test extracting parent ID from None metadata."""
    parent_id = ConfluenceHierarchyExtractor.extract_parent_id(None)
    assert parent_id is None


def test_build_parent_urn_default_platform() -> None:
    """Test building parent URN with default platform."""
    urn = ConfluenceHierarchyExtractor.build_parent_urn("12345")
    assert urn == "urn:li:document:confluence-12345"


def test_build_parent_urn_custom_platform() -> None:
    """Test building parent URN with custom platform."""
    urn = ConfluenceHierarchyExtractor.build_parent_urn("12345", platform="custom")
    assert urn == "urn:li:document:custom-12345"


def test_extract_space_key_nested() -> None:
    """Test extracting space key from nested structure."""
    page_metadata = {"id": "12345", "space": {"key": "TEAM", "name": "Team Space"}}

    space_key = ConfluenceHierarchyExtractor.extract_space_key(page_metadata)
    assert space_key == "TEAM"


def test_extract_space_key_top_level() -> None:
    """Test extracting space key from top-level field."""
    page_metadata = {"id": "12345", "spaceKey": "DOCS"}

    space_key = ConfluenceHierarchyExtractor.extract_space_key(page_metadata)
    assert space_key == "DOCS"


def test_extract_space_key_missing() -> None:
    """Test extracting space key when not present."""
    page_metadata = {
        "id": "12345",
    }

    space_key = ConfluenceHierarchyExtractor.extract_space_key(page_metadata)
    assert space_key is None


def test_extract_page_title() -> None:
    """Test extracting page title."""
    page_metadata = {"id": "12345", "title": "Test Page Title"}

    title = ConfluenceHierarchyExtractor.extract_page_title(page_metadata)
    assert title == "Test Page Title"


def test_extract_page_title_missing() -> None:
    """Test extracting page title when not present."""
    page_metadata = {
        "id": "12345",
    }

    title = ConfluenceHierarchyExtractor.extract_page_title(page_metadata)
    assert title is None


def test_extract_page_url_from_links() -> None:
    """Test extracting page URL from _links.webui."""
    page_metadata = {
        "id": "12345",
        "_links": {"webui": "/wiki/spaces/TEAM/pages/12345/Test+Page"},
    }

    url = ConfluenceHierarchyExtractor.extract_page_url(page_metadata)
    assert url == "/wiki/spaces/TEAM/pages/12345/Test+Page"


def test_extract_page_url_direct() -> None:
    """Test extracting page URL from direct url field."""
    page_metadata = {
        "id": "12345",
        "url": "https://test.atlassian.net/wiki/spaces/TEAM/pages/12345",
    }

    url = ConfluenceHierarchyExtractor.extract_page_url(page_metadata)
    assert url == "https://test.atlassian.net/wiki/spaces/TEAM/pages/12345"


def test_extract_page_url_missing() -> None:
    """Test extracting page URL when not present."""
    page_metadata = {
        "id": "12345",
    }

    url = ConfluenceHierarchyExtractor.extract_page_url(page_metadata)
    assert url is None


def test_extract_space_name_nested() -> None:
    """Test extracting space name from nested structure."""
    page_metadata = {"id": "12345", "space": {"key": "TEAM", "name": "Team Space"}}

    space_name = ConfluenceHierarchyExtractor.extract_space_name(page_metadata)
    assert space_name == "Team Space"


def test_extract_space_name_missing() -> None:
    """Test extracting space name when not present."""
    page_metadata = {"id": "12345", "space": {"key": "TEAM"}}

    space_name = ConfluenceHierarchyExtractor.extract_space_name(page_metadata)
    assert space_name is None


def test_extract_ancestors_multi_level() -> None:
    """Test extracting 3-level ancestor chain."""
    page_metadata = {
        "id": "12345",
        "ancestors": [
            {"id": "11111", "title": "Root Page"},
            {"id": "22222", "title": "Middle Page"},
            {"id": "33333", "title": "Parent Page"},
        ],
    }

    ancestors = ConfluenceHierarchyExtractor.extract_ancestors(page_metadata)
    assert len(ancestors) == 3
    assert ancestors[0]["id"] == "11111"
    assert ancestors[0]["title"] == "Root Page"
    assert ancestors[1]["id"] == "22222"
    assert ancestors[1]["title"] == "Middle Page"
    assert ancestors[2]["id"] == "33333"
    assert ancestors[2]["title"] == "Parent Page"


def test_extract_ancestors_empty() -> None:
    """Test extracting ancestors when array is empty."""
    page_metadata = {"id": "12345", "ancestors": []}

    ancestors = ConfluenceHierarchyExtractor.extract_ancestors(page_metadata)
    assert ancestors == []


def test_build_browse_path_v2_root_page() -> None:
    """Test building browse path for root page (space only, no ancestors)."""
    page_metadata = {
        "id": "12345",
        "space": {"key": "TEAM", "name": "Team Space"},
        "ancestors": [],
    }

    browse_path = ConfluenceHierarchyExtractor.build_browse_path_v2(
        page_metadata=page_metadata, platform="confluence", instance_id="test-instance"
    )

    assert browse_path is not None
    assert isinstance(browse_path, BrowsePathsV2Class)
    assert len(browse_path.path) == 1
    assert browse_path.path[0].id == "Team Space"
    assert browse_path.path[0].urn is None


def test_build_browse_path_v2_with_ancestors() -> None:
    """Test building browse path with multi-level hierarchy and URNs."""
    page_metadata = {
        "id": "12345",
        "space": {"key": "TEAM", "name": "Team Space"},
        "ancestors": [
            {"id": "11111", "title": "API Documentation"},
            {"id": "22222", "title": "REST API"},
        ],
    }

    ingested_page_ids = {"11111", "22222", "12345"}

    browse_path = ConfluenceHierarchyExtractor.build_browse_path_v2(
        page_metadata=page_metadata,
        platform="confluence",
        instance_id="test-instance",
        ingested_page_ids=ingested_page_ids,
    )

    assert browse_path is not None
    assert len(browse_path.path) == 3

    # Space entry (no URN)
    assert browse_path.path[0].id == "Team Space"
    assert browse_path.path[0].urn is None

    # First ancestor (with URN)
    assert browse_path.path[1].id == "API Documentation"
    assert browse_path.path[1].urn == "urn:li:document:confluence-test-instance-11111"

    # Second ancestor (with URN)
    assert browse_path.path[2].id == "REST API"
    assert browse_path.path[2].urn == "urn:li:document:confluence-test-instance-22222"


def test_build_browse_path_v2_ancestor_not_ingested() -> None:
    """Test building browse path when ancestor is not being ingested (no URN)."""
    page_metadata = {
        "id": "12345",
        "space": {"key": "TEAM", "name": "Team Space"},
        "ancestors": [
            {"id": "11111", "title": "API Documentation"},
            {"id": "22222", "title": "REST API"},
        ],
    }

    # Only the current page is being ingested, ancestors are not
    ingested_page_ids = {"12345"}

    browse_path = ConfluenceHierarchyExtractor.build_browse_path_v2(
        page_metadata=page_metadata,
        platform="confluence",
        instance_id="test-instance",
        ingested_page_ids=ingested_page_ids,
    )

    assert browse_path is not None
    assert len(browse_path.path) == 3

    # Space entry (no URN)
    assert browse_path.path[0].id == "Team Space"
    assert browse_path.path[0].urn is None

    # First ancestor (no URN - not ingested)
    assert browse_path.path[1].id == "API Documentation"
    assert browse_path.path[1].urn is None

    # Second ancestor (no URN - not ingested)
    assert browse_path.path[2].id == "REST API"
    assert browse_path.path[2].urn is None


def test_build_browse_path_v2_missing_space() -> None:
    """Test building browse path returns None when space is missing."""
    page_metadata = {
        "id": "12345",
        "ancestors": [{"id": "11111", "title": "Parent Page"}],
    }

    browse_path = ConfluenceHierarchyExtractor.build_browse_path_v2(
        page_metadata=page_metadata, platform="confluence", instance_id="test-instance"
    )

    assert browse_path is None


def test_build_browse_path_v2_fallback_to_space_key() -> None:
    """Test building browse path uses space key when name is unavailable."""
    page_metadata = {
        "id": "12345",
        "space": {"key": "TEAM"},
        "ancestors": [{"id": "11111", "title": "Parent Page"}],
    }

    browse_path = ConfluenceHierarchyExtractor.build_browse_path_v2(
        page_metadata=page_metadata, platform="confluence", instance_id="test-instance"
    )

    assert browse_path is not None
    assert len(browse_path.path) == 2
    # Should use space key as fallback
    assert browse_path.path[0].id == "TEAM"
    assert browse_path.path[0].urn is None


def test_collect_folder_nodes_basic() -> None:
    """A page nested under a folder yields a FolderNode whose parent is the
    page above the folder in the ancestry."""
    pages = [
        {
            "id": "67043329",
            "title": "Doc inside a folder",
            "space": {"key": "FB", "name": "Fiction Bank"},
            "ancestors": [
                {"id": "16351389", "title": "Fiction Bank", "type": "page"},
                {"id": "67010561", "title": "Nick demo", "type": "folder"},
            ],
        }
    ]

    folders = ConfluenceHierarchyExtractor.collect_folder_nodes(pages)

    assert set(folders.keys()) == {"67010561"}
    node = folders["67010561"]
    assert node.title == "Nick demo"
    assert node.parent_id == "16351389"
    assert node.space_key == "FB"
    assert node.space_name == "Fiction Bank"
    # ancestor_prefix is everything before the folder (used for its browse path)
    assert [a["id"] for a in node.ancestor_prefix] == ["16351389"]


def test_collect_folder_nodes_nested_folders() -> None:
    """Folder-under-folder chains are reconstructed from a single page's
    ancestry, each folder pointing at the entry immediately above it."""
    pages = [
        {
            "id": "900",
            "ancestors": [
                {"id": "100", "title": "Home", "type": "page"},
                {"id": "200", "title": "Outer", "type": "folder"},
                {"id": "300", "title": "Inner", "type": "folder"},
            ],
        }
    ]

    folders = ConfluenceHierarchyExtractor.collect_folder_nodes(pages)

    assert folders["200"].parent_id == "100"
    assert folders["300"].parent_id == "200"


def test_collect_folder_nodes_folder_directly_under_space() -> None:
    """A folder that is the first ancestor has no parent (it is a root)."""
    pages = [
        {
            "id": "900",
            "ancestors": [
                {"id": "200", "title": "Top Folder", "type": "folder"},
            ],
        }
    ]

    folders = ConfluenceHierarchyExtractor.collect_folder_nodes(pages)

    assert folders["200"].parent_id is None


def test_collect_folder_nodes_ignores_pages_and_dedupes() -> None:
    """Page ancestors are ignored, and a folder shared by multiple pages is
    only emitted once."""
    pages = [
        {
            "id": "1",
            "ancestors": [{"id": "200", "title": "Shared", "type": "folder"}],
        },
        {
            "id": "2",
            "ancestors": [{"id": "200", "title": "Shared", "type": "folder"}],
        },
        {
            "id": "3",
            "ancestors": [{"id": "50", "title": "Just a page", "type": "page"}],
        },
    ]

    folders = ConfluenceHierarchyExtractor.collect_folder_nodes(pages)

    assert set(folders.keys()) == {"200"}
