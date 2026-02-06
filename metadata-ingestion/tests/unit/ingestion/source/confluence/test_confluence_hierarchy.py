"""Unit tests for Confluence hierarchy extraction."""

from datahub.ingestion.source.confluence.confluence_hierarchy import (
    ConfluenceHierarchyExtractor,
)


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
