"""Comprehensive tests for Confluence allow/deny filtering using filesystem fixtures."""

from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.confluence.confluence_config import (
    ConfluenceSourceConfig,
)
from datahub.ingestion.source.confluence.confluence_source import ConfluenceSource
from tests.unit.ingestion.source.confluence.confluence_test_fixtures import (  # type: ignore[import-untyped]
    create_mock_confluence_client_from_fixture,
)


@pytest.fixture
def pipeline_context() -> PipelineContext:
    """Fixture for pipeline context."""
    return PipelineContext(run_id="test-filtering")


@pytest.fixture
def base_config_dict() -> dict:
    """Base configuration dictionary for Cloud instance."""
    return {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "cloud": True,
    }


# ============================================================================
# Space Filtering Tests
# ============================================================================


def test_no_filters_ingests_all_spaces(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test that with no filters, all spaces are discovered and ingested."""
    config = ConfluenceSourceConfig.model_validate(base_config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)
        spaces = source._get_spaces()

        # Should get all 5 spaces
        assert len(spaces) == 5
        assert set(spaces) == {"TEAM", "DOCS", "PUBLIC", "~johndoe", "ARCHIVE"}


def test_space_allow_includes_only_specified_spaces(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test that space_allow limits ingestion to specified spaces."""
    config_dict = {**base_config_dict, "space_allow": ["TEAM", "DOCS"]}
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)
        spaces = source._get_spaces()

        # Should only get TEAM and DOCS
        assert len(spaces) == 2
        assert set(spaces) == {"TEAM", "DOCS"}


def test_space_deny_excludes_specified_spaces(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test that space_deny excludes specified spaces from ingestion."""
    config_dict = {**base_config_dict, "space_deny": ["~johndoe", "ARCHIVE"]}
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)
        spaces = source._get_spaces()

        # Should get all except personal space and archive
        assert len(spaces) == 3
        assert set(spaces) == {"TEAM", "DOCS", "PUBLIC"}
        assert "~johndoe" not in spaces
        assert "ARCHIVE" not in spaces


def test_space_allow_and_deny_combined(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test that space_deny takes precedence over space_allow."""
    config_dict = {
        **base_config_dict,
        "space_allow": ["TEAM", "DOCS", "PUBLIC"],
        "space_deny": ["PUBLIC"],
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)
        spaces = source._get_spaces()

        # PUBLIC should be excluded even though it's in allow list
        assert len(spaces) == 2
        assert set(spaces) == {"TEAM", "DOCS"}
        assert "PUBLIC" not in spaces


def test_space_allow_with_urls(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test that space_allow works with space URLs (not just keys)."""
    config_dict = {
        **base_config_dict,
        "space_allow": [
            "https://test.atlassian.net/wiki/spaces/TEAM",
            "https://test.atlassian.net/wiki/spaces/DOCS",
        ],
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)
        spaces = source._get_spaces()

        # URL parsing should extract space keys
        assert len(spaces) == 2
        assert set(spaces) == {"TEAM", "DOCS"}


# ============================================================================
# Page Filtering Tests
# ============================================================================


def test_no_page_filters_ingests_all_pages(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test that with no page filters, all pages in allowed spaces are ingested."""
    config_dict = {**base_config_dict, "space_allow": ["TEAM"]}
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)

        # All pages in TEAM space should be allowed
        assert source._is_page_allowed("10001") is True  # Team Home
        assert source._is_page_allowed("10002") is True  # Meeting Notes
        assert source._is_page_allowed("10003") is True  # Project Plans


def test_page_allow_includes_only_specified_pages(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test that page_allow limits ingestion to specified pages."""
    config_dict = {**base_config_dict, "page_allow": ["20001", "20002"]}
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)

        # Only specified pages should be allowed
        assert source._is_page_allowed("20001") is True  # API Documentation
        assert source._is_page_allowed("20002") is True  # REST API
        assert source._is_page_allowed("20003") is False  # Authentication (not in list)
        assert source._is_page_allowed("20004") is False  # OAuth Guide (not in list)


def test_page_deny_excludes_specified_pages(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test that page_deny excludes specified pages from ingestion."""
    config_dict = {**base_config_dict, "page_deny": ["20003", "20004"]}
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)

        # Denied pages should be excluded
        assert source._is_page_allowed("20001") is True  # API Documentation
        assert source._is_page_allowed("20002") is True  # REST API
        assert source._is_page_allowed("20003") is False  # Authentication (denied)
        assert source._is_page_allowed("20004") is False  # OAuth Guide (denied)
        assert source._is_page_allowed("20005") is True  # Endpoints


def test_page_allow_and_deny_combined(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test that page_deny takes precedence over page_allow."""
    config_dict = {
        **base_config_dict,
        "page_allow": ["20001", "20002", "20003"],
        "page_deny": ["20003"],
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)

        # Page 20003 should be excluded even though it's in allow list
        assert source._is_page_allowed("20001") is True
        assert source._is_page_allowed("20002") is True
        assert (
            source._is_page_allowed("20003") is False
        )  # Denied despite being in allow
        assert source._is_page_allowed("20004") is False  # Not in allow list


def test_page_allow_with_urls(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test that page_allow works with page URLs (not just IDs)."""
    config_dict = {
        **base_config_dict,
        "page_allow": [
            "https://test.atlassian.net/wiki/spaces/DOCS/pages/20001/API+Documentation",
            "https://test.atlassian.net/wiki/spaces/DOCS/pages/20002/REST+API",
        ],
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)

        # URL parsing should extract page IDs
        assert source._is_page_allowed("20001") is True
        assert source._is_page_allowed("20002") is True
        assert source._is_page_allowed("20003") is False


# ============================================================================
# Combined Space and Page Filtering Tests
# ============================================================================


def test_space_and_page_filters_combined(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test combining space and page filters."""
    config_dict = {
        **base_config_dict,
        "space_allow": ["TEAM", "DOCS"],
        "space_deny": ["TEAM"],
        "page_deny": ["20003"],
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)
        spaces = source._get_spaces()

        # TEAM should be excluded, only DOCS remains
        assert len(spaces) == 1
        assert spaces == ["DOCS"]

        # Page 20003 should be excluded
        assert source._is_page_allowed("20001") is True
        assert source._is_page_allowed("20002") is True
        assert source._is_page_allowed("20003") is False


def test_exclude_personal_spaces_common_pattern(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test common pattern: exclude personal spaces from ingestion."""
    config_dict = {**base_config_dict, "space_deny": ["~johndoe"]}
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)
        spaces = source._get_spaces()

        # Personal space should be excluded
        assert "~johndoe" not in spaces
        assert len(spaces) == 4
        assert set(spaces) == {"TEAM", "DOCS", "PUBLIC", "ARCHIVE"}


def test_exclude_archived_content_common_pattern(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test common pattern: exclude archived spaces from ingestion."""
    config_dict = {**base_config_dict, "space_deny": ["ARCHIVE"]}
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)
        spaces = source._get_spaces()

        # ARCHIVE space should be excluded
        assert "ARCHIVE" not in spaces
        assert len(spaces) == 4


def test_specific_documentation_tree_common_pattern(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test common pattern: ingest only specific documentation trees."""
    config_dict = {
        **base_config_dict,
        "page_allow": ["20001"],  # API Documentation root page
        "recursive": True,
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)

        # Only page 20001 is in page_allow
        assert source._is_page_allowed("20001") is True
        assert source._is_page_allowed("20002") is False


# ============================================================================
# Fixture Validation Tests
# ============================================================================


def test_fixture_structure() -> None:
    """Test that the default fixture loads correctly."""
    mock_client = create_mock_confluence_client_from_fixture("default_repo")

    # Verify spaces loaded
    assert len(mock_client._test_impl.spaces) == 5
    assert "TEAM" in mock_client._test_impl.spaces
    assert "DOCS" in mock_client._test_impl.spaces
    assert "PUBLIC" in mock_client._test_impl.spaces
    assert "~johndoe" in mock_client._test_impl.spaces
    assert "ARCHIVE" in mock_client._test_impl.spaces

    # Verify pages loaded
    assert len(mock_client._test_impl.pages) >= 15

    # Verify page structure
    team_home = mock_client._test_impl.pages.get("10001")
    assert team_home is not None
    assert team_home["title"] == "Team Home"
    assert team_home["space"]["key"] == "TEAM"
    assert len(team_home["ancestors"]) == 0  # Root page

    # Verify hierarchical structure
    meeting_notes = mock_client._test_impl.pages.get("10002")
    assert meeting_notes is not None
    assert meeting_notes["title"] == "Meeting Notes"
    assert len(meeting_notes["ancestors"]) == 1
    assert meeting_notes["ancestors"][0]["id"] == "10001"


def test_fixture_api_calls() -> None:
    """Test that fixture client responds to API calls correctly."""
    mock_client = create_mock_confluence_client_from_fixture("default_repo")

    # Test get_all_spaces
    spaces_response = mock_client.get_all_spaces(start=0, limit=25)
    assert len(spaces_response["results"]) == 5

    # Test get_page_by_id
    page = mock_client.get_page_by_id("10001", expand="body.storage")
    assert page is not None
    assert page["title"] == "Team Home"

    # Test get_all_pages_from_space
    docs_pages = mock_client.get_all_pages_from_space("DOCS", start=0, limit=100)
    assert len(docs_pages["results"]) == 6  # All pages in DOCS space

    # Test get_child_pages
    children = list(mock_client.get_child_pages("10001"))
    assert (
        len(children) == 2
    )  # Meeting Notes and Project Plans are children of Team Home


# ============================================================================
# Additional Filtering Tests - Complete Coverage
# ============================================================================


# Group 1: Page Deny Without Page Allow (Space-Based Ingestion)


def test_page_deny_only_filters_across_all_spaces(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test page_deny without space filters filters pages across all spaces."""
    config_dict = {**base_config_dict, "page_deny": ["10002", "20003"]}
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)

        # All spaces should be discovered (no space filters)
        spaces = source._get_spaces()
        assert len(spaces) == 5

        # Pages 10002 and 20003 should be filtered
        assert source._is_page_allowed("10001") is True  # Team Home
        assert source._is_page_allowed("10002") is False  # Meeting Notes (denied)
        assert source._is_page_allowed("20001") is True  # API Documentation
        assert source._is_page_allowed("20003") is False  # Authentication (denied)


def test_page_deny_with_space_deny(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test page_deny + space_deny filters both spaces and pages."""
    config_dict = {
        **base_config_dict,
        "space_deny": ["ARCHIVE"],
        "page_deny": ["10002"],
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)
        spaces = source._get_spaces()

        # ARCHIVE space should be excluded
        assert len(spaces) == 4
        assert "ARCHIVE" not in spaces

        # Page 10002 should be denied
        assert source._is_page_allowed("10001") is True
        assert source._is_page_allowed("10002") is False


# Group 2: page_allow Overrides Space Filters


def test_page_allow_overrides_space_allow(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test that page_allow overrides space_allow (page-based mode)."""
    config_dict = {
        **base_config_dict,
        "space_allow": ["TEAM"],  # Would limit to TEAM only
        "page_allow": ["20001"],  # Page in DOCS space
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)

        # When page_allow is specified, space discovery is skipped
        # Page 20001 (from DOCS space) should be allowed despite space_allow=TEAM
        assert source._is_page_allowed("20001") is True
        assert source._is_page_allowed("10001") is False  # Not in page_allow


def test_page_allow_ignores_both_space_filters(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test that page_allow bypasses all space filtering."""
    config_dict = {
        **base_config_dict,
        "space_allow": ["TEAM"],
        "space_deny": ["DOCS"],
        "page_allow": ["20001"],  # Page in DOCS (denied space)
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)

        # Page 20001 should still be allowed (space filters ignored in page-based mode)
        assert source._is_page_allowed("20001") is True


def test_page_allow_with_page_deny_complex(
    base_config_dict: dict, pipeline_context: PipelineContext
) -> None:
    """Test page_allow + page_deny with space filters present."""
    config_dict = {
        **base_config_dict,
        "space_allow": ["TEAM"],  # Ignored in page-based mode
        "page_allow": ["20001", "20002", "10001"],
        "page_deny": ["20002"],
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client_from_fixture(
            "default_repo"
        )

        source = ConfluenceSource(config, pipeline_context)

        # Only pages 20001 and 10001 should be allowed (20002 denied)
        assert source._is_page_allowed("20001") is True
        assert source._is_page_allowed("20002") is False  # Denied
        assert source._is_page_allowed("10001") is True
        assert source._is_page_allowed("20003") is False  # Not in allow list


# Group 3: URL Parsing Edge Cases


def test_space_url_without_wiki() -> None:
    """Test that malformed space URLs raise clear errors."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "space_allow": ["https://test.atlassian.net/TEAM"],  # Missing /wiki/spaces/
    }

    with pytest.raises(ValueError, match="Could not extract space key from URL"):
        ConfluenceSourceConfig.model_validate(config_dict)


def test_page_url_malformed() -> None:
    """Test that malformed page URLs raise clear errors."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "page_allow": [
            "https://test.atlassian.net/wiki/spaces/TEAM"
        ],  # Missing /pages/
    }

    with pytest.raises(ValueError, match="Could not extract page ID from URL"):
        ConfluenceSourceConfig.model_validate(config_dict)


def test_mixed_valid_invalid_page_ids() -> None:
    """Test that non-numeric page IDs are rejected."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "page_allow": ["123456", "invalid", "789012"],
    }

    with pytest.raises(ValueError, match="Page ID must be numeric"):
        ConfluenceSourceConfig.model_validate(config_dict)


# Group 4: Empty String Handling


def test_empty_strings_in_space_allow() -> None:
    """Test that empty strings in space_allow are rejected."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "space_allow": ["TEAM", "", "DOCS"],
    }

    # Empty strings should be stripped by validator and result in error
    with pytest.raises(ValueError):
        ConfluenceSourceConfig.model_validate(config_dict)


def test_whitespace_only_strings() -> None:
    """Test that whitespace-only strings are rejected."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "space_allow": ["TEAM", "   ", "DOCS"],
    }

    # Whitespace-only strings should be stripped and rejected
    with pytest.raises(ValueError):
        ConfluenceSourceConfig.model_validate(config_dict)
