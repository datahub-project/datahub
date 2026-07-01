"""Unit tests for Confluence source."""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.confluence.confluence_config import (
    ConfluenceSourceConfig,
)
from datahub.ingestion.source.confluence.confluence_source import (
    EXTRACTION_ALGO_VERSION,
    ConfluenceSource,
)


@pytest.fixture
def cloud_config() -> ConfluenceSourceConfig:
    """Fixture for Cloud configuration."""
    return ConfluenceSourceConfig.model_validate(
        {
            "url": "https://test.atlassian.net/wiki",
            "username": "test@example.com",
            "api_token": "test-token-123",
            "cloud": True,
        }
    )


@pytest.fixture
def datacenter_config() -> ConfluenceSourceConfig:
    """Fixture for Data Center configuration."""
    return ConfluenceSourceConfig.model_validate(
        {
            "url": "https://confluence.company.com",
            "personal_access_token": "test-pat-123",
            "cloud": False,
        }
    )


@pytest.fixture
def pipeline_context() -> PipelineContext:
    """Fixture for pipeline context."""
    return PipelineContext(run_id="test-run")


def test_source_initialization_cloud(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test successful source initialization with Cloud config."""
    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source = ConfluenceSource(cloud_config, pipeline_context)
        assert source.platform == "confluence"
        assert source.config == cloud_config
        assert source.report is not None
        assert source.confluence_client is not None


def test_source_initialization_datacenter(
    datacenter_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test successful source initialization with Data Center config."""
    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source = ConfluenceSource(datacenter_config, pipeline_context)
        assert source.platform == "confluence"
        assert source.config == datacenter_config
        assert source.confluence_client is not None


def test_get_spaces_auto_discover(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test space auto-discovery when no space_allow is configured."""
    # No space_allow configured = auto-discovery mode

    mock_spaces_response = {
        "results": [
            {"key": "TEAM", "name": "Team Space"},
            {"key": "DOCS", "name": "Documentation"},
            {"key": "ENG", "name": "Engineering"},
        ]
    }

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_client = MagicMock()
        mock_client.get_all_spaces.return_value = mock_spaces_response
        mock_confluence.return_value = mock_client

        source = ConfluenceSource(cloud_config, pipeline_context)
        spaces = source._get_spaces()

        assert spaces == ["TEAM", "DOCS", "ENG"]
        mock_client.get_all_spaces.assert_called_once()


def test_get_spaces_with_space_allow(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test that space_allow limits spaces to specified list."""
    # Set space_allow directly
    cloud_config._parsed_space_allow = ["TEAM", "DOCS"]

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_client = MagicMock()
        mock_confluence.return_value = mock_client

        source = ConfluenceSource(cloud_config, pipeline_context)
        spaces = source._get_spaces()

        # Should return exactly the spaces in space_allow
        assert spaces == ["TEAM", "DOCS"]
        # Should NOT call get_all_spaces when space_allow is provided
        mock_client.get_all_spaces.assert_not_called()


def test_get_spaces_with_space_deny(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test that space_deny excludes specified spaces."""
    # Set space_deny
    cloud_config._parsed_space_deny = ["ENG"]

    mock_spaces_response = {
        "results": [
            {"key": "TEAM"},
            {"key": "DOCS"},
            {"key": "ENG"},  # This should be filtered out
        ]
    }

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_client = MagicMock()
        mock_client.get_all_spaces.return_value = mock_spaces_response
        mock_confluence.return_value = mock_client

        source = ConfluenceSource(cloud_config, pipeline_context)
        spaces = source._get_spaces()

        # ENG should be filtered out by space_deny
        assert "ENG" not in spaces
        assert "TEAM" in spaces
        assert "DOCS" in spaces


def test_page_allow_is_seed_not_emission_filter(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """pages.allow seeds the crawl; it must not gate emission.

    A recursively-discovered child page whose ID is not literally in
    pages.allow must still be emitted (not denied), otherwise recursive
    ingestion would only ever yield the root/seed pages.
    """
    cloud_config._parsed_page_allow = ["123456", "789012"]

    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source = ConfluenceSource(cloud_config, pipeline_context)

        # Seed pages are not denied.
        assert source._is_page_denied("123456") is False
        assert source._is_page_denied("789012") is False

        # A child page discovered via recursion (not in pages.allow) is also
        # not denied - this is the regression the fix addresses.
        assert source._is_page_denied("999999") is False


def test_is_page_denied_with_page_deny(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test page_deny filtering."""
    cloud_config._parsed_page_deny = ["999999", "888888"]

    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source = ConfluenceSource(cloud_config, pipeline_context)

        # Pages in deny list should be denied
        assert source._is_page_denied("999999") is True
        assert source._is_page_denied("888888") is True

        # Pages not in deny list should not be denied
        assert source._is_page_denied("123456") is False


def test_build_page_urn(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test page URN generation with hash-based instance ID."""
    import hashlib

    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source = ConfluenceSource(cloud_config, pipeline_context)
        urn = source._build_page_urn("12345")

        # URN should include platform, hash-based instance ID, and page ID
        assert urn.startswith("urn:li:document:confluence-")
        assert urn.endswith("-12345")

        # Instance ID should be 8-char hash of URL
        expected_hash = hashlib.sha256(cloud_config.url.encode()).hexdigest()[:8]
        assert urn == f"urn:li:document:confluence-{expected_hash}-12345"


def test_extract_text_from_page(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test text extraction from page metadata."""
    page = {
        "id": "12345",
        "title": "Test Page",
        "body": {
            "storage": {
                "value": "<h2>Section</h2><p>This is <strong>test</strong> content.</p>"
            }
        },
    }

    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source = ConfluenceSource(cloud_config, pipeline_context)
        text = source._extract_text_from_page(page)

        assert "Test Page" in text
        assert "## Section" in text
        assert "test" in text.lower()
        assert "content" in text
        assert "<" not in text


def test_extract_parent_urn_with_parent(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test parent URN extraction when page has parent."""
    import hashlib

    page = {
        "id": "12345",
        "ancestors": [
            {"id": "11111"},
            {"id": "22222"},  # Immediate parent
        ],
    }
    ingested_ids = {"11111", "12345", "22222"}

    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source = ConfluenceSource(cloud_config, pipeline_context)
        parent_urn = source._extract_parent_urn(page, ingested_ids)

        # Parent URN should include instance ID
        instance_id = hashlib.sha256(cloud_config.url.encode()).hexdigest()[:8]
        assert parent_urn == f"urn:li:document:confluence-{instance_id}-22222"


def test_extract_parent_urn_no_parent(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test parent URN extraction when page has no parent."""
    page = {"id": "12345", "ancestors": []}
    ingested_ids = {"12345"}

    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source = ConfluenceSource(cloud_config, pipeline_context)
        parent_urn = source._extract_parent_urn(page, ingested_ids)

        assert parent_urn is None


def test_extract_parent_urn_parent_not_ingested(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test parent URN extraction when parent not in ingestion scope."""
    page = {
        "id": "12345",
        "ancestors": [
            {"id": "99999"},  # Parent not being ingested
        ],
    }
    ingested_ids = {"12345"}

    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source = ConfluenceSource(cloud_config, pipeline_context)
        parent_urn = source._extract_parent_urn(page, ingested_ids)

        # Should return None when parent not in ingestion scope
        assert parent_urn is None


def test_connection_successful() -> None:
    """Test successful connection test."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
    }

    mock_spaces_response = {"results": [{"key": "TEAM"}]}

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_client = MagicMock()
        mock_client.get_all_spaces.return_value = mock_spaces_response
        mock_confluence.return_value = mock_client

        report = ConfluenceSource.test_connection(config_dict)

        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is True


def test_connection_failed() -> None:
    """Test failed connection test."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "invalid-token",
    }

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.side_effect = Exception("Authentication failed")

        report = ConfluenceSource.test_connection(config_dict)

        assert report.internal_failure is True
        assert report.internal_failure_reason is not None
        assert "Authentication failed" in report.internal_failure_reason


def test_get_report(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test report retrieval."""
    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source = ConfluenceSource(cloud_config, pipeline_context)
        report = source.get_report()

        assert report.spaces_scanned == 0
        assert report.pages_scanned == 0
        assert report.pages_processed == 0


def test_get_pages_recursively(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test recursive page crawling."""
    cloud_config.recursive = True

    # Mock page tree: parent -> child1, child2 -> grandchild
    mock_pages = {
        "12345": {"id": "12345", "title": "Parent Page"},
        "23456": {"id": "23456", "title": "Child 1"},
        "34567": {"id": "34567", "title": "Child 2"},
        "45678": {"id": "45678", "title": "Grandchild"},
    }

    mock_children = {
        "12345": [mock_pages["23456"], mock_pages["34567"]],
        "23456": [],
        "34567": [mock_pages["45678"]],
        "45678": [],
    }

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_client = MagicMock()

        # Mock get_page_by_id
        def get_page_side_effect(page_id, expand=None):
            return mock_pages.get(page_id)

        mock_client.get_page_by_id.side_effect = get_page_side_effect

        # Mock get_child_pages
        def get_child_pages_side_effect(page_id):
            return mock_children.get(page_id, [])

        mock_client.get_child_pages.side_effect = get_child_pages_side_effect
        mock_confluence.return_value = mock_client

        source = ConfluenceSource(cloud_config, pipeline_context)
        pages = list(source._get_pages_recursively("12345"))

        # Should get parent + 2 children + 1 grandchild = 4 total
        assert len(pages) == 4
        page_ids = [str(p["id"]) for p in pages]
        assert "12345" in page_ids
        assert "23456" in page_ids
        assert "34567" in page_ids
        assert "45678" in page_ids


def test_get_pages_recursively_non_recursive(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test non-recursive page fetching (single page only)."""
    cloud_config.recursive = False

    mock_page = {"id": "12345", "title": "Single Page"}

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_client = MagicMock()
        mock_client.get_page_by_id.return_value = mock_page
        mock_confluence.return_value = mock_client

        source = ConfluenceSource(cloud_config, pipeline_context)
        pages = list(source._get_pages_recursively("12345"))

        # Should only get the single page (no children)
        assert len(pages) == 1
        assert pages[0]["id"] == "12345"


def test_cycle_detection_prevents_infinite_loop(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test that cycle detection prevents infinite loops in page hierarchies."""
    cloud_config.recursive = True

    # Create circular reference: A -> B -> C -> A
    mock_pages = {
        "A": {"id": "A", "title": "Page A"},
        "B": {"id": "B", "title": "Page B"},
        "C": {"id": "C", "title": "Page C"},
    }

    mock_children = {
        "A": [mock_pages["B"]],
        "B": [mock_pages["C"]],
        "C": [mock_pages["A"]],  # Creates cycle
    }

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_client = MagicMock()
        mock_client.get_page_by_id.side_effect = lambda page_id, expand=None: (
            mock_pages.get(page_id)
        )
        mock_client.get_child_pages.side_effect = lambda page_id: iter(
            mock_children.get(page_id, [])
        )
        mock_confluence.return_value = mock_client

        source = ConfluenceSource(cloud_config, pipeline_context)
        pages = list(source._get_pages_recursively("A"))

        # Should get each page exactly once (no infinite loop)
        assert len(pages) == 3
        page_ids = [p["id"] for p in pages]
        assert page_ids.count("A") == 1
        assert page_ids.count("B") == 1
        assert page_ids.count("C") == 1


# ============================================================================
# Instance ID and URN Uniqueness Tests
# ============================================================================


def test_hash_based_instance_id(pipeline_context: PipelineContext) -> None:
    """Test hash-based instance ID generation from URL."""
    import hashlib

    config = ConfluenceSourceConfig.model_validate(
        {
            "url": "https://company-a.atlassian.net/wiki",
            "username": "user@example.com",
            "api_token": "token",
        }
    )

    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source = ConfluenceSource(config, pipeline_context)
        instance_id = source._get_instance_id()

        # Should be first 8 chars of SHA256 hash
        expected_hash = hashlib.sha256(config.url.encode()).hexdigest()[:8]
        assert instance_id == expected_hash

        # URN includes hash
        urn = source._build_page_urn("123456")
        assert f"confluence-{expected_hash}-123456" in urn


def test_multi_instance_urn_uniqueness(pipeline_context: PipelineContext) -> None:
    """Test URNs are unique across different Confluence instances."""
    config_a = ConfluenceSourceConfig.model_validate(
        {
            "url": "https://company-a.atlassian.net/wiki",
            "username": "user@example.com",
            "api_token": "token",
        }
    )
    config_b = ConfluenceSourceConfig.model_validate(
        {
            "url": "https://company-b.atlassian.net/wiki",
            "username": "user@example.com",
            "api_token": "token",
        }
    )

    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source_a = ConfluenceSource(config_a, pipeline_context)
        source_b = ConfluenceSource(config_b, pipeline_context)

        # Different URLs produce different hashes
        instance_a = source_a._get_instance_id()
        instance_b = source_b._get_instance_id()
        assert instance_a != instance_b

        # Same page ID in different instances has different URNs
        urn_a = source_a._build_page_urn("123456")
        urn_b = source_b._build_page_urn("123456")
        assert urn_a != urn_b


def test_data_center_hash_uniqueness(pipeline_context: PipelineContext) -> None:
    """Test hash-based IDs work for Data Center installations."""
    import hashlib

    config = ConfluenceSourceConfig.model_validate(
        {
            "url": "https://confluence.mycompany.com",
            "personal_access_token": "token",
            "cloud": False,
        }
    )

    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source = ConfluenceSource(config, pipeline_context)

        # Hash-based instance ID
        expected_hash = hashlib.sha256(config.url.encode()).hexdigest()[:8]
        assert source._get_instance_id() == expected_hash

        # URN uses hash
        urn = source._build_page_urn("123456")
        assert f"confluence-{expected_hash}-123456" in urn


def test_explicit_platform_instance(pipeline_context: PipelineContext) -> None:
    """Test explicit platform_instance config overrides hash-based ID."""
    config = ConfluenceSourceConfig.model_validate(
        {
            "url": "https://test.atlassian.net/wiki",
            "username": "user@example.com",
            "api_token": "token",
            "platform_instance": "mycompany-prod",
        }
    )

    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        source = ConfluenceSource(config, pipeline_context)

        # Should use explicit value instead of hash
        assert source._get_instance_id() == "mycompany-prod"

        # URN uses explicit value
        urn = source._build_page_urn("123456")
        assert urn == "urn:li:document:confluence-mycompany-prod-123456"


def test_browse_path_emitted_for_root_page(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test that BrowsePathsV2 is emitted for root pages with just space name."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import BrowsePathsV2Class
    from tests.unit.ingestion.source.confluence.confluence_test_fixtures import (  # type: ignore[import-untyped]
        create_mock_confluence_client,
    )

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client("default")

        source = ConfluenceSource(cloud_config, pipeline_context)

        # Mock the chunking source to avoid embedding generation
        with patch.object(
            source.chunking_source, "process_elements_inline", return_value=iter([])
        ):
            workunits = list(source.get_workunits())

        # Find the BrowsePathsV2 aspect for page 10001 (Team Home - root page)
        browse_path = None
        for wu in workunits:
            if isinstance(wu.metadata, MetadataChangeProposalWrapper):
                if "10001" in str(wu.metadata.entityUrn) and isinstance(
                    wu.metadata.aspect, BrowsePathsV2Class
                ):
                    browse_path = wu.metadata.aspect
                    break

        assert browse_path is not None, (
            "Should find BrowsePathsV2 aspect for Team Home page"
        )
        assert len(browse_path.path) == 1, "Root page should have only space in path"
        assert browse_path.path[0].id == "Team Space"
        assert browse_path.path[0].urn is None, "Space entry should not have URN"


def test_browse_path_emitted_for_nested_page(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test that BrowsePathsV2 is emitted for nested pages with full hierarchy."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import BrowsePathsV2Class
    from tests.unit.ingestion.source.confluence.confluence_test_fixtures import (  # type: ignore[import-untyped]
        create_mock_confluence_client,
    )

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client("default")

        source = ConfluenceSource(cloud_config, pipeline_context)

        # Mock the chunking source to avoid embedding generation
        with patch.object(
            source.chunking_source, "process_elements_inline", return_value=iter([])
        ):
            workunits = list(source.get_workunits())

        # Find the BrowsePathsV2 aspect for page 20003 (Authentication - nested under API Documentation -> REST API)
        browse_path = None
        for wu in workunits:
            if isinstance(wu.metadata, MetadataChangeProposalWrapper):
                if "20003" in str(wu.metadata.entityUrn) and isinstance(
                    wu.metadata.aspect, BrowsePathsV2Class
                ):
                    browse_path = wu.metadata.aspect
                    break

        assert browse_path is not None, (
            "Should find BrowsePathsV2 aspect for Authentication page"
        )
        assert len(browse_path.path) == 3, "Should have space + 2 ancestors in path"

        # Verify space entry
        assert browse_path.path[0].id == "Documentation Space"
        assert browse_path.path[0].urn is None

        # Verify first ancestor (API Documentation - page 20001)
        assert browse_path.path[1].id == "API Documentation"
        assert browse_path.path[1].urn is not None
        assert "20001" in browse_path.path[1].urn

        # Verify second ancestor (REST API - page 20002)
        assert browse_path.path[2].id == "REST API"
        assert browse_path.path[2].urn is not None
        assert "20002" in browse_path.path[2].urn


def test_browse_path_deep_hierarchy(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test that BrowsePathsV2 handles deeply nested pages correctly."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import BrowsePathsV2Class
    from tests.unit.ingestion.source.confluence.confluence_test_fixtures import (  # type: ignore[import-untyped]
        create_mock_confluence_client,
    )

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client("default")

        source = ConfluenceSource(cloud_config, pipeline_context)

        # Mock the chunking source to avoid embedding generation
        with patch.object(
            source.chunking_source, "process_elements_inline", return_value=iter([])
        ):
            workunits = list(source.get_workunits())

        # Find the BrowsePathsV2 aspect for page 20004 (OAuth 2.0 Guide - 3 levels deep)
        browse_path = None
        for wu in workunits:
            if isinstance(wu.metadata, MetadataChangeProposalWrapper):
                if "20004" in str(wu.metadata.entityUrn) and isinstance(
                    wu.metadata.aspect, BrowsePathsV2Class
                ):
                    browse_path = wu.metadata.aspect
                    break

        assert browse_path is not None, (
            "Should find BrowsePathsV2 aspect for OAuth 2.0 Guide page"
        )
        assert len(browse_path.path) == 4, "Should have space + 3 ancestors in path"

        # Verify full hierarchy
        assert browse_path.path[0].id == "Documentation Space"
        assert browse_path.path[1].id == "API Documentation"
        assert browse_path.path[2].id == "REST API"
        assert browse_path.path[3].id == "Authentication"

        # All ancestors should have URNs (all being ingested)
        assert all(entry.urn is not None for entry in browse_path.path[1:]), (
            "All ancestors should have URNs"
        )


def test_browse_path_ancestor_not_ingested(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test that ancestors not being ingested don't get URNs in browse path."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import BrowsePathsV2Class
    from tests.unit.ingestion.source.confluence.confluence_test_fixtures import (  # type: ignore[import-untyped]
        create_mock_confluence_client,
    )

    # Configure to only ingest specific pages (exclude ancestors)
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "cloud": True,
        "pages": {"allow": ["20004"]},  # Only OAuth 2.0 Guide, not its ancestors
    }
    config = ConfluenceSourceConfig.model_validate(config_dict)

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = create_mock_confluence_client("default")

        source = ConfluenceSource(config, pipeline_context)

        # Mock the chunking source to avoid embedding generation
        with patch.object(
            source.chunking_source, "process_elements_inline", return_value=iter([])
        ):
            workunits = list(source.get_workunits())

        # Find the BrowsePathsV2 aspect for page 20004
        browse_path = None
        for wu in workunits:
            if isinstance(wu.metadata, MetadataChangeProposalWrapper):
                if "20004" in str(wu.metadata.entityUrn) and isinstance(
                    wu.metadata.aspect, BrowsePathsV2Class
                ):
                    browse_path = wu.metadata.aspect
                    break

        assert browse_path is not None, (
            "Should find BrowsePathsV2 aspect for OAuth 2.0 Guide page"
        )
        assert len(browse_path.path) == 4, "Should have space + 3 ancestors in path"

        # Space should not have URN
        assert browse_path.path[0].urn is None

        # Ancestors should NOT have URNs (not being ingested)
        assert browse_path.path[1].urn is None, (
            "API Documentation not ingested, should not have URN"
        )
        assert browse_path.path[2].urn is None, (
            "REST API not ingested, should not have URN"
        )
        assert browse_path.path[3].urn is None, (
            "Authentication not ingested, should not have URN"
        )

        # But titles should still be present
        assert browse_path.path[1].id == "API Documentation"
        assert browse_path.path[2].id == "REST API"
        assert browse_path.path[3].id == "Authentication"


# ============================================================================
# max_documents Limit Tests
# ============================================================================


def test_max_documents_default(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Test that max_documents defaults to 10000."""
    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = MagicMock()
        source = ConfluenceSource(cloud_config, pipeline_context)
        assert source.chunking_source.config.max_documents == 10000


def test_max_documents_limit_raises_error(
    pipeline_context: PipelineContext,
) -> None:
    """Test that RuntimeError is raised when max_documents limit is hit."""
    config = ConfluenceSourceConfig.model_validate(
        {
            "url": "https://test.atlassian.net/wiki",
            "username": "test@example.com",
            "api_token": "test-token-123",
            "cloud": True,
        }
    )

    # Minimal page dict that passes all filters
    def make_page(page_id: str) -> dict:
        return {
            "id": page_id,
            "title": f"Page {page_id}",
            "body": {
                "storage": {
                    "value": "<p>This is enough content to pass the minimum text length filter.</p>"
                }
            },
            "ancestors": [],
            "space": {"key": "TEST", "name": "Test Space"},
            "_links": {"webui": f"/spaces/TEST/pages/{page_id}/Title"},
        }

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_client = MagicMock()
        mock_confluence.return_value = mock_client

        source = ConfluenceSource(config, pipeline_context)
        source.chunking_source.config.max_documents = 2
        ingested_ids = {"11111", "22222"}
        parent_ids: set = set()

        source.chunking_source.embedding_model = None
        dummy_chunk = [{"text": "Some content", "type": "NarrativeText"}]

        with patch.object(
            source.chunking_source,
            "_chunk_elements",
            return_value=dummy_chunk,
        ):
            # Process first page - should succeed
            list(
                source._create_document_entity(
                    make_page("11111"), ingested_ids, parent_ids
                )
            )
            assert source.report.pages_processed == 1
            assert source.report.num_documents_limit_reached is False

            # Process second page - should hit the limit and raise
            with pytest.raises(RuntimeError, match="Document limit of 2 reached"):
                list(
                    source._create_document_entity(
                        make_page("22222"), ingested_ids, parent_ids
                    )
                )

    assert source.report.num_documents_limit_reached is True
    assert source.report.pages_processed == 1


def test_max_documents_limit_reached_flag(
    pipeline_context: PipelineContext,
) -> None:
    """Test that num_documents_limit_reached is set to True when limit is hit."""
    config = ConfluenceSourceConfig.model_validate(
        {
            "url": "https://test.atlassian.net/wiki",
            "username": "test@example.com",
            "api_token": "test-token-123",
            "cloud": True,
        }
    )

    page = {
        "id": "12345",
        "title": "Test Page",
        "body": {
            "storage": {
                "value": "<p>This is enough content to pass the minimum text length filter.</p>"
            }
        },
        "ancestors": [],
        "space": {"key": "TEST", "name": "Test Space"},
        "_links": {"webui": "/spaces/TEST/pages/12345/Test-Page"},
    }

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_client = MagicMock()
        mock_confluence.return_value = mock_client

        source = ConfluenceSource(config, pipeline_context)
        source.chunking_source.config.max_documents = 1

        source.chunking_source.embedding_model = None
        dummy_chunk = [{"text": "Some content", "type": "NarrativeText"}]

        with (
            patch.object(
                source.chunking_source,
                "_chunk_elements",
                return_value=dummy_chunk,
            ),
            pytest.raises(RuntimeError),
        ):
            list(source._create_document_entity(page, {"12345"}, set()))

    assert source.report.num_documents_limit_reached is True


def _make_page_with_timestamps(created_date: str, modified_date: str) -> dict:
    return {
        "id": "12345",
        "title": "Test Page",
        "body": {
            "storage": {
                "value": "<p>This is enough content to pass the minimum text length filter.</p>"
            }
        },
        "ancestors": [],
        "space": {"key": "TEST", "name": "Test Space"},
        "_links": {"webui": "/spaces/TEST/pages/12345"},
        "version": {"when": modified_date},
        "history": {"createdDate": created_date},
    }


def test_created_time_read_from_history(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """created_time must come from history.createdDate, not default to ingestion time."""
    import datetime

    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import DocumentInfoClass

    page = _make_page_with_timestamps(
        created_date="2024-03-15T10:00:00.000Z",
        modified_date="2025-06-20T12:00:00.000Z",
    )

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = MagicMock()
        source = ConfluenceSource(cloud_config, pipeline_context)
        source.chunking_source.embedding_model = None

        with patch.object(
            source.chunking_source,
            "_chunk_elements",
            return_value=[
                {"text": "Content here for testing.", "type": "NarrativeText"}
            ],
        ):
            wus = list(source._create_document_entity(page, {"12345"}, set()))

    doc_info = next(
        (
            wu.metadata.aspect
            for wu in wus
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, DocumentInfoClass)
        ),
        None,
    )
    assert doc_info is not None

    assert doc_info.created is not None
    created_dt = datetime.datetime.fromtimestamp(
        doc_info.created.time / 1000, tz=datetime.timezone.utc
    )
    assert created_dt.year == 2024
    assert created_dt.month == 3

    assert doc_info.lastModified is not None
    modified_dt = datetime.datetime.fromtimestamp(
        doc_info.lastModified.time / 1000, tz=datetime.timezone.utc
    )
    assert modified_dt.year == 2025
    assert modified_dt.month == 6


def test_missing_history_does_not_crash(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Source must not crash when API response lacks a history field."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import DocumentInfoClass

    page = {
        "id": "12345",
        "title": "Test Page",
        "body": {
            "storage": {
                "value": "<p>This is enough content to pass the minimum text length filter.</p>"
            }
        },
        "ancestors": [],
        "space": {"key": "TEST", "name": "Test Space"},
        "_links": {"webui": "/spaces/TEST/pages/12345"},
        "version": {"when": "2025-01-01T00:00:00.000Z"},
        # no "history" key
    }

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = MagicMock()
        source = ConfluenceSource(cloud_config, pipeline_context)
        source.chunking_source.embedding_model = None

        with patch.object(
            source.chunking_source,
            "_chunk_elements",
            return_value=[
                {"text": "Content here for testing.", "type": "NarrativeText"}
            ],
        ):
            wus = list(source._create_document_entity(page, {"12345"}, set()))

    assert any(
        isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DocumentInfoClass)
        for wu in wus
    )


def test_content_hash_in_custom_properties(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """Document custom_properties must include content_hash and extraction_algo_version.

    The content_hash is read by DocumentChunkingSource to decide whether to
    re-embed a document. Bumping EXTRACTION_ALGO_VERSION changes the hash even
    if the raw page body is unchanged, forcing a full re-ingest after algorithm
    improvements.
    """
    import hashlib
    import json

    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.metadata.schema_classes import DocumentInfoClass

    page = {
        "id": "99999",
        "title": "Hash Test Page",
        "body": {
            "storage": {
                "value": "<p>Some content that is long enough to pass the minimum length filter for testing purposes.</p>"
            }
        },
        "ancestors": [],
        "space": {"key": "HS", "name": "Hash Space"},
        "_links": {"webui": "/spaces/HS/pages/99999"},
        "version": {"when": "2025-01-01T00:00:00.000Z"},
    }

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_confluence.return_value = MagicMock()
        source = ConfluenceSource(cloud_config, pipeline_context)
        source.chunking_source.embedding_model = None

        with patch.object(
            source.chunking_source,
            "_chunk_elements",
            return_value=[{"text": "Some content here.", "type": "NarrativeText"}],
        ):
            wus = list(source._create_document_entity(page, {"99999"}, set()))

    # Find the DocumentInfo aspect (carries custom_properties)
    props_aspect = None
    for wu in wus:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
            wu.metadata.aspect, DocumentInfoClass
        ):
            props_aspect = wu.metadata.aspect
            break

    assert props_aspect is not None, "DocumentInfoClass aspect not found"
    custom_props = props_aspect.customProperties
    assert custom_props is not None

    # content_hash must be a 64-char SHA-256 hex string
    assert "content_hash" in custom_props
    assert len(custom_props["content_hash"]) == 64

    # extraction_algo_version must match the module constant
    assert "extraction_algo_version" in custom_props
    assert custom_props["extraction_algo_version"] == EXTRACTION_ALGO_VERSION

    # Verify the hash is deterministic: same body + same version → same hash
    raw_body = "<p>Some content that is long enough to pass the minimum length filter for testing purposes.</p>"
    expected_hash = hashlib.sha256(
        json.dumps(
            {"body": raw_body, "algo_version": EXTRACTION_ALGO_VERSION}, sort_keys=True
        ).encode("utf-8")
    ).hexdigest()
    assert custom_props["content_hash"] == expected_hash

    # Verify bumping algo_version changes the hash (cache busting works)
    different_hash = hashlib.sha256(
        json.dumps({"body": raw_body, "algo_version": "999"}, sort_keys=True).encode(
            "utf-8"
        )
    ).hexdigest()
    assert different_hash != expected_hash


def _build_source(
    config: ConfluenceSourceConfig, ctx: PipelineContext
) -> ConfluenceSource:
    """Instantiate a ConfluenceSource with the network client patched out."""
    with patch("datahub.ingestion.source.confluence.confluence_source.Confluence"):
        return ConfluenceSource(config, ctx)


def test_folder_entity_emits_folder_subtype_document(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """A folder ancestor is emitted as a Folder-subtyped document with no text."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.ingestion.source.confluence.confluence_hierarchy import FolderNode
    from datahub.ingestion.source.confluence.confluence_source import FOLDER_SUBTYPE
    from datahub.metadata.schema_classes import (
        DocumentInfoClass,
        SubTypesClass,
    )

    source = _build_source(cloud_config, pipeline_context)
    folder = FolderNode(
        id="900",
        title="Engineering",
        parent_id=None,
        space_name="Team Space",
        space_key="TEAM",
    )

    workunits = list(source._create_folder_entity(folder, ingested_page_ids={"900"}))

    assert source.report.folders_ingested == 1
    assert workunits, "Folder should produce at least one workunit"

    subtype = None
    doc_info = None
    for wu in workunits:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper):
            if isinstance(wu.metadata.aspect, SubTypesClass):
                subtype = wu.metadata.aspect
            elif isinstance(wu.metadata.aspect, DocumentInfoClass):
                doc_info = wu.metadata.aspect

    assert subtype is not None and FOLDER_SUBTYPE in subtype.typeNames
    assert doc_info is not None
    assert doc_info.title == "Engineering"
    # Folders carry no content but keep their Confluence id for traceability.
    assert doc_info.customProperties.get("folder_id") == "900"
    assert doc_info.customProperties.get("space_key") == "TEAM"


def test_folder_entity_links_to_ingested_parent(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """When a folder's parent is also ingested, the folder links to it."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.ingestion.source.confluence.confluence_hierarchy import FolderNode
    from datahub.metadata.schema_classes import DocumentInfoClass

    source = _build_source(cloud_config, pipeline_context)
    instance_id = source._get_instance_id()
    folder = FolderNode(
        id="900",
        title="Sub Folder",
        parent_id="100",
        space_name="Team Space",
        space_key="TEAM",
    )

    workunits = list(
        source._create_folder_entity(folder, ingested_page_ids={"100", "900"})
    )

    parent = None
    for wu in workunits:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
            wu.metadata.aspect, DocumentInfoClass
        ):
            parent = wu.metadata.aspect.parentDocument
    assert parent is not None
    assert parent.document == f"urn:li:document:confluence-{instance_id}-100"


def test_folder_entity_omits_parent_when_not_ingested(
    cloud_config: ConfluenceSourceConfig, pipeline_context: PipelineContext
) -> None:
    """A folder whose parent is outside the ingestion scope is a root document."""
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.ingestion.source.confluence.confluence_hierarchy import FolderNode
    from datahub.metadata.schema_classes import DocumentInfoClass

    source = _build_source(cloud_config, pipeline_context)
    folder = FolderNode(
        id="900",
        title="Orphan Folder",
        parent_id="100",  # parent not in ingested_page_ids
        space_name="Team Space",
        space_key="TEAM",
    )

    workunits = list(source._create_folder_entity(folder, ingested_page_ids={"900"}))

    for wu in workunits:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper) and isinstance(
            wu.metadata.aspect, DocumentInfoClass
        ):
            assert wu.metadata.aspect.parentDocument is None


def test_connection_reports_inaccessible_configured_space() -> None:
    """Configured spaces that can't be reached surface as a failed capability."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "spaces": {"allow": ["SECRET"]},
    }

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_client = MagicMock()
        mock_client.get_all_spaces.return_value = {"results": [{"key": "TEAM"}]}
        mock_client.get_space.side_effect = Exception("403 Forbidden")
        mock_confluence.return_value = mock_client

        report = ConfluenceSource.test_connection(config_dict)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True
    assert report.capability_report is not None
    access = report.capability_report["Space/Page Access"]
    assert access.capable is False
    assert "SECRET" in (access.failure_reason or "")


def test_connection_validates_accessible_configured_space() -> None:
    """When configured spaces are reachable, access is reported as capable."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
        "spaces": {"allow": ["TEAM"]},
    }

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_client = MagicMock()
        mock_client.get_all_spaces.return_value = {"results": [{"key": "TEAM"}]}
        mock_client.get_space.return_value = {"key": "TEAM", "name": "Team Space"}
        mock_client.get_all_pages_from_space.return_value = [{"id": "1"}, {"id": "2"}]
        mock_confluence.return_value = mock_client

        report = ConfluenceSource.test_connection(config_dict)

    assert report.capability_report is not None
    assert report.capability_report["Space/Page Access"].capable is True


def test_connection_auto_discovery_reports_no_spaces() -> None:
    """Auto-discovery surfaces a clear capability failure when no spaces exist."""
    config_dict = {
        "url": "https://test.atlassian.net/wiki",
        "username": "test@example.com",
        "api_token": "test-token-123",
    }

    with patch(
        "datahub.ingestion.source.confluence.confluence_source.Confluence"
    ) as mock_confluence:
        mock_client = MagicMock()
        # First call (basic connectivity) succeeds; the discovery call returns empty.
        mock_client.get_all_spaces.side_effect = [
            {"results": [{"key": "TEAM"}]},
            {},
        ]
        mock_confluence.return_value = mock_client

        report = ConfluenceSource.test_connection(config_dict)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True
    assert report.capability_report is not None
    assert report.capability_report["Auto-Discovery"].capable is False
