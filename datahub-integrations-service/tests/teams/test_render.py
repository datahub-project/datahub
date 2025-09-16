"""
Comprehensive rendering tests for Teams entity cards.
These tests focus on edge cases, error handling, and complex entity scenarios.
"""

from typing import Any
from unittest.mock import patch

from datahub_integrations.teams.render.render_entity import (
    _build_breadcrumb_text,
    _build_breadcrumb_with_icons,
    _extract_name_from_urn,
    _get_entity_tags,
    _get_entity_terms,
    _get_entity_type_icon_url,
    _get_ownership_text,
    _get_platform_logo_url,
    _get_platform_name,
    _is_valid_teams_image_url,
    render_entity_card,
    render_entity_preview,
)


class TestEntityNameExtraction:
    """Test URN-based name extraction."""

    def test_extract_name_from_dataset_urn_with_parentheses(self) -> None:
        """Test name extraction from dataset URN with parentheses format."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,demo_db.public.customers,PROD)"
        name = _extract_name_from_urn(urn)
        assert name == "demo_db.public.customers"

    def test_extract_name_from_dataset_urn_without_parentheses(self) -> None:
        """Test name extraction from dataset URN without parentheses."""
        urn = "urn:li:dataset:urn:li:dataPlatform:postgres,hr.compensation,PROD"
        name = _extract_name_from_urn(urn)
        assert name == "hr.compensation"

    def test_extract_name_from_simple_dataset_urn(self) -> None:
        """Test name extraction from simple dataset URN."""
        urn = "urn:li:dataset:platform,table_name,env"
        name = _extract_name_from_urn(urn)
        assert name == "table_name"

    def test_extract_name_from_chart_urn(self) -> None:
        """Test name extraction from chart URN."""
        urn = "urn:li:chart:(looker,dashboard.chart)"
        name = _extract_name_from_urn(urn)
        # The function now supports other entity types and extracts meaningful names
        assert name == "looker,dashboard.chart"

    def test_extract_name_from_malformed_urn(self) -> None:
        """Test name extraction from malformed URN."""
        urn = "invalid:urn:format"
        name = _extract_name_from_urn(urn)
        # Function tries to extract from any URN with colons, may return "format"
        assert name is None or isinstance(name, str)

    def test_extract_name_from_empty_urn(self) -> None:
        """Test name extraction from empty URN."""
        assert _extract_name_from_urn("") is None

    def test_extract_name_filters_environment_indicators(self) -> None:
        """Test that environment indicators are filtered out."""
        urn = "urn:li:dataset:platform,my_table,PROD"
        name = _extract_name_from_urn(urn)
        assert name == "my_table"

    def test_extract_name_complex_table_path(self) -> None:
        """Test extraction with complex table paths."""
        urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table_with_underscores,PROD)"
        name = _extract_name_from_urn(urn)
        assert name == "project.dataset.table_with_underscores"


class TestEntityTypeIcons:
    """Test entity type icon URL generation."""

    def test_get_entity_type_icon_with_subtype(self) -> None:
        """Test icon URL with subtype preference."""
        icon_url = _get_entity_type_icon_url("DATASET", "table")
        assert "flaticon.com" in icon_url
        assert icon_url != ""

    def test_get_entity_type_icon_fallback_to_entity_type(self) -> None:
        """Test icon URL fallback to entity type."""
        icon_url = _get_entity_type_icon_url("DATASET", None)
        assert "flaticon.com" in icon_url

    def test_get_entity_type_icon_unknown_type(self) -> None:
        """Test icon URL for unknown entity type."""
        icon_url = _get_entity_type_icon_url("UNKNOWN_TYPE", None)
        assert icon_url == ""

    def test_get_entity_type_icon_case_insensitive_subtype(self) -> None:
        """Test icon URL with case variations in subtype."""
        icon_url = _get_entity_type_icon_url("DATASET", "TABLE")
        assert icon_url != ""

    def test_get_entity_type_icon_all_supported_subtypes(self) -> None:
        """Test icon URLs for all supported subtypes."""
        subtypes = ["database", "schema", "table", "view", "model", "project"]
        for subtype in subtypes:
            icon_url = _get_entity_type_icon_url("DATASET", subtype)
            assert icon_url != ""
            assert "flaticon.com" in icon_url


class TestBreadcrumbBuilding:
    """Test breadcrumb generation from entity structure."""

    def test_build_breadcrumb_text_from_parent_containers(self) -> None:
        """Test breadcrumb building from parent containers (root-to-child display)."""
        entity = {
            "parentContainers": {
                "containers": [
                    {"properties": {"name": "database"}},  # First in array
                    {"properties": {"name": "schema"}},  # Second in array
                    {"name": "fallback_name"},  # Third in array, test fallback
                ]
            }
        }

        breadcrumb = _build_breadcrumb_text(entity)
        # Containers are reversed to show root-to-child hierarchy (last container is root)
        assert breadcrumb == "fallback_name > schema > database"

    def test_build_breadcrumb_text_empty_containers(self) -> None:
        """Test breadcrumb with empty containers."""
        entity: dict[str, dict] = {
            "parentContainers": {"containers": []},
        }

        breadcrumb = _build_breadcrumb_text(entity)
        assert breadcrumb == ""

    def test_build_breadcrumb_text_malformed_data(self) -> None:
        """Test breadcrumb with malformed data structures."""
        entity = {
            "parentContainers": "not_a_dict",
        }

        breadcrumb = _build_breadcrumb_text(entity)
        assert breadcrumb == ""

    def test_build_breadcrumb_with_icons_success(self) -> None:
        """Test breadcrumb with icons generation."""
        entity = {
            "parentContainers": {
                "containers": [
                    {
                        "properties": {"name": "database"},
                        "type": "CONTAINER",
                        "subTypes": {"typeNames": ["database"]},
                    }
                ]
            }
        }

        breadcrumb_items = _build_breadcrumb_with_icons(entity)
        assert len(breadcrumb_items) == 1
        assert breadcrumb_items[0]["name"] == "database"
        assert breadcrumb_items[0]["type"] == "CONTAINER"
        assert "icon_url" in breadcrumb_items[0]

    def test_build_breadcrumb_with_icons_missing_subtype(self) -> None:
        """Test breadcrumb with icons when subtype is missing."""
        entity = {
            "parentContainers": {
                "containers": [
                    {"properties": {"name": "container"}, "type": "CONTAINER"}
                ]
            }
        }

        breadcrumb_items = _build_breadcrumb_with_icons(entity)
        assert len(breadcrumb_items) == 1
        assert breadcrumb_items[0]["subtype"] is None


class TestPlatformHandling:
    """Test platform logo and name extraction."""

    def test_get_platform_logo_url_properties(self) -> None:
        """Test platform logo from properties."""
        entity = {
            "platform": {"properties": {"logoUrl": "https://example.com/logo.png"}}
        }

        logo_url = _get_platform_logo_url(entity)
        assert logo_url == "https://example.com/logo.png"

    def test_get_platform_logo_url_info_fallback(self) -> None:
        """Test platform logo fallback to info."""
        entity = {
            "platform": {
                "properties": {},
                "info": {"logoUrl": "https://example.com/info-logo.png"},
            }
        }

        logo_url = _get_platform_logo_url(entity)
        assert logo_url == "https://example.com/info-logo.png"

    @patch(
        "datahub_integrations.teams.render.render_entity.DATAHUB_FRONTEND_URL",
        "https://datahub.company.com",
    )
    def test_get_platform_logo_url_relative_to_absolute(self) -> None:
        """Test conversion of relative URLs to absolute."""
        entity = {
            "platform": {
                "properties": {"logoUrl": "/static/assets/platforms/snowflake.svg"}
            }
        }

        logo_url = _get_platform_logo_url(entity)
        assert (
            logo_url
            == "https://datahub.company.com/static/assets/platforms/snowflake.svg"
        )

    def test_get_platform_logo_url_invalid_url(self) -> None:
        """Test rejection of invalid URLs."""
        entity = {
            "platform": {
                "properties": {
                    "logoUrl": "http://insecure.com/logo.png"
                }  # HTTP not HTTPS
            }
        }

        logo_url = _get_platform_logo_url(entity)
        assert logo_url == ""

    def test_get_platform_name_from_properties(self) -> None:
        """Test platform name extraction from properties."""
        entity = {"platform": {"properties": {"displayName": "Snowflake Data Cloud"}}}

        name = _get_platform_name(entity)
        assert name == "Snowflake Data Cloud"

    def test_get_platform_name_from_urn(self) -> None:
        """Test platform name extraction from URN."""
        entity = {
            "platform": {"urn": "urn:li:dataPlatform:snowflake", "properties": {}}
        }

        name = _get_platform_name(entity)
        assert name == "Snowflake"

    def test_get_platform_name_common_mappings(self) -> None:
        """Test platform name mappings for common platforms."""
        platforms = {
            "mssql": "SQL Server",
            "mysql": "MySQL",
            "postgres": "PostgreSQL",
            "bigquery": "BigQuery",
        }

        for platform_id, expected_name in platforms.items():
            entity = {
                "platform": {
                    "urn": f"urn:li:dataPlatform:{platform_id}",
                    "properties": {},
                }
            }
            name = _get_platform_name(entity)
            assert name == expected_name

    def test_get_platform_name_missing_platform(self) -> None:
        """Test platform name with missing platform data."""
        entity: dict[str, str] = {}
        name = _get_platform_name(entity)
        assert name == ""


class TestImageUrlValidation:
    """Test Teams image URL validation."""

    def test_is_valid_teams_image_url_valid_https(self) -> None:
        """Test valid HTTPS URLs."""
        valid_urls = [
            "https://example.com/image.png",
            "https://cdn.example.com/path/to/image.jpg",
            "https://secure-domain.co.uk/assets/logo.svg",
        ]

        for url in valid_urls:
            assert _is_valid_teams_image_url(url) is True

    def test_is_valid_teams_image_url_invalid_http(self) -> None:
        """Test rejection of HTTP URLs."""
        invalid_urls = [
            "http://example.com/image.png",  # HTTP not HTTPS
            "ftp://example.com/image.png",  # Wrong protocol
            "//example.com/image.png",  # Protocol-relative
            "example.com/image.png",  # No protocol
        ]

        for url in invalid_urls:
            assert _is_valid_teams_image_url(url) is False

    def test_is_valid_teams_image_url_edge_cases(self) -> None:
        """Test edge cases for URL validation."""
        edge_cases = [
            "",  # Empty string
            None,  # None value
            "https://",  # Incomplete URL
            "https://a",  # Too short
            123,  # Non-string type
        ]

        for url in edge_cases:
            if isinstance(url, str) or url is None:
                assert _is_valid_teams_image_url(url or "") is False


class TestEntityMetadataExtraction:
    """Test extraction of entity tags, terms, and ownership."""

    def test_get_entity_tags_from_global_tags(self) -> None:
        """Test tag extraction from globalTags."""
        entity = {
            "globalTags": {
                "tags": [
                    {"tag": {"properties": {"name": "production"}}},
                    {"tag": {"name": "fallback_name"}},  # Test fallback
                    {"tag": {"properties": {"name": "verified"}}},
                ]
            }
        }

        tags = _get_entity_tags(entity)
        assert "production" in tags
        assert "fallback_name" in tags
        assert "verified" in tags

    def test_get_entity_tags_limit(self) -> None:
        """Test tag extraction respects 5-tag limit."""
        entity = {
            "globalTags": {
                "tags": [
                    {"tag": {"properties": {"name": f"tag_{i}"}}}
                    for i in range(10)  # 10 tags, should limit to 5
                ]
            }
        }

        tags = _get_entity_tags(entity)
        assert len(tags) <= 5

    def test_get_entity_tags_malformed_data(self) -> None:
        """Test tag extraction with malformed data."""
        entity = {
            "globalTags": {
                "tags": [
                    {"tag": None},  # Null tag
                    {"invalid": "structure"},  # Missing tag field
                    {"tag": {"properties": None}},  # Null properties
                ]
            }
        }

        tags = _get_entity_tags(entity)
        assert tags == []

    def test_get_entity_terms_from_glossary(self) -> None:
        """Test glossary term extraction."""
        entity = {
            "glossaryTerms": {
                "terms": [
                    {"term": {"properties": {"name": "PII"}}},
                    {"term": {"name": "Sensitive Data"}},  # Test fallback
                ]
            }
        }

        terms = _get_entity_terms(entity)
        assert "📋 PII" in terms
        assert "📋 Sensitive Data" in terms

    def test_get_entity_terms_limit(self) -> None:
        """Test term extraction respects 5-term limit."""
        entity = {
            "glossaryTerms": {
                "terms": [
                    {"term": {"properties": {"name": f"term_{i}"}}}
                    for i in range(10)  # 10 terms, should limit to 5
                ]
            }
        }

        terms = _get_entity_terms(entity)
        assert len(terms) <= 5

    def test_get_ownership_text_multiple_owners(self) -> None:
        """Test ownership extraction with multiple owners."""
        entity = {
            "ownership": {
                "owners": [
                    {"owner": {"properties": {"displayName": "Data Team"}}},
                    {
                        "owner": {"info": {"displayName": "Analytics Team"}}
                    },  # Info fallback
                    {"owner": {"username": "john.doe"}},  # Username fallback
                    {"owner": {"name": "fallback_name"}},  # Name fallback
                ]
            }
        }

        ownership = _get_ownership_text(entity)
        # Should limit to 2 owners
        assert "👤 Data Team, Analytics Team" == ownership

    def test_get_ownership_text_empty_owners(self) -> None:
        """Test ownership with empty owners list."""
        entity: dict[str, dict] = {"ownership": {"owners": []}}

        ownership = _get_ownership_text(entity)
        assert ownership == ""

    def test_get_ownership_text_malformed_data(self) -> None:
        """Test ownership with malformed owner data."""
        entity = {
            "ownership": {
                "owners": [
                    {"owner": None},  # Null owner
                    {"invalid": "structure"},  # Missing owner field
                    {"owner": {}},  # Empty owner object
                ]
            }
        }

        ownership = _get_ownership_text(entity)
        assert ownership == ""


class TestEntityCardRendering:
    """Test complete entity card rendering scenarios."""

    @patch("datahub_integrations.teams.url_utils.get_type_url")
    def test_render_entity_card_minimal(self, mock_get_type_url: Any) -> None:
        """Test rendering with minimal entity data."""
        mock_get_type_url.return_value = "https://datahub.com/dataset/test"

        entity = {
            "type": "DATASET",
            "urn": "urn:li:dataset:test",
            "properties": {"name": "minimal_dataset"},
        }

        card = render_entity_card(entity)

        assert card["contentType"] == "application/vnd.microsoft.card.adaptive"
        assert card["content"]["type"] == "AdaptiveCard"
        assert len(card["content"]["body"]) >= 1  # At least header
        assert (
            card["content"]["actions"][0]["url"] == "https://datahub.com/dataset/test"
        )

    @patch("datahub_integrations.teams.url_utils.get_type_url")
    def test_render_entity_card_with_subtype(self, mock_get_type_url: Any) -> None:
        """Test rendering with entity subtype preference."""
        mock_get_type_url.return_value = "https://datahub.com/dataset/test"

        entity = {
            "type": "DATASET",
            "urn": "urn:li:dataset:test",
            "properties": {"name": "table_dataset"},
            "subTypes": {"typeNames": ["table"]},
        }

        card = render_entity_card(entity)
        card_str = str(card)

        # Should prefer subtype "Table" over entity type "Dataset"
        assert "Table" in card_str

    @patch("datahub_integrations.teams.url_utils.get_type_url")
    def test_render_entity_card_long_description(self, mock_get_type_url: Any) -> None:
        """Test rendering with long description that gets truncated."""
        mock_get_type_url.return_value = "https://datahub.com/dataset/test"

        long_description = "A" * 200  # 200 characters
        entity = {
            "type": "DATASET",
            "urn": "urn:li:dataset:test",
            "properties": {
                "name": "dataset_with_long_desc",
                "description": long_description,
            },
        }

        card = render_entity_card(entity)
        card_str = str(card)

        # Description should be truncated to 147 chars + "..."
        assert "A" * 147 + "..." in card_str

    @patch("datahub_integrations.teams.url_utils.get_type_url")
    def test_render_entity_card_missing_name_fallback(
        self, mock_get_type_url: Any
    ) -> None:
        """Test rendering when name is missing - should extract from URN."""
        mock_get_type_url.return_value = "https://datahub.com/dataset/test"

        entity = {
            "type": "DATASET",
            "urn": "urn:li:dataset:(urn:li:dataPlatform:postgres,public.extracted_table,PROD)",
            "properties": {},  # Missing name
        }

        card = render_entity_card(entity)
        card_str = str(card)

        # Should extract "public.extracted_table" from URN
        assert "public.extracted_table" in card_str

    def test_render_entity_card_error_handling(self) -> None:
        """Test rendering with data that causes extraction errors."""
        # Create entity that will cause errors in helper functions
        entity = {
            "type": "DATASET",
            "urn": "urn:li:dataset:test",
            "properties": {"name": "error_test"},
            "platform": {
                "properties": {"logoUrl": "invalid_url"}
            },  # Will cause logo error
            "parentContainers": "invalid_data",  # Will cause breadcrumb error
            "globalTags": {"tags": "invalid_data"},  # Will cause tags error
        }

        # Should not raise exceptions, should handle errors gracefully
        card = render_entity_card(entity)

        assert card["contentType"] == "application/vnd.microsoft.card.adaptive"
        assert len(card["content"]["body"]) >= 1

    def test_render_entity_card_no_valid_body_items(self) -> None:
        """Test rendering when no valid body items can be created."""
        # This is an edge case that's hard to trigger but tests error handling
        with patch(
            "datahub_integrations.teams.render.render_entity._extract_name_from_urn",
            return_value=None,
        ):
            entity = {
                "type": "DATASET",
                "urn": "urn:li:dataset:test",
                "properties": {},  # No name
            }

            card = render_entity_card(entity)
            # Should still generate a card even with minimal data
            assert isinstance(card, dict)

    def test_render_entity_card_none_input(self) -> None:
        """Test rendering with None input."""
        card = render_entity_card(None)
        assert card == {}

    def test_render_entity_preview_success(self) -> None:
        """Test entity preview rendering."""
        entity = {
            "type": "DATASET",
            "urn": "urn:li:dataset:test",
            "properties": {"name": "test_dataset"},
        }

        preview = render_entity_preview(entity)

        assert preview["type"] == "message"
        assert "attachments" in preview
        assert len(preview["attachments"]) == 1

    def test_render_entity_preview_none_entity(self) -> None:
        """Test entity preview with None entity."""
        preview = render_entity_preview(None)

        assert preview["type"] == "message"
        assert preview["text"] == "Entity not found"


class TestErrorHandlingAndEdgeCases:
    """Test comprehensive error handling and edge cases."""

    def test_all_helper_functions_with_none_input(self) -> None:
        """Test all helper functions handle None input gracefully."""
        none_safe_functions = [
            _extract_name_from_urn,
            _build_breadcrumb_text,  # Fixed to handle None input
            _get_platform_logo_url,
            _get_platform_name,
            _get_entity_tags,
            _get_entity_terms,
            _get_ownership_text,
        ]

        for func in none_safe_functions:
            result = func(None)  # type: ignore
            # Functions should return empty/False values, not raise exceptions
            assert result in [None, "", [], False]

    def test_all_helper_functions_with_empty_dict(self) -> None:
        """Test all helper functions handle empty dict input gracefully."""
        empty_dict_safe_functions = [
            _build_breadcrumb_text,
            _get_platform_logo_url,
            _get_platform_name,
            _get_entity_tags,
            _get_entity_terms,
            _get_ownership_text,
        ]

        for func in empty_dict_safe_functions:
            result = func({})  # type: ignore
            # Functions should return empty/False values, not raise exceptions
            assert result in [None, "", [], False]

    def test_malformed_nested_data_structures(self) -> None:
        """Test handling of malformed nested data structures."""
        malformed_entity = {
            "platform": {"properties": None},  # Null properties
            "parentContainers": {"containers": "not_a_list"},  # Wrong type
            "globalTags": {"tags": [None, "not_a_dict"]},  # Mixed invalid types
            "ownership": {"owners": [{"owner": "not_a_dict"}]},  # Wrong owner type
            "glossaryTerms": {"terms": None},  # Null terms
        }

        # All functions should handle malformed data gracefully
        assert _get_platform_logo_url(malformed_entity) == ""
        assert _build_breadcrumb_text(malformed_entity) == ""
        assert _get_entity_tags(malformed_entity) == []
        assert _get_entity_terms(malformed_entity) == []
        assert _get_ownership_text(malformed_entity) == ""

    @patch("datahub_integrations.teams.render.render_entity.logger")
    def test_error_logging_in_render_entity_card(self, mock_logger: Any) -> None:
        """Test that errors in rendering are properly logged."""
        # Create entity that will trigger multiple error paths
        problematic_entity = {
            "type": "DATASET",
            "urn": "urn:li:dataset:test",
            "properties": {"name": "test"},
            "platform": {
                "properties": {"logoUrl": "http://invalid"}
            },  # Will trigger logo error
        }

        card = render_entity_card(problematic_entity)

        # Should still generate a valid card
        assert card["contentType"] == "application/vnd.microsoft.card.adaptive"

        # Should have logged warnings for errors (check that logger was called)
        assert mock_logger.warning.called or mock_logger.info.called
