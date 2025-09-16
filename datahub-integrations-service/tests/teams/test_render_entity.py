from typing import Any
from unittest.mock import patch

from datahub_integrations.teams.render.render_entity import (
    _build_breadcrumb_text,
    _get_entity_tags,
    _get_ownership_text,
    _get_platform_logo_url,
    render_entity_card,
)


class TestEntityCardRendering:
    def test_build_breadcrumb_text(self) -> None:
        """Test breadcrumb building from containers (platform should not be included)."""
        entity = {
            "platform": {
                "properties": {"displayName": "Snowflake"}
            },  # Should be ignored
            "parentContainers": {
                "containers": [
                    {"properties": {"name": "demo_db"}},
                    {"properties": {"name": "public"}},
                ]
            },
        }

        breadcrumb = _build_breadcrumb_text(entity)
        # Containers are reversed to show root-to-child hierarchy
        assert breadcrumb == "public > demo_db"

    def test_build_breadcrumb_text_empty(self) -> None:
        """Test breadcrumb with no platform or containers."""
        entity: dict[str, Any] = {}
        breadcrumb = _build_breadcrumb_text(entity)
        assert breadcrumb == ""

    def test_get_platform_logo_url(self) -> None:
        """Test platform logo URL extraction."""
        entity = {
            "platform": {"properties": {"logoUrl": "https://example.com/snowflake.png"}}
        }

        logo_url = _get_platform_logo_url(entity)
        assert logo_url == "https://example.com/snowflake.png"

    def test_get_platform_logo_url_fallback(self) -> None:
        """Test platform logo URL with fallback."""
        entity = {"platform": {"info": {"logoUrl": "https://example.com/fallback.png"}}}

        logo_url = _get_platform_logo_url(entity)
        assert logo_url == "https://example.com/fallback.png"

    def test_get_entity_tags(self) -> None:
        """Test entity tags extraction."""
        entity = {
            "globalTags": {
                "tags": [
                    {"tag": {"properties": {"name": "production"}}},
                    {"tag": {"properties": {"name": "verified"}}},
                ]
            },
            "glossaryTerms": {"terms": [{"term": {"properties": {"name": "PII"}}}]},
        }

        tags = _get_entity_tags(entity)
        assert "production" in tags
        assert "verified" in tags
        assert "📋 PII" in tags

    def test_get_ownership_text(self) -> None:
        """Test ownership text extraction."""
        entity = {
            "ownership": {
                "owners": [
                    {"owner": {"properties": {"displayName": "Data Team"}}},
                    {"owner": {"username": "john.doe"}},
                ]
            }
        }

        ownership = _get_ownership_text(entity)
        assert ownership == "👤 Data Team, john.doe"

    @patch("datahub_integrations.teams.url_utils.get_type_url")
    def test_render_entity_card_basic(self, mock_get_type_url: Any) -> None:
        """Test basic entity card rendering."""
        mock_get_type_url.return_value = (
            "https://datahub.example.com/dataset/urn:li:dataset:test"
        )

        entity = {
            "type": "DATASET",
            "urn": "urn:li:dataset:test",
            "properties": {"name": "test_dataset", "description": "A test dataset"},
        }

        card = render_entity_card(entity)

        assert card["contentType"] == "application/vnd.microsoft.card.adaptive"
        assert card["content"]["type"] == "AdaptiveCard"
        assert card["content"]["version"] == "1.2"
        assert len(card["content"]["body"]) >= 2  # Header + description
        assert card["content"]["actions"][0]["title"] == "View in DataHub"

    @patch("datahub_integrations.teams.url_utils.get_type_url")
    def test_render_entity_card_enhanced(self, mock_get_type_url: Any) -> None:
        """Test enhanced entity card rendering with all features."""
        mock_get_type_url.return_value = (
            "https://datahub.example.com/dataset/urn:li:dataset:test"
        )

        entity = {
            "type": "DATASET",
            "urn": "urn:li:dataset:test",
            "properties": {
                "name": "pet_profiles",
                "description": "This table contains profile details of all pets",
            },
            "platform": {
                "properties": {
                    "displayName": "Snowflake",
                    "logoUrl": "https://example.com/snowflake.png",
                }
            },
            "parentContainers": {
                "containers": [
                    {"properties": {"name": "demo_db"}},
                    {"properties": {"name": "public"}},
                ]
            },
            "globalTags": {"tags": [{"tag": {"properties": {"name": "verified"}}}]},
            "ownership": {
                "owners": [{"owner": {"properties": {"displayName": "Data Team"}}}]
            },
        }

        card = render_entity_card(entity)

        # Check that enhanced features are present
        body = card["content"]["body"]

        # Should have header, breadcrumb, description, tags, ownership
        assert len(body) >= 4

        # Check for breadcrumb
        breadcrumb_found = any("📁" in str(item) for item in body)
        assert breadcrumb_found

        # Check for ownership
        ownership_found = any("👤" in str(item) for item in body)
        assert ownership_found

    def test_render_entity_card_empty(self) -> None:
        """Test rendering with empty entity."""
        card = render_entity_card({})
        assert card == {}

    def test_render_entity_card_none(self) -> None:
        """Test rendering with None entity."""
        card = render_entity_card(None)
        assert card == {}
