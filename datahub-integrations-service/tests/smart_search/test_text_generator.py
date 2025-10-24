"""
Unit tests for EntityTextGenerator.

Tests that text generator handles all entity types and edge cases gracefully.
"""

from unittest.mock import patch

import pytest

from datahub_integrations.smart_search.text_generator import (
    EntityTextGenerator,
    _sort_by_keyword_matches,
)


class TestSortByKeywordMatches:
    """Tests for the _sort_by_keyword_matches helper function."""

    def test_basic_sorting(self):
        """Test basic sorting by keyword matches."""
        items = [
            {"name": "user_id", "desc": "User identifier"},
            {"name": "premium_plan", "desc": "Premium subscription plan"},
            {"name": "created_at", "desc": "Creation date"},
        ]

        keywords = ["premium", "plan"]
        sorted_items = _sort_by_keyword_matches(
            items=items,
            keywords=keywords,
            text_extractor=lambda x: [x["name"], x["desc"]],
        )

        # premium_plan should be first (2 matches)
        assert sorted_items[0]["name"] == "premium_plan"
        # created_at should be last (0 matches)
        assert sorted_items[-1]["name"] == "created_at"

    def test_max_items_cap(self):
        """Test that max_items limits results."""
        items = [{"name": f"field_{i}"} for i in range(10)]
        keywords = ["field"]

        sorted_items = _sort_by_keyword_matches(
            items=items,
            keywords=keywords,
            text_extractor=lambda x: [x["name"]],
            max_items=5,
        )

        assert len(sorted_items) == 5

    def test_empty_list(self):
        """Test handling of empty input list."""
        sorted_items = _sort_by_keyword_matches(
            items=[],
            keywords=["test"],
            text_extractor=lambda x: [x.get("name", "")],
        )

        assert sorted_items == []

    def test_no_keywords(self):
        """Test with no keywords (all items have 0 matches)."""
        items = [{"name": "a"}, {"name": "b"}, {"name": "c"}]

        sorted_items = _sort_by_keyword_matches(
            items=items,
            keywords=[],
            text_extractor=lambda x: [x["name"]],
        )

        # Should return all items in original order
        assert len(sorted_items) == 3


class TestEntityTextGenerator:
    """Tests for EntityTextGenerator."""

    @pytest.fixture
    def generator(self):
        """Create a text generator instance."""
        return EntityTextGenerator()

    # Test basic dataset entity

    def test_minimal_dataset(self, generator):
        """Test dataset with only URN and name."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        assert "test_table" in text
        assert len(text) > 0

    def test_dataset_with_description(self, generator):
        """Test dataset with description."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {
                "name": "test_table",
                "description": "A test table for unit testing",
            },
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        assert "test table for unit testing" in text.lower()
        assert "test_table" in text

    def test_dataset_with_schema_fields(self, generator):
        """Test dataset with schema fields."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {"name": "test_table"},
            "schemaMetadata": {
                "fields": [
                    {
                        "fieldPath": "id",
                        "description": "Primary key identifier",
                    },
                    {
                        "fieldPath": "premium_plan_name",
                        "description": "Name of the premium subscription plan",
                    },
                    {
                        "fieldPath": "created_at",
                        "description": None,  # No description
                    },
                ]
            },
        }

        keywords = ["premium", "plan"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should include keyword-matching field first
        assert "premium_plan_name" in text
        # Should include fields
        assert "id" in text or "created_at" in text

    def test_dataset_with_many_fields(self, generator):
        """Test dataset with many fields (>50)."""
        fields = [
            {"fieldPath": f"field_{i}", "description": f"Description {i}"}
            for i in range(100)
        ]
        # Add some keyword-matching fields
        fields.append(
            {"fieldPath": "premium_tier", "description": "Premium tier level"}
        )
        fields.append({"fieldPath": "plan_name", "description": "Plan name"})

        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {"name": "test_table"},
            "schemaMetadata": {"fields": fields},
        }

        keywords = ["premium", "plan"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should include keyword-matching fields
        assert "premium_tier" in text
        assert "plan_name" in text
        # Should not fail with 100 fields
        assert len(text) > 0

    def test_dataset_with_tags(self, generator):
        """Test dataset with tags."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {"name": "test_table"},
            "tags": {
                "tags": [
                    {"tag": {"properties": {"name": "PII"}}},
                    {"tag": {"properties": {"name": "Sensitive"}}},
                ]
            },
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        assert "PII" in text or "Tagged" in text

    def test_dataset_with_owners(self, generator):
        """Test dataset with ownership."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {"name": "test_table"},
            "ownership": {
                "owners": [
                    {"owner": {"properties": {"displayName": "John Doe"}}},
                    {
                        "owner": {"properties": {"email": "jane@example.com"}}
                    },  # No displayName
                ]
            },
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should include at least one owner
        assert "John Doe" in text or "jane@example.com" in text

    def test_dataset_with_stats(self, generator):
        """Test dataset with usage statistics."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {"name": "test_table"},
            "statsSummary": {
                "queryCountLast30Days": 500,
                "rowCount": 1000000,
            },
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        assert "500" in text or "Queried" in text
        assert "1,000,000" in text or "rows" in text.lower()

    def test_dataset_with_custom_properties(self, generator):
        """Test dataset with custom properties."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {
                "name": "test_table",
                "customProperties": [
                    {"key": "business_unit", "value": "Finance"},
                    {"key": "data_sensitivity_level", "value": "Sensitive"},
                    {"key": "premium_tier", "value": "true"},  # Keyword match
                ],
            },
        }

        keywords = ["premium"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should include keyword-matching custom property
        assert "premium" in text.lower() or "Premium Tier" in text

    # Test schemaField entities

    def test_schema_field_basic(self, generator):
        """Test basic schemaField entity."""
        entity = {
            "urn": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),field_name)",
            "description": "Field description",
        }

        keywords = ["field"]
        text = generator.generate(entity, search_keywords=keywords)

        assert "field_name" in text
        assert "table" in text.lower()

    def test_schema_field_malformed_urn(self, generator):
        """Test schemaField with malformed URN doesn't crash."""
        entity = {
            "urn": "invalid_urn_format",
            "description": "Field description",
        }

        keywords = ["test"]
        # Should not raise exception
        text = generator.generate(entity, search_keywords=keywords)

        assert len(text) > 0
        # Without "schemaField:" in URN, it's treated as regular entity
        # Should include the description
        assert "Field description" in text

    # Test edge cases

    def test_empty_entity(self, generator):
        """Test empty entity dict."""
        entity = {}
        keywords = ["test"]

        # Should not crash
        text = generator.generate(entity, search_keywords=keywords)

        assert isinstance(text, str)
        assert len(text) > 0  # Should return something, not crash

    def test_entity_with_none_values(self, generator):
        """Test entity with None values."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": None,
            "properties": {
                "name": None,
                "description": None,
                "customProperties": None,
            },
            "tags": None,
            "ownership": None,
            "schemaMetadata": None,
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should handle None gracefully
        assert isinstance(text, str)
        assert len(text) > 0

    def test_entity_with_empty_nested_structures(self, generator):
        """Test entity with empty lists/dicts."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {
                "name": "test_table",
                "customProperties": [],  # Empty list
            },
            "tags": {"tags": []},  # Empty tags
            "ownership": {"owners": []},  # No owners
            "schemaMetadata": {"fields": []},  # No fields
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should still generate text with name at minimum
        assert "test_table" in text
        assert len(text) > 10

    def test_entity_with_long_description(self, generator):
        """Test entity with very long description."""
        long_desc = "A" * 10000  # 10k character description

        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {
                "name": "test_table",
                "description": long_desc,
            },
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should include the description (might be truncated in output but shouldn't crash)
        assert isinstance(text, str)
        assert len(text) > 0

    def test_field_description_truncation(self, generator):
        """Test that field descriptions get truncated to 120 chars."""
        long_field_desc = "This is a very long field description " * 10  # >120 chars

        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {"name": "test_table"},
            "schemaMetadata": {
                "fields": [
                    {"fieldPath": "long_desc_field", "description": long_field_desc}
                ]
            },
        }

        keywords = ["long"]
        text = generator.generate(entity, search_keywords=keywords)

        # Field should be mentioned
        assert "long_desc_field" in text
        # Description should be truncated to 120 chars (not full 400+ chars)
        # Original description is 400+ chars, truncated should only show first 120
        # Check that the field description portion is reasonable length
        field_desc_start = text.find(": This is a very long")
        if field_desc_start > 0:
            # Find the next field or end of fields section
            remaining = text[field_desc_start:]
            # The description part should be truncated, not showing all 10 repetitions
            # It should be cut off around 120 chars
            assert len(remaining) < 200  # Reasonable bound for truncated description

    def test_special_characters_in_names(self, generator):
        """Test entities with special characters."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table_$#@!",
            "properties": {
                "name": "test_table_$#@!",
                "description": "Table with <special> & characters",
            },
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should handle special characters without crashing
        assert isinstance(text, str)
        assert len(text) > 0

    def test_unicode_in_entity(self, generator):
        """Test entities with unicode characters."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "テーブル_table",  # Japanese characters
            "properties": {
                "name": "テーブル_table",
                "description": "Table with émojis 🎉 and ümlauts",
            },
        }

        keywords = ["table"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should handle unicode gracefully
        assert isinstance(text, str)
        assert len(text) > 0

    def test_missing_required_urn(self, generator):
        """Test entity without URN."""
        entity = {
            "name": "test_table",
            "properties": {"name": "test_table"},
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should handle missing URN gracefully
        assert "test_table" in text
        assert isinstance(text, str)

    def test_platform_name_extraction(self, generator):
        """Test that platform names are extracted correctly."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {"name": "test_table"},
            "platform": {
                "urn": "urn:li:dataPlatform:snowflake",
                "name": "snowflake",
            },
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should include platform name
        assert "Snowflake" in text or "snowflake" in text.lower()

    def test_unknown_platform(self, generator):
        """Test entity with unknown platform."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:custom_platform,test.table,PROD)",
            "name": "test_table",
            "properties": {"name": "test_table"},
            "platform": {
                "urn": "urn:li:dataPlatform:custom_platform",
                "name": "custom_platform",
            },
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should handle unknown platform gracefully
        assert isinstance(text, str)
        assert len(text) > 0

    def test_glossary_terms(self, generator):
        """Test entity with glossary terms."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {"name": "test_table"},
            "glossaryTerms": {
                "terms": [
                    {"term": {"properties": {"name": "Customer Data"}}},
                    {"term": {"properties": {"name": "Premium"}}},
                ]
            },
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should include glossary terms
        assert "Glossary" in text or "Customer Data" in text

    def test_nested_none_in_tags(self, generator):
        """Test entity with malformed tags structure."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {"name": "test_table"},
            "tags": {
                "tags": [
                    {"tag": None},  # Malformed
                    {"tag": {"properties": None}},  # Malformed
                    {"tag": {"properties": {"name": "ValidTag"}}},  # Good
                ]
            },
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should handle malformed tags gracefully
        assert isinstance(text, str)
        # Should include the valid tag if possible
        # But shouldn't crash on malformed ones

    def test_nested_none_in_ownership(self, generator):
        """Test entity with malformed ownership structure."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {"name": "test_table"},
            "ownership": {
                "owners": [
                    {"owner": None},  # Malformed
                    {"owner": {"properties": None}},  # Malformed
                    {"owner": {"properties": {"displayName": "Valid Owner"}}},  # Good
                ]
            },
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should handle malformed owners gracefully
        assert isinstance(text, str)

    def test_field_without_fieldpath(self, generator):
        """Test schema fields without fieldPath."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "name": "test_table",
            "properties": {"name": "test_table"},
            "schemaMetadata": {
                "fields": [
                    {"description": "Field without path"},  # No fieldPath
                    {"fieldPath": "valid_field", "description": "Valid field"},
                ]
            },
        }

        keywords = ["test"]
        text = generator.generate(entity, search_keywords=keywords)

        # Should include valid field, skip invalid one
        assert "valid_field" in text
        # Should not crash
        assert isinstance(text, str)

    # Test other entity types

    def test_dashboard_entity(self, generator):
        """Test dashboard entity."""
        entity = {
            "urn": "urn:li:dashboard:(looker,dashboard_123)",
            "name": "Sales Dashboard",
            "properties": {
                "name": "Sales Dashboard",
                "description": "Dashboard showing sales metrics",
            },
            "platform": {
                "urn": "urn:li:dataPlatform:looker",
                "name": "looker",
            },
        }

        keywords = ["sales"]
        text = generator.generate(entity, search_keywords=keywords)

        assert "Sales Dashboard" in text
        assert "looker" in text.lower() or "Looker" in text

    def test_chart_entity(self, generator):
        """Test chart entity."""
        entity = {
            "urn": "urn:li:chart:(looker,chart_456)",
            "name": "Revenue Chart",
            "properties": {
                "name": "Revenue Chart",
                "description": "Chart showing monthly revenue",
            },
        }

        keywords = ["revenue"]
        text = generator.generate(entity, search_keywords=keywords)

        assert "Revenue Chart" in text
        assert isinstance(text, str)


class TestEntityTextGeneratorWithMocks:
    """Tests for EntityTextGenerator with mocked EntityNormalizer."""

    @pytest.fixture
    def generator(self):
        """Create a text generator instance."""
        return EntityTextGenerator()

    @patch("datahub_integrations.smart_search.text_generator.EntityNormalizer")
    def test_uses_normalizer_for_name(self, mock_normalizer, generator):
        """Test that generator uses EntityNormalizer.get_name()."""
        # Setup mock
        mock_normalizer.get_name.return_value = "Mocked Name"
        mock_normalizer.get_description.return_value = "Mocked description"
        mock_normalizer.get_qualified_name.return_value = ""

        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "properties": {"name": "real_name"},
        }

        text = generator.generate(entity, search_keywords=[])

        # Verify normalizer was called
        mock_normalizer.get_name.assert_called_once_with(entity)
        mock_normalizer.get_description.assert_called_once_with(entity)

        # Verify mocked name is used in output
        assert "Mocked Name" in text

    @patch("datahub_integrations.smart_search.text_generator.EntityNormalizer")
    def test_uses_normalizer_for_description(self, mock_normalizer, generator):
        """Test that generator uses EntityNormalizer.get_description()."""
        # Setup mock
        mock_normalizer.get_name.return_value = "Test Table"
        mock_normalizer.get_description.return_value = "This is a mocked description"
        mock_normalizer.get_qualified_name.return_value = ""

        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
        }

        text = generator.generate(entity, search_keywords=[])

        # Verify normalizer was called
        mock_normalizer.get_description.assert_called_once_with(entity)

        # Verify mocked description is used in output
        assert "This is a mocked description" in text

    @patch("datahub_integrations.smart_search.text_generator.EntityNormalizer")
    def test_uses_normalizer_for_qualified_name(self, mock_normalizer, generator):
        """Test that generator uses EntityNormalizer.get_qualified_name()."""
        # Setup mock
        mock_normalizer.get_name.return_value = "table"
        mock_normalizer.get_description.return_value = ""
        mock_normalizer.get_qualified_name.return_value = "prod.schema.table"

        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
        }

        text = generator.generate(entity, search_keywords=[])

        # Verify normalizer was called
        mock_normalizer.get_qualified_name.assert_called_once_with(entity)

        # Verify qualified name is used in output
        assert "prod.schema.table" in text
        assert "Located at" in text

    @patch("datahub_integrations.smart_search.text_generator.EntityNormalizer")
    def test_schema_field_uses_normalizer(self, mock_normalizer, generator):
        """Test that schemaField entity uses EntityNormalizer for description."""
        # Setup mock
        mock_normalizer.get_description.return_value = (
            "Column description from normalizer"
        )

        entity = {
            "urn": "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD),field_name)",
        }

        text = generator.generate(entity, search_keywords=[])

        # Verify normalizer was called for schema field
        mock_normalizer.get_description.assert_called_with(entity)

        # Verify description is in output
        assert "Column description from normalizer" in text
        assert "field_name" in text

    @patch("datahub_integrations.smart_search.text_generator.EntityNormalizer")
    def test_handles_empty_name_from_normalizer(self, mock_normalizer, generator):
        """Test that generator handles empty name gracefully."""
        # Setup mock to return empty values
        mock_normalizer.get_name.return_value = ""
        mock_normalizer.get_description.return_value = "Some description"
        mock_normalizer.get_qualified_name.return_value = ""

        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
        }

        text = generator.generate(entity, search_keywords=[])

        # Should not crash and should include description
        assert isinstance(text, str)
        assert "Some description" in text

    @patch("datahub_integrations.smart_search.text_generator.EntityNormalizer")
    def test_handles_empty_description_from_normalizer(
        self, mock_normalizer, generator
    ):
        """Test that generator handles empty description gracefully."""
        # Setup mock to return empty description
        mock_normalizer.get_name.return_value = "Test Table"
        mock_normalizer.get_description.return_value = ""
        mock_normalizer.get_qualified_name.return_value = ""

        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
        }

        text = generator.generate(entity, search_keywords=[])

        # Should not crash and should include name
        assert isinstance(text, str)
        assert "Test Table" in text

    @patch("datahub_integrations.smart_search.text_generator.EntityNormalizer")
    def test_normalizer_called_for_all_entity_types(self, mock_normalizer, generator):
        """Test that normalizer is called for different entity types."""
        # Setup mock
        mock_normalizer.get_name.return_value = "Entity Name"
        mock_normalizer.get_description.return_value = "Entity Description"
        mock_normalizer.get_qualified_name.return_value = ""

        # Test with Dashboard
        dashboard = {
            "urn": "urn:li:dashboard:(looker,dash_123)",
        }

        text = generator.generate(dashboard, search_keywords=[])

        assert mock_normalizer.get_name.called
        assert mock_normalizer.get_description.called
        assert "Entity Name" in text

        # Reset mocks
        mock_normalizer.reset_mock()

        # Test with Chart
        chart = {
            "urn": "urn:li:chart:(looker,chart_456)",
        }

        text = generator.generate(chart, search_keywords=[])

        assert mock_normalizer.get_name.called
        assert mock_normalizer.get_description.called
        assert "Entity Name" in text
