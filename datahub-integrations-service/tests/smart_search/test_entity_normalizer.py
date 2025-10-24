"""
Unit tests for EntityNormalizer.

Tests that the normalizer correctly extracts fields from entities across
different GraphQL schema versions and entity types.
"""

from datahub_integrations.smart_search.entity_normalizer import (
    EntityNormalizer,
)


class TestGetName:
    """Tests for EntityNormalizer.get_name()."""

    def test_name_from_editable_properties(self):
        """Test name extraction from editableProperties (highest priority)."""
        entity = {
            "editableProperties": {"name": "User Edited Name"},
            "properties": {"name": "Original Name"},
            "name": "Top Level Name",
        }

        assert EntityNormalizer.get_name(entity) == "User Edited Name"

    def test_name_from_editable_properties_display_name(self):
        """Test displayName extraction from editableProperties."""
        entity = {
            "editableProperties": {"displayName": "Edited Display Name"},
            "properties": {"name": "Original Name"},
        }

        assert EntityNormalizer.get_name(entity) == "Edited Display Name"

    def test_name_from_properties(self):
        """Test name extraction from properties."""
        entity = {
            "properties": {"name": "Properties Name"},
            "name": "Top Level Name",
        }

        assert EntityNormalizer.get_name(entity) == "Properties Name"

    def test_display_name_from_properties(self):
        """Test displayName extraction from properties (for CorpUser, CorpGroup)."""
        entity = {
            "properties": {"displayName": "Display Name"},
        }

        assert EntityNormalizer.get_name(entity) == "Display Name"

    def test_name_from_info_deprecated(self):
        """Test name extraction from info field (deprecated)."""
        entity = {
            "info": {"name": "Info Name"},
            "name": "Top Level Name",
        }

        assert EntityNormalizer.get_name(entity) == "Info Name"

    def test_display_name_from_info_deprecated(self):
        """Test displayName extraction from info field (deprecated)."""
        entity = {
            "info": {"displayName": "Info Display Name"},
        }

        assert EntityNormalizer.get_name(entity) == "Info Display Name"

    def test_display_name_from_editable_info_deprecated(self):
        """Test displayName extraction from editableInfo (deprecated)."""
        entity = {
            "editableInfo": {"displayName": "Editable Info Display Name"},
        }

        assert EntityNormalizer.get_name(entity) == "Editable Info Display Name"

    def test_name_from_top_level(self):
        """Test name extraction from top-level field (deprecated for Dataset)."""
        entity = {
            "name": "Top Level Name",
        }

        assert EntityNormalizer.get_name(entity) == "Top Level Name"

    def test_priority_order(self):
        """Test that editableProperties has highest priority."""
        entity = {
            "editableProperties": {"name": "Edited"},
            "properties": {"name": "Properties", "displayName": "Display"},
            "info": {"name": "Info", "displayName": "Info Display"},
            "editableInfo": {"displayName": "Editable Info Display"},
            "name": "Top Level",
        }

        # Should pick editableProperties.name first
        assert EntityNormalizer.get_name(entity) == "Edited"

    def test_priority_display_name_over_properties_name(self):
        """Test displayName priority when no editableProperties."""
        entity = {
            "editableProperties": {"displayName": "Edited Display"},
            "properties": {"name": "Properties Name"},
        }

        # editableProperties.displayName comes before properties.name
        assert EntityNormalizer.get_name(entity) == "Edited Display"

    def test_empty_entity(self):
        """Test with empty entity."""
        entity = {}

        assert EntityNormalizer.get_name(entity) == ""

    def test_none_values(self):
        """Test with None values."""
        entity = {
            "editableProperties": {"name": None},
            "properties": {"name": None, "displayName": None},
            "name": None,
        }

        assert EntityNormalizer.get_name(entity) == ""

    def test_empty_strings(self):
        """Test with empty strings."""
        entity = {
            "editableProperties": {"name": ""},
            "properties": {"name": "Valid Name"},
        }

        # Empty string is falsy, should fall through to properties.name
        assert EntityNormalizer.get_name(entity) == "Valid Name"

    def test_missing_nested_keys(self):
        """Test with missing nested keys."""
        entity = {
            "editableProperties": {},
            "properties": {},
        }

        assert EntityNormalizer.get_name(entity) == ""

    def test_none_nested_objects(self):
        """Test with None nested objects."""
        entity = {
            "editableProperties": None,
            "properties": None,
            "info": None,
            "name": "Valid Name",
        }

        assert EntityNormalizer.get_name(entity) == "Valid Name"


class TestGetDescription:
    """Tests for EntityNormalizer.get_description()."""

    def test_description_from_properties(self):
        """Test description extraction from properties (most common)."""
        entity = {
            "properties": {"description": "Properties Description"},
            "editableProperties": {"description": "Edited Description"},
        }

        # properties has priority over editableProperties for description
        assert EntityNormalizer.get_description(entity) == "Properties Description"

    def test_description_from_editable_properties(self):
        """Test description extraction from editableProperties."""
        entity = {
            "editableProperties": {"description": "Edited Description"},
            "info": {"description": "Info Description"},
        }

        assert EntityNormalizer.get_description(entity) == "Edited Description"

    def test_description_from_info(self):
        """Test description extraction from info field (for Dashboard, Chart)."""
        entity = {
            "info": {"description": "Info Description"},
        }

        assert EntityNormalizer.get_description(entity) == "Info Description"

    def test_description_from_editable_info_deprecated(self):
        """Test description extraction from editableInfo (deprecated)."""
        entity = {
            "editableInfo": {"description": "Editable Info Description"},
        }

        assert EntityNormalizer.get_description(entity) == "Editable Info Description"

    def test_description_from_top_level(self):
        """Test description extraction from top-level field."""
        entity = {
            "description": "Top Level Description",
        }

        assert EntityNormalizer.get_description(entity) == "Top Level Description"

    def test_priority_order(self):
        """Test that properties.description has highest priority."""
        entity = {
            "properties": {"description": "Properties"},
            "editableProperties": {"description": "Edited"},
            "info": {"description": "Info"},
            "editableInfo": {"description": "Editable Info"},
            "description": "Top Level",
        }

        # Should pick properties.description first
        assert EntityNormalizer.get_description(entity) == "Properties"

    def test_empty_entity(self):
        """Test with empty entity."""
        entity = {}

        assert EntityNormalizer.get_description(entity) == ""

    def test_none_values(self):
        """Test with None values."""
        entity = {
            "properties": {"description": None},
            "editableProperties": {"description": None},
            "description": None,
        }

        assert EntityNormalizer.get_description(entity) == ""

    def test_empty_strings(self):
        """Test with empty strings."""
        entity = {
            "properties": {"description": ""},
            "editableProperties": {"description": "Valid Description"},
        }

        # Empty string is falsy, should fall through
        assert EntityNormalizer.get_description(entity) == "Valid Description"

    def test_long_description(self):
        """Test with very long description."""
        long_desc = "A" * 10000

        entity = {
            "properties": {"description": long_desc},
        }

        # Should return full description without truncation
        assert EntityNormalizer.get_description(entity) == long_desc
        assert len(EntityNormalizer.get_description(entity)) == 10000

    def test_special_characters(self):
        """Test description with special characters."""
        entity = {
            "properties": {
                "description": "Description with <special> & characters, émojis 🎉"
            },
        }

        assert (
            EntityNormalizer.get_description(entity)
            == "Description with <special> & characters, émojis 🎉"
        )

    def test_multiline_description(self):
        """Test description with newlines."""
        entity = {
            "properties": {"description": "Line 1\nLine 2\nLine 3"},
        }

        # Should preserve newlines
        assert EntityNormalizer.get_description(entity) == "Line 1\nLine 2\nLine 3"


class TestGetQualifiedName:
    """Tests for EntityNormalizer.get_qualified_name()."""

    def test_qualified_name_from_properties(self):
        """Test qualifiedName extraction from properties."""
        entity = {
            "properties": {
                "name": "table",
                "qualifiedName": "database.schema.table",
            },
        }

        assert EntityNormalizer.get_qualified_name(entity) == "database.schema.table"

    def test_qualified_name_only_location(self):
        """Test that qualifiedName only exists in properties."""
        entity = {
            "properties": {"qualifiedName": "db.schema.table"},
            "editableProperties": {
                "qualifiedName": "should_not_exist"
            },  # This shouldn't exist
        }

        # Should only check properties
        assert EntityNormalizer.get_qualified_name(entity) == "db.schema.table"

    def test_missing_qualified_name(self):
        """Test entity without qualifiedName."""
        entity = {
            "properties": {"name": "table"},
        }

        assert EntityNormalizer.get_qualified_name(entity) == ""

    def test_empty_entity(self):
        """Test with empty entity."""
        entity = {}

        assert EntityNormalizer.get_qualified_name(entity) == ""

    def test_none_properties(self):
        """Test with None properties."""
        entity = {
            "properties": None,
        }

        assert EntityNormalizer.get_qualified_name(entity) == ""

    def test_none_qualified_name(self):
        """Test with None qualifiedName."""
        entity = {
            "properties": {"qualifiedName": None},
        }

        assert EntityNormalizer.get_qualified_name(entity) == ""

    def test_empty_qualified_name(self):
        """Test with empty qualifiedName."""
        entity = {
            "properties": {"qualifiedName": ""},
        }

        assert EntityNormalizer.get_qualified_name(entity) == ""

    def test_special_characters_in_qualified_name(self):
        """Test qualifiedName with special characters."""
        entity = {
            "properties": {"qualifiedName": "database-prod.schema_v2.table$snapshot"},
        }

        assert (
            EntityNormalizer.get_qualified_name(entity)
            == "database-prod.schema_v2.table$snapshot"
        )


class TestIntegrationScenarios:
    """Integration tests for realistic entity scenarios."""

    def test_dataset_entity_complete(self):
        """Test typical Dataset entity with all common fields."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
            "name": "table",  # Top-level deprecated
            "properties": {
                "name": "customers",
                "qualifiedName": "prod_db.analytics.customers",
                "description": "Customer data table",
            },
            "editableProperties": {
                "name": "Customers (Marketing)",  # User-edited display name
                "description": "Updated: Marketing customer data",
            },
        }

        # Should pick user-edited name
        assert EntityNormalizer.get_name(entity) == "Customers (Marketing)"
        # Should pick system description (properties has priority)
        assert EntityNormalizer.get_description(entity) == "Customer data table"
        # Should get qualified name
        assert (
            EntityNormalizer.get_qualified_name(entity) == "prod_db.analytics.customers"
        )

    def test_dashboard_entity_with_info(self):
        """Test Dashboard entity with deprecated info field."""
        entity = {
            "urn": "urn:li:dashboard:(looker,dashboard_123)",
            "properties": {
                "name": "Sales Dashboard",
                "description": "Dashboard for sales metrics",
            },
            "info": {
                "name": "Sales Dashboard (Deprecated)",
                "description": "Old description",
            },
        }

        # Should pick properties over info
        assert EntityNormalizer.get_name(entity) == "Sales Dashboard"
        assert EntityNormalizer.get_description(entity) == "Dashboard for sales metrics"

    def test_corp_user_entity(self):
        """Test CorpUser entity with displayName."""
        entity = {
            "urn": "urn:li:corpuser:jdoe",
            "properties": {
                "displayName": "John Doe",
                "email": "jdoe@example.com",
            },
            "editableProperties": {
                "displayName": "John Doe (VP Engineering)",
            },
        }

        # Should pick edited displayName
        assert EntityNormalizer.get_name(entity) == "John Doe (VP Engineering)"

    def test_corp_user_with_deprecated_fields(self):
        """Test CorpUser with deprecated info/editableInfo."""
        entity = {
            "info": {"displayName": "Jane Smith"},
            "editableInfo": {"displayName": "Jane Smith (CTO)"},
        }

        # editableInfo.displayName should have priority over info.displayName
        # but properties should have priority over both
        # Since no properties/editableProperties, should pick info
        assert EntityNormalizer.get_name(entity) == "Jane Smith"

    def test_chart_entity(self):
        """Test Chart entity."""
        entity = {
            "urn": "urn:li:chart:(looker,chart_456)",
            "properties": {
                "name": "Revenue Chart",
                "description": "Monthly revenue trend",
            },
            "editableProperties": {
                "description": "Updated monthly revenue trend with projections",
            },
        }

        assert EntityNormalizer.get_name(entity) == "Revenue Chart"
        # properties.description has priority
        assert EntityNormalizer.get_description(entity) == "Monthly revenue trend"

    def test_schema_field_entity(self):
        """Test schemaField entity (column)."""
        entity = {
            "urn": "urn:li:schemaField:(urn:li:dataset:(...),customer_id)",
            "properties": {
                "description": "Customer identifier",
            },
        }

        # SchemaField doesn't have name in properties, only description
        assert EntityNormalizer.get_name(entity) == ""
        assert EntityNormalizer.get_description(entity) == "Customer identifier"

    def test_glossary_term_entity(self):
        """Test GlossaryTerm entity."""
        entity = {
            "urn": "urn:li:glossaryTerm:CustomerData",
            "properties": {
                "name": "Customer Data",
                "description": "Data related to customers",
            },
        }

        assert EntityNormalizer.get_name(entity) == "Customer Data"
        assert EntityNormalizer.get_description(entity) == "Data related to customers"

    def test_container_entity(self):
        """Test Container entity."""
        entity = {
            "urn": "urn:li:container:123",
            "properties": {
                "name": "Analytics Schema",
                "description": "Schema containing analytics tables",
                "qualifiedName": "prod_db.analytics",
            },
        }

        assert EntityNormalizer.get_name(entity) == "Analytics Schema"
        assert (
            EntityNormalizer.get_description(entity)
            == "Schema containing analytics tables"
        )
        assert EntityNormalizer.get_qualified_name(entity) == "prod_db.analytics"

    def test_minimal_entity(self):
        """Test minimal entity with only URN."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)",
        }

        assert EntityNormalizer.get_name(entity) == ""
        assert EntityNormalizer.get_description(entity) == ""
        assert EntityNormalizer.get_qualified_name(entity) == ""

    def test_entity_with_only_top_level_name(self):
        """Test Dataset with only deprecated top-level name."""
        entity = {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)",
            "name": "table",  # Deprecated but should still work
        }

        assert EntityNormalizer.get_name(entity) == "table"


class TestEdgeCases:
    """Edge case tests for EntityNormalizer."""

    # Whitespace handling

    def test_name_with_only_whitespace(self):
        """Test name that is only whitespace (should be treated as falsy)."""
        entity = {
            "editableProperties": {"name": "   "},
            "properties": {"name": "Valid Name"},
        }

        # Whitespace-only string is truthy in Python, so it should be returned
        # This is intentional - if a user sets a whitespace name, we preserve it
        assert EntityNormalizer.get_name(entity) == "   "

    def test_description_with_only_whitespace(self):
        """Test description that is only whitespace."""
        entity = {
            "properties": {"description": "  \n  \t  "},
            "editableProperties": {"description": "Valid Description"},
        }

        # Should return whitespace (truthy in Python)
        assert EntityNormalizer.get_description(entity) == "  \n  \t  "

    def test_mixed_whitespace_and_empty(self):
        """Test mixed whitespace and empty strings."""
        entity = {
            "editableProperties": {"name": ""},
            "properties": {"name": "  "},  # Whitespace
        }

        # Empty string is falsy, should fall through to whitespace
        assert EntityNormalizer.get_name(entity) == "  "

    # Non-string type handling

    def test_name_as_number(self):
        """Test when name field is a number (unusual but possible)."""
        entity = {
            "properties": {"name": 12345},
        }

        # Should return the number (truthy)
        assert EntityNormalizer.get_name(entity) == 12345

    def test_name_as_boolean_true(self):
        """Test when name field is boolean True."""
        entity = {
            "properties": {"name": True},
        }

        assert EntityNormalizer.get_name(entity) is True

    def test_name_as_boolean_false(self):
        """Test when name field is boolean False (falsy)."""
        entity = {
            "properties": {"name": False},
            "name": "Fallback Name",
        }

        # False is falsy, should fall through
        assert EntityNormalizer.get_name(entity) == "Fallback Name"

    def test_name_as_zero(self):
        """Test when name field is 0 (falsy)."""
        entity = {
            "properties": {"name": 0},
            "name": "Fallback",
        }

        # 0 is falsy, should fall through
        assert EntityNormalizer.get_name(entity) == "Fallback"

    def test_description_as_list(self):
        """Test when description is a list (malformed data)."""
        entity = {
            "properties": {"description": ["First", "Second"]},
        }

        # List is truthy, should be returned as-is
        assert EntityNormalizer.get_description(entity) == ["First", "Second"]

    def test_description_as_empty_list(self):
        """Test when description is empty list (falsy)."""
        entity = {
            "properties": {"description": []},
            "editableProperties": {"description": "Valid"},
        }

        # Empty list is falsy, should fall through
        assert EntityNormalizer.get_description(entity) == "Valid"

    def test_description_as_dict(self):
        """Test when description is a dict (malformed data)."""
        entity = {
            "properties": {"description": {"text": "Description"}},
        }

        # Dict is truthy, should be returned as-is
        assert EntityNormalizer.get_description(entity) == {"text": "Description"}

    def test_description_as_empty_dict(self):
        """Test when description is empty dict (falsy)."""
        entity = {
            "properties": {"description": {}},
            "editableProperties": {"description": "Valid"},
        }

        # Empty dict is falsy, should fall through
        assert EntityNormalizer.get_description(entity) == "Valid"

    # Properties as wrong types

    def test_properties_as_string(self):
        """Test when properties is a string instead of dict."""
        entity = {
            "properties": "not a dict",
            "name": "Fallback Name",
        }

        # Should handle gracefully and fall through
        assert EntityNormalizer.get_name(entity) == "Fallback Name"

    def test_properties_as_list(self):
        """Test when properties is a list instead of dict."""
        entity = {
            "properties": ["item1", "item2"],
            "name": "Fallback Name",
        }

        # Should handle gracefully and fall through
        assert EntityNormalizer.get_name(entity) == "Fallback Name"

    def test_editable_properties_as_number(self):
        """Test when editableProperties is a number."""
        entity = {
            "editableProperties": 123,
            "properties": {"name": "Valid Name"},
        }

        # Should handle gracefully
        assert EntityNormalizer.get_name(entity) == "Valid Name"

    def test_info_as_boolean(self):
        """Test when info is a boolean."""
        entity = {
            "info": True,
            "properties": {"description": "Valid"},
        }

        assert EntityNormalizer.get_description(entity) == "Valid"

    # Multiple None/empty scenarios

    def test_all_name_fields_empty_or_none(self):
        """Test when all name fields are either None or empty."""
        entity = {
            "editableProperties": {"name": None, "displayName": ""},
            "properties": {"name": "", "displayName": None},
            "info": {"name": None},
            "name": None,
        }

        # Should return empty string when nothing is found
        assert EntityNormalizer.get_name(entity) == ""

    def test_all_description_fields_none(self):
        """Test when all description fields are None."""
        entity = {
            "properties": {"description": None},
            "editableProperties": {"description": None},
            "info": {"description": None},
            "editableInfo": {"description": None},
            "description": None,
        }

        assert EntityNormalizer.get_description(entity) == ""

    def test_alternating_none_and_empty(self):
        """Test alternating None and empty string values."""
        entity = {
            "editableProperties": {"name": None},
            "properties": {"name": "", "displayName": None},
            "info": {"name": ""},
            "editableInfo": {"displayName": None},
            "name": "Final Fallback",
        }

        # Should eventually reach the final fallback
        assert EntityNormalizer.get_name(entity) == "Final Fallback"

    # Very long strings

    def test_very_long_name(self):
        """Test with very long name (10K characters)."""
        long_name = "A" * 10000

        entity = {
            "properties": {"name": long_name},
        }

        result = EntityNormalizer.get_name(entity)
        assert result == long_name
        assert len(result) == 10000

    def test_very_long_description(self):
        """Test with very long description (100K characters)."""
        long_desc = "B" * 100000

        entity = {
            "properties": {"description": long_desc},
        }

        result = EntityNormalizer.get_description(entity)
        assert result == long_desc
        assert len(result) == 100000

    # Special string formats

    def test_name_with_null_bytes(self):
        """Test name with null bytes."""
        entity = {
            "properties": {"name": "Name\x00WithNull"},
        }

        assert EntityNormalizer.get_name(entity) == "Name\x00WithNull"

    def test_description_with_control_characters(self):
        """Test description with various control characters."""
        desc = "Line1\r\nLine2\tTabbed\x1b[31mColored\x1b[0m"

        entity = {
            "properties": {"description": desc},
        }

        assert EntityNormalizer.get_description(entity) == desc

    def test_name_with_only_unicode_spaces(self):
        """Test name with unicode non-breaking spaces."""
        entity = {
            "properties": {"name": "\u00a0\u2000\u2001"},  # Various unicode spaces
        }

        # Unicode spaces are truthy (not empty strings)
        assert EntityNormalizer.get_name(entity) == "\u00a0\u2000\u2001"

    # Qualified name edge cases

    def test_qualified_name_as_number(self):
        """Test qualified name as a number."""
        entity = {
            "properties": {"qualifiedName": 12345},
        }

        # Should return the number converted by the if check
        result = EntityNormalizer.get_qualified_name(entity)
        assert result == 12345

    def test_qualified_name_as_false(self):
        """Test qualified name as boolean False."""
        entity = {
            "properties": {"qualifiedName": False},
        }

        # False is falsy, should return empty string
        assert EntityNormalizer.get_qualified_name(entity) == ""

    def test_qualified_name_with_whitespace(self):
        """Test qualified name with only whitespace."""
        entity = {
            "properties": {"qualifiedName": "   "},
        }

        # Whitespace is truthy, should be returned
        assert EntityNormalizer.get_qualified_name(entity) == "   "

    # Nested dict issues

    def test_deeply_nested_none_propagation(self):
        """Test that None properly propagates through all levels."""
        entity = {
            "editableProperties": None,
            "properties": None,
            "info": None,
            "editableInfo": None,
            "description": "Final Fallback",
        }

        assert EntityNormalizer.get_description(entity) == "Final Fallback"

    def test_partial_nested_dict(self):
        """Test with properties that has some keys but not the ones we want."""
        entity = {
            "properties": {
                "qualifiedName": "db.schema.table",
                "customProperties": [],
                # No 'name' or 'description'
            },
            "name": "Fallback Name",
        }

        assert EntityNormalizer.get_name(entity) == "Fallback Name"
        assert EntityNormalizer.get_description(entity) == ""
        assert EntityNormalizer.get_qualified_name(entity) == "db.schema.table"

    # Combined field scenarios

    def test_name_in_properties_and_displayname_in_editable(self):
        """Test when name is in properties and displayName in editableProperties."""
        entity = {
            "editableProperties": {"displayName": "Edited Display"},
            "properties": {"name": "Properties Name"},
        }

        # editableProperties.displayName should win
        assert EntityNormalizer.get_name(entity) == "Edited Display"

    def test_description_priority_with_all_fields_present(self):
        """Test description priority when all possible fields have values."""
        entity = {
            "properties": {"description": "Properties Desc"},
            "editableProperties": {"description": "Edited Desc"},
            "info": {"description": "Info Desc"},
            "editableInfo": {"description": "Editable Info Desc"},
            "description": "Top Level Desc",
        }

        # properties.description should have highest priority
        assert EntityNormalizer.get_description(entity) == "Properties Desc"

    # Entity with mixed valid and invalid fields

    def test_mixed_valid_invalid_fields(self):
        """Test entity with mix of valid and invalid field types."""
        entity = {
            "editableProperties": 123,  # Invalid
            "properties": {
                "name": None,
                "displayName": False,
                "qualifiedName": "valid.qualified.name",
            },
            "info": ["not", "a", "dict"],  # Invalid
            "name": "Final Name",
        }

        assert EntityNormalizer.get_name(entity) == "Final Name"
        assert EntityNormalizer.get_qualified_name(entity) == "valid.qualified.name"

    # Stress test with many fields

    def test_entity_with_many_unrelated_fields(self):
        """Test that normalizer ignores unrelated fields."""
        entity = {
            "urn": "urn:li:dataset:(...)",
            "type": "DATASET",
            "platform": {"urn": "urn:li:dataPlatform:snowflake"},
            "schemaMetadata": {"fields": []},
            "tags": {"tags": []},
            "ownership": {"owners": []},
            "properties": {
                "name": "Table Name",
                "description": "Table Description",
                "qualifiedName": "db.schema.table",
                "created": {"time": 1234567890},
                "customProperties": [],
            },
            "editableProperties": {},
        }

        assert EntityNormalizer.get_name(entity) == "Table Name"
        assert EntityNormalizer.get_description(entity) == "Table Description"
        assert EntityNormalizer.get_qualified_name(entity) == "db.schema.table"
