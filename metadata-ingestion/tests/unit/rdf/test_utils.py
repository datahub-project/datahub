"""
Tests for RDF utility functions.
"""

import unittest

from datahub.ingestion.source.rdf.core.utils import entity_type_to_field_name


class TestUtils(unittest.TestCase):
    """Test cases for utility functions."""

    def test_entity_type_to_field_name_basic(self):
        """Test basic entity type to field name conversion."""
        self.assertEqual(entity_type_to_field_name("glossary_term"), "glossary_terms")
        self.assertEqual(entity_type_to_field_name("domain"), "domains")
        self.assertEqual(entity_type_to_field_name("relationship"), "relationships")

    def test_entity_type_to_field_name_already_plural(self):
        """Test entity types that are already plural."""
        self.assertEqual(entity_type_to_field_name("glossary_terms"), "glossary_terms")
        self.assertEqual(entity_type_to_field_name("domains"), "domains")
        self.assertEqual(entity_type_to_field_name("relationships"), "relationships")

    def test_entity_type_to_field_name_ends_with_y(self):
        """Test entity types ending with 'y' (should become 'ies')."""
        self.assertEqual(entity_type_to_field_name("category"), "categories")
        self.assertEqual(entity_type_to_field_name("property"), "properties")

    def test_entity_type_to_field_name_lineage_special_case(self):
        """Test that 'lineage' entity type is no longer supported (removed for MVP)."""
        # Lineage special case removed - should now just pluralize normally
        self.assertEqual(entity_type_to_field_name("lineage"), "lineages")

    def test_entity_type_to_field_name_edge_cases(self):
        """Test edge cases."""
        # Empty string gets pluralized (adds 's')
        self.assertEqual(entity_type_to_field_name(""), "s")
        self.assertEqual(entity_type_to_field_name("a"), "as")
        self.assertEqual(entity_type_to_field_name("entity"), "entities")


if __name__ == "__main__":
    unittest.main()
