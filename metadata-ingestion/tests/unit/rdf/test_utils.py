"""
Tests for RDF utility functions.
"""

import unittest

from datahub.ingestion.source.rdf.core.utils import entity_type_to_field_name


class TestUtils(unittest.TestCase):
    """Test cases for utility functions."""

    def test_entity_type_to_field_name_basic(self):
        """Test basic entity type to field name conversion."""
        self.assertEqual(entity_type_to_field_name("dataset"), "datasets")
        self.assertEqual(entity_type_to_field_name("glossary_term"), "glossary_terms")
        self.assertEqual(
            entity_type_to_field_name("structured_property"), "structured_properties"
        )

    def test_entity_type_to_field_name_already_plural(self):
        """Test entity types that are already plural."""
        self.assertEqual(entity_type_to_field_name("datasets"), "datasets")
        self.assertEqual(entity_type_to_field_name("terms"), "terms")

    def test_entity_type_to_field_name_ends_with_y(self):
        """Test entity types ending with 'y' (should become 'ies')."""
        self.assertEqual(entity_type_to_field_name("category"), "categories")
        self.assertEqual(entity_type_to_field_name("property"), "properties")

    def test_entity_type_to_field_name_lineage_special_case(self):
        """Test special case for 'lineage' entity type."""
        self.assertEqual(entity_type_to_field_name("lineage"), "lineage_relationships")

    def test_entity_type_to_field_name_edge_cases(self):
        """Test edge cases."""
        # Empty string gets pluralized (adds 's')
        self.assertEqual(entity_type_to_field_name(""), "s")
        self.assertEqual(entity_type_to_field_name("a"), "as")
        self.assertEqual(entity_type_to_field_name("entity"), "entities")


if __name__ == "__main__":
    unittest.main()
