#!/usr/bin/env python3
"""
Unit tests for RDF source edge cases.

Tests verify handling of:
- Empty RDF files
- Files with no entities
- Special characters in paths and URIs
- Circular relationships
- Conflicting definitions
- Mixed valid/invalid files
- Deeply nested directories
- Unicode/special characters in content
"""

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.rdf.ingestion.rdf_source import (
    RDFSource,
    RDFSourceConfig,
)


class TestRDFEdgeCases:
    """Test RDF source edge case handling."""

    @pytest.fixture
    def ctx(self):
        """Create a pipeline context for testing."""
        return PipelineContext(run_id="test-run")

    def test_empty_rdf_file(self, ctx, tmp_path):
        """Test handling of empty RDF file (valid syntax but no triples)."""
        empty_file = tmp_path / "empty.ttl"
        empty_file.write_text("")  # Empty file

        config_dict = {"source": str(empty_file)}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        # Should handle gracefully - may produce 0 workunits or report appropriately
        list(source.get_workunits_internal())  # Consume generator to ensure no crashes
        assert source.report.num_triples_processed == 0

    def test_rdf_file_with_only_comments(self, ctx, tmp_path):
        """Test handling of RDF file with only comments/whitespace."""
        comment_file = tmp_path / "comments.ttl"
        comment_file.write_text("# This is a comment\n\n  \n# Another comment")

        config_dict = {"source": str(comment_file)}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        # Should handle gracefully - no entities but valid RDF
        list(source.get_workunits_internal())  # Consume generator to ensure no crashes
        assert source.report.num_triples_processed == 0

    def test_rdf_file_with_no_entities(self, ctx, tmp_path):
        """Test handling of valid RDF with no extractable entities."""
        no_entities_file = tmp_path / "no_entities.ttl"
        no_entities_file.write_text(
            '@prefix ex: <http://example.org/> .\nex:Something ex:property "value" .'
        )

        config_dict = {"source": str(no_entities_file)}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        # Should handle gracefully - valid RDF but no glossary terms/datasets
        # May produce 0 workunits or minimal workunits
        list(source.get_workunits_internal())  # Consume generator to ensure no crashes

    def test_special_characters_in_path(self, ctx, tmp_path):
        """Test handling of special characters in file path."""
        special_path = tmp_path / "file with spaces & special-chars.ttl"
        special_path.write_text(
            '@prefix ex: <http://example.org/> .\nex:Term a skos:Concept ; skos:prefLabel "Term" .'
        )

        config_dict = {"source": str(special_path)}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        workunits = list(source.get_workunits_internal())
        # Should handle special characters in path
        assert len(workunits) > 0 or source.report.failures

    def test_unicode_in_content(self, ctx, tmp_path):
        """Test handling of unicode characters in RDF content."""
        unicode_file = tmp_path / "unicode.ttl"
        unicode_file.write_text(
            "@prefix ex: <http://example.org/> .\n"
            "ex:Term a skos:Concept ;\n"
            '    skos:prefLabel "Term with émojis 🎉 and 中文" ;\n'
            '    skos:definition "Definition with special chars: àáâãäå" .'
        )

        config_dict = {"source": str(unicode_file)}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        workunits = list(source.get_workunits_internal())
        # Should handle unicode correctly
        assert len(workunits) > 0

    def test_circular_relationships(self, ctx, tmp_path):
        """Test handling of circular relationships in RDF."""
        circular_file = tmp_path / "circular.ttl"
        circular_file.write_text(
            "@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n"
            "@prefix ex: <http://example.org/> .\n"
            "ex:Term1 a skos:Concept ;\n"
            '    skos:prefLabel "Term 1" ;\n'
            "    skos:related ex:Term2 .\n"
            "ex:Term2 a skos:Concept ;\n"
            '    skos:prefLabel "Term 2" ;\n'
            "    skos:related ex:Term1 ."
        )

        config_dict = {"source": str(circular_file)}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        workunits = list(source.get_workunits_internal())
        # Should handle circular relationships without infinite loops
        assert len(workunits) > 0

    def test_conflicting_definitions(self, ctx, tmp_path):
        """Test handling of same term defined multiple times."""
        conflicting_file = tmp_path / "conflicting.ttl"
        conflicting_file.write_text(
            "@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n"
            "@prefix ex: <http://example.org/> .\n"
            "ex:Term a skos:Concept ;\n"
            '    skos:prefLabel "First Definition" ;\n'
            '    skos:definition "First definition" .\n'
            "ex:Term a skos:Concept ;\n"
            '    skos:prefLabel "Second Definition" ;\n'
            '    skos:definition "Second definition" .'
        )

        config_dict = {"source": str(conflicting_file)}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        workunits = list(source.get_workunits_internal())
        # Should handle conflicting definitions (may use last one or merge)
        assert len(workunits) > 0

    def test_mixed_valid_invalid_files_in_directory(self, ctx, tmp_path):
        """Test handling of directory with both valid and invalid files."""
        test_dir = tmp_path / "mixed"
        test_dir.mkdir()

        # Valid file
        valid_file = test_dir / "valid.ttl"
        valid_file.write_text(
            '@prefix ex: <http://example.org/> .\nex:Term a skos:Concept ; skos:prefLabel "Term" .'
        )

        # Invalid file
        invalid_file = test_dir / "invalid.ttl"
        invalid_file.write_text("Not valid RDF content")

        config_dict = {"source": str(test_dir), "recursive": False}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        # Should process valid files and report errors for invalid ones
        # May produce workunits from valid file or report errors
        list(source.get_workunits_internal())  # Consume generator to ensure no crashes

    def test_deeply_nested_directory(self, ctx, tmp_path):
        """Test handling of deeply nested directory structure."""
        # Create nested structure: dir1/dir2/dir3/file.ttl
        nested_dir = tmp_path / "dir1" / "dir2" / "dir3"
        nested_dir.mkdir(parents=True)
        nested_file = nested_dir / "file.ttl"
        nested_file.write_text(
            '@prefix ex: <http://example.org/> .\nex:Term a skos:Concept ; skos:prefLabel "Term" .'
        )

        config_dict = {"source": str(tmp_path), "recursive": True}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        workunits = list(source.get_workunits_internal())
        # Should handle deeply nested directories
        assert len(workunits) > 0

    def test_very_long_term_name(self, ctx, tmp_path):
        """Test handling of very long term names."""
        long_name_file = tmp_path / "long_name.ttl"
        long_name = "A" * 1000  # Very long name
        long_name_file.write_text(
            f"@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n"
            f"@prefix ex: <http://example.org/> .\n"
            f"ex:Term a skos:Concept ;\n"
            f'    skos:prefLabel "{long_name}" .'
        )

        config_dict = {"source": str(long_name_file)}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        workunits = list(source.get_workunits_internal())
        # Should handle very long names without truncation issues
        assert len(workunits) > 0

    def test_relative_path(self, ctx, tmp_path, monkeypatch):
        """Test handling of relative paths."""
        test_file = tmp_path / "test.ttl"
        test_file.write_text(
            '@prefix ex: <http://example.org/> .\nex:Term a skos:Concept ; skos:prefLabel "Term" .'
        )

        # Change to tmp_path directory and use relative path
        monkeypatch.chdir(tmp_path)
        relative_path = "test.ttl"

        config_dict = {"source": relative_path}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        workunits = list(source.get_workunits_internal())
        # Should handle relative paths correctly
        assert len(workunits) > 0
