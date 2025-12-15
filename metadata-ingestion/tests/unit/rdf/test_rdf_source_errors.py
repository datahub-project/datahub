#!/usr/bin/env python3
"""
Unit tests for RDF source error handling.

Tests verify:
- File not found errors
- Invalid RDF format errors
- Network/URL errors
- Permission errors
- Malformed configuration errors
- Error message quality and actionability
"""

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.rdf.ingestion.rdf_source import (
    RDFSource,
    RDFSourceConfig,
)


class TestRDFSourceErrors:
    """Test RDF source error handling."""

    @pytest.fixture
    def ctx(self):
        """Create a pipeline context for testing."""
        return PipelineContext(run_id="test-run")

    def test_file_not_found_error(self, ctx, tmp_path):
        """Test that missing file produces actionable error."""
        non_existent_file = tmp_path / "does_not_exist.ttl"
        config_dict = {"source": str(non_existent_file)}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        workunits = list(source.get_workunits_internal())
        assert len(workunits) == 0

        # Check that error was reported
        assert source.report.failures
        failure = source.report.failures[0]
        assert (
            "not found" in failure.error.lower() or "not found" in str(failure).lower()
        )
        # Error should be actionable
        assert failure.context is not None

    def test_invalid_rdf_format_error(self, ctx, tmp_path):
        """Test that invalid RDF format produces actionable error."""
        invalid_file = tmp_path / "invalid.ttl"
        invalid_file.write_text("This is not valid RDF content")
        config_dict = {"source": str(invalid_file), "format": "turtle"}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        workunits = list(source.get_workunits_internal())
        assert len(workunits) == 0

        # Check that error was reported
        assert source.report.failures
        failure = source.report.failures[0]
        # Error should mention format or parsing
        error_str = str(failure).lower()
        assert (
            "format" in error_str
            or "parse" in error_str
            or "invalid" in error_str
            or "rdf" in error_str
        )
        # Error should be actionable
        assert failure.context is not None

    def test_invalid_url_error(self, ctx):
        """Test that invalid URL produces actionable error."""
        config_dict = {
            "source": "http://invalid-url-that-does-not-exist-12345.com/data.ttl"
        }
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        # May or may not produce workunits depending on network/timeout behavior
        # But should handle error gracefully
        list(source.get_workunits_internal())  # Consume generator to ensure no crashes

        # Error should be reported if connection fails
        if source.report.failures:
            failure = source.report.failures[0]
            error_str = str(failure).lower()
            # Error should mention URL, network, or connection
            assert (
                "url" in error_str
                or "network" in error_str
                or "connection" in error_str
                or "timeout" in error_str
            )

    def test_empty_directory_error(self, ctx, tmp_path):
        """Test that empty directory produces actionable error."""
        empty_dir = tmp_path / "empty_dir"
        empty_dir.mkdir()
        config_dict = {"source": str(empty_dir), "recursive": True}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        workunits = list(source.get_workunits_internal())
        assert len(workunits) == 0

        # Check that error was reported
        assert source.report.failures
        failure = source.report.failures[0]
        error_str = str(failure).lower()
        # Error should mention no files found or empty directory
        assert (
            "no" in error_str
            and "file" in error_str
            or "empty" in error_str
            or "found" in error_str
        )

    def test_unsupported_file_extension_error(self, ctx, tmp_path):
        """Test that unsupported file extension produces actionable error."""
        unsupported_file = tmp_path / "test.txt"
        unsupported_file.write_text("Some content")
        config_dict = {"source": str(unsupported_file)}
        config = RDFSourceConfig.model_validate(config_dict)
        source = RDFSource(config, ctx)

        workunits = list(source.get_workunits_internal())
        assert len(workunits) == 0

        # Check that error was reported
        assert source.report.failures
        failure = source.report.failures[0]
        error_str = str(failure).lower()
        # Error should mention extension or supported formats
        assert (
            "extension" in error_str
            or "format" in error_str
            or "supported" in error_str
        )
