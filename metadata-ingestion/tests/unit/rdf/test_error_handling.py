#!/usr/bin/env python3
"""
Unit tests for RDF source error handling.

These tests verify that the RDF source handles errors gracefully
and provides actionable error messages.
"""

from unittest.mock import MagicMock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.rdf.ingestion.rdf_source import (
    RDFSource,
    RDFSourceConfig,
)


@pytest.fixture
def mock_ctx():
    """Create a mock pipeline context."""
    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = MagicMock()
    ctx.pipeline_name = "test_pipeline"
    return ctx


class TestErrorHandling:
    """Test error handling in RDF source."""

    def test_missing_file_error_message(self, tmp_path, mock_ctx):
        """Test that missing file errors provide actionable messages."""
        missing_file = tmp_path / "nonexistent.ttl"
        config = RDFSourceConfig(
            source=str(missing_file),
            format="turtle",
            environment="PROD",
        )

        source = RDFSource(config, mock_ctx)
        workunits = list(source.get_workunits_internal())

        # Should produce no work units
        assert len(workunits) == 0

        # Should have failures reported
        assert len(source.report.failures) > 0

        # Check error message contains actionable information
        # Failures are tuples of (index, StructuredLogEntry)
        failure_texts = []
        for failure in source.report.failures:
            if isinstance(failure, tuple):
                _, entry = failure
                failure_texts.append(entry.message.lower())
                # Context is a LossyList[str], join it into a string
                if entry.context:
                    context_str = " ".join(entry.context).lower()
                    failure_texts.append(context_str)
            else:
                # Fallback for other formats
                failure_texts.append(str(failure).lower())

        # Check that "not found" appears in either message or context
        assert any("not found" in text for text in failure_texts)
        # Check context contains helpful information
        assert any("verify" in text or "accessible" in text for text in failure_texts)

    def test_malformed_rdf_error_message(self, tmp_path, mock_ctx):
        """Test that malformed RDF errors provide actionable messages."""
        malformed_file = tmp_path / "malformed.ttl"
        malformed_file.write_text("This is not valid RDF content")

        config = RDFSourceConfig(
            source=str(malformed_file),
            format="turtle",
            environment="PROD",
        )

        source = RDFSource(config, mock_ctx)
        list(source.get_workunits_internal())

        # Should have failures reported
        assert len(source.report.failures) > 0

        # Check error message contains actionable information
        # Failures are tuples of (index, StructuredLogEntry)
        failure_texts = []
        for failure in source.report.failures:
            if isinstance(failure, tuple):
                _, entry = failure
                failure_texts.append(entry.message.lower())
                # Context is a LossyList[str], join it into a string
                if entry.context:
                    context_str = " ".join(entry.context).lower()
                    failure_texts.append(context_str)
            else:
                # Fallback for other formats
                failure_texts.append(str(failure).lower())

        # Should mention format or validation
        assert any(
            "format" in text or "valid" in text or "invalid" in text
            for text in failure_texts
        )

    def test_large_file_performance_warning(self, tmp_path, mock_ctx, caplog):
        """Test that large files trigger performance warnings."""
        # Create a large RDF file (simulated with many triples)
        large_file = tmp_path / "large.ttl"
        # Write a file that will result in > 10000 triples when parsed
        # For testing, we'll create a file with many statements
        content = "@prefix ex: <http://example.org/> .\n"
        # Add many triples to trigger the warning
        for i in range(10001):
            content += f'ex:term{i} a ex:Concept ; ex:label "Term {i}" .\n'
        large_file.write_text(content)

        config = RDFSourceConfig(
            source=str(large_file),
            format="turtle",
            environment="PROD",
        )

        source = RDFSource(config, mock_ctx)
        with caplog.at_level("WARNING"):
            list(source.get_workunits_internal())

        # Check if performance warning was logged
        warning_messages = [
            record.message for record in caplog.records if record.levelname == "WARNING"
        ]
        assert any(
            "large" in msg.lower() or "performance" in msg.lower()
            for msg in warning_messages
        )

    def test_invalid_format_error(self, tmp_path, mock_ctx):
        """Test error handling for invalid format specification."""
        valid_file = tmp_path / "valid.ttl"
        valid_file.write_text(
            """@prefix ex: <http://example.org/> .
ex:Term a ex:Concept ; ex:label "Term" ."""
        )

        config = RDFSourceConfig(
            source=str(valid_file),
            format="invalid_format",
            environment="PROD",
        )

        source = RDFSource(config, mock_ctx)
        workunits = list(source.get_workunits_internal())

        # Should handle invalid format gracefully
        # May produce work units if format is ignored, or may fail
        # The important thing is it doesn't crash
        assert source.report is not None
        assert isinstance(workunits, list)

    def test_empty_file_handling(self, tmp_path, mock_ctx):
        """Test handling of empty RDF files."""
        empty_file = tmp_path / "empty.ttl"
        empty_file.write_text("")

        config = RDFSourceConfig(
            source=str(empty_file),
            format="turtle",
            environment="PROD",
        )

        source = RDFSource(config, mock_ctx)
        workunits = list(source.get_workunits_internal())

        # Should handle empty file gracefully
        # May produce no work units, but shouldn't crash
        assert source.report is not None
        assert isinstance(workunits, list)

    def test_error_context_includes_source_path(self, tmp_path, mock_ctx):
        """Test that error messages include the source path."""
        missing_file = tmp_path / "missing.ttl"
        config = RDFSourceConfig(
            source=str(missing_file),
            format="turtle",
            environment="PROD",
        )

        source = RDFSource(config, mock_ctx)
        list(source.get_workunits_internal())

        # Check that failure context includes source path
        for failure in source.report.failures:
            # Failures are tuples of (index, StructuredLogEntry)
            if isinstance(failure, tuple):
                _, entry = failure
                # Context is a LossyList[str], join it into a string
                failure_context_str = " ".join(entry.context) if entry.context else ""
                failure_text = f"{entry.message} {failure_context_str}"
            else:
                failure_text = str(failure)
            assert str(missing_file) in failure_text
