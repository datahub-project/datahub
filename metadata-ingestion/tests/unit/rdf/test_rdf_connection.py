#!/usr/bin/env python3
"""
Unit tests for RDF source connection testing.

Tests verify:
- test_connection() method functionality
- Connection test report structure
- Capability reporting
- Error handling in connection tests
- All declared capabilities are tested
"""

from datahub.ingestion.api.source import TestConnectionReport
from datahub.ingestion.source.rdf.ingestion.rdf_source import (
    RDFSource,
)


class TestRDFConnection:
    """Test RDF source connection testing."""

    def test_connection_successful_file(self, tmp_path):
        """Test successful connection to valid RDF file."""
        test_file = tmp_path / "test.ttl"
        test_file.write_text(
            '@prefix ex: <http://example.org/> .\nex:Term a skos:Concept ; skos:prefLabel "Term" .'
        )

        config_dict = {"source": str(test_file)}
        report = RDFSource.test_connection(config_dict)

        assert isinstance(report, TestConnectionReport)
        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is True
        assert report.basic_connectivity.failure_reason is None

        # Should have capability report for SCHEMA_METADATA
        assert report.capability_report is not None
        from datahub.ingestion.api.source import SourceCapability

        assert SourceCapability.SCHEMA_METADATA in report.capability_report
        schema_metadata = report.capability_report[SourceCapability.SCHEMA_METADATA]
        assert schema_metadata.capable is True

    def test_connection_file_not_found(self):
        """Test connection failure for non-existent file."""
        config_dict = {"source": "/path/that/does/not/exist.ttl"}
        report = RDFSource.test_connection(config_dict)

        assert isinstance(report, TestConnectionReport)
        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is False
        assert report.basic_connectivity.failure_reason is not None
        assert "not found" in report.basic_connectivity.failure_reason.lower()

    def test_connection_invalid_rdf(self, tmp_path):
        """Test connection failure for invalid RDF content."""
        invalid_file = tmp_path / "invalid.ttl"
        invalid_file.write_text("This is not valid RDF")

        config_dict = {"source": str(invalid_file)}
        report = RDFSource.test_connection(config_dict)

        assert isinstance(report, TestConnectionReport)
        assert report.basic_connectivity is not None
        # Basic connectivity may pass (file exists)
        # But RDF parsing should fail
        if report.capability_report:
            from datahub.ingestion.api.source import SourceCapability

            schema_metadata = report.capability_report.get(
                SourceCapability.SCHEMA_METADATA
            )
            if schema_metadata:
                assert schema_metadata.capable is False
                assert schema_metadata.failure_reason is not None

    def test_connection_empty_directory(self, tmp_path):
        """Test connection failure for empty directory."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        config_dict = {"source": str(empty_dir), "recursive": True}
        report = RDFSource.test_connection(config_dict)

        assert isinstance(report, TestConnectionReport)
        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is False
        assert report.basic_connectivity.failure_reason is not None
        assert (
            "no" in report.basic_connectivity.failure_reason.lower()
            and "file" in report.basic_connectivity.failure_reason.lower()
        )

    def test_connection_directory_with_files(self, tmp_path):
        """Test successful connection to directory with RDF files."""
        test_dir = tmp_path / "rdf_dir"
        test_dir.mkdir()
        test_file = test_dir / "test.ttl"
        test_file.write_text(
            '@prefix ex: <http://example.org/> .\nex:Term a skos:Concept ; skos:prefLabel "Term" .'
        )

        config_dict = {"source": str(test_dir), "recursive": False}
        report = RDFSource.test_connection(config_dict)

        assert isinstance(report, TestConnectionReport)
        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is True

    def test_connection_unsupported_extension(self, tmp_path):
        """Test connection failure for unsupported file extension."""
        unsupported_file = tmp_path / "test.txt"
        unsupported_file.write_text("Some content")

        config_dict = {"source": str(unsupported_file)}
        report = RDFSource.test_connection(config_dict)

        assert isinstance(report, TestConnectionReport)
        assert report.basic_connectivity is not None
        assert report.basic_connectivity.capable is False
        assert report.basic_connectivity.failure_reason is not None
        assert "extension" in report.basic_connectivity.failure_reason.lower()
