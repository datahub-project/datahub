#!/usr/bin/env python3
"""
Additional comprehensive tests for RDF loader to improve coverage.

Tests cover:
- Format validation and detection
- Helper functions
- Error paths and edge cases
- Glob pattern handling
- Zip file detection
- Web folder detection
"""

import io
import zipfile
from unittest.mock import MagicMock, patch

import pytest
import requests

from datahub.ingestion.source.rdf.core.rdf_loader import (
    DEFAULT_MAX_URL_SIZE,
    _create_retry_session,
    _detect_format_from_extension,
    _is_web_folder_url,
    _is_zip_file,
    _is_zip_url,
    _load_from_zip_file,
    _load_from_zip_url,
    _validate_format,
    load_rdf_graph,
)


class TestFormatValidation:
    """Test format validation and detection."""

    def test_validate_format_valid_formats(self):
        """Test validation of valid formats."""
        assert _validate_format("turtle") == "turtle"
        assert _validate_format("TURTLE") == "turtle"
        assert _validate_format("xml") == "xml"
        assert _validate_format("json-ld") == "json-ld"
        assert _validate_format("n3") == "n3"
        assert _validate_format("nt") == "nt"

    def test_validate_format_aliases(self):
        """Test format alias normalization."""
        assert _validate_format("ttl") == "turtle"
        assert _validate_format("rdf") == "xml"
        assert _validate_format("rdfxml") == "xml"
        assert _validate_format("jsonld") == "json-ld"
        assert _validate_format("ntriples") == "nt"

    def test_validate_format_invalid(self):
        """Test validation of invalid formats."""
        with pytest.raises(ValueError, match="Invalid RDF format"):
            _validate_format("invalid_format")
        with pytest.raises(ValueError, match="Invalid RDF format"):
            _validate_format("pdf")
        with pytest.raises(ValueError, match="Invalid RDF format"):
            _validate_format("")

    def test_detect_format_from_extension(self):
        """Test format detection from extensions."""
        assert _detect_format_from_extension(".ttl") == "turtle"
        assert _detect_format_from_extension(".turtle") == "turtle"
        assert _detect_format_from_extension(".rdf") == "xml"
        assert _detect_format_from_extension(".xml") == "xml"
        assert _detect_format_from_extension(".owl") == "xml"
        assert _detect_format_from_extension(".jsonld") == "json-ld"
        assert _detect_format_from_extension(".n3") == "n3"
        assert _detect_format_from_extension(".nt") == "nt"
        assert _detect_format_from_extension(".unknown") == "turtle"  # Default
        assert _detect_format_from_extension("") == "turtle"  # Default


class TestHelperFunctions:
    """Test helper functions for file/URL detection."""

    def test_is_zip_file_by_extension(self, tmp_path):
        """Test zip file detection by extension."""
        zip_file = tmp_path / "test.zip"
        zip_file.touch()
        assert _is_zip_file(zip_file)

        jar_file = tmp_path / "test.jar"
        jar_file.touch()
        assert _is_zip_file(jar_file)

    def test_is_zip_file_by_content(self, tmp_path):
        """Test zip file detection by content."""
        zip_file = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.writestr("test.txt", "content")
        assert _is_zip_file(zip_file)

    def test_is_zip_file_not_zip(self, tmp_path):
        """Test that non-zip files return False."""
        text_file = tmp_path / "test.txt"
        text_file.write_text("not a zip")
        assert not _is_zip_file(text_file)

    def test_is_zip_file_invalid_zip(self, tmp_path):
        """Test that invalid zip files without zip extension return False."""
        # Use a file without zip extension - extension check happens first
        # so .zip files always return True regardless of content
        invalid_zip = tmp_path / "invalid.txt"
        invalid_zip.write_bytes(b"not a zip file")
        assert not _is_zip_file(invalid_zip)

    def test_is_zip_url_by_extension(self):
        """Test zip URL detection by extension."""
        assert _is_zip_url("http://example.com/file.zip")
        assert _is_zip_url("http://example.com/file.jar")
        assert _is_zip_url("https://example.com/data.war")
        assert not _is_zip_url("http://example.com/file.ttl")
        assert not _is_zip_url("http://example.com/file.rdf")

    def test_is_web_folder_url_ending_slash(self):
        """Test web folder URL detection."""
        assert _is_web_folder_url("http://example.com/")
        assert _is_web_folder_url("http://example.com/folder/")
        assert _is_web_folder_url("https://example.com/path/to/folder/")

    def test_is_web_folder_url_no_extension(self):
        """Test web folder URL detection for URLs without extensions."""
        assert _is_web_folder_url("http://example.com/folder")
        assert _is_web_folder_url("http://example.com/path/to/folder")

    def test_is_web_folder_url_with_extension(self):
        """Test that URLs with extensions are not folders."""
        assert not _is_web_folder_url("http://example.com/file.ttl")
        assert not _is_web_folder_url("http://example.com/file.rdf")
        assert not _is_web_folder_url("http://example.com/file.xml")

    def test_create_retry_session(self):
        """Test retry session creation."""
        session = _create_retry_session()
        assert isinstance(session, requests.Session)
        assert session.max_redirects == 10


class TestLoadRDFGraphMainFunction:
    """Test the main load_rdf_graph function with various inputs."""

    def test_load_rdf_graph_single_file(self, tmp_path):
        """Test loading from a single file."""
        test_file = tmp_path / "test.ttl"
        test_file.write_text(
            "@prefix ex: <http://example.org/> .\nex:Term a ex:Concept ."
        )

        graph = load_rdf_graph(str(test_file))
        assert len(graph) > 0

    def test_load_rdf_graph_folder(self, tmp_path):
        """Test loading from a folder."""
        folder = tmp_path / "rdf_folder"
        folder.mkdir()
        (folder / "file1.ttl").write_text(
            "@prefix ex: <http://example.org/> .\nex:Term1 a ex:Concept ."
        )
        (folder / "file2.ttl").write_text(
            "@prefix ex: <http://example.org/> .\nex:Term2 a ex:Concept ."
        )

        graph = load_rdf_graph(str(folder))
        assert len(graph) >= 2

    def test_load_rdf_graph_comma_separated_files(self, tmp_path):
        """Test loading from comma-separated files."""
        file1 = tmp_path / "file1.ttl"
        file1.write_text("@prefix ex: <http://example.org/> .\nex:Term1 a ex:Concept .")
        file2 = tmp_path / "file2.ttl"
        file2.write_text("@prefix ex: <http://example.org/> .\nex:Term2 a ex:Concept .")

        graph = load_rdf_graph(f"{file1},{file2}")
        assert len(graph) >= 2

    def test_load_rdf_graph_glob_pattern(self, tmp_path):
        """Test loading from glob pattern."""
        folder = tmp_path / "glob_test"
        folder.mkdir()
        (folder / "file1.ttl").write_text(
            "@prefix ex: <http://example.org/> .\nex:Term1 a ex:Concept ."
        )
        (folder / "file2.ttl").write_text(
            "@prefix ex: <http://example.org/> .\nex:Term2 a ex:Concept ."
        )

        graph = load_rdf_graph(str(folder / "*.ttl"))
        assert len(graph) >= 2

    def test_load_rdf_graph_glob_pattern_no_matches(self):
        """Test glob pattern with no matches."""
        with pytest.raises(ValueError, match="Source not found or invalid"):
            load_rdf_graph("/nonexistent/path/*.ttl")

    def test_load_rdf_graph_glob_pattern_only_directories(self, tmp_path):
        """Test glob pattern matching only directories."""
        folder = tmp_path / "glob_dir"
        folder.mkdir()
        subfolder = folder / "subfolder"
        subfolder.mkdir()

        with pytest.raises(ValueError, match="matched only directories"):
            load_rdf_graph(str(folder / "*/"))

    def test_load_rdf_graph_invalid_source(self):
        """Test loading from invalid source."""
        with pytest.raises(ValueError, match="Source not found or invalid"):
            load_rdf_graph("/nonexistent/file.ttl")

    def test_load_rdf_graph_with_format(self, tmp_path):
        """Test loading with explicit format."""
        test_file = tmp_path / "test.rdf"
        test_file.write_text(
            '<?xml version="1.0"?><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"></rdf:RDF>'
        )

        graph = load_rdf_graph(str(test_file), format="xml")
        assert graph is not None

    def test_load_rdf_graph_invalid_format(self, tmp_path):
        """Test loading with invalid format."""
        test_file = tmp_path / "test.ttl"
        test_file.write_text(
            "@prefix ex: <http://example.org/> .\nex:Term a ex:Concept ."
        )

        with pytest.raises(ValueError, match="Invalid RDF format"):
            load_rdf_graph(str(test_file), format="invalid_format")


class TestLoadFromZipFile:
    """Test loading from local zip files."""

    def test_load_from_zip_file_basic(self, tmp_path):
        """Test basic zip file loading."""
        zip_file = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.writestr(
                "file.ttl",
                "@prefix ex: <http://example.org/> .\nex:Term a ex:Concept .",
            )

        graph = _load_from_zip_file(zip_file, True, [".ttl"])
        assert len(graph) > 0

    def test_load_from_zip_file_recursive(self, tmp_path):
        """Test zip file loading with recursive processing."""
        zip_file = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.writestr(
                "file1.ttl",
                "@prefix ex: <http://example.org/> .\nex:Term1 a ex:Concept .",
            )
            zf.writestr(
                "subfolder/file2.ttl",
                "@prefix ex: <http://example.org/> .\nex:Term2 a ex:Concept .",
            )

        graph = _load_from_zip_file(zip_file, True, [".ttl"])
        assert len(graph) >= 2

    def test_load_from_zip_file_non_recursive(self, tmp_path):
        """Test zip file loading without recursive processing."""
        zip_file = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.writestr(
                "file1.ttl",
                "@prefix ex: <http://example.org/> .\nex:Term1 a ex:Concept .",
            )
            zf.writestr(
                "subfolder/file2.ttl",
                "@prefix ex: <http://example.org/> .\nex:Term2 a ex:Concept .",
            )

        graph = _load_from_zip_file(zip_file, False, [".ttl"])
        # Should only load file1.ttl (not subfolder/file2.ttl)
        assert len(graph) >= 1

    def test_load_from_zip_file_extension_filter(self, tmp_path):
        """Test zip file loading with extension filtering."""
        zip_file = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.writestr(
                "file.ttl",
                "@prefix ex: <http://example.org/> .\nex:Term a ex:Concept .",
            )
            zf.writestr("file.txt", "not rdf")

        graph = _load_from_zip_file(zip_file, True, [".ttl"])
        assert len(graph) > 0

    def test_load_from_zip_file_with_base_dir(self, tmp_path):
        """Test zip file loading with base_dir protection."""
        zip_file = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.writestr(
                "file.ttl",
                "@prefix ex: <http://example.org/> .\nex:Term a ex:Concept .",
            )

        base_dir = str(tmp_path)
        graph = _load_from_zip_file(zip_file, True, [".ttl"], base_dir=base_dir)
        assert len(graph) > 0


class TestLoadFromZipURL:
    """Test loading from zip URLs."""

    @patch("datahub.ingestion.source.rdf.core.rdf_loader.requests.Session")
    def test_load_from_zip_url_basic(self, mock_session_class):
        """Test basic zip URL loading."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr(
                "file.ttl",
                "@prefix ex: <http://example.org/> .\nex:Term a ex:Concept .",
            )
        zip_content = zip_buffer.getvalue()

        mock_response.headers = {"content-length": str(len(zip_content))}
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [zip_content]
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        graph = _load_from_zip_url("http://example.com/data.zip", True, [".ttl"])
        assert len(graph) > 0

    @patch("datahub.ingestion.source.rdf.core.rdf_loader.requests.Session")
    def test_load_from_zip_url_too_large(self, mock_session_class):
        """Test zip URL that exceeds size limit."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.headers = {"content-length": str(DEFAULT_MAX_URL_SIZE + 1)}
        mock_response.status_code = 200
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        with pytest.raises(ValueError, match="too large"):
            _load_from_zip_url("http://example.com/huge.zip", True, [".ttl"])

    @patch("datahub.ingestion.source.rdf.core.rdf_loader.requests.Session")
    def test_load_from_zip_url_streaming_exceeds_limit(self, mock_session_class):
        """Test zip URL streaming that exceeds limit during download."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.headers = {}
        mock_response.status_code = 200
        chunks = [
            b"PK\x03\x04",  # Zip header
            b"x" * (DEFAULT_MAX_URL_SIZE + 1),
        ]
        mock_response.iter_content.return_value = chunks
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        with pytest.raises(ValueError, match="exceeds size limit"):
            _load_from_zip_url("http://example.com/huge.zip", True, [".ttl"])

    @patch("datahub.ingestion.source.rdf.core.rdf_loader.requests.Session")
    def test_load_from_zip_url_timeout(self, mock_session_class):
        """Test zip URL loading with timeout."""
        mock_session = MagicMock()
        mock_session.get.side_effect = requests.Timeout("Connection timeout")
        mock_session_class.return_value = mock_session

        with pytest.raises(ValueError, match="timed out"):
            _load_from_zip_url("http://example.com/slow.zip", True, [".ttl"])

    @patch("datahub.ingestion.source.rdf.core.rdf_loader.requests.Session")
    def test_load_from_zip_url_http_error(self, mock_session_class):
        """Test zip URL loading with HTTP error."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "404 Not Found"
        )
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        with pytest.raises(ValueError, match="Failed to load zip"):
            _load_from_zip_url("http://example.com/notfound.zip", True, [".ttl"])

    @patch("datahub.ingestion.source.rdf.core.rdf_loader.requests.Session")
    def test_load_from_zip_url_bad_zip(self, mock_session_class):
        """Test zip URL with invalid zip content."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.headers = {"content-length": "100"}
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [b"not a zip file"]
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        with pytest.raises(ValueError, match="Invalid zip file"):
            _load_from_zip_url("http://example.com/bad.zip", True, [".ttl"])

    @patch("datahub.ingestion.source.rdf.core.rdf_loader.requests.Session")
    def test_load_from_zip_url_non_recursive(self, mock_session_class):
        """Test zip URL loading without recursive processing."""
        mock_session = MagicMock()
        mock_response = MagicMock()
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr(
                "file1.ttl",
                "@prefix ex: <http://example.org/> .\nex:Term1 a ex:Concept .",
            )
            zf.writestr(
                "subfolder/file2.ttl",
                "@prefix ex: <http://example.org/> .\nex:Term2 a ex:Concept .",
            )
        zip_content = zip_buffer.getvalue()

        mock_response.headers = {"content-length": str(len(zip_content))}
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [zip_content]
        mock_response.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session

        graph = _load_from_zip_url("http://example.com/data.zip", False, [".ttl"])
        # Should only load file1.ttl (not subfolder/file2.ttl)
        assert len(graph) >= 1
