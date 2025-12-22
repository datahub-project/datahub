"""Tests for S3 log streaming utilities."""

import gzip
import io
from unittest.mock import Mock, patch

from datahub_integrations.chat.agents.tools.s3_log_streaming import (
    S3LogStreamError,
    stream_grep_mode,
    stream_lines_from_url,
    stream_windowing_mode,
)


class TestStreamGrepMode:
    """Tests for stream_grep_mode function."""

    @patch(
        "datahub_integrations.chat.agents.tools.s3_log_streaming.stream_lines_from_url"
    )
    def test_finds_single_match(self, mock_stream: Mock) -> None:
        """Test finding a single grep match."""
        mock_stream.return_value = iter(
            ["Log line 1", "ERROR: Something failed", "Log line 3", "Log line 4"]
        )

        result = stream_grep_mode(
            presigned_url="https://example.com/logs.txt",
            grep_phrase="ERROR",
            lines_after_match=2,
            lines_before_match=1,
        )

        assert result["matches_found"] == 1
        assert result["matches_returned"] == 1
        assert "ERROR: Something failed" in result["logs"]
        assert ">>> Line 2:" in result["logs"]

    @patch(
        "datahub_integrations.chat.agents.tools.s3_log_streaming.stream_lines_from_url"
    )
    def test_finds_multiple_matches(self, mock_stream: Mock) -> None:
        """Test finding multiple grep matches."""
        mock_stream.return_value = iter(
            [
                "Log line 1",
                "ERROR: First error",
                "Log line 3",
                "Log line 4",
                "ERROR: Second error",
                "Log line 6",
            ]
        )

        result = stream_grep_mode(
            presigned_url="https://example.com/logs.txt",
            grep_phrase="ERROR",
            lines_after_match=1,
            lines_before_match=1,
        )

        assert result["matches_found"] == 2
        assert result["matches_returned"] == 2
        assert "--- Next Match ---" in result["logs"]

    @patch(
        "datahub_integrations.chat.agents.tools.s3_log_streaming.stream_lines_from_url"
    )
    def test_respects_max_matches_returned(self, mock_stream: Mock) -> None:
        """Test that max_matches_returned limits results."""
        mock_stream.return_value = iter(
            [f"ERROR line {i}" for i in range(1, 11)]  # 10 error lines
        )

        result = stream_grep_mode(
            presigned_url="https://example.com/logs.txt",
            grep_phrase="ERROR",
            max_matches_returned=3,
            lines_after_match=0,
            lines_before_match=0,
        )

        assert result["matches_returned"] == 3
        assert result["truncated"] is True
        assert "TRUNCATED" in result["message"]

    @patch(
        "datahub_integrations.chat.agents.tools.s3_log_streaming.stream_lines_from_url"
    )
    def test_no_matches_found(self, mock_stream: Mock) -> None:
        """Test when no matches are found."""
        mock_stream.return_value = iter(["Log line 1", "Log line 2", "Log line 3"])

        result = stream_grep_mode(
            presigned_url="https://example.com/logs.txt",
            grep_phrase="ERROR",
        )

        assert result["matches_found"] == 0
        assert result["matches_returned"] == 0
        assert result["logs"] == ""
        assert "No matches found" in result["message"]

    @patch(
        "datahub_integrations.chat.agents.tools.s3_log_streaming.stream_lines_from_url"
    )
    def test_includes_context_lines(self, mock_stream: Mock) -> None:
        """Test that context lines before and after are included."""
        mock_stream.return_value = iter(
            [
                "Before 1",
                "Before 2",
                "ERROR: The error",
                "After 1",
                "After 2",
                "After 3",
            ]
        )

        result = stream_grep_mode(
            presigned_url="https://example.com/logs.txt",
            grep_phrase="ERROR",
            lines_before_match=2,
            lines_after_match=3,
        )

        logs = result["logs"]
        assert "Before 1" in logs
        assert "Before 2" in logs
        assert "ERROR: The error" in logs
        assert "After 1" in logs
        assert "After 2" in logs
        assert "After 3" in logs

    @patch(
        "datahub_integrations.chat.agents.tools.s3_log_streaming.stream_lines_from_url"
    )
    def test_handles_stream_error(self, mock_stream: Mock) -> None:
        """Test handling of streaming errors."""
        mock_stream.side_effect = S3LogStreamError("Connection failed")

        result = stream_grep_mode(
            presigned_url="https://example.com/logs.txt",
            grep_phrase="ERROR",
        )

        assert "Connection failed" in result["message"]
        assert result["matches_found"] == 0

    @patch(
        "datahub_integrations.chat.agents.tools.s3_log_streaming.stream_lines_from_url"
    )
    def test_handles_overlapping_matches(self, mock_stream: Mock) -> None:
        """Test grep behavior when matches occur within each other's context window.

        This test verifies the documented limitation: when a match occurs within
        the after-context window of a previous match, it's counted in matches_found
        but may not have full before-context preserved.
        """
        mock_stream.return_value = iter(
            [
                "Line 1",
                "ERROR first",  # Match 1 at line 2
                "Line 3",
                "ERROR second",  # Match 2 at line 4 (within 3-line after-context of match 1)
                "Line 5",
                "Line 6",
                "Line 7",
            ]
        )

        result = stream_grep_mode(
            presigned_url="https://example.com/logs.txt",
            grep_phrase="ERROR",
            lines_after_match=3,  # Overlaps with next match
            lines_before_match=1,
            max_matches_returned=10,
        )

        # Both matches should be counted
        assert result["matches_found"] == 2
        # The second match appears within the first match's context, so it's included
        # in the first match's output rather than being a separate match section
        assert "ERROR first" in result["logs"]
        assert "ERROR second" in result["logs"]


class TestStreamWindowingMode:
    """Tests for stream_windowing_mode function."""

    @patch(
        "datahub_integrations.chat.agents.tools.s3_log_streaming.stream_lines_from_url"
    )
    def test_returns_last_n_lines(self, mock_stream: Mock) -> None:
        """Test getting last N lines (tail mode)."""
        mock_stream.return_value = iter([f"Log line {i}" for i in range(1, 201)])

        result = stream_windowing_mode(
            presigned_url="https://example.com/logs.txt",
            lines_from_end=50,
            offset_from_end=0,
        )

        assert result["total_lines"] == 200
        assert result["lines_returned"] == 50
        assert result["window_start"] == 151
        assert result["window_end"] == 200

    @patch(
        "datahub_integrations.chat.agents.tools.s3_log_streaming.stream_lines_from_url"
    )
    def test_windowing_with_offset(self, mock_stream: Mock) -> None:
        """Test windowing with offset from end."""
        mock_stream.return_value = iter([f"Log line {i}" for i in range(1, 301)])

        result = stream_windowing_mode(
            presigned_url="https://example.com/logs.txt",
            lines_from_end=100,
            offset_from_end=150,
        )

        assert result["total_lines"] == 300
        assert result["lines_returned"] == 100
        assert result["window_start"] == 51
        assert result["window_end"] == 150

    @patch(
        "datahub_integrations.chat.agents.tools.s3_log_streaming.stream_lines_from_url"
    )
    def test_offset_beyond_available_logs(self, mock_stream: Mock) -> None:
        """Test when offset is beyond available logs."""
        mock_stream.return_value = iter([f"Log line {i}" for i in range(1, 101)])

        result = stream_windowing_mode(
            presigned_url="https://example.com/logs.txt",
            lines_from_end=50,
            offset_from_end=200,  # Beyond 100 total lines
        )

        assert result["lines_returned"] == 0
        assert "beyond the available logs" in result["message"]

    @patch(
        "datahub_integrations.chat.agents.tools.s3_log_streaming.stream_lines_from_url"
    )
    def test_includes_line_numbers(self, mock_stream: Mock) -> None:
        """Test that line numbers are included in output."""
        mock_stream.return_value = iter([f"Log line {i}" for i in range(1, 11)])

        result = stream_windowing_mode(
            presigned_url="https://example.com/logs.txt",
            lines_from_end=5,
        )

        logs = result["logs"]
        assert "Line 6:" in logs
        assert "Line 10:" in logs

    @patch(
        "datahub_integrations.chat.agents.tools.s3_log_streaming.stream_lines_from_url"
    )
    def test_handles_stream_error(self, mock_stream: Mock) -> None:
        """Test handling of streaming errors."""
        mock_stream.side_effect = S3LogStreamError("Connection failed")

        result = stream_windowing_mode(
            presigned_url="https://example.com/logs.txt",
            lines_from_end=50,
        )

        assert "Connection failed" in result["message"]
        assert result["total_lines"] == 0

    @patch(
        "datahub_integrations.chat.agents.tools.s3_log_streaming.stream_lines_from_url"
    )
    def test_tail_all_lines_when_requested_more_than_available(
        self, mock_stream: Mock
    ) -> None:
        """Test requesting more lines than available returns all lines."""
        mock_stream.return_value = iter([f"Log line {i}" for i in range(1, 11)])

        result = stream_windowing_mode(
            presigned_url="https://example.com/logs.txt",
            lines_from_end=50,  # More than 10 available
            offset_from_end=0,
        )

        assert result["total_lines"] == 10
        assert result["lines_returned"] == 10
        assert result["window_start"] == 1
        assert result["window_end"] == 10


class TestStreamLinesFromUrl:
    """Tests for stream_lines_from_url with gzip support."""

    @patch("datahub_integrations.chat.agents.tools.s3_log_streaming.requests.get")
    def test_handles_plain_text_logs(self, mock_get: Mock) -> None:
        """Test streaming plain text (non-gzip) logs."""
        # Create plain text content as BytesIO
        plain_text = io.BytesIO(b"Line 1\nLine 2\nLine 3\n")

        mock_response = Mock()
        mock_response.headers = {"Content-Type": "text/plain"}
        mock_response.raw = plain_text
        mock_get.return_value = mock_response

        lines = list(stream_lines_from_url("https://example.com/logs.txt"))

        assert lines == ["Line 1", "Line 2", "Line 3"]
        mock_get.assert_called_once_with(
            "https://example.com/logs.txt", stream=True, timeout=30
        )

    @patch("datahub_integrations.chat.agents.tools.s3_log_streaming.requests.get")
    def test_handles_gzip_compressed_logs_by_magic_bytes(self, mock_get: Mock) -> None:
        """Test streaming gzip-compressed logs (detected by magic bytes)."""
        # Create gzip-compressed content
        original_content = b"Line 1\nLine 2\nLine 3\n"
        compressed = io.BytesIO()
        with gzip.open(compressed, "wb") as f:
            f.write(original_content)

        # Get the compressed data and magic bytes
        compressed_data = compressed.getvalue()
        compressed.seek(0)

        mock_response = Mock()
        mock_response.headers = {"Content-Type": "application/octet-stream"}
        # Mock raw to support both peek() and gzip.open()
        mock_response.raw = compressed
        mock_response.raw.peek = Mock(return_value=compressed_data[:10])  # type: ignore[attr-defined]
        mock_get.return_value = mock_response

        lines = list(stream_lines_from_url("https://example.com/logs.txt"))

        assert lines == ["Line 1", "Line 2", "Line 3"]
        mock_get.assert_called_once_with(
            "https://example.com/logs.txt", stream=True, timeout=30
        )

    @patch("datahub_integrations.chat.agents.tools.s3_log_streaming.requests.get")
    def test_handles_gzip_fallback_to_url_extension(self, mock_get: Mock) -> None:
        """Test gzip detection falls back to URL .gz extension when peek unavailable."""
        # Create gzip-compressed content
        original_content = b"Line 1\nLine 2\nLine 3\n"
        compressed = io.BytesIO()
        with gzip.open(compressed, "wb") as f:
            f.write(original_content)
        compressed.seek(0)

        mock_response = Mock()
        mock_response.headers = {"Content-Type": "application/octet-stream"}
        # Mock raw without peek() method - should fallback to URL check
        mock_response.raw = compressed
        mock_get.return_value = mock_response

        lines = list(stream_lines_from_url("https://example.com/logs.txt.gz"))

        assert lines == ["Line 1", "Line 2", "Line 3"]

    @patch("datahub_integrations.chat.agents.tools.s3_log_streaming.requests.get")
    def test_handles_gzip_with_s3_presigned_url(self, mock_get: Mock) -> None:
        """Test gzip detection works with S3 presigned URLs (with query params)."""
        # Create gzip-compressed content
        original_content = b"Log from S3\nAnother line\n"
        compressed = io.BytesIO()
        with gzip.open(compressed, "wb") as f:
            f.write(original_content)

        compressed_data = compressed.getvalue()
        compressed.seek(0)

        mock_response = Mock()
        mock_response.headers = {}
        mock_response.raw = compressed
        mock_response.raw.peek = Mock(return_value=compressed_data[:10])  # type: ignore[attr-defined]
        mock_get.return_value = mock_response

        # S3 presigned URL with .gz before query params
        url = "https://bucket.s3.amazonaws.com/logs/file.log.gz?X-Amz-Signature=abc123"
        lines = list(stream_lines_from_url(url))

        assert lines == ["Log from S3", "Another line"]

    @patch("datahub_integrations.chat.agents.tools.s3_log_streaming.requests.get")
    def test_handles_request_error(self, mock_get: Mock) -> None:
        """Test handling of request errors."""
        import pytest
        import requests

        mock_get.side_effect = requests.exceptions.RequestException("Connection failed")

        with pytest.raises(S3LogStreamError) as exc_info:
            list(stream_lines_from_url("https://example.com/logs.txt"))

        assert "Connection failed" in str(exc_info.value)

    @patch("datahub_integrations.chat.agents.tools.s3_log_streaming.requests.get")
    def test_handles_gzip_decompression_error(self, mock_get: Mock) -> None:
        """Test handling of gzip decompression errors."""
        import pytest

        # Create invalid gzip content that has gzip magic bytes but is corrupted
        invalid_gzip = io.BytesIO(b"\x1f\x8b\x00\x00not valid gzip data after magic")

        mock_response = Mock()
        mock_response.headers = {"Content-Type": "application/octet-stream"}
        mock_response.raw = invalid_gzip
        mock_response.raw.peek = Mock(return_value=b"\x1f\x8b\x00")  # type: ignore[attr-defined]
        mock_get.return_value = mock_response

        with pytest.raises(S3LogStreamError) as exc_info:
            list(stream_lines_from_url("https://example.com/logs.txt"))

        error_msg = str(exc_info.value).lower()
        assert "decompress" in error_msg or "failed" in error_msg

    @patch("datahub_integrations.chat.agents.tools.s3_log_streaming.requests.get")
    def test_handles_tar_gz_with_log_file(self, mock_get: Mock) -> None:
        """Test streaming from a tar.gz archive containing a .log file."""
        import tarfile

        # Create a tar.gz archive with a .log file
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            # Add a .log file to the tar
            log_content = b"Log line 1\nLog line 2\nLog line 3\n"
            log_info = tarfile.TarInfo(name="ingestion-logs.log")
            log_info.size = len(log_content)
            tar.addfile(log_info, io.BytesIO(log_content))

        # Compress with gzip
        tar_buffer.seek(0)
        gzip_buffer = io.BytesIO()
        with gzip.open(gzip_buffer, "wb") as gz:
            gz.write(tar_buffer.read())

        gzip_buffer.seek(0)
        compressed_data = gzip_buffer.getvalue()
        gzip_buffer.seek(0)

        mock_response = Mock()
        mock_response.headers = {"Content-Type": "application/octet-stream"}
        mock_response.raw = gzip_buffer
        mock_response.raw.peek = Mock(return_value=compressed_data[:10])  # type: ignore[attr-defined]
        mock_get.return_value = mock_response

        lines = list(stream_lines_from_url("https://example.com/logs.tar.gz"))

        assert lines == ["Log line 1", "Log line 2", "Log line 3"]

    @patch("datahub_integrations.chat.agents.tools.s3_log_streaming.requests.get")
    def test_handles_tar_gz_with_no_log_file(self, mock_get: Mock) -> None:
        """Test error when tar.gz contains no .log files."""
        import tarfile

        import pytest

        # Create a tar.gz archive with NO .log file
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            # Add a .txt file instead
            txt_content = b"Not a log file\n"
            txt_info = tarfile.TarInfo(name="data.txt")
            txt_info.size = len(txt_content)
            tar.addfile(txt_info, io.BytesIO(txt_content))

        # Compress with gzip
        tar_buffer.seek(0)
        gzip_buffer = io.BytesIO()
        with gzip.open(gzip_buffer, "wb") as gz:
            gz.write(tar_buffer.read())

        gzip_buffer.seek(0)
        compressed_data = gzip_buffer.getvalue()
        gzip_buffer.seek(0)

        mock_response = Mock()
        mock_response.headers = {"Content-Type": "application/octet-stream"}
        mock_response.raw = gzip_buffer
        mock_response.raw.peek = Mock(return_value=compressed_data[:10])  # type: ignore[attr-defined]
        mock_get.return_value = mock_response

        with pytest.raises(S3LogStreamError) as exc_info:
            list(stream_lines_from_url("https://example.com/logs.tar.gz"))

        assert "does not contain any .log files" in str(exc_info.value)

    @patch("datahub_integrations.chat.agents.tools.s3_log_streaming.requests.get")
    def test_handles_tar_gz_with_multiple_log_files(self, mock_get: Mock) -> None:
        """Test that only the first .log file is extracted from tar.gz."""
        import tarfile

        # Create a tar.gz archive with multiple .log files
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            # Add first .log file
            log1_content = b"First log line 1\nFirst log line 2\n"
            log1_info = tarfile.TarInfo(name="first.log")
            log1_info.size = len(log1_content)
            tar.addfile(log1_info, io.BytesIO(log1_content))

            # Add second .log file
            log2_content = b"Second log line 1\nSecond log line 2\n"
            log2_info = tarfile.TarInfo(name="second.log")
            log2_info.size = len(log2_content)
            tar.addfile(log2_info, io.BytesIO(log2_content))

        # Compress with gzip
        tar_buffer.seek(0)
        gzip_buffer = io.BytesIO()
        with gzip.open(gzip_buffer, "wb") as gz:
            gz.write(tar_buffer.read())

        gzip_buffer.seek(0)
        compressed_data = gzip_buffer.getvalue()
        gzip_buffer.seek(0)

        mock_response = Mock()
        mock_response.headers = {"Content-Type": "application/octet-stream"}
        mock_response.raw = gzip_buffer
        mock_response.raw.peek = Mock(return_value=compressed_data[:10])  # type: ignore[attr-defined]
        mock_get.return_value = mock_response

        lines = list(stream_lines_from_url("https://example.com/logs.tar.gz"))

        # Should only get lines from first .log file
        assert lines == ["First log line 1", "First log line 2"]
