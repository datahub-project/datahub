"""Tests for log formatting utilities."""

from datahub_integrations.chat.agents.tools.log_formatting import (
    MATCH_SEPARATOR,
    build_error_result_grep,
    build_error_result_windowing,
    build_grep_result,
    build_windowing_result,
    format_context_line,
    format_match_line,
    format_regular_line,
)


class TestLineFormatters:
    """Tests for line formatting functions."""

    def test_format_context_line(self) -> None:
        """Test formatting context lines."""
        result = format_context_line(42, "Some log line")
        assert result == "    Line 42: Some log line"

    def test_format_match_line(self) -> None:
        """Test formatting match lines with highlight prefix."""
        result = format_match_line(42, "ERROR: Something failed")
        assert result == ">>> Line 42: ERROR: Something failed"

    def test_format_regular_line(self) -> None:
        """Test formatting regular lines."""
        result = format_regular_line(42, "Regular log line")
        assert result == "Line 42: Regular log line"


class TestMatchSeparator:
    """Tests for the match separator constant."""

    def test_match_separator_value(self) -> None:
        """Test that match separator has expected value."""
        assert MATCH_SEPARATOR == "\n\n--- Next Match ---\n\n"

    def test_match_separator_joins_sections(self) -> None:
        """Test using match separator to join sections."""
        sections = ["Section 1", "Section 2", "Section 3"]
        result = MATCH_SEPARATOR.join(sections)
        assert "--- Next Match ---" in result
        assert result.count("--- Next Match ---") == 2


class TestBuildGrepResult:
    """Tests for building grep mode results."""

    def test_builds_successful_grep_result(self) -> None:
        """Test building result with matches found."""
        result = build_grep_result(
            logs=">>> Line 5: ERROR found",
            total_lines=100,
            lines_returned=10,
            matches_found=3,
            matches_returned=3,
            truncated=False,
            grep_phrase="ERROR",
        )

        assert result["logs"] == ">>> Line 5: ERROR found"
        assert result["total_lines"] == 100
        assert result["lines_returned"] == 10
        assert result["matches_found"] == 3
        assert result["matches_returned"] == 3
        assert result["truncated"] is False
        assert result["grep_phrase"] == "ERROR"
        assert "Found 3 matches for 'ERROR'" in result["message"]
        assert "TRUNCATED" not in result["message"]

    def test_builds_truncated_grep_result(self) -> None:
        """Test building result with truncation."""
        result = build_grep_result(
            logs=">>> Line 5: ERROR found",
            total_lines=100,
            lines_returned=10,
            matches_found=50,
            matches_returned=3,
            truncated=True,
            grep_phrase="ERROR",
        )

        assert result["truncated"] is True
        assert result["matches_found"] == 50
        assert result["matches_returned"] == 3
        assert "TRUNCATED" in result["message"]
        assert "Showing first 3 matches" in result["message"]

    def test_builds_no_matches_grep_result(self) -> None:
        """Test building result when no matches found."""
        result = build_grep_result(
            logs="",
            total_lines=100,
            lines_returned=0,
            matches_found=0,
            matches_returned=0,
            truncated=False,
            grep_phrase="NOTFOUND",
        )

        assert result["logs"] == ""
        assert result["matches_found"] == 0
        assert result["matches_returned"] == 0
        assert "No matches found for 'NOTFOUND'" in result["message"]


class TestBuildWindowingResult:
    """Tests for building windowing mode results."""

    def test_builds_windowing_result_with_auto_message(self) -> None:
        """Test building result with auto-generated message."""
        result = build_windowing_result(
            logs="Line 1: log\nLine 2: log",
            total_lines=100,
            lines_returned=2,
            window_start=1,
            window_end=2,
        )

        assert result["logs"] == "Line 1: log\nLine 2: log"
        assert result["total_lines"] == 100
        assert result["lines_returned"] == 2
        assert result["window_start"] == 1
        assert result["window_end"] == 2
        assert result["message"] == "Showing lines 1-2 of 100 total lines"

    def test_builds_windowing_result_with_custom_message(self) -> None:
        """Test building result with custom message."""
        result = build_windowing_result(
            logs="",
            total_lines=100,
            lines_returned=0,
            window_start=0,
            window_end=0,
            message="Custom error message",
        )

        assert result["message"] == "Custom error message"

    def test_builds_empty_windowing_result(self) -> None:
        """Test building result with no logs."""
        result = build_windowing_result(
            logs="",
            total_lines=100,
            lines_returned=0,
            window_start=0,
            window_end=0,
        )

        assert result["logs"] == ""
        assert result["message"] == "No lines in requested window"


class TestBuildErrorResults:
    """Tests for building error results."""

    def test_builds_grep_error_result(self) -> None:
        """Test building error result for grep mode."""
        result = build_error_result_grep("ERROR", "Connection failed")

        assert "Connection failed" in result["message"]
        assert result["logs"] == ""
        assert result["total_lines"] == 0
        assert result["lines_returned"] == 0
        assert result["matches_found"] == 0
        assert result["matches_returned"] == 0
        assert result["truncated"] is False
        assert result["grep_phrase"] == "ERROR"

    def test_builds_windowing_error_result(self) -> None:
        """Test building error result for windowing mode."""
        result = build_error_result_windowing("Connection failed")

        assert "Connection failed" in result["message"]
        assert result["logs"] == ""
        assert result["total_lines"] == 0
        assert result["lines_returned"] == 0
        assert result["window_start"] == 0
        assert result["window_end"] == 0
