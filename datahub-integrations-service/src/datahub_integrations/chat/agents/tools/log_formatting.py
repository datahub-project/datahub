"""Shared utilities for formatting log search results (grep and windowing modes)."""

from typing_extensions import TypedDict

# Constants
MATCH_SEPARATOR = "\n\n--- Next Match ---\n\n"


class GrepResult(TypedDict):
    """Result dictionary from grep mode log streaming."""

    logs: str
    total_lines: int
    lines_returned: int
    matches_found: int
    matches_returned: int
    truncated: bool
    grep_phrase: str
    message: str


class WindowingResult(TypedDict):
    """Result dictionary from windowing mode log streaming."""

    logs: str
    total_lines: int
    lines_returned: int
    window_start: int
    window_end: int
    message: str


def format_context_line(line_num: int, line: str) -> str:
    """Format a context line (non-matching line in grep results)."""
    return f"    Line {line_num}: {line}"


def format_match_line(line_num: int, line: str) -> str:
    """Format a matching line (highlighted with >>> prefix)."""
    return f">>> Line {line_num}: {line}"


def format_regular_line(line_num: int, line: str) -> str:
    """Format a regular line (windowing mode or general display)."""
    return f"Line {line_num}: {line}"


def build_grep_result(
    logs: str,
    total_lines: int,
    lines_returned: int,
    matches_found: int,
    matches_returned: int,
    truncated: bool,
    grep_phrase: str,
) -> GrepResult:
    """
    Build a standardized result dictionary for grep mode.

    Args:
        logs: Formatted log content
        total_lines: Total number of lines in the log file
        lines_returned: Number of lines included in the result
        matches_found: Total number of matches found
        matches_returned: Number of matches included in the result
        truncated: Whether results were truncated (more matches than max_matches_returned)
        grep_phrase: The search phrase used

    Returns:
        Standardized result dictionary
    """
    if logs:
        message = f"Found {matches_found} matches for '{grep_phrase}'"
        if truncated:
            message += f". TRUNCATED: Showing first {matches_returned} matches (limit: max_matches_returned). Consider being more specific with grep_phrase or adjusting parameters."
    else:
        message = f"No matches found for '{grep_phrase}' in the logs"

    return {
        "logs": logs,
        "total_lines": total_lines,
        "lines_returned": lines_returned,
        "matches_found": matches_found,
        "matches_returned": matches_returned,
        "truncated": truncated,
        "grep_phrase": grep_phrase,
        "message": message,
    }


def build_windowing_result(
    logs: str,
    total_lines: int,
    lines_returned: int,
    window_start: int,
    window_end: int,
    message: str | None = None,
) -> WindowingResult:
    """
    Build a standardized result dictionary for windowing mode.

    Args:
        logs: Formatted log content
        total_lines: Total number of lines in the log file
        lines_returned: Number of lines included in the result
        window_start: Starting line number of the window (1-indexed)
        window_end: Ending line number of the window (1-indexed)
        message: Optional custom message (auto-generated if not provided)

    Returns:
        Standardized result dictionary
    """
    if message is None:
        if logs:
            message = f"Showing lines {window_start}-{window_end} of {total_lines} total lines"
        else:
            message = "No lines in requested window"

    return {
        "logs": logs,
        "total_lines": total_lines,
        "lines_returned": lines_returned,
        "window_start": window_start,
        "window_end": window_end,
        "message": message,
    }


def build_error_result_grep(grep_phrase: str, error: str) -> GrepResult:
    """Build an error result for grep mode."""
    return {
        "logs": "",
        "total_lines": 0,
        "lines_returned": 0,
        "matches_found": 0,
        "matches_returned": 0,
        "truncated": False,
        "grep_phrase": grep_phrase,
        "message": f"Error: {error}",
    }


def build_error_result_windowing(error: str) -> WindowingResult:
    """Build an error result for windowing mode."""
    return {
        "logs": "",
        "total_lines": 0,
        "lines_returned": 0,
        "window_start": 0,
        "window_end": 0,
        "message": f"Error: {error}",
    }
