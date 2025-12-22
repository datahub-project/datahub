"""Utilities for streaming large log files from S3 presigned URLs."""

import gzip
import io
import tarfile
from collections import deque
from typing import Any, BinaryIO, Iterator, Optional
from urllib.parse import urlparse

import requests
from loguru import logger

from datahub_integrations.chat.agents.tools.log_formatting import (
    MATCH_SEPARATOR,
    GrepResult,
    WindowingResult,
    build_error_result_grep,
    build_error_result_windowing,
    build_grep_result,
    build_windowing_result,
    format_context_line,
    format_match_line,
    format_regular_line,
)


class S3LogStreamError(Exception):
    """Error occurred while streaming logs from S3."""


class RawStreamWrapper:
    """Wrapper for requests response.raw that provides full file-like interface.

    The requests library's response.raw doesn't implement all file-like methods
    (e.g., seekable()), which causes io.BufferedReader to fail. This wrapper
    adds the missing methods.

    Also keeps a reference to the response object to prevent it from being
    garbage collected, which would close the connection.
    """

    def __init__(self, raw_stream: BinaryIO, keep_alive: Optional[Any] = None) -> None:
        self.raw_stream = raw_stream
        self._closed = False
        # Keep reference to prevent garbage collection (e.g., requests Response object)
        self._keep_alive = keep_alive

    def read(self, size: int = -1) -> bytes:
        return self.raw_stream.read(size)

    def readinto(self, b: bytearray) -> int:
        """Read data into a pre-allocated buffer."""
        data = self.raw_stream.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            if hasattr(self.raw_stream, "close"):
                self.raw_stream.close()


class PrependBytesWrapper:
    """Wrapper that prepends bytes to the beginning of a stream.

    Necessary because gzip streams aren't seekable - once we read bytes to detect
    format (tar vs plain text), we can't seek back to byte 0. This wrapper
    prepends the consumed bytes so the next reader (tarfile or TextIOWrapper)
    sees them as if we never read them.
    """

    def __init__(self, prepend_data: bytes, stream: BinaryIO) -> None:
        self.prepend_data = prepend_data
        self.stream = stream
        self.prepend_consumed = False
        self._closed = False

    def read(self, size: int = -1) -> bytes:
        if not self.prepend_consumed:
            self.prepend_consumed = True
            if size == -1:
                # Read all
                return self.prepend_data + self.stream.read()
            else:
                # Read requested amount from prepended data first
                if len(self.prepend_data) >= size:
                    result = self.prepend_data[:size]
                    self.prepend_data = self.prepend_data[size:]
                    self.prepend_consumed = False  # Still have prepend data left
                    return result
                else:
                    # Need to read from both prepend and stream
                    result = self.prepend_data
                    remaining = size - len(self.prepend_data)
                    result += self.stream.read(remaining)
                    return result
        else:
            return self.stream.read(size)

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            if hasattr(self.stream, "close"):
                self.stream.close()


def stream_lines_from_url(presigned_url: str) -> Iterator[str]:
    """
    Stream lines from an S3 presigned URL without loading full file into memory.

    Handles multiple formats automatically:
    - Plain text files
    - Gzip-compressed files (.gz)
    - Tar archives (with or without gzip compression)

    For tar archives, extracts the first .log file found and streams its contents.
    Falls back to plain text if tar extraction fails.

    Args:
        presigned_url: Presigned S3 URL (valid for ~1 hour)

    Yields:
        Individual lines from the log file

    Raises:
        S3LogStreamError: If streaming fails
    """
    try:
        response = requests.get(presigned_url, stream=True, timeout=30)
        response.raise_for_status()

        # Wrap to keep response object alive and prevent garbage collection
        try:
            wrapped_raw = RawStreamWrapper(response.raw, keep_alive=response)
            buffered_raw = io.BufferedReader(wrapped_raw, buffer_size=8192)  # type: ignore[arg-type]
        except Exception as e:
            logger.error(f"Failed to create BufferedReader: {e}")
            raise

        # Check if the file is gzip-compressed by reading the magic bytes
        try:
            first_bytes = buffered_raw.peek(10)[:10]
        except (AttributeError, OSError) as e:
            logger.warning(
                f"BufferedReader peek failed: {e}, falling back to URL check"
            )
            first_bytes = b""

        is_gzip = len(first_bytes) >= 2 and first_bytes[:2] == b"\x1f\x8b"

        # Fallback: Check URL path for .gz extension
        if not is_gzip and len(first_bytes) < 2:
            parsed_url = urlparse(presigned_url)
            is_gzip = parsed_url.path.endswith(".gz")

        if is_gzip:
            # Decompress gzip content while streaming
            # Note: We use try-finally for cleanup instead of 'with' statements
            # because this is a generator and resources must stay open until
            # the generator is fully consumed or closed
            logger.debug("Detected gzip-compressed logs, decompressing stream")

            # Check if it's a tar archive by examining decompressed content
            # Tar files have "ustar" magic at offset 257 in the header
            gzip_file_binary = gzip.open(buffered_raw, mode="rb")
            try:
                # Read first 512 bytes (tar header size) to detect format
                # Note: This consumes the stream and we can't seek back, so we'll
                # need to prepend these bytes before passing to tarfile/TextIOWrapper
                first_block = gzip_file_binary.read(512)
                is_tar = len(first_block) >= 263 and first_block[257:262] == b"ustar"

                if is_tar:
                    logger.debug("Detected tar archive format")
                    # Prepend the consumed bytes back onto the stream for tar parsing
                    wrapped_stream = PrependBytesWrapper(first_block, gzip_file_binary)  # type: ignore[arg-type]
                    # PrependBytesWrapper implements the file-like interface but mypy doesn't recognize it
                    tar = tarfile.open(fileobj=wrapped_stream, mode="r|")  # type: ignore[call-overload]
                    try:
                        log_file_found = False
                        for member in tar:
                            if member.isfile() and member.name.endswith(".log"):
                                logger.info(
                                    f"Found log file in tar archive: {member.name} "
                                    f"({member.size} bytes)"
                                )
                                log_file_found = True

                                # Extract the log file and stream its contents
                                extracted_file = tar.extractfile(member)
                                if extracted_file:
                                    # Wrap extracted file to provide full file-like interface
                                    # tarfile's extracted files don't have seekable() method
                                    wrapped_extracted = RawStreamWrapper(extracted_file)
                                    text_stream = io.TextIOWrapper(  # type: ignore[type-var]
                                        wrapped_extracted,
                                        encoding="utf-8",
                                        errors="replace",
                                    )
                                    for line in text_stream:
                                        yield line.rstrip("\n\r")
                                    break

                        if not log_file_found:
                            logger.warning("No .log files found in tar archive")
                            raise S3LogStreamError(
                                "Tar archive does not contain any .log files"
                            )
                    finally:
                        tar.close()
                else:
                    # Not a tar file, treat as plain gzipped text
                    logger.debug("Not a tar archive, treating as plain gzipped text")
                    # Prepend the consumed bytes back onto the stream
                    wrapped_stream = PrependBytesWrapper(first_block, gzip_file_binary)  # type: ignore[arg-type]
                    text_stream = io.TextIOWrapper(  # type: ignore[type-var,assignment]
                        wrapped_stream, encoding="utf-8", errors="replace"
                    )
                    for line in text_stream:
                        yield line.rstrip("\n\r")
            finally:
                gzip_file_binary.close()

        else:
            # Plain text content - read from buffered_raw as text
            logger.debug("Processing plain text logs")
            text_wrapper = io.TextIOWrapper(
                buffered_raw, encoding="utf-8", errors="replace"
            )
            for line in text_wrapper:
                yield line.rstrip("\n\r")

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to stream logs from S3: {e}")
        raise S3LogStreamError(f"Failed to stream logs from S3: {e}") from e
    except (OSError, tarfile.TarError) as e:
        # Catch gzip decompression errors and tar extraction errors
        logger.error(f"Failed to process logs from S3: {e}")
        raise S3LogStreamError(f"Failed to process logs from S3: {e}") from e


def stream_grep_mode(
    presigned_url: str,
    grep_phrase: str,
    lines_after_match: int = 30,
    lines_before_match: int = 10,
    max_matches_returned: int = 3,
) -> GrepResult:
    """
    Stream logs from S3 and find lines matching a grep phrase.

    Args:
        presigned_url: Presigned S3 URL
        grep_phrase: Phrase to search for
        lines_after_match: Lines of context after each match
        lines_before_match: Lines of context before each match
        max_matches_returned: Maximum number of matches to return

    Returns:
        Dictionary with logs, match counts, and metadata

    Note:
        When matches occur within the after-context window of a previous match,
        they are counted in matches_found but may not have the full before-context
        preserved, as lines are not added to the before_buffer while collecting
        after-context. This is an expected limitation of the streaming approach.
    """
    matching_sections = []
    matches_found = 0
    matches_returned = 0
    total_lines_accumulated = 0
    truncated = False
    total_lines = 0

    # Circular buffer for "lines before match"
    before_buffer: deque[tuple[int, str]] = deque(maxlen=lines_before_match)

    # State for collecting lines after a match
    collecting_after_match = False
    lines_after_collected = 0
    current_match_section = []

    try:
        for line_num, line in enumerate(stream_lines_from_url(presigned_url), start=1):
            total_lines += 1

            # Are we currently collecting lines after a match?
            if collecting_after_match:
                current_match_section.append(format_context_line(line_num, line))
                lines_after_collected += 1

                # Done collecting after-context?
                if lines_after_collected >= lines_after_match:
                    # Finalize this match section
                    section_text = "\n".join(current_match_section)
                    section_line_count = len(current_match_section)

                    matching_sections.append(section_text)
                    matches_returned += 1
                    total_lines_accumulated += section_line_count

                    # Reset state
                    collecting_after_match = False
                    current_match_section = []
                    before_buffer.clear()

                # Check if line is another match while collecting context
                # Note: If a match occurs within the after-context window, it's counted
                # but won't have full before-context since we don't populate before_buffer
                # during collection. This line will be part of the previous match's context.
                if grep_phrase in line:
                    matches_found += 1

                continue

            # Check if this line matches
            if grep_phrase in line:
                matches_found += 1

                # Check if we've hit max matches
                if matches_returned >= max_matches_returned:
                    truncated = True
                    break

                # Start building the context section
                current_match_section = []

                # Add lines before match from buffer
                for buf_line_num, buf_line in before_buffer:
                    current_match_section.append(
                        format_context_line(buf_line_num, buf_line)
                    )

                # Add the matching line itself
                current_match_section.append(format_match_line(line_num, line))

                # Start collecting lines after match
                collecting_after_match = True
                lines_after_collected = 0
            else:
                # Not a match, add to circular buffer for context
                before_buffer.append((line_num, line))

        # Handle case where stream ended while still collecting after-match lines
        if collecting_after_match and current_match_section:
            section_text = "\n".join(current_match_section)
            section_line_count = len(current_match_section)

            matching_sections.append(section_text)
            matches_returned += 1
            total_lines_accumulated += section_line_count

    except S3LogStreamError as e:
        return build_error_result_grep(grep_phrase, str(e))

    result_logs = MATCH_SEPARATOR.join(matching_sections) if matching_sections else ""

    return build_grep_result(
        logs=result_logs,
        total_lines=total_lines,
        lines_returned=total_lines_accumulated,
        matches_found=matches_found,
        matches_returned=matches_returned,
        truncated=truncated,
        grep_phrase=grep_phrase,
    )


def stream_windowing_mode(
    presigned_url: str,
    lines_from_end: int = 150,
    offset_from_end: int = 0,
) -> WindowingResult:
    """
    Stream logs from S3 and return a window of lines from the end.

    This implementation reads the entire file once to count lines and collect
    the requested window in a deque. For tail operations (offset_from_end=0),
    this is very memory efficient as we only keep the last N lines.

    Args:
        presigned_url: Presigned S3 URL
        lines_from_end: Number of lines to fetch from end
        offset_from_end: How many lines from end to skip before starting window

    Returns:
        Dictionary with logs and metadata
    """
    total_lines = 0
    window: deque[tuple[int, str]] = deque(
        maxlen=lines_from_end + offset_from_end
        if offset_from_end > 0
        else lines_from_end
    )

    try:
        # Stream through entire file, keeping only the window we need
        for line_num, line in enumerate(stream_lines_from_url(presigned_url), start=1):
            total_lines += 1
            window.append((line_num, line))

        # Check if offset is beyond available logs
        if offset_from_end >= total_lines:
            return build_windowing_result(
                logs="",
                total_lines=total_lines,
                lines_returned=0,
                window_start=0,
                window_end=0,
                message=f"Offset {offset_from_end} is beyond the available logs",
            )

        # Extract the window from the deque
        # The deque contains the last (lines_from_end + offset_from_end) lines
        if offset_from_end > 0:
            # Need to skip offset_from_end lines from the end
            window_lines = list(window)[
                : -offset_from_end if offset_from_end > 0 else None
            ]
        else:
            # Take all lines in the window
            window_lines = list(window)

        if not window_lines:
            return build_windowing_result(
                logs="",
                total_lines=total_lines,
                lines_returned=0,
                window_start=0,
                window_end=0,
                message="No lines in requested window",
            )

        result_logs = "\n".join(
            format_regular_line(num, line) for num, line in window_lines
        )

        return build_windowing_result(
            logs=result_logs,
            total_lines=total_lines,
            lines_returned=len(window_lines),
            window_start=window_lines[0][0],
            window_end=window_lines[-1][0],
        )

    except S3LogStreamError as e:
        return build_error_result_windowing(str(e))
