"""Text partitioner for converting markdown text to unstructured elements."""

import contextlib
import logging
import signal
import threading
from types import FrameType
from typing import Any, Iterator, Optional

logger = logging.getLogger(__name__)

# Mirrors the default extensions in unstructured/partition/md.py. Passing extensions to
# partition_md replaces that default, so these must be repeated alongside our own.
_DEFAULT_MARKDOWN_EXTENSIONS = ("tables", "fenced_code")


class PartitionTimeoutError(Exception):
    """Raised when partitioning one document exceeds the configured time limit."""


@contextlib.contextmanager
def _time_limit(seconds: float) -> Iterator[None]:
    """Raise PartitionTimeoutError if the block runs longer than ``seconds``.

    Uses SIGALRM, which also interrupts compiled code that returns to the interpreter
    between operations (e.g. spaCy's tokenizer). Only usable on the main thread of a
    platform that has SIGALRM; callers must check that first.
    """

    def _on_alarm(signum: int, frame: Optional[FrameType]) -> None:
        raise PartitionTimeoutError(f"Partitioning exceeded {seconds:g}s")

    previous_handler = signal.signal(signal.SIGALRM, _on_alarm)
    try:
        signal.setitimer(signal.ITIMER_REAL, seconds)
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


class TextPartitioner:
    """Partitions markdown text into unstructured.io elements."""

    def __init__(
        self,
        disable_markdown_link_parsing: bool = True,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.disable_markdown_link_parsing = disable_markdown_link_parsing
        self.timeout_seconds = timeout_seconds
        self._logged_no_time_limit = False

    def _can_enforce_time_limit(self) -> bool:
        if self.timeout_seconds <= 0:
            return False
        if (
            hasattr(signal, "SIGALRM")
            and threading.current_thread() is threading.main_thread()
        ):
            return True
        if not self._logged_no_time_limit:
            logger.debug(
                "Partition time limit not enforced: SIGALRM is only available "
                "on the main thread of POSIX platforms"
            )
            self._logged_no_time_limit = True
        return False

    def partition_text(self, text: str) -> list[dict[str, Any]]:
        """Partition markdown text into structured elements.

        Uses unstructured.partition.md for markdown-aware partitioning,
        which preserves structure (headers, paragraphs, lists, etc.)

        Args:
            text: Markdown text content from Document.text field

        Returns:
            List of element dictionaries compatible with chunking pipeline

        Raises:
            ImportError: If unstructured library is not installed
            PartitionTimeoutError: If partitioning exceeds ``timeout_seconds``
        """
        try:
            from unstructured.partition.md import partition_md
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(
                "unstructured is required for text partitioning. "
                "Install with: pip install 'acryl-datahub[unstructured]'"
            ) from e

        # Handle empty text - return empty list
        # This avoids potential issues with empty input in various unstructured versions
        if not text or text.strip() == "":
            logger.debug("Empty text provided, returning empty element list")
            return []

        time_limit = (
            _time_limit(self.timeout_seconds)
            if self._can_enforce_time_limit()
            else contextlib.nullcontext()
        )
        with time_limit:
            if self.disable_markdown_link_parsing:
                from datahub.ingestion.source.datahub_documents.markdown_link_guard import (
                    NoInlineLinksExtension,
                )

                elements = partition_md(
                    text=text,
                    extensions=[
                        *_DEFAULT_MARKDOWN_EXTENSIONS,
                        NoInlineLinksExtension(),
                    ],
                )
            else:
                elements = partition_md(text=text)

        # Convert to dict format expected by chunking
        element_dicts = [elem.to_dict() for elem in elements]

        # unstructured.partition_md yields zero elements for minimal inputs
        # (e.g. a single character like "x"), which would otherwise make the
        # document look empty and get silently dropped downstream. Fall back to
        # one text element built from the raw content so any non-empty document
        # stays embeddable.
        if not element_dicts:
            from unstructured.documents.elements import Text

            element_dicts = [Text(text=text.strip()).to_dict()]

        logger.debug(f"Partitioned text into {len(element_dicts)} elements")
        return element_dicts
