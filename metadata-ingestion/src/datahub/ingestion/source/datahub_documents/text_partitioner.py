"""Text partitioner for converting markdown text to unstructured elements."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class TextPartitioner:
    """Partitions markdown text into unstructured.io elements."""

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

        # Partition as markdown
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
