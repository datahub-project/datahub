"""Convert Confluence storage-format HTML to Markdown for document ingestion."""

import logging
import re

logger = logging.getLogger(__name__)


def html_storage_to_markdown(html: str) -> str:
    """Convert Confluence body.storage HTML to well-formed Markdown.

    Preserves headings, lists, tables, code blocks, links, and emphasis where
    possible. Falls back to whitespace-normalized plain text if conversion fails.
    """
    if not html or not html.strip():
        return ""

    try:
        from markdownify import markdownify as md

        markdown = md(
            html,
            heading_style="ATX",
            bullets="-",
            strip=["script", "style"],
        )
        return _normalize_markdown(markdown)
    except Exception as e:
        logger.warning(
            "Failed to convert Confluence HTML to Markdown, falling back to plain text: %s",
            e,
        )
        return _strip_html_tags(html)


def _normalize_markdown(text: str) -> str:
    """Collapse excessive blank lines while keeping paragraph breaks."""
    text = text.replace("\r\n", "\n").strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def _strip_html_tags(html: str) -> str:
    """Last-resort plain-text extraction when Markdown conversion is unavailable."""
    clean_text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", clean_text).strip()
