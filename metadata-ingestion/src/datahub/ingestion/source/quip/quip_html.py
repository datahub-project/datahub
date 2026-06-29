import logging
import re

from markdownify import ATX, markdownify as md

logger = logging.getLogger(__name__)

# Quip injects zero-width spaces (U+200B) into cells/paragraphs; strip them so
# they don't pollute the indexed text or inflate the character count.
ZERO_WIDTH_SPACE = "\u200b"

_BLANK_LINES = re.compile(r"\n{3,}")
_HTML_TAG = re.compile(r"<[^>]+>")
_WHITESPACE = re.compile(r"\s+")


def quip_html_to_markdown(html: str) -> str:
    """Convert Quip thread HTML to Markdown, falling back to plain text on failure."""
    if not html or not html.strip():
        return ""

    try:
        markdown = md(
            html,
            heading_style=ATX,
            bullets="-",
            strip=["script", "style"],
        )
        return _normalize_markdown(markdown)
    except Exception as e:
        logger.warning(
            "Failed to convert Quip HTML to Markdown, falling back to plain text: %s",
            e,
        )
        return _strip_html_tags(html)


def _normalize_markdown(text: str) -> str:
    text = text.replace(ZERO_WIDTH_SPACE, "")
    text = text.replace("\r\n", "\n").strip()
    return _BLANK_LINES.sub("\n\n", text)


def _strip_html_tags(html: str) -> str:
    clean_text = _HTML_TAG.sub(" ", html)
    clean_text = clean_text.replace(ZERO_WIDTH_SPACE, "")
    return _WHITESPACE.sub(" ", clean_text).strip()
