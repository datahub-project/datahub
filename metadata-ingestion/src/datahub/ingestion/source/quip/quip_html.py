import logging
import re

logger = logging.getLogger(__name__)


def quip_html_to_markdown(html: str) -> str:
    """Convert Quip thread HTML to Markdown.

    Falls back to whitespace-normalized plain text if conversion fails (e.g. the
    optional ``markdownify`` dependency is missing).
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
            "Failed to convert Quip HTML to Markdown, falling back to plain text: %s",
            e,
        )
        return _strip_html_tags(html)


def _normalize_markdown(text: str) -> str:
    # Quip injects zero-width spaces (U+200B) into cells/paragraphs; strip them so
    # they don't pollute the indexed text or inflate the character count.
    text = text.replace("\u200b", "")
    text = text.replace("\r\n", "\n").strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def _strip_html_tags(html: str) -> str:
    clean_text = re.sub(r"<[^>]+>", " ", html)
    clean_text = clean_text.replace("\u200b", "")
    return re.sub(r"\s+", " ", clean_text).strip()
