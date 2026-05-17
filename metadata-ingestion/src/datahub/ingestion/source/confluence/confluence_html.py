"""Convert Confluence storage-format HTML to Markdown for document ingestion."""

import logging
import re

logger = logging.getLogger(__name__)

# Macros that are pure navigation/UI chrome and carry no semantic content.
_DISCARD_MACROS = frozenset(
    [
        "toc",
        "table-of-contents",
        "anchor",
        "pagetree",
        "children",
        "recently-updated",
        "page-index",
        "attachments",
        "contributors",
        "contributors-summary",
    ]
)

# Macros whose ac:rich-text-body / ac:plain-text-body we want to keep,
# but whose surrounding wrapper and parameters should be dropped.
_PASSTHROUGH_MACROS = frozenset(
    [
        "panel",
        "info",
        "warning",
        "tip",
        "note",
        "expand",
        "section",
        "column",
        "div",
        "html",
    ]
)


def _preprocess_confluence_storage(html: str) -> str:
    """Transform Confluence Storage Format XML into clean HTML before markdownify.

    Handles:
    - ac:structured-macro: discard navigation macros, convert code macros to
      <pre><code> blocks, unwrap content macros.
    - ac:parameter: always stripped (parameter values must never appear as text).
    - ac:plain-text-body / ac:rich-text-body: unwrapped, body kept.
    - ac:image / ac:link: dropped or simplified.
    """
    try:
        from bs4 import BeautifulSoup, NavigableString, Tag
    except ImportError:
        return html

    # Confluence storage format uses XML namespaces — parse as html.parser
    # which is lenient about unknown tags.
    soup = BeautifulSoup(html, "html.parser")

    # --- 1. Handle ac:structured-macro -----------------------------------------
    for macro in soup.find_all("ac:structured-macro"):
        assert isinstance(macro, Tag)
        name = (macro.get("ac:name") or "").lower()

        if name in _DISCARD_MACROS:
            macro.decompose()
            continue

        if name == "code" or name == "noformat":
            # Extract language param (best-effort)
            lang_tag = macro.find("ac:parameter", attrs={"ac:name": "language"})
            language = lang_tag.get_text(strip=True) if lang_tag else ""

            # Extract body — prefer plain-text-body, fall back to rich-text-body
            body_tag = macro.find("ac:plain-text-body") or macro.find(
                "ac:rich-text-body"
            )
            body = body_tag.get_text() if body_tag else ""

            code_tag = soup.new_tag("code")
            if language:
                code_tag["class"] = f"language-{language}"
            code_tag.append(NavigableString(body))

            pre_tag = soup.new_tag("pre")
            pre_tag.append(code_tag)
            macro.replace_with(pre_tag)
            continue

        if name in _PASSTHROUGH_MACROS or name not in _DISCARD_MACROS:
            # Keep rich/plain text body content, discard the macro shell.
            body_tag = macro.find("ac:rich-text-body") or macro.find(
                "ac:plain-text-body"
            )
            if body_tag:
                assert isinstance(body_tag, Tag)
                macro.replace_with(body_tag)
                # Unwrap the body tag itself so its children flow inline.
                body_tag.unwrap()
            else:
                macro.decompose()
            continue

    # --- 2. Strip all remaining ac:parameter elements --------------------------
    for param in soup.find_all("ac:parameter"):
        param.decompose()

    # --- 3. Unwrap residual ac:* wrapper tags (ac:image, ac:link, etc.) --------
    for tag in soup.find_all(re.compile(r"^ac:")):
        assert isinstance(tag, Tag)
        tag.unwrap()

    return str(soup)


def html_storage_to_markdown(html: str) -> str:
    """Convert Confluence body.storage HTML to well-formed Markdown.

    Preserves headings, lists, tables, code blocks, links, and emphasis where
    possible. Falls back to whitespace-normalized plain text if conversion fails.
    """
    if not html or not html.strip():
        return ""

    try:
        from markdownify import markdownify as md

        clean_html = _preprocess_confluence_storage(html)
        markdown = md(
            clean_html,
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
