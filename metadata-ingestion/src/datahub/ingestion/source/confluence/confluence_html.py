"""Convert Confluence storage-format HTML to Markdown for document ingestion.

Confluence pages are stored in an XHTML-based format called "Confluence Storage
Format" which uses custom XML namespaces (ac:, ri:, at:) for macros, links,
images, and resource identifiers.

Official spec:
  https://confluence.atlassian.com/doc/confluence-storage-format-790796544.html

Full macro reference:
  https://confluence.atlassian.com/display/DOC/Macros
"""

import logging
import re

logger = logging.getLogger(__name__)

# Macros that are pure navigation/UI chrome and carry no authored content.
# Sources: "Content Organization", "Content Aggregation & Reporting", and
# "User & Collaboration" categories from the Confluence macro reference.
_DISCARD_MACROS = frozenset(
    [
        # Table of contents / navigation
        "toc",
        "table-of-contents",
        "toc-zone",
        "anchor",
        "pagetree",
        "pagetreesearch",
        "page-tree",
        "page-index",
        "navmap",
        # Children / related pages
        "children",
        "children-display",
        "include",
        "include-page",
        # Dynamic content aggregators (not authored text)
        "recently-updated",
        "recently-updated-dashboard",
        "blogposts",
        "contentbylabel",
        "contentbyuser",
        "content-report-table",
        "taskreport",
        "labels-list",
        "popular-labels",
        "recently-used-labels",
        "global-reports",
        # Attachments / files / media viewers
        "attachments",
        "gallery",
        "pdf",
        "viewfile",
        "multimedia",
        "office-excel",
        "office-powerpoint",
        "office-word",
        # User / collaboration chrome
        "contributors",
        "contributors-summary",
        "profile",
        "user-profile",
        "profilepicture",
        "im",
        "network",
        "favpages",
        # Search / live widgets
        "livesearch",
        "search",
        "rss",
        "rssfeed",
        "chart",
        "jira",
        "jiraissues",
        "jirachart",
        "widget",
        "roadmapplanner",
        "calendar",
        # Template / space scaffolding
        "create-from-template",
        "create-space-button",
        # Status badge — decorative label, title usually redundant with context
        "status",
    ]
)

# Macros whose ac:rich-text-body / ac:plain-text-body we want to keep
# but whose surrounding wrapper and all ac:parameter values are dropped.
# Sources: "Formatting & Layout" category from the Confluence macro reference.
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
        "excerpt",
        "page-properties",
    ]
)


def _preprocess_confluence_storage(html: str) -> str:
    """Transform Confluence Storage Format XML into clean HTML before markdownify.

    Processing rules (see module docstring for spec links):
    - Navigation/UI macros (_DISCARD_MACROS): removed entirely.
    - code/noformat macros: converted to <pre><code> blocks with language class.
    - Content wrapper macros (_PASSTHROUGH_MACROS): body kept, shell + params dropped.
    - All other unknown macros: body kept if present, else dropped.
    - ac:parameter elements: always stripped — values must never appear as text.
    - Residual ac:*/ri:*/at:* tags: unwrapped (children kept, tag removed).
    """
    try:
        from bs4 import BeautifulSoup, NavigableString, Tag
    except ImportError:
        return html

    # html.parser is lenient about unknown XML namespace tags.
    soup = BeautifulSoup(html, "html.parser")

    # --- 1. Handle ac:structured-macro -----------------------------------------
    for macro in soup.find_all("ac:structured-macro"):
        assert isinstance(macro, Tag)
        name_val = macro.get("ac:name")
        name = (name_val if isinstance(name_val, str) else "").lower()

        if name in _DISCARD_MACROS:
            macro.decompose()
            continue

        if name in ("code", "noformat"):
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

        # Passthrough and unknown macros: preserve body content, drop shell.
        body_tag = macro.find("ac:rich-text-body") or macro.find("ac:plain-text-body")
        if body_tag:
            assert isinstance(body_tag, Tag)
            macro.replace_with(body_tag)
            body_tag.unwrap()
        else:
            macro.decompose()

    # --- 2. Strip all remaining ac:parameter elements --------------------------
    for param in soup.find_all("ac:parameter"):
        param.decompose()

    # --- 3. Unwrap residual namespace tags (ac:*, ri:*, at:*) ------------------
    # e.g. ac:image, ac:link, ac:task-list, ri:page, at:var
    for tag in soup.find_all(re.compile(r"^(ac|ri|at):")):
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
