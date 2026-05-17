"""Unit tests for Confluence HTML to Markdown conversion."""

import pytest

from datahub.ingestion.source.confluence.confluence_html import html_storage_to_markdown


def test_html_storage_to_markdown_preserves_headings() -> None:
    html = "<h1>Overview</h1><h2>Details</h2><p>Body text</p>"
    markdown = html_storage_to_markdown(html)

    assert "# Overview" in markdown
    assert "## Details" in markdown
    assert "Body text" in markdown


def test_html_storage_to_markdown_preserves_lists_and_code() -> None:
    html = """
    <ul><li>First</li><li>Second</li></ul>
    <pre><code>SELECT 1</code></pre>
    <p>Inline <code>flag</code> value</p>
    """
    markdown = html_storage_to_markdown(html)

    assert "First" in markdown
    assert "Second" in markdown
    assert "SELECT 1" in markdown
    assert "flag" in markdown


def test_html_storage_to_markdown_preserves_tables() -> None:
    html = """
    <table>
      <tr><th>Column</th><th>Value</th></tr>
      <tr><td>name</td><td>widget</td></tr>
    </table>
    """
    markdown = html_storage_to_markdown(html)

    assert "Column" in markdown
    assert "Value" in markdown
    assert "widget" in markdown


def test_html_storage_to_markdown_preserves_links_and_emphasis() -> None:
    html = '<p>See <a href="https://example.com">docs</a> for <strong>important</strong> info.</p>'
    markdown = html_storage_to_markdown(html)

    assert "docs" in markdown
    assert "example.com" in markdown
    assert "important" in markdown


def test_toc_macro_is_removed() -> None:
    """TOC macro parameters must not leak into output text."""
    html = (
        '<ac:structured-macro ac:name="toc" ac:schema-version="1">'
        '<ac:parameter ac:name="minLevel">2</ac:parameter>'
        '<ac:parameter ac:name="maxLevel">3</ac:parameter>'
        '<ac:parameter ac:name="style">default</ac:parameter>'
        '<ac:parameter ac:name="type">list</ac:parameter>'
        '<ac:parameter ac:name="printable">true</ac:parameter>'
        "</ac:structured-macro>"
        "<h1>Heading</h1><p>Body text.</p>"
    )
    markdown = html_storage_to_markdown(html)

    assert "# Heading" in markdown
    assert "Body text" in markdown
    # Parameter values must not appear
    for leaked in ["minLevel", "maxLevel", "default", "printable", "23defaultlisttrue"]:
        assert leaked not in markdown


def test_code_macro_renders_as_fenced_block() -> None:
    """Code macro must become a fenced code block; parameter values must not leak."""
    html = (
        '<ac:structured-macro ac:name="code" ac:schema-version="1">'
        '<ac:parameter ac:name="language">sql</ac:parameter>'
        '<ac:parameter ac:name="borderStyle">wide</ac:parameter>'
        '<ac:parameter ac:name="width">1800</ac:parameter>'
        "<ac:plain-text-body><![CDATA[SELECT 1 FROM dual;]]></ac:plain-text-body>"
        "</ac:structured-macro>"
    )
    markdown = html_storage_to_markdown(html)

    assert "SELECT 1 FROM dual;" in markdown
    # Parameter values must not appear as bare text
    for leaked in ["sqlwide1800", "wide", "1800"]:
        assert leaked not in markdown


def test_panel_macro_body_is_preserved() -> None:
    """Panel/info macros should keep their body content, drop the macro wrapper."""
    html = (
        '<ac:structured-macro ac:name="info">'
        '<ac:parameter ac:name="title">Note</ac:parameter>'
        "<ac:rich-text-body><p>Important note here.</p></ac:rich-text-body>"
        "</ac:structured-macro>"
    )
    markdown = html_storage_to_markdown(html)

    assert "Important note here." in markdown
    assert "Note" not in markdown  # parameter value dropped


def test_unknown_macro_without_body_is_dropped() -> None:
    """A passthrough macro with no rich/plain-text-body must be silently dropped."""
    html = (
        '<ac:structured-macro ac:name="panel">'
        '<ac:parameter ac:name="title">Title</ac:parameter>'
        # no ac:rich-text-body
        "</ac:structured-macro>"
        "<p>After macro.</p>"
    )
    markdown = html_storage_to_markdown(html)
    assert "After macro." in markdown
    assert "Title" not in markdown


def test_residual_ac_tags_are_stripped() -> None:
    """Residual ac:* tags (e.g. ac:image, ac:link) must not appear in output."""
    html = (
        "<p>See <ac:link><ri:page ri:content-title='Home'/></ac:link> for details.</p>"
    )
    markdown = html_storage_to_markdown(html)
    assert "for details" in markdown
    assert "ac:" not in markdown


def test_html_storage_to_markdown_empty_input() -> None:
    assert html_storage_to_markdown("") == ""
    assert html_storage_to_markdown("   ") == ""


def test_html_storage_to_markdown_fallback_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sys
    from unittest.mock import MagicMock

    broken_markdownify = MagicMock()
    broken_markdownify.markdownify.side_effect = RuntimeError("conversion failed")
    monkeypatch.setitem(sys.modules, "markdownify", broken_markdownify)

    markdown = html_storage_to_markdown("<p>Hello <b>world</b></p>")

    assert "Hello" in markdown
    assert "world" in markdown
    assert "<" not in markdown
