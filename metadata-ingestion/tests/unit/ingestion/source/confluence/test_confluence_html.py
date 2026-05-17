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
