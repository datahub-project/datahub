from unittest.mock import patch

import pytest

from datahub_integrations.gen_ai.description_context import (
    sanitize_and_truncate_description,
    sanitize_html_content,
    sanitize_markdown_content,
    truncate_with_ellipsis,
)


@pytest.mark.parametrize(
    "before_text, after_text",
    [
        (
            "This is a description with an embedded image ![alt text](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==) and more text",
            "This is a description with an embedded image alt text and more text",
        ),
        (
            "Description ![Complex alt text with (parentheses) and  émojis 🎉](data:image/jpeg;base64,abc123) end",
            "Description Complex alt text with (parentheses) and  émojis 🎉 end",
        ),
        (
            "Description with empty alt text![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAMgAAACqC) end",
            "Description with empty alt text end",
        ),
        (
            "Start ![First](data:image/png;base64,abc123) middle ![Second](data:image/jpeg;base64,xyz789) end",
            "Start First middle Second end",
        ),
        (
            "Description ![alt](data:image/png;base64,abc123) and [regular link](http://example.com)",
            "Description alt and [regular link](http://example.com)",
        ),
    ],
)
def test_santitize_markdown_content(before_text: str, after_text: str) -> None:
    """Test that markdown image embeds with data URLs are removed but alt text is preserved."""
    assert sanitize_markdown_content(before_text) == after_text


@pytest.mark.parametrize(
    "before_text, after_text",
    [
        (
            'This is a description with an image <img alt="test" src="image.jpg"> and more text',
            "This is a description with an image  and more text",
        ),
        (
            '<p>This is a <strong>bold</strong> paragraph with <a href="link">link</a></p>',
            "This is a bold paragraph with link",
        ),
        ("This &amp; that &lt; 10 &gt; 5", "This & that < 10 > 5"),
        (
            "<div><p>Text <span>inside</span> nested tags</p></div>",
            "Text inside nested tags",
        ),
        ("   \n\t  ", ""),
        (
            "This is plain text without any HTML tags",
            "This is plain text without any HTML tags",
        ),
        (
            "Text with <br/> line break and <hr/> horizontal rule",
            "Text with  line break and  horizontal rule",
        ),
    ],
)
def test_santitize_html_content(before_text: str, after_text: str) -> None:
    """Test that img tags are properly removed."""
    assert sanitize_html_content(before_text) == after_text


@pytest.mark.parametrize(
    "before_text, after_text",
    [
        ("Short text", "Short text"),
        (
            "This is a very long text that needs to be truncated",
            "This is a very long ...",
        ),
        ("Exactly same length tex", "Exactly same length tex"),
    ],
)
def test_truncate_with_ellipsis(before_text: str, after_text: str) -> None:
    """Test when text is shorter than max_length."""
    assert truncate_with_ellipsis(before_text, 23, suffix="...") == after_text


@pytest.mark.parametrize(
    "before_text, after_text",
    [
        (
            "<p>This is a <strong>very long</strong> description with HTML tags that needs to be both sanitized and truncated to fit within the specified length limit</p>",
            "This is a very long descrip...",
        ),
        (
            "This is a very long plain text without any HTML tags that needs truncation",
            "This is a very long plain t...",
        ),
        ("", ""),
        (
            'Description with image <img alt="test" src="image.jpg"> and more content that exceeds the limit',
            "Description with image  and...",
        ),
        ("<p>" + "A" * 10000 + "</p>", "A" * 27 + "..."),
    ],
)
def test_sanitize_and_truncate_combined(before_text: str, after_text: str) -> None:
    """Test that both sanitization and truncation work together."""
    assert sanitize_and_truncate_description(before_text, 30) == after_text


def test_keep_as_is_within_max_length_if_error() -> None:
    """Test sanitization without truncation."""
    with patch(
        "datahub_integrations.gen_ai.description_context.sanitize_html_content"
    ) as mock_sanitize_html_content:
        mock_sanitize_html_content.side_effect = Exception("Test error")
        text = "<p>Short <strong>text</strong></p>"
        result = sanitize_and_truncate_description(text, 50)
        assert result == text
