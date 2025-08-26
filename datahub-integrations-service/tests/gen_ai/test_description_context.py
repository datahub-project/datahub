from unittest.mock import patch

from datahub_integrations.gen_ai.description_context import (
    sanitize_and_truncate_description,
    sanitize_html_content,
    sanitize_markdown_content,
    truncate_with_ellipsis,
)


def test_remove_markdown_data_image_embeds_preserve_alt_text() -> None:
    """Test that markdown image embeds with data URLs are removed but alt text is preserved."""
    text = "This is a description with an embedded image ![alt text](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==) and more text"
    result = sanitize_markdown_content(text)
    expected = "This is a description with an embedded image alt text and more text"
    assert result == expected


def test_preserve_alt_text_with_special_characters() -> None:
    """Test that alt text with special characters is preserved."""
    text = "Description ![Complex alt text with (parentheses) and  émojis 🎉](data:image/jpeg;base64,abc123) end"
    result = sanitize_markdown_content(text)
    expected = "Description Complex alt text with (parentheses) and  émojis 🎉 end"
    assert result == expected


def test_empty_alt_text() -> None:
    """Test that alt text with special characters is preserved."""
    text = "Description with empty alt text![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAMgAAACqC) end"
    result = sanitize_markdown_content(text)
    expected = "Description with empty alt text end"
    assert result == expected


def test_remove_img_tags() -> None:
    """Test that img tags are properly removed."""
    text = 'This is a description with an image <img alt="test" src="image.jpg"> and more text'
    result = sanitize_html_content(text)
    expected = "This is a description with an image  and more text"
    assert result == expected


def test_remove_multiple_html_tags() -> None:
    """Test that multiple HTML tags are removed."""
    text = (
        '<p>This is a <strong>bold</strong> paragraph with <a href="link">link</a></p>'
    )
    result = sanitize_html_content(text)
    expected = "This is a bold paragraph with link"
    assert result == expected


def test_decode_html_entities() -> None:
    """Test that HTML entities are properly decoded."""
    text = "This &amp; that &lt; 10 &gt; 5"
    result = sanitize_html_content(text)
    expected = "This & that < 10 > 5"
    assert result == expected


def test_remove_nested_tags() -> None:
    """Test that nested HTML tags are properly removed."""
    text = "<div><p>Text <span>inside</span> nested tags</p></div>"
    result = sanitize_html_content(text)
    expected = "Text inside nested tags"
    assert result == expected


def test_handle_whitespace_only() -> None:
    """Test handling of whitespace-only input."""
    result = sanitize_html_content("   \n\t  ")
    assert result == ""


def test_preserve_text_without_tags() -> None:
    """Test that text without HTML tags is preserved."""
    text = "This is plain text without any HTML tags"
    result = sanitize_html_content(text)
    assert result == text


def test_remove_self_closing_tags() -> None:
    """Test that self-closing tags are removed."""
    text = "Text with <br/> line break and <hr/> horizontal rule"
    result = sanitize_html_content(text)
    expected = "Text with  line break and  horizontal rule"
    assert result == expected


def test_no_truncation_needed() -> None:
    """Test when text is shorter than max_length."""
    text = "Short text"
    result = truncate_with_ellipsis(text, 23, suffix="...")
    assert result == text


def test_truncation_with_default_suffix() -> None:
    """Test truncation with default ellipsis suffix."""
    text = "This is a very long text that needs to be truncated"
    result = truncate_with_ellipsis(text, 22, suffix="...")
    expected = "This is a very long..."
    assert result == expected


def test_truncation_with_custom_suffix() -> None:
    """Test truncation with custom suffix."""
    text = "Long text to truncate"
    result = truncate_with_ellipsis(text, 11, suffix=" [more]")
    expected = "Long [more]"
    assert result == expected


def test_truncation_at_exact_length() -> None:
    """Test truncation when text length equals max_length."""
    text = "Exactly ten"
    result = truncate_with_ellipsis(text, 11)
    assert result == text


def test_truncation_with_unicode_characters() -> None:
    """Test truncation with unicode characters."""
    text = "Text with émojis 🎉 and special chars"
    result = truncate_with_ellipsis(text, 19)
    expected = "Text with émojis..."
    assert result == expected


def test_suffix_longer_than_max_length() -> None:
    """Test when suffix is longer than max_length."""
    text = "Short"
    result = truncate_with_ellipsis(text, 3, suffix="...")
    expected = "..."
    assert result == expected


def test_sanitize_and_truncate_combined() -> None:
    """Test that both sanitization and truncation work together."""
    text = "<p>This is a <strong>very long</strong> description with HTML tags that needs to be both sanitized and truncated to fit within the specified length limit</p>"
    result = sanitize_and_truncate_description(text, 27)
    expected = "This is a very long desc..."
    assert result == expected


def test_keep_as_is_within_max_length_if_error() -> None:
    """Test sanitization without truncation."""
    with patch(
        "datahub_integrations.gen_ai.description_context.sanitize_html_content"
    ) as mock_sanitize_html_content:
        mock_sanitize_html_content.side_effect = Exception("Test error")
        text = "<p>Short <strong>text</strong></p>"
        result = sanitize_and_truncate_description(text, 50)
        assert result == text


def test_truncate_only_no_sanitization() -> None:
    """Test truncation without sanitization."""
    text = "This is a very long plain text without any HTML tags that needs truncation"
    result = sanitize_and_truncate_description(text, 22)
    expected = "This is a very long..."
    assert result == expected


def test_handle_empty_string() -> None:
    """Test handling of empty string."""
    result = sanitize_and_truncate_description("", 100)
    assert result == ""


def test_remove_img_tags_and_truncate() -> None:
    """Test removal of img tags and truncation."""
    text = 'Description with image <img alt="test" src="image.jpg"> and more content that exceeds the limit'
    result = sanitize_and_truncate_description(text, 25)
    expected = "Description with image..."
    assert result == expected


def test_decode_entities_and_truncate() -> None:
    """Test decoding HTML entities and truncation."""
    text = "Text with &amp; entities &lt; 10 &gt; 5 and more content"
    result = sanitize_and_truncate_description(text, 23)
    expected = "Text with & entities..."
    assert result == expected


def test_edge_case_very_long_html() -> None:
    """Test with extremely long HTML content."""
    # Create a very long HTML string
    long_html = "<p>" + "A" * 10000 + "</p>"
    result = sanitize_and_truncate_description(long_html, 100)

    assert len(result) <= 100
    assert result.endswith("...")
    assert "<p>" not in result
    assert result.startswith("A")


def test_sanitize_markdown_and_html_combined() -> None:
    """Test that both markdown and HTML sanitization work together."""
    text = "Description with ![alt text](data:image/png;base64,abc123) and <img src='image.jpg'> HTML"
    result = sanitize_and_truncate_description(text, 50)
    expected = "Description with alt text and  HTML"
    assert result == expected


def test_sanitize_markdown_with_html_entities() -> None:
    """Test markdown sanitization with HTML entities."""
    text = "Description ![Alt &amp; text](data:image/png;base64,abc123) with entities"
    result = sanitize_and_truncate_description(text, 50)
    expected = "Description Alt & text with entities"
    assert result == expected


def test_multiple_markdown_images() -> None:
    """Test handling of multiple markdown images."""
    text = "Start ![First](data:image/png;base64,abc123) middle ![Second](data:image/jpeg;base64,xyz789) end"
    result = sanitize_markdown_content(text)
    expected = "Start First middle Second end"
    assert result == expected


def test_markdown_images_with_regular_links() -> None:
    """Test that regular markdown links are not affected."""
    text = "Description ![alt](data:image/png;base64,abc123) and [regular link](http://example.com)"
    result = sanitize_markdown_content(text)
    expected = "Description alt and [regular link](http://example.com)"
    assert result == expected
