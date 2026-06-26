from datahub.ingestion.source.quip.quip_html import quip_html_to_markdown


def test_empty_html_returns_empty() -> None:
    assert quip_html_to_markdown("") == ""
    assert quip_html_to_markdown("   ") == ""


def test_basic_html_converted_to_markdown() -> None:
    md = quip_html_to_markdown("<h1>Title</h1><p>Hello <b>world</b></p>")
    assert "Title" in md
    assert "world" in md
    assert "<h1>" not in md


def test_zero_width_spaces_stripped() -> None:
    # Quip injects U+200B into cells/paragraphs; it should be stripped from the output.
    md = quip_html_to_markdown("<p>a\u200bb</p>")
    assert "\u200b" not in md
    assert "ab" in md


def test_excessive_blank_lines_collapsed() -> None:
    md = quip_html_to_markdown("<p>a</p><br/><br/><br/><p>b</p>")
    assert "\n\n\n" not in md
