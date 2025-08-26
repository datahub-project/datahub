import pytest

from datahub_integrations.gen_ai.description_context import (
    DescriptionParsingError,
    parse_columns_llm_output,
    parse_table_desc_llm_output,
)


def test_parse_table_desc_llm_output_valid_markdown() -> None:
    """Test parsing a valid markdown heading with description."""
    input_text = """### This is a table description
    Some additional text that should be included
    More details about the table"""
    result = parse_table_desc_llm_output(input_text)
    assert (
        result
        == "### This is a table description\n    Some additional text that should be included\n    More details about the table"
    )


def test_parse_table_desc_llm_output_multiple_headings() -> None:
    """Test parsing text with multiple markdown headings."""
    input_text = """### First heading
    Some text
    More details
    ### Second heading
    More text"""
    result = parse_table_desc_llm_output(input_text)
    assert (
        result
        == "### First heading\n    Some text\n    More details\n    ### Second heading\n    More text"
    )


def test_parse_table_desc_llm_output_no_heading() -> None:
    """Test parsing text with no markdown heading."""
    input_text = "This is just some text without a markdown heading"
    with pytest.raises(DescriptionParsingError) as exc_info:
        parse_table_desc_llm_output(input_text)
    assert str(exc_info.value) == "No markdown heading found in the text."


def test_parse_table_desc_llm_output_empty_text() -> None:
    """Test parsing empty text."""
    input_text = ""
    with pytest.raises(DescriptionParsingError) as exc_info:
        parse_table_desc_llm_output(input_text)
    assert str(exc_info.value) == "No markdown heading found in the text."


def test_parse_table_desc_llm_output_with_quotes() -> None:
    """Test parsing text with quotes around the description."""
    input_text = """### 'This is a quoted description'
    Some additional text
    More details about the table"""
    result = parse_table_desc_llm_output(input_text)
    assert (
        result
        == "### 'This is a quoted description'\n    Some additional text\n    More details about the table"
    )


def test_parse_table_desc_llm_output_with_text_before_heading() -> None:
    """Test parsing text with content before the markdown heading."""
    input_text = """Some introductory text
    More context
    ### This is the actual description
    Some additional text
    More details about the table"""
    result = parse_table_desc_llm_output(input_text)
    assert (
        result
        == "### This is the actual description\n    Some additional text\n    More details about the table"
    )


def test_parse_columns_llm_output_valid_dict() -> None:
    """Test parsing a valid dictionary string."""
    input_text = """Here are column descriptions
    {
        "id": "Primary key for the table",
        "name": "Name of the entity",
        "created_at": "Creation timestamp"
    }
    """
    result, error = parse_columns_llm_output(input_text)
    assert result == {
        "id": "Primary key for the table",
        "name": "Name of the entity",
        "created_at": "Creation timestamp",
    }
    assert error is None


def test_parse_columns_llm_output_with_apostrophes() -> None:
    """Test parsing a dictionary string containing apostrophes in values."""
    input_text = """Here are column descriptions
    {
        "id": "Primary key for the table",
        "name": "User's name",
        "description": "Customer's description"
    }
    """
    result, error = parse_columns_llm_output(input_text)
    assert result == {
        "id": "Primary key for the table",
        "name": "User's name",
        "description": "Customer's description",
    }
    assert error is None


def test_parse_columns_llm_output_no_dict() -> None:
    """Test parsing text with no dictionary."""
    input_text = "This is just some text without a dictionary"
    result, error = parse_columns_llm_output(input_text)
    assert result is None
    assert error == "No dictionary found in the text."


def test_parse_columns_llm_output_invalid_dict() -> None:
    """Test parsing an invalid dictionary string."""
    input_text = """Here are column descriptions
    {
        "id": "Primary key for the table",
        "name": "Name of the entity",
        "created_at": "Creation timestamp"
    """
    result, error = parse_columns_llm_output(input_text)
    assert result is None
    assert error == "No dictionary found in the text."


def test_parse_columns_llm_output_empty_dict() -> None:
    """Test parsing an empty dictionary string."""
    input_text = """Here are column descriptions
    {
    }
    """
    result, error = parse_columns_llm_output(input_text)
    assert result == {}
    assert error is None


def test_parse_columns_llm_output_with_newlines() -> None:
    """Test parsing a dictionary string with newlines in values."""
    input_text = """Here are column descriptions
    {
        "id": "Primary key for the table",
        "description": '''This is a multi-line
description with
newlines'''
    }
    """
    result, error = parse_columns_llm_output(input_text)
    assert result == {
        "id": "Primary key for the table",
        "description": "This is a multi-line\ndescription with\nnewlines",
    }
    assert error is None
