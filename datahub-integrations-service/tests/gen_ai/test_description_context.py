from unittest.mock import patch

import pytest
from pydantic import ConfigDict

from datahub_integrations.gen_ai.description_context import (
    ExtractedTableInfo,
    SchemaFieldMetadata,
    sanitize_and_truncate_description,
    sanitize_html_content,
    sanitize_markdown_content,
    transform_table_info_for_llm,
    truncate_with_ellipsis,
)

# TODO: Fix test data - SchemaFieldMetadata doesn't have 'field_path' field
# Temporarily allow extra fields for pydantic v1→v2 compatibility
SchemaFieldMetadata.model_config = ConfigDict(extra="ignore")


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


@pytest.mark.parametrize(
    "urn_field_path, expected_column_name, description",
    [
        (
            "Client Revenue %28Local%29",
            "Client Revenue (Local)",
            "Field with parentheses should be URL-decoded",
        ),
        (
            "Revenue%2C Total",
            "Revenue, Total",
            "Field with comma should be URL-decoded",
        ),
        (
            "Cost%20%28USD%29",
            "Cost (USD)",
            "Field with space and parentheses should be URL-decoded",
        ),
        (
            "Mix%2Ced%28Char%29s",
            "Mix,ed(Char)s",
            "Field with multiple special characters should be URL-decoded",
        ),
        (
            "NormalFieldName",
            "NormalFieldName",
            "Field without special characters should remain unchanged",
        ),
        (
            "Field%20With%20Spaces",
            "Field With Spaces",
            "Field with URL-encoded spaces should be decoded",
        ),
    ],
)
def test_transform_table_info_url_decodes_field_names(
    urn_field_path: str, expected_column_name: str, description: str
) -> None:
    """Test that transform_table_info_for_llm properly URL-decodes field names from SchemaField URNs."""

    # Create mock dataset URN
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:test,test_table,PROD)"

    # Create SchemaField URN with URL-encoded field path (simulating real DataHub URNs)
    schema_field_urn = f"urn:li:schemaField:({dataset_urn},{urn_field_path})"

    # Create mock extracted table info with URL-encoded field name in the URN
    extracted_table_info = ExtractedTableInfo(
        urn=dataset_urn,
        column_names={schema_field_urn: urn_field_path},  # URN maps to encoded path
        column_metadata={
            schema_field_urn: SchemaFieldMetadata(field_path=urn_field_path)  # type: ignore[call-arg]
        },
        column_descriptions={
            schema_field_urn: f"Test description for {expected_column_name}"
        },
        column_upstream_lineages={},
        column_sample_values={},
        column_tags={},
        column_glossary_terms={},
        table_tags=[],  # Empty list instead of None
        table_glossary_terms=[],  # Empty list instead of None
        table_view_properties=None,
        table_name="Test Table",
        table_description="Test table for URL decoding",
        table_subtype=None,
        table_domains_info=None,
        table_owners_info=None,
        table_upstream_lineage_info=None,
        table_downstream_lineage_info=[],  # Empty list instead of None
    )

    # Transform the table info (this is where URL decoding happens)
    table_info, column_info = transform_table_info_for_llm(extracted_table_info)

    # Verify that the column name was properly URL-decoded
    assert expected_column_name in column_info, (
        f"Expected column '{expected_column_name}' not found in column_info keys: {list(column_info.keys())}"
    )

    # Verify the column info has the correct decoded name
    column = column_info[expected_column_name]
    assert column.column_name == expected_column_name, (
        f"Column name should be '{expected_column_name}', got '{column.column_name}'"
    )

    # Verify that the description is properly associated
    assert f"Test description for {expected_column_name}" in str(column.description), (
        f"Description not properly associated for {description}"
    )


def test_transform_table_info_url_decoding_multiple_fields() -> None:
    """Test URL decoding works correctly when multiple fields with different encodings are present."""

    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:test,mixed_fields_table,PROD)"

    # Test cases with mixed URL-encoded and normal field names
    test_fields = [
        ("Client Revenue %28Local%29", "Client Revenue (Local)"),
        ("Revenue%2C Total", "Revenue, Total"),
        ("NormalFieldName", "NormalFieldName"),
        ("Mix%2Ced%28Special%29", "Mix,ed(Special)"),
    ]

    column_names = {}
    column_metadata = {}
    column_descriptions: dict[str, str | None] = {}

    for encoded_path, decoded_name in test_fields:
        schema_field_urn = f"urn:li:schemaField:({dataset_urn},{encoded_path})"
        column_names[schema_field_urn] = encoded_path
        column_metadata[schema_field_urn] = SchemaFieldMetadata(field_path=encoded_path)  # type: ignore[call-arg]
        column_descriptions[schema_field_urn] = f"Description for {decoded_name}"

    extracted_table_info = ExtractedTableInfo(
        urn=dataset_urn,
        column_names=column_names,
        column_metadata=column_metadata,
        column_descriptions=column_descriptions,
        column_upstream_lineages={},
        column_sample_values={},
        column_tags={},
        column_glossary_terms={},
        table_tags=[],  # Empty list instead of None
        table_glossary_terms=[],  # Empty list instead of None
        table_view_properties=None,
        table_name="Mixed Fields Test Table",
        table_description="Table with mixed URL-encoded field names",
        table_subtype=None,
        table_domains_info=None,
        table_owners_info=None,
        table_upstream_lineage_info=None,
        table_downstream_lineage_info=[],  # Empty list instead of None
    )

    # Transform the table info
    table_info, column_info = transform_table_info_for_llm(extracted_table_info)

    # Verify all field names are properly decoded
    expected_field_names = [decoded_name for _, decoded_name in test_fields]
    actual_field_names = list(column_info.keys())

    assert len(column_info) == len(test_fields), (
        f"Expected {len(test_fields)} columns, got {len(column_info)}"
    )

    for expected_name in expected_field_names:
        assert expected_name in actual_field_names, (
            f"Expected field '{expected_name}' not found. Actual fields: {actual_field_names}"
        )

        # Verify each column has correct data
        column = column_info[expected_name]
        assert column.column_name == expected_name, (
            f"Column name mismatch for {expected_name}"
        )
