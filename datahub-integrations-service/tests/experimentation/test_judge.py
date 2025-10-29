import json

from json_repair import repair_json

from datahub_integrations.experimentation.chatbot.judge import (
    _extract_json_from_markdown,
)


def test_extract_json_from_markdown_with_code_block() -> None:
    """Test extracting JSON from markdown code blocks."""
    markdown_response = """```json
{
    "choice": true,
    "justification": "The response meets all criteria."
}
```"""

    result = _extract_json_from_markdown(markdown_response)
    parsed = json.loads(result)

    assert parsed["choice"] is True
    assert parsed["justification"] == "The response meets all criteria."


def test_extract_json_from_markdown_without_lang() -> None:
    """Test extracting JSON from markdown code blocks without language tag."""
    markdown_response = """```
{
    "choice": false,
    "justification": "Does not meet criteria."
}
```"""

    result = _extract_json_from_markdown(markdown_response)
    parsed = json.loads(result)

    assert parsed["choice"] is False


def test_extract_json_from_plain_text() -> None:
    """Test that plain JSON without markdown is returned as-is."""
    plain_json = '{"choice": true, "justification": "Good response"}'

    result = _extract_json_from_markdown(plain_json)

    assert result == plain_json


def test_repair_json_with_unescaped_newlines() -> None:
    """Test that json_repair fixes unescaped newlines in JSON strings."""
    # This is the actual issue we encountered - JSON with literal newlines
    malformed_json = """{
    "choice": false,
    "justification": "The response has issues:

1. Missing key information
2. Contains errors

Overall, it fails the criteria."
}"""

    # This should fail to parse as-is
    try:
        json.loads(malformed_json)
        raise AssertionError("Should have raised JSONDecodeError")
    except json.JSONDecodeError:
        pass  # Expected

    # But repair_json should fix it
    repaired = repair_json(malformed_json)
    parsed = json.loads(repaired)

    assert parsed["choice"] is False
    assert "Missing key information" in parsed["justification"]
    assert "Contains errors" in parsed["justification"]


def test_repair_json_with_trailing_commas() -> None:
    """Test that json_repair fixes trailing commas."""
    malformed_json = """{
    "choice": true,
    "justification": "Good response",
}"""

    repaired = repair_json(malformed_json)
    parsed = json.loads(repaired)

    assert parsed["choice"] is True


def test_full_pipeline_markdown_and_repair() -> None:
    """Test the full pipeline: extract from markdown then repair JSON."""
    markdown_with_newlines = """```json
{
    "choice": true,
    "justification": "Response evaluation:

- Point 1: Correct
- Point 2: Valid

Conclusion: Meets criteria."
}
```"""

    # Extract from markdown
    extracted = _extract_json_from_markdown(markdown_with_newlines)

    # Repair JSON
    repaired = repair_json(extracted)

    # Should parse successfully
    parsed = json.loads(repaired)

    assert parsed["choice"] is True
    assert "Point 1: Correct" in parsed["justification"]
    assert "Conclusion: Meets criteria." in parsed["justification"]
