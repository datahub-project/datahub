import pytest

from datahub.ingestion.source.bigquery_v2.bigquery_helper import (
    unquote_and_decode_escape_seq,
    unquote_and_decode_unicode_escape_seq,
)


@pytest.mark.parametrize(
    "string, expected",
    [
        ('"line one\\nline two\\ttabbed"', "line one\nline two\ttabbed"),
        ('"a \\"quoted\\" word"', 'a "quoted" word'),
        ('"a backslash: \\\\"', "a backslash: \\"),
        ('"caf\\u00e9"', "café"),
        ('"\\ud83d\\ude00"', "\U0001f600"),  # surrogate pair (astral plane)
    ],
)
def test_unquote_and_decode_escape_seq(string: str, expected: str) -> None:
    assert unquote_and_decode_escape_seq(string) == expected


def test_unquote_and_decode_escape_seq_falls_back_on_invalid_json() -> None:
    # \a is valid GoogleSQL but not valid JSON
    assert unquote_and_decode_escape_seq('"\\a"') == "\\a"


def test_unquote_and_decode_escape_seq_falls_back_for_non_double_quotes() -> None:
    assert unquote_and_decode_escape_seq("'caf\\u00e9'", leading_quote="'") == "café"


def test_unquote_and_decode_unicode_escape_seq():
    # Test with a string that starts and ends with quotes and has Unicode escape sequences
    input_string = '"Hello \\u003cWorld\\u003e"'
    expected_output = "Hello <World>"
    result = unquote_and_decode_unicode_escape_seq(input_string)
    assert result == expected_output

    # Test with a string that does not start and end with quotes
    input_string = "Hello \\u003cWorld\\u003e"
    expected_output = "Hello <World>"
    result = unquote_and_decode_unicode_escape_seq(input_string)
    assert result == expected_output

    # Test with an empty string
    input_string = ""
    expected_output = ""
    result = unquote_and_decode_unicode_escape_seq(input_string)
    assert result == expected_output

    # Test with a string that does not have Unicode escape sequences
    input_string = "No escape sequences here"
    expected_output = "No escape sequences here"
    result = unquote_and_decode_unicode_escape_seq(input_string)
    assert result == expected_output

    # Test with a string that starts and ends with quotes but does not have escape sequences
    input_string = '"No escape sequences here"'
    expected_output = "No escape sequences here"
    result = unquote_and_decode_unicode_escape_seq(input_string)
    assert result == expected_output

    # Test with invalid Unicode escape sequences
    input_string = '"No escape \\u123 sequences here"'
    expected_output = "No escape \\u123 sequences here"
    result = unquote_and_decode_unicode_escape_seq(input_string)
    assert result == expected_output

    # Test with a string that has multiple Unicode escape sequences
    input_string = '"Hello \\u003cWorld\\u003e \\u003cAgain\\u003e \\u003cAgain\\u003e \\u003cAgain\\u003e"'
    expected_output = "Hello <World> <Again> <Again> <Again>"
    result = unquote_and_decode_unicode_escape_seq(input_string)
    assert result == expected_output

    # Test with a string that has a Unicode escape sequence at the beginning
    input_string = '"Hello \\utest"'
    expected_output = "Hello \\utest"
    result = unquote_and_decode_unicode_escape_seq(input_string)
    assert result == expected_output

    # Test with special characters
    input_string = (
        '"Hello \\u003cWorld\\u003e \\u003cçãâÁÁà|{}()[].,/;\\+=--_*&%$#@!?\\u003e"'
    )
    expected_output = "Hello <World> <çãâÁÁà|{}()[].,/;\\+=--_*&%$#@!?>"
    result = unquote_and_decode_unicode_escape_seq(input_string)
    assert result == expected_output
