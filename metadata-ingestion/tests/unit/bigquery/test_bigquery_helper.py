import pytest

from datahub.ingestion.source.bigquery_v2.bigquery_helper import (
    unquote_and_decode_escape_seq,
)


@pytest.mark.parametrize(
    "string, expected",
    [
        ('"line one\\nline two\\ttabbed"', "line one\nline two\ttabbed"),
        ('"a \\"quoted\\" word"', 'a "quoted" word'),
        ('"a backslash: \\\\"', "a backslash: \\"),
        ('"caf\\u00e9"', "café"),
        ('"\\ud83d\\ude00"', "\U0001f600"),  # JSON surrogate pair (astral plane)
        ('"bell: \\a"', "bell: \a"),  # GoogleSQL-only escape
        ('"vtab: \\v"', "vtab: \v"),  # GoogleSQL-only escape
        ('"hex: \\x41"', "hex: A"),  # GoogleSQL-only escape
        ('"octal: \\101"', "octal: A"),  # GoogleSQL-only escape
        ('"\\U0001f600"', "\U0001f600"),  # GoogleSQL 8-hex unicode, astral plane
        ('"\\?"', "?"),  # GoogleSQL-only escape, no-op outside JSON
        ('"\\\'"', "'"),  # GoogleSQL-only escape, no-op outside JSON
        ('"\\`"', "`"),  # GoogleSQL-only escape, no-op outside JSON
        ('"a\\nb\\x0bc"', "a\nb\x0bc"),  # mix of JSON and GoogleSQL escapes
        ('"\\\\x41"', "\\x41"),  # escaped backslash, not a hex escape
        ('"\\\\\\x41"', "\\A"),  # escaped backslash, then a real hex escape
    ],
)
def test_unquote_and_decode_escape_seq(string: str, expected: str) -> None:
    assert unquote_and_decode_escape_seq(string) == expected


def test_unquote_and_decode_escape_seq_falls_back_on_unrecognized_escape() -> None:
    # \q is not a valid escape in GoogleSQL or JSON
    assert unquote_and_decode_escape_seq('"\\q"') == "\\q"


def test_unquote_and_decode_escape_seq_falls_back_on_invalid_unicode_escape() -> None:
    assert (
        unquote_and_decode_escape_seq('"No escape \\u123 sequences here"')
        == "No escape \\u123 sequences here"
    )


def test_unquote_and_decode_escape_seq_leaves_unquoted_string_untouched() -> None:
    assert unquote_and_decode_escape_seq("no quotes here") == "no quotes here"


def test_unquote_and_decode_escape_seq_empty_string() -> None:
    assert unquote_and_decode_escape_seq("") == ""
