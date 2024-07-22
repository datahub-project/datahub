import re
from typing import Dict, Optional


def unquote_and_decode_unicode_escape_seq(
    string: str,
    leading_quote: str = '"',
    trailing_quote: Optional[str] = None,
) -> str:
    """
    If string starts and ends with a quote, unquote it and decode Unicode escape sequences
    """
    unicode_seq_pattern = re.compile(r"\\(u|U)[0-9a-fA-F]{4}")
    trailing_quote = trailing_quote if trailing_quote else leading_quote

    if string.startswith(leading_quote) and string.endswith(trailing_quote):
        string = string[1:-1]

    # Decode Unicode escape sequences. This avoid issues with encoding
    # This process does not handle unicode from "\U00010000" to "\U0010FFFF"
    while unicode_seq_pattern.search(string):
        # Get the first Unicode escape sequence.
        # mypy: unicode_seq_pattern.search(string) is not None because of the while loop
        unicode_seq = unicode_seq_pattern.search(string).group(0)  # type: ignore
        # Replace the Unicode escape sequence with the decoded character
        try:
            string = string.replace(
                unicode_seq, unicode_seq.encode("utf-8").decode("unicode-escape")
            )
        except UnicodeDecodeError:
            # Skip decoding if is not possible to decode the Unicode escape sequence
            break  # avoid infinite loop
    return string


def parse_labels(labels_str: str) -> Dict[str, str]:
    pattern = r'STRUCT\("([^"]+)", "([^"]+)"\)'

    # Map of BigQuery label keys to label values
    return dict(re.findall(pattern, labels_str))
