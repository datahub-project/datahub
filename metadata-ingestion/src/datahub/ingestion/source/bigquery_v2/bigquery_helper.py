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
    trailing_quote = trailing_quote if trailing_quote else leading_quote

    if string.startswith(leading_quote) and string.endswith(trailing_quote):
        string = string[1:-1]

    # Decode Unicode escape sequences. This avoid issues with encoding
    while string.find("\\u") >= 0:
        index = string.find("\\u")  # The first occurrence of the substring
        unicode_seq = string[index: (index + 6)]  # The Unicode escape sequence
        # Replace the Unicode escape sequence with the decoded character
        string = string.replace(
            unicode_seq, unicode_seq.encode("utf-8").decode("unicode-escape")
        )
    return string


def parse_labels(labels_str: str) -> Dict[str, str]:
    pattern = r'STRUCT\("([^"]+)", "([^"]+)"\)'

    # Map of BigQuery label keys to label values
    return dict(re.findall(pattern, labels_str))
