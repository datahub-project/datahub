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

    cleaned_string = string.encode().decode("unicode-escape")

    return cleaned_string


def parse_labels(labels_str: str) -> Dict[str, str]:
    pattern = r'STRUCT\("([^"]+)", "([^"]+)"\)'

    # Map of BigQuery label keys to label values
    return dict(re.findall(pattern, labels_str))
