import json
import re
from typing import Dict

# GoogleSQL escapes JSON doesn't accept: \a \v \? \' \` \xHH, octal, \U above the BMP.
_GOOGLESQL_SIMPLE_ESCAPES = {
    "a": "\\u0007",
    "v": "\\u000b",
    "?": "?",
    "'": "'",
    "`": "`",
}
_GOOGLESQL_ESCAPE_PATTERN = re.compile(
    r"(?P<backslashes>\\+)(?:(?P<simple>[av?'`])|[xX](?P<hex>[0-9a-fA-F]{2})"
    r"|(?P<oct>[0-7]{3})|U(?P<unicode8>[0-9a-fA-F]{8}))"
)


def _translate_googlesql_escape(match: "re.Match[str]") -> str:
    backslashes = match.group("backslashes")
    if len(backslashes) % 2 == 0:
        # An even run of backslashes is just that many literal backslashes once
        # JSON-decoded; what follows is plain text, not an escape.
        return match.group(0)
    prefix = backslashes[:-1]

    if match.group("simple") is not None:
        return prefix + _GOOGLESQL_SIMPLE_ESCAPES[match.group("simple")]
    if match.group("hex") is not None:
        return prefix + f"\\u{int(match.group('hex'), 16):04x}"
    if match.group("oct") is not None:
        return prefix + f"\\u{int(match.group('oct'), 8):04x}"

    codepoint = int(match.group("unicode8"), 16)
    if codepoint <= 0xFFFF:
        return prefix + f"\\u{codepoint:04x}"
    codepoint -= 0x10000
    high_surrogate = 0xD800 + (codepoint >> 10)
    low_surrogate = 0xDC00 + (codepoint & 0x3FF)
    return prefix + f"\\u{high_surrogate:04x}\\u{low_surrogate:04x}"


def unquote_and_decode_escape_seq(string: str) -> str:
    """
    Unquotes a double-quoted GoogleSQL string literal and decodes its escape
    sequences by translating GoogleSQL-only escapes to JSON ones, then parsing as JSON.
    """
    translated = _GOOGLESQL_ESCAPE_PATTERN.sub(_translate_googlesql_escape, string)
    try:
        decoded = json.loads(translated)
    except json.JSONDecodeError:
        if string.startswith('"') and string.endswith('"'):
            return string[1:-1]
        return string
    else:
        return decoded if isinstance(decoded, str) else string


def parse_labels(labels_str: str) -> Dict[str, str]:
    pattern = r'STRUCT\("([^"]+)", "([^"]+)"\)'

    # Map of BigQuery label keys to label values
    return dict(re.findall(pattern, labels_str))
