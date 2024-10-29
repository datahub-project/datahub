"""
Here write down functions which are operating on string. Like replacing some character and so on
"""

import re


def remove_suffix(original: str, suffix: str) -> str:
    # This can be removed in favour of original.removesuffix for python>3.8
    if original.endswith(suffix):
        return original[: -len(suffix)]
    return original


def remove_extra_spaces_and_newlines(original: str) -> str:
    """
    python-liquid library is not removing extra spaces and new lines from template and hence spaces and newlines
    are appearing in urn. This function can be used to remove such characters from urn or text.
    """
    return re.sub(r"\s*\n\s*", "", original)


def replace_quotes(value: str) -> str:
    return value.replace('"', "").replace("`", "")
