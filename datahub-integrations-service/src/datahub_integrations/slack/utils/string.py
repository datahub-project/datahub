import functools


@functools.cache
def _get_inflect_engine():
    # The inflect library can be pretty slow to import, so we lazy-load it to improve
    # startup time.
    import inflect

    return inflect.engine()


def pluralize(word: str, count: int) -> str:
    return _get_inflect_engine().plural(word, count)


def truncate(text: str, max_length: int, show_length: bool = False) -> str:
    """Truncate text to a maximum length, appending an ellipsis if the text is cut off."""
    if len(text) > max_length:
        if show_length:
            return text[:max_length] + f"[{len(text) - max_length} chars truncated]"
        else:
            return text[: max_length - 3] + "..."
    return text
