import inflect

inflect_engine = inflect.engine()


def pluralize(word: str, count: int) -> str:
    return inflect_engine.plural(word, count)


def truncate(text: str, max_length: int) -> str:
    """Truncate text to a maximum length, appending an ellipsis if the text is cut off."""
    if len(text) > max_length:
        return text[: max_length - 3] + "..."
    return text
