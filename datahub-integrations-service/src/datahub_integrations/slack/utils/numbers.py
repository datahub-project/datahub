def abbreviate_number(number: int) -> str:
    """Abbreviates a number to a human-readable format.

    Examples:
        >>> abbreviate_number(1000)
        '1k'
        >>> abbreviate_number(1200)
        '1k'
        >>> abbreviate_number(1000000)
        '1M'
        >>> abbreviate_number(1000000000)
        '1B'
    """

    # TODO: Ideally 1200 would become 1.2k, not 1k
    if number < 1000:
        return str(number)
    if number < 1000000:
        return f"{number // 1000}k"
    if number < 1000000000:
        return f"{number // 1000000}M"
    return f"{number // 1000000000}B"


def format_number(number: int) -> str:
    """Formats a number with commas.

    Examples:
        >>> format_number(1000)
        '1,000'
        >>> format_number(1000000)
        '1,000,000'
    """

    return f"{number:,}"
