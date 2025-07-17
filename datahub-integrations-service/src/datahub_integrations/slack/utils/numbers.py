def abbreviate_number(number: int) -> str:
    """Abbreviates a number to a human-readable format.

    Examples:
        >>> abbreviate_number(1000)
        '1k'
        >>> abbreviate_number(1002)
        '1.0k'
        >>> abbreviate_number(1200)
        '1.2k'
        >>> abbreviate_number(1000000)
        '1M'
        >>> abbreviate_number(1000000000)
        '1B'
        >>> abbreviate_number(1000050300)
        '1.0B'
    """

    def _smart_decimal(number: int, divisor: int) -> str:
        # If it goes in evenly, we skip the decimal entirely.
        if number % divisor == 0:
            return f"{number // divisor}"
        else:
            return f"{(number // (divisor / 10)) / 10:.1f}"

    if number < 1000:
        return str(number)
    elif number < 1_000_000:
        return f"{_smart_decimal(number, 1000)}k"
    elif number < 1_000_000_000:
        return f"{_smart_decimal(number, 1_000_000)}M"
    return f"{_smart_decimal(number, 1_000_000_000)}B"


def format_number(number: int) -> str:
    """Formats a number with commas.

    Examples:
        >>> format_number(1000)
        '1,000'
        >>> format_number(1000000)
        '1,000,000'
    """

    return f"{number:,}"
