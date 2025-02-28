def abbreviate_number(number: int) -> str:
    """
    Abbreviates a number to a human-readable format.
    1000 -> 1k
    1200 -> 1.2k
    1000000 -> 1M
    1000000000 -> 1B
    """
    if number < 1000:
        return str(number)
    if number < 1000000:
        return f"{number // 1000}k"
    if number < 1000000000:
        return f"{number // 1000000}M"
    return f"{number // 1000000000}B"


def format_number(number: int) -> str:
    """
    Formats a number with commas.
    1000 -> 1,000
    1000000 -> 1,000,000
    """
    return f"{number:,}"
