URN_RESERVED_CHARS = {"%"}


def check_reserved_char(value: str) -> bool:
    for reserved_char in URN_RESERVED_CHARS:
        if reserved_char in value:
            return True
    return False
