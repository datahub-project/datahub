from typing import Optional

from datahub.configuration.env_vars import get_max_transform_operation_length

_ELLIPSIS = "..."

TRANSFORM_OPERATION_CHAR_LIMIT = get_max_transform_operation_length()


def truncate_transform_operation(value: str, char_limit: int) -> str:
    """Cut value to char_limit characters plus "..." when that is shorter."""
    if len(value) <= char_limit + len(_ELLIPSIS):
        return value
    return f"{value[:char_limit]}{_ELLIPSIS}"


def cap_transform_operation(value: Optional[str]) -> Optional[str]:
    """Apply DATAHUB_MAX_TRANSFORM_OPERATION_LENGTH to a transformOperation value."""
    if value is None or TRANSFORM_OPERATION_CHAR_LIMIT is None:
        return value
    return truncate_transform_operation(value, TRANSFORM_OPERATION_CHAR_LIMIT)
