from typing import Optional

from datahub.configuration.env_vars import get_max_transform_operation_length

_ELLIPSIS = "..."


def truncate_transform_operation(value: str, max_length: int) -> str:
    """Cut value to max_length characters plus "..." when that is shorter."""
    if len(value) <= max_length + len(_ELLIPSIS):
        return value
    return f"{value[:max_length]}{_ELLIPSIS}"


def cap_transform_operation(value: Optional[str]) -> Optional[str]:
    """Apply DATAHUB_MAX_TRANSFORM_OPERATION_LENGTH to a transformOperation value."""
    max_length = get_max_transform_operation_length()
    if value is None or max_length is None:
        return value
    return truncate_transform_operation(value, max_length)
