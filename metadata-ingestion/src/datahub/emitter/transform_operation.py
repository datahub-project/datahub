import re
from typing import Optional

from datahub.configuration.env_vars import get_max_transform_operation_length

# SqlParsingAggregator prefixes the logic with "SQL: " or "COPY: ". A bare label is
# what a previous cut at 0 left, so it must stay unchanged when cut again.
_LABEL_PREFIX = re.compile(r"^(SQL|COPY)(?:: |$)")
_ELLIPSIS = "..."


def truncate_transform_operation(value: str, max_length: int) -> Optional[str]:
    """Cut the logic after any label to max_length characters plus "..." when that is shorter; 0 keeps only the label, or None if unlabelled."""
    match = _LABEL_PREFIX.match(value)
    label = match.group(1) if match else None
    logic = value[match.end() :] if match else value
    if max_length == 0:
        return label
    if len(logic) <= max_length + len(_ELLIPSIS):
        return value
    truncated = f"{logic[:max_length]}{_ELLIPSIS}"
    return f"{label}: {truncated}" if label else truncated


def cap_transform_operation(value: Optional[str]) -> Optional[str]:
    """Apply DATAHUB_MAX_TRANSFORM_OPERATION_LENGTH to a transformOperation value."""
    max_length = get_max_transform_operation_length()
    if value is None or max_length is None:
        return value
    return truncate_transform_operation(value, max_length)
