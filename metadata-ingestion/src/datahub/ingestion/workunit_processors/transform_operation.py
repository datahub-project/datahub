import re
from typing import List, Optional

from datahub.metadata.schema_classes import FineGrainedLineageClass

# SqlParsingAggregator prefixes the logic with a label, e.g. "SQL: " or "COPY: ".
_LABEL_PREFIX = re.compile(r"^([A-Z]+): ")
_ELLIPSIS = "..."


def truncate_transform_operation(value: str, max_length: int) -> Optional[str]:
    """Cut the logic after any label to max_length characters plus "..."; 0 keeps only the label, or None if unlabelled."""
    match = _LABEL_PREFIX.match(value)
    label = match.group(1) if match else None
    logic = value[match.end() :] if match else value
    if max_length == 0:
        return label
    if len(logic) <= max_length + len(_ELLIPSIS):
        return value
    truncated = f"{logic[:max_length]}{_ELLIPSIS}"
    return f"{label}: {truncated}" if label else truncated


def logic_length(value: str) -> int:
    match = _LABEL_PREFIX.match(value)
    return len(value[match.end() :]) if match else len(value)


def apply_transform_operation_length(
    fine_grained_lineages: Optional[List[FineGrainedLineageClass]],
    max_length: int,
) -> int:
    """Apply a fixed retained logic length to each transformOperation. Returns how many values changed."""
    truncated_count = 0
    for fine_grained in fine_grained_lineages or []:
        if fine_grained.transformOperation is None:
            continue
        truncated = truncate_transform_operation(
            fine_grained.transformOperation, max_length
        )
        if truncated != fine_grained.transformOperation:
            fine_grained.transformOperation = truncated
            truncated_count += 1
    return truncated_count
