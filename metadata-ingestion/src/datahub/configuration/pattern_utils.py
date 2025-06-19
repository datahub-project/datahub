from typing import Union

from datahub.configuration.common import AllowDenyPattern

UUID_REGEX = r"[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}"


def is_schema_allowed(
    schema_pattern: AllowDenyPattern,
    schema_name: str,
    db_name: str,
) -> bool:
    return schema_pattern.allowed(f"{db_name}.{schema_name}")


def is_tag_allowed(tag_pattern: Union[bool, AllowDenyPattern], tag: str) -> bool:
    if isinstance(tag_pattern, AllowDenyPattern):
        return tag_pattern.allowed(tag)
    return tag_pattern
