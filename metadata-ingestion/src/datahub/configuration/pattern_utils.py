# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import Union

from datahub.configuration.common import AllowDenyPattern

UUID_REGEX = r"[a-f0-9]{8}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{4}[-_][a-f0-9]{12}"


def is_schema_allowed(
    schema_pattern: AllowDenyPattern,
    schema_name: str,
    db_name: str,
    match_fully_qualified_schema_name: bool,
) -> bool:
    if match_fully_qualified_schema_name:
        return schema_pattern.allowed(f"{db_name}.{schema_name}")
    else:
        return schema_pattern.allowed(schema_name)


def is_tag_allowed(tag_pattern: Union[bool, AllowDenyPattern], tag: str) -> bool:
    if isinstance(tag_pattern, AllowDenyPattern):
        return tag_pattern.allowed(tag)
    return tag_pattern
