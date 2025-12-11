# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from enum import Enum


class ConfigEnum(Enum):
    def _generate_next_value_(  # type: ignore
        name: str, start, count, last_values
    ) -> str:
        return name

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):  # type: ignore
        from pydantic_core import core_schema

        return core_schema.no_info_before_validator_function(
            cls.validate, handler(source_type)
        )

    @classmethod
    def validate(cls, v):  # type: ignore[no-untyped-def]
        if v and isinstance(v, str):
            return v.upper()
        return v
