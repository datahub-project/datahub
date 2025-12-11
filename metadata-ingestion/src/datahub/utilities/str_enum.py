# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from enum import Enum


class StrEnum(str, Enum):
    """String Enum class."""

    # This is required for compatibility with Python 3.11+, which changed the
    # behavior of enums in format() and f-strings.
    # Once we're using only Python 3.11+, we can replace this with enum.StrEnum.
    # See https://blog.pecar.me/python-enum for more details.

    def __str__(self) -> str:
        """Return the string representation of the enum."""
        return str(self.value)
