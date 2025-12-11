# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import List, Type

from typing_extensions import LiteralString


def get_enum_options(class_: Type[object]) -> List[LiteralString]:
    """Get the valid values for an enum in the datahub.metadata.schema_classes module."""

    return [
        value
        for name, value in vars(class_).items()
        if not callable(value) and not name.startswith("_")
    ]
