# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import Any


def delta_type_to_hive_type(field_type: Any) -> str:
    if isinstance(field_type, str):
        """
        return the field type
        """
        return field_type
    else:
        if field_type.get("type") == "array":
            """
            if array is of complex type, recursively parse the
            fields and create the native datatype
            """
            return (
                "array<" + delta_type_to_hive_type(field_type.get("elementType")) + ">"
            )
        elif field_type.get("type") == "struct":
            parsed_struct = ""
            for field in field_type.get("fields"):
                """
                if field is of complex type, recursively parse
                and create the native datatype
                """
                parsed_struct += (
                    "{}:{}".format(
                        field.get("name"),
                        delta_type_to_hive_type(field.get("type")),
                    )
                    + ","
                )
            return "struct<" + parsed_struct.rstrip(",") + ">"
        return ""
