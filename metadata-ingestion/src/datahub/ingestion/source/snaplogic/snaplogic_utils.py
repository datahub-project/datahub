# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)


class SnaplogicUtils:
    @staticmethod
    def get_datahub_type(type_str: str) -> SchemaFieldDataTypeClass:
        """
        Maps a string-based type to a DataHub SchemaFieldDataTypeClass.

        Args:
            type_str (str): The input type (e.g., "string", "int", "boolean").

        Returns:
            SchemaFieldDataTypeClass: The mapped DataHub type.
        """
        normalized_type = type_str.lower()

        if normalized_type in ["string", "varchar"]:
            return SchemaFieldDataTypeClass(type=StringTypeClass())
        elif normalized_type in ["number", "long", "float", "double", "int"]:
            return SchemaFieldDataTypeClass(type=NumberTypeClass())
        elif normalized_type == "boolean":
            return SchemaFieldDataTypeClass(type=BooleanTypeClass())
        else:
            # Default fallback: String
            return SchemaFieldDataTypeClass(type=StringTypeClass())
