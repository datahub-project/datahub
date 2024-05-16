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
