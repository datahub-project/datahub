from typing import Any, Dict, Optional, Type, Union

from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanTypeClass,
    EnumType,
    NullTypeClass,
    NumberTypeClass,
    SchemaFieldDataType,
    SchemalessClass,
    StringTypeClass,
)


class AmplitudeFieldTypeMapping:

    _field_to_type: Dict[str, Type] = {
        "null": NullTypeClass,
        "number": NumberTypeClass,
        "enum": EnumType,
        "string": StringTypeClass,
        "any": SchemalessClass,
        "boolean": BooleanTypeClass,
    }

    @staticmethod
    def get_field_type(field_type: str) -> Union[Any]:
        type_class: Optional[Type] = AmplitudeFieldTypeMapping._field_to_type.get(
            field_type
        )
        if type_class is None:
            type_class = NullTypeClass

        schema_field_type = SchemaFieldDataType(type_class())

        return schema_field_type.type


class MetadataIngestionException(Exception):
    pass
