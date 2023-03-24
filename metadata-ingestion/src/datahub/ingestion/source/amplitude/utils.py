from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanTypeClass,
    EnumType,
    NullTypeClass,
    NumberTypeClass,
    SchemalessClass,
    StringTypeClass,
)

FIELD_TYPE_MAPPING = {
    "null": NullTypeClass,
    "number": NumberTypeClass,
    "enum": EnumType,
    "string": StringTypeClass,
    "any": SchemalessClass,
    "boolean": BooleanTypeClass,
}


class MetadataIngestionException(Exception):
    pass
