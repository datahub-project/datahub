import logging
import avro.schema
"""A helper file for Avro schema -> MCE schema transformations"""


logger = logging.getLogger(__name__)

#TODO: Broken (UnionSchemas)
_field_type_mapping = {
    "int" : "int",
    "string" : "string",
    "record" : "struct",
}

#TODO: Broken  
def _get_column_type(field_type):
    return _field_type_mapping.get(str(field_type), str(field_type))

#TODO: Broken
def avro_schema_to_mce_fields(avro_schema_string):
    """Converts an avro schema into a schema compatible with MCE"""
    schema: avro.schema.RecordSchema = avro.schema.Parse(avro_schema_string)
    canonical_fields = []
    fields_skipped = 0
    for field in schema.fields:
        # only transform the fields we can, ignore the rest
        if _field_type_mapping.get(str(field.type),None):
            canonical_field = {
                'fieldPath': field.name,
                'nativeDataType': str(field.type),
                'type': { "type": _get_column_type(field.type)},
                'description': field.doc,
            }
            canonical_fields.append(canonical_field)
        else:
            fields_skipped = fields_skipped + 1
    logger.warn(f'Schema {schema.name}: Skipped {fields_skipped} fields during Avro schema to canonical schema conversion')
    return canonical_fields
