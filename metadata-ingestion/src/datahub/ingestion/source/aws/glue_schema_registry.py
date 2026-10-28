import json
from typing import List, Optional

from datahub.ingestion.extractor import protobuf_util, schema_util
from datahub.ingestion.extractor.json_schema_util import JsonSchemaTranslator
from datahub.ingestion.extractor.protobuf_util import ProtobufSchema
from datahub.metadata.schema_classes import SchemaFieldClass


def parse_glue_schema_fields(
    schema_def: str, data_format: str, name: str
) -> Optional[List[SchemaFieldClass]]:
    """Schema fields for a Glue Schema Registry definition.

    Returns None for a format other than AVRO, JSON or PROTOBUF. Parse errors are
    raised (their types differ per format) for the caller to report."""
    if data_format == "AVRO":
        return schema_util.avro_schema_to_mce_fields(
            schema_def, is_key_schema=False, swallow_exceptions=False
        )
    if data_format == "JSON":
        return list(
            JsonSchemaTranslator.get_fields_from_schema(
                json.loads(schema_def), is_key_schema=False
            )
        )
    if data_format == "PROTOBUF":
        # grpc derives a Python module name from the file name, and dots break that
        # import, so sanitize as the Confluent Schema Registry path does.
        proto_name = name.replace(".", "_")
        return protobuf_util.protobuf_schema_to_mce_fields(
            ProtobufSchema(f"{proto_name}.proto", schema_def),
            imported_schemas=[],
            is_key_schema=False,
        )
    return None
