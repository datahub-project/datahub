from typing import IO, List

from avro.datafile import DataFileReader
from avro.io import DatumReader

from datahub.ingestion.extractor import schema_util
from datahub.ingestion.source.schema_inference.base import SchemaInferenceBase
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField


class AvroInferrer(SchemaInferenceBase):
    def infer_schema(self, file: IO[bytes]) -> List[SchemaField]:

        reader = DataFileReader(file, DatumReader())
        fields = schema_util.avro_schema_to_mce_fields(reader.schema)

        return fields
