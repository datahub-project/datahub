# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
