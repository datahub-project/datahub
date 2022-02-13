from io import TextIOWrapper
from typing import List

from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField


class SchemaInferenceBase:
    """
    Base class for file schema inference.
    """

    @staticmethod
    def infer_schema(file: TextIOWrapper) -> List[SchemaField]:
        """
        Infer schema from file.
        """
        raise NotImplementedError("infer_schema not implemented")
