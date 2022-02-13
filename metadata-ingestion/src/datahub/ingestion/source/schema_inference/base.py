from io import TextIOWrapper
from os import PathLike
from typing import List, Union

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
