from typing import IO, List

from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField


class SchemaInferenceBase:
    """
    Base class for file schema inference.
    """

    def infer_schema(self, file: IO[bytes]) -> List[SchemaField]:
        """
        Infer schema from file.
        """
        raise NotImplementedError("infer_schema not implemented")
