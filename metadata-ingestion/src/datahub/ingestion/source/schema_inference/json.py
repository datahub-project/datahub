from collections import Counter
from os import PathLike
from typing import Any, List
from typing import Counter as CounterType
from typing import Dict, Iterable, Tuple, TypedDict, Union

from datahub.ingestion.source.schema_inference.base import SchemaInferenceBase
from datahub.ingestion.source.schema_inference.object import construct_schema

import ujson
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField


class JsonInferrer(SchemaInferenceBase):
    def infer_schema(file_path: Union[str, PathLike]) -> List[SchemaField]:

        with open(file_path, "r") as f:
            datastore = ujson.load(f)
            schema = construct_schema(datastore, delimiter=".")