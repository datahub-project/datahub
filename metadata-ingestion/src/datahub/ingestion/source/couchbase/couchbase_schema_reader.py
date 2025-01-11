from __future__ import annotations
from collections import defaultdict
from typing import Any, Dict, List

from datahub.ingestion.source.common.data_reader import DataReader
from datahub.ingestion.source.couchbase.couchbase_kv_schema import json_schema, flatten

PAGE_SIZE = 100


class CouchbaseCollectionItemsReader(DataReader):

    @staticmethod
    def create(schema: dict) -> CouchbaseCollectionItemsReader:
        return CouchbaseCollectionItemsReader(schema)

    def __init__(self, schema: dict) -> None:
        # The lifecycle of this client is managed externally
        self.schema = schema

    def get_sample_data_for_table(
        self, table_id: List[str], sample_size: int, **kwargs: Any
    ) -> Dict[str, list]:
        column_values: Dict[str, list] = defaultdict(list)

        parsed = json_schema(self.schema)
        for field_path, field_data in flatten([], parsed):
            column_values[field_path].extend(field_data.samples)

        return column_values

    def close(self) -> None:
        pass
