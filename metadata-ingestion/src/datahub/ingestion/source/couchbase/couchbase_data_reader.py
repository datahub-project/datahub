from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, Dict, List

from datahub.ingestion.source.common.data_reader import DataReader
from datahub.ingestion.source.couchbase.couchbase_aggregate import CouchbaseAggregate
from datahub.ingestion.source.couchbase.couchbase_common import flatten
from datahub.ingestion.source.couchbase.couchbase_connect import CouchbaseConnect

PAGE_SIZE = 100


class CouchbaseCollectionItemsReader(DataReader):
    """
    Couchbase Data Reader for use cases that can't use the SQL++ INFER query
    """

    @staticmethod
    def create(client: CouchbaseConnect) -> CouchbaseCollectionItemsReader:
        return CouchbaseCollectionItemsReader(client)

    def __init__(self, client: CouchbaseConnect) -> None:
        # The lifecycle of this client is managed externally
        self.client = client

        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()

    async def get_documents(self, keyspace: str, sample_size: int) -> List[dict]:
        documents = []
        aggregator = CouchbaseAggregate(
            self.client, keyspace, max_sample_size=sample_size
        )
        async for chunk in aggregator.get_documents():
            documents.extend(chunk)

        return documents

    def get_sample_data_for_table(
        self, table_id: List[str], sample_size: int, **kwargs: Any
    ) -> Dict[str, list]:
        """
        For Couchbase, table_id should be in formation (bucket, scope, collection)
        """
        column_values: Dict[str, list] = defaultdict(list)
        keyspace = ".".join(table_id)

        documents = self.loop.run_until_complete(
            self.get_documents(keyspace, sample_size)
        )
        for document in documents:
            for field, data in flatten([], document):
                column_values[field].append(data)

        return column_values

    def close(self) -> None:
        pass
