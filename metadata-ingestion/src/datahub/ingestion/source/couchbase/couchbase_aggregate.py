import asyncio
import logging
from typing import AsyncGenerator, List

from acouchbase.collection import AsyncCollection
from acouchbase.scope import AsyncScope
from couchbase.exceptions import DocumentNotFoundException
from couchbase.result import GetResult

from datahub.ingestion.source.couchbase.couchbase_connect import CouchbaseConnect
from datahub.ingestion.source.couchbase.couchbase_sql import (
    SELECT_COLLECTION_COUNT,
    SELECT_DOC_IDS,
)
from datahub.ingestion.source.couchbase.retry import retry

logger = logging.getLogger(__name__)


class CouchbaseAggregate:
    scope: AsyncScope
    collection: AsyncCollection

    def __init__(
        self,
        connector: CouchbaseConnect,
        keyspace: str,
        batch_size: int = 100,
        max_sample_size: int = 0,
    ):
        self.connector = connector
        self.keyspace = keyspace
        self.batch_size = batch_size
        self.max_sample_size = max_sample_size

        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")

    async def init(self):
        await self.connector.cluster_init_async()
        self.scope, self.collection = await self.connector.connect_keyspace_async(
            self.keyspace
        )

    @retry(factor=0.05)
    async def collection_get(self, key: str) -> dict:
        try:
            result: GetResult = await self.collection.get(key)
            return result.content_as[dict]
        except DocumentNotFoundException:
            logger.warning(f"Document ID {key} not found")
            return {}

    @retry(factor=0.05)
    async def run_query(
        self, query: str, offset: int = 0, limit: int = 0
    ) -> List[dict]:
        if offset > 0:
            query += f" OFFSET {offset}"
        if limit > 0:
            query += f" LIMIT {limit}"
        result = self.scope.query(query)
        documents = [row async for row in result]
        return documents

    async def collection_count(self) -> int:
        query = SELECT_COLLECTION_COUNT.format(self.connector.collection_name)

        result = await self.run_query(query)
        document = [row for row in result]
        return document[0].get("count") if document else 0

    async def get_keys(self):
        query = SELECT_DOC_IDS.format(self.connector.collection_name)

        results = await self.run_query(query, limit=self.max_sample_size)
        for row in results:
            yield row.get("id")

    async def get_key_chunks(self) -> AsyncGenerator[List[str], None]:
        keys = []
        async for key in self.get_keys():
            keys.append(key)
            if len(keys) == self.batch_size:
                yield keys
                keys.clear()
        if len(keys) > 0:
            yield keys

    async def get_documents(self) -> AsyncGenerator[List[dict], None]:
        tasks = []
        await self.init()

        async for chunk in self.get_key_chunks():
            for key in chunk:
                tasks.append(asyncio.create_task(self.collection_get(key)))

            errors = 0
            if len(tasks) > 0:
                batch = []
                await asyncio.sleep(0)
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(result)
                        errors += 1
                    elif isinstance(result, dict):
                        if result:
                            batch.append(result)
                yield batch

            if errors > 0:
                raise RuntimeError(f"batch get: {errors} errors retrieving documents")

            tasks.clear()
