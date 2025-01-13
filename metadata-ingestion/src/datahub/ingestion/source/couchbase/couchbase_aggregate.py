import asyncio
import logging
from typing import AsyncGenerator, List

from acouchbase.collection import AsyncCollection
from acouchbase.scope import AsyncScope
from couchbase.result import GetResult

from datahub.ingestion.source.couchbase.couchbase_connect import CouchbaseConnect

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

    async def collection_get(self, key: str) -> GetResult:
        return await self.collection.get(key)

    async def get_keys(self):
        query = f"select meta().id from {self.connector.collection_name}"
        if self.max_sample_size > 0:
            query += f" limit {self.max_sample_size}"

        result = self.scope.query(query)
        async for row in result:
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
                    elif isinstance(result, GetResult):
                        batch.append(result.content_as[dict])
                yield batch

            if errors > 0:
                raise RuntimeError(f"batch get: {errors} errors retrieving documents")

            tasks.clear()
