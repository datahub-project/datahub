import logging
import asyncio
import math
import random
from typing import List, AsyncGenerator
from acouchbase.scope import AsyncScope
from acouchbase.collection import AsyncCollection
from couchbase.result import GetResult

from datahub.ingestion.source.couchbase.couchbase_connect import CouchbaseConnect

logger = logging.getLogger(__name__)


class CouchbaseAggregate:
    scope: AsyncScope
    collection: AsyncCollection

    def __init__(self,
                 connector: CouchbaseConnect,
                 keyspace: str,
                 batch_size: int = 100,
                 randomize: bool = False,
                 sample_size: int = 50,
                 fraction: float = 1.0,
                 max_sample_size: int = 0):
        self.connector = connector
        self.keyspace = keyspace
        self.batch_size = batch_size
        self.randomize = randomize
        self.sample_size = sample_size
        self.fraction = fraction
        self.max_sample_size = max_sample_size

        if self.fraction < 0 or self.fraction > 1:
            raise ValueError("fraction must be between 0 and 1")

        if batch_size <= 0:
            raise ValueError("batch_size must be greater than 0")

        if self.sample_size <= 0:
            raise ValueError("sample_size must be greater than 0")

        if self.sample_size >= self.batch_size:
            raise ValueError("sample_size must be less than batch_size")

        if self.fraction < 1.0 and self.randomize:
            raise ValueError("fraction cannot be combined with randomize")

        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            
        self.loop.run_until_complete(connector.cluster_init_async())

    def list_reduce(self, chunk: List[str]) -> List[str]:
        if self.randomize:
            return random.sample(chunk, k=self.sample_size)
        elif self.fraction < 1.0:
            return chunk[0:math.ceil(len(chunk) * self.fraction)]
        else:
            return chunk

    def aggregate(self) -> List[dict]:
        return self.loop.run_until_complete(self.run_aggregate())

    async def run_aggregate(self) -> List[dict]:
        await self.init()
        return await self.get_documents()

    async def init(self):
        self.scope, self.collection = await self.connector.connect_keyspace_async(self.keyspace)
    
    async def collection_get(self, key: str):
        return await self.collection.get(key)

    async def get_keys(self):
        query = f"select meta().id from {self.connector.collection_name}"
        if self.max_sample_size > 0:
            query += f" limit {self.max_sample_size}"

        result = self.scope.query(query)
        async for row in result:
            yield row.get('id')

    async def get_key_chunks(self) -> AsyncGenerator[List[str], None]:
        keys = []
        async for key in self.get_keys():
            keys.append(key)
            if len(keys) == self.batch_size:
                yield keys
                keys.clear()
        if len(keys) > 0:
            yield keys

    async def get_documents(self) -> List[dict]:
        tasks = []
        documents = []

        async for chunk in self.get_key_chunks():
            for key in self.list_reduce(chunk):
                tasks.append(self.loop.create_task(self.collection_get(key)))

            errors = 0
            if len(tasks) > 0:
                await asyncio.sleep(0)
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(result)
                        errors += 1
                    elif isinstance(result, GetResult):
                        documents.append(result.content_as[dict])

            if errors > 0:
                raise RuntimeError(f"batch get: {errors} errors retrieving documents")

            tasks.clear()

        return documents
