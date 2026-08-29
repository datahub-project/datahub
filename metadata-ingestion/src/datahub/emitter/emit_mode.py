from enum import auto

from datahub.configuration._config_enum import ConfigEnum


class EmitMode(ConfigEnum):
    # Fully synchronous processing that updates both primary storage (SQL) and search storage (Elasticsearch) before returning.
    # Provides the strongest consistency guarantee but with the highest cost. Best for critical operations where immediate
    # searchability and consistent reads are required.
    SYNC_WAIT = auto()
    # Synchronously updates the primary storage (SQL) but asynchronously updates search storage (Elasticsearch). Provides
    # a balance between consistency and performance. Suitable for updates that need to be immediately reflected in direct
    # entity retrievals but where search index consistency can be slightly delayed.
    SYNC_PRIMARY = auto()
    # Queues the metadata change for asynchronous processing and returns immediately. The client continues execution without
    # waiting for the change to be fully processed. Best for high-throughput scenarios where eventual consistency is acceptable.
    ASYNC = auto()
    # Queues the metadata change asynchronously but blocks until confirmation that the write has been fully persisted.
    # More efficient than fully synchronous operations due to backend parallelization and batching while still providing
    # strong consistency guarantees. Useful when you need confirmation of successful persistence without sacrificing performance.
    ASYNC_WAIT = auto()

    @property
    def is_async(self) -> bool:
        return self in (EmitMode.ASYNC, EmitMode.ASYNC_WAIT)
