import logging
from datetime import timedelta
from typing import Iterable, List, Tuple

from acouchbase.bucket import AsyncBucket
from acouchbase.cluster import AsyncCluster
from acouchbase.collection import AsyncCollection
from acouchbase.scope import AsyncScope
from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster
from couchbase.exceptions import BucketNotFoundException, InternalServerFailureException
from couchbase.management.buckets import BucketManager, BucketSettings
from couchbase.management.collections import CollectionManager, ScopeSpec
from couchbase.options import (
    ClusterOptions,
    ClusterTimeoutOptions,
    LockMode,
    TLSVerifyMode,
)

from datahub.ingestion.source.couchbase.retry import retry

logger = logging.getLogger(__name__)


class CouchbaseConnect:
    cluster: Cluster
    cluster_async: AsyncCluster
    bucket_manager: BucketManager
    cb_connect_string: str
    cluster_options: ClusterOptions
    bucket_name: str
    scope_name: str
    collection_name: str

    def __init__(
        self,
        connect_string: str,
        username: str,
        password: str,
        kv_timeout: float = 5,
        query_timeout: float = 60,
    ):
        self.cb_connect_string = connect_string

        auth = PasswordAuthenticator(username, password)
        timeouts = ClusterTimeoutOptions(
            query_timeout=timedelta(seconds=query_timeout),
            kv_timeout=timedelta(seconds=kv_timeout),
            bootstrap_timeout=timedelta(seconds=kv_timeout * 2),
            resolve_timeout=timedelta(seconds=kv_timeout),
            connect_timeout=timedelta(seconds=kv_timeout),
            management_timeout=timedelta(seconds=kv_timeout * 2),
        )

        self.cluster_options = ClusterOptions(
            auth,
            timeout_options=timeouts,
            tls_verify=TLSVerifyMode.NO_VERIFY,
            lockmode=LockMode.WAIT,
        )

        logger.debug(
            f"couchbase: connect string = {connect_string} username = {username}"
        )

    @retry()
    def connect(self) -> Cluster:
        cluster = Cluster.connect(self.cb_connect_string, self.cluster_options)
        cluster.wait_until_ready(timedelta(seconds=10))
        return cluster

    @retry(always_raise_list=(BucketNotFoundException,))
    def bucket(self, cluster: Cluster, name: str) -> Bucket:
        if name is None:
            raise TypeError("bucket name can not be None")
        logger.debug(f"bucket: connect {name}")
        return cluster.bucket(name)

    @retry()
    async def connect_async(self) -> AsyncCluster:
        cluster = await AsyncCluster.connect(
            self.cb_connect_string, self.cluster_options
        )
        await cluster.on_connect()
        await cluster.wait_until_ready(timedelta(seconds=10))
        return cluster

    @retry(always_raise_list=(BucketNotFoundException,))
    async def bucket_async(self, cluster: AsyncCluster, name: str) -> AsyncBucket:
        if name is None:
            raise TypeError("bucket name can not be None")
        logger.debug(f"bucket: connect async {name}")
        bucket = cluster.bucket(name)
        await bucket.on_connect()
        return bucket

    def cluster_init(self):
        self.cluster = self.connect()
        self.bucket_manager = self.cluster.buckets()

    async def cluster_init_async(self):
        self.cluster_async = await self.connect_async()

    async def connect_keyspace_async(
        self, keyspace: str
    ) -> Tuple[AsyncScope, AsyncCollection]:
        if not self.cluster_async:
            raise RuntimeError("cluster is not initialized")

        keyspace_vector: List[str] = keyspace.split(".")
        if len(keyspace_vector) != 3:
            raise ValueError(
                f"keyspace: invalid format: {keyspace}. Expected format is <bucket>.<scope>.<collection>"
            )

        self.bucket_name = keyspace_vector[0]
        self.scope_name = keyspace_vector[1]
        self.collection_name = keyspace_vector[2]

        bucket = await self.bucket_async(self.cluster_async, self.bucket_name)
        scope = bucket.scope(self.scope_name)
        collection = scope.collection(self.collection_name)
        return scope, collection

    def bucket_list(self) -> List[str]:
        if not self.bucket_manager:
            raise RuntimeError("cluster is not initialized")
        buckets: List[BucketSettings] = self.bucket_manager.get_all_buckets()
        return [b.name for b in buckets]

    def scope_list(self, bucket_name: str) -> List[str]:
        if not self.cluster:
            raise RuntimeError("cluster is not initialized")
        bucket = self.bucket(self.cluster, bucket_name)
        collection_manager: CollectionManager = bucket.collections()
        scopes: Iterable[ScopeSpec] = collection_manager.get_all_scopes()
        return [scope.name for scope in scopes]

    def collection_list(self, bucket_name: str, scope_name: str) -> List[str]:
        if not self.cluster:
            raise RuntimeError("cluster is not initialized")
        bucket = self.bucket(self.cluster, bucket_name)
        collection_manager: CollectionManager = bucket.collections()
        scopes: Iterable[ScopeSpec] = collection_manager.get_all_scopes()
        scope = next((scope for scope in scopes if scope.name == scope_name), None)
        if scope is None:
            raise ValueError(f"Scope {scope_name} not found in bucket {bucket_name}")
        return [c.name for c in scope.collections]

    @staticmethod
    def expand_keyspace(keyspace: str) -> Tuple[str, ...]:
        keyspace_vector: List[str] = keyspace.split(".")
        if len(keyspace_vector) != 3:
            raise ValueError(
                f"keyspace: invalid format: {keyspace}. Expected format is <bucket>.<scope>.<collection>"
            )
        return tuple(keyspace_vector)

    def collection_count(self, keyspace: str) -> int:
        if not self.cluster:
            raise RuntimeError("cluster is not initialized")

        bucket_name, scope_name, collection_name = self.expand_keyspace(keyspace)

        bucket = self.bucket(self.cluster, bucket_name)
        scope = bucket.scope(scope_name)

        query = f"SELECT COUNT(*) AS count FROM {collection_name}"

        results = scope.query(query)
        for row in results:
            return int(row.get("count"))
        return 0

    def bucket_info(self, bucket_name: str) -> BucketSettings:
        if not self.bucket_manager:
            raise RuntimeError("cluster is not initialized")
        settings: BucketSettings = self.bucket_manager.get_bucket(bucket_name)
        return settings

    @retry()
    def collection_infer(
        self, sample_size: int, sample_values: int, keyspace: str
    ) -> dict:
        if not self.cluster:
            raise RuntimeError("cluster is not initialized")

        schemas = []

        bucket_name, scope_name, collection_name = self.expand_keyspace(keyspace)

        bucket = self.bucket(self.cluster, bucket_name)
        scope = bucket.scope(scope_name)

        query = f'INFER {collection_name} WITH {{"sample_size": {sample_size}, "num_sample_values": {sample_values}, "similarity_metric": 0.0}}'

        results = scope.query(query)
        try:
            for row in results:
                schemas.extend(row)
            return schemas[0]
        except InternalServerFailureException as e:
            if e.error_code == 7014:
                return {}
            else:
                raise e
