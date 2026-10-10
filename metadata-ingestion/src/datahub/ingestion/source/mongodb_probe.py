from typing import List, Optional

from pymongo import MongoClient
from pymongo.errors import OperationFailure

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import (
    ProbeProviderBase,
    resolve_name,
    take,
)
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.mongodb import (
    MONGODB_COLLECTION_KIND,
    MongoDBConfig,
    create_mongo_client,
)

# pymongo's default server selection timeout is 30s; a probe against an
# unreachable host should fail sooner. A recipe's own `options` still win.
_PROBE_CLIENT_DEFAULTS = {"serverSelectionTimeoutMS": 10_000}


class MongoDBMetadataProbe(ProbeProviderBase):
    """Metadata-only probe over MongoDB: database and collection names, read
    with the client ingestion builds. Never documents, samples or indexes."""

    def __init__(self, config: MongoDBConfig) -> None:
        self._config = config

    @classmethod
    def for_config(cls, config: MongoDBConfig) -> "MongoDBMetadataProbe":
        return cls(config)

    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        # A server-side refusal carries a code name (AuthenticationFailed,
        # Unauthorized) that says more than the class.
        if isinstance(exc, OperationFailure):
            name = (exc.details or {}).get("codeName")
            if isinstance(name, str):
                return name
        return None

    def _client(self) -> MongoClient:
        return self._open_once(
            "client",
            lambda: create_mongo_client(self._config, _PROBE_CLIENT_DEFAULTS),
            close=lambda client: client.close(),
        )

    def _database_names(self) -> List[str]:
        return sorted(self._client().list_database_names())

    @probe_method(kind=DatasetContainerSubTypes.DATABASE, row_limit_param="limit")
    def databases(self, limit: int = 500) -> List[str]:
        """Databases the credential can list, including ones ingestion skips:
        the system databases (admin, config, local) and those database_pattern
        denies are reported, not hidden, so `probe filter` can explain them.
        Metadata only."""
        return take(self._database_names(), limit)

    @probe_method(
        kind=MONGODB_COLLECTION_KIND,
        row_limit_param="limit",
        parent_params=("database",),
    )
    def collections(self, database: str, limit: int = 500) -> List[str]:
        """Collections and views in one database, by name, including system.*
        collections and ones collection_pattern denies. collection_pattern is
        matched against `database.collection`, which `probe filter` builds from
        the database echoed as parent_path. Metadata only: never documents."""
        name = resolve_name(
            database,
            self._database_names(),
            key=lambda listed: listed,
            kind="database",
            list_command="probe run databases",
        ).name
        return take(sorted(self._client()[name].list_collection_names()), limit)
