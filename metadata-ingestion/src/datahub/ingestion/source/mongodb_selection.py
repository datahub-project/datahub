"""Which MongoDB databases and collections ingestion reads, as pure functions.

MongoDBSource and the probe's verdict hook both call these, so `probe filter`
cannot drift from what ingestion picks up. No I/O and no client here.
"""

from typing import FrozenSet, Protocol

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.verdicts import Verdict

# MongoDB-internal databases, skipped whatever database_pattern says.
# See https://docs.mongodb.com/manual/reference/local-database/ and
# https://docs.mongodb.com/manual/reference/config-database/ and
# https://stackoverflow.com/a/48273736/5004662.
SYSTEM_DATABASES: FrozenSet[str] = frozenset({"admin", "config", "local"})
SYSTEM_DATABASE_RULE = "system_database"
SYSTEM_COLLECTION_PREFIX = "system."
SYSTEM_COLLECTIONS_FIELD = "excludeSystemCollections"


class MongoDBFilterConfig(Protocol):
    @property
    def database_pattern(self) -> AllowDenyPattern: ...

    @property
    def collection_pattern(self) -> AllowDenyPattern: ...

    @property
    def excludeSystemCollections(self) -> bool: ...


def collection_target(database: str, collection: str) -> str:
    """The string collection_pattern is matched against, which is also the
    dataset name ingestion emits."""
    return f"{database}.{collection}"


def database_verdict(config: MongoDBFilterConfig, database: str) -> Verdict:
    if database in SYSTEM_DATABASES:
        return Verdict.exclude(SYSTEM_DATABASE_RULE)
    if not config.database_pattern.allowed(database):
        return Verdict.exclude("database_pattern")
    return Verdict.include()


def collection_verdict(
    config: MongoDBFilterConfig, collection: str, target: str
) -> Verdict:
    """`target` is collection_target(database, collection); the probe passes the
    bare name when the caller gave no database, as no other target exists."""
    # system.profile needs dbAdmin and exists only while profiling is on, and
    # system.views holds view definitions: both make garbage schemas.
    if config.excludeSystemCollections and collection.startswith(
        SYSTEM_COLLECTION_PREFIX
    ):
        return Verdict.exclude(SYSTEM_COLLECTIONS_FIELD)
    if not config.collection_pattern.allowed(target):
        return Verdict.exclude("collection_pattern")
    return Verdict.include()
