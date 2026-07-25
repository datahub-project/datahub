from typing import Any, Callable

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.elastic_search_probe import list_elasticsearch_children
from datahub.ingestion.source.mongodb_probe import list_mongodb_children


class _MongoDB(dict):
    def __init__(self, collections):
        super().__init__()
        self._collections = collections

    def list_collection_names(self):
        return self._collections


class _MongoClient:
    def __init__(self, dbs):
        self._dbs = dbs
        self.closed = False

    def list_database_names(self):
        return list(self._dbs.keys())

    def __getitem__(self, name):
        return _MongoDB(self._dbs[name])

    def close(self):
        self.closed = True


# A real pydantic config (not a plain SimpleNamespace) so resolve_pattern_field can
# introspect model_fields for database_pattern, which the probe now resolves by
# convention rather than declaring explicitly.
class _MongoConfig(ConfigModel):
    get_mongo_client: Callable[[], Any]
    database_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    collection_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


def test_mongodb_probe_db_then_collections():
    client = _MongoClient({"app": ["orders", "sessions"]})
    config = _MongoConfig(
        get_mongo_client=lambda: client,
        database_pattern=AllowDenyPattern.allow_all(),
        collection_pattern=AllowDenyPattern(allow=[".*"], deny=["^sessions$"]),
    )
    dbs = list_mongodb_children(config, [], 100)
    assert {n.name for n in dbs.nodes} == {"app"}
    assert dbs.nodes[0].kind == DatasetContainerSubTypes.DATABASE

    cols = list_mongodb_children(config, ["app"], 100)
    by_name = {n.name: n for n in cols.nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].included is True
    assert by_name["sessions"].included is False
    assert by_name["sessions"].excluded_by == "collection_pattern"
    assert client.closed


class _EsIndices:
    def __init__(self, names):
        self._names = names

    def get_alias(self):
        return {n: {} for n in self._names}


class _EsClient:
    def __init__(self, names):
        self.indices = _EsIndices(names)


# A real pydantic config (not a plain SimpleNamespace) so resolve_pattern_field can
# introspect model_fields for index_pattern, which the probe now resolves by
# convention rather than declaring explicitly.
class _EsConfig(ConfigModel):
    get_client: Callable[[], Any]
    index_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


def test_elasticsearch_probe_lists_indices():
    config = _EsConfig(
        get_client=lambda: _EsClient(["orders-2026", ".kibana"]),
        index_pattern=AllowDenyPattern(allow=[".*"], deny=["^\\..*"]),
    )
    result = list_elasticsearch_children(config, [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders-2026"].kind == DatasetSubTypes.ELASTIC_INDEX
    assert by_name["orders-2026"].included is True
    # index_pattern deny (^\.) reused: the internal .kibana index is excluded.
    assert by_name[".kibana"].included is False
    assert by_name[".kibana"].excluded_by == "index_pattern"
