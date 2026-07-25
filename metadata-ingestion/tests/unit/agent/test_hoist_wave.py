from types import SimpleNamespace

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.elastic_search_probe import list_elasticsearch_children
from datahub.ingestion.source.mongodb_probe import list_mongodb_children
from tests.unit.agent.pattern_hint_fixtures import config_with_hints


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


def _config(collection_pattern: AllowDenyPattern, client: _MongoClient) -> object:
    return config_with_hints(
        {"collection_pattern": DatasetSubTypes.TABLE},
        get_mongo_client=lambda: client,
        database_pattern=AllowDenyPattern.allow_all(),
        collection_pattern=collection_pattern,
    )


def test_mongodb_probe_db_then_collections():
    client = _MongoClient({"app": ["orders", "sessions"]})
    config = _config(AllowDenyPattern(allow=[".*"], deny=["^sessions$"]), client)
    dbs = list_mongodb_children(config, [], 100)
    assert {n.name for n in dbs.nodes} == {"app"}
    assert dbs.nodes[0].kind == DatasetContainerSubTypes.DATABASE

    cols = list_mongodb_children(config, ["app"], 100)
    by_name = {n.name: n for n in cols.nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].included is True
    assert client.closed


def test_mongodb_collection_pattern_matches_the_fully_qualified_name():
    # mongodb.py's get_workunits_internal matches collection_pattern against
    # "<database>.<collection>" (e.g. "app.sessions"), not the bare collection
    # name — so a deny anchored to the bare name ("^sessions$") never matches
    # and does NOT exclude the collection; this is what real ingestion does too.
    client = _MongoClient({"app": ["orders", "sessions"]})
    config = _config(AllowDenyPattern(allow=[".*"], deny=["^sessions$"]), client)
    by_name = {n.name: n for n in list_mongodb_children(config, ["app"], 100).nodes}
    assert by_name["sessions"].included is True

    # A deny anchored to the fully qualified name does exclude it.
    fqn_anchored = AllowDenyPattern(allow=[".*"], deny=[r"^app\.sessions$"])
    config = _config(fqn_anchored, client)
    by_name = {n.name: n for n in list_mongodb_children(config, ["app"], 100).nodes}
    assert by_name["sessions"].included is False
    assert by_name["sessions"].excluded_by == "collection_pattern"
    assert by_name["orders"].included is True


class _EsIndices:
    def __init__(self, names):
        self._names = names

    def get_alias(self):
        return {n: {} for n in self._names}


class _EsClient:
    def __init__(self, names):
        self.indices = _EsIndices(names)


def test_elasticsearch_probe_lists_indices():
    config = SimpleNamespace(
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
