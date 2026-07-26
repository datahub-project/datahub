from types import SimpleNamespace
from typing import Any, Dict, List

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.cassandra.cassandra_probe import list_cassandra_children
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)


class _FakeKeyspace:
    def __init__(self, keyspace_name: str) -> None:
        self.keyspace_name = keyspace_name


class _FakeTable:
    def __init__(self, table_name: str) -> None:
        self.table_name = table_name


class _FakeCassandraClient:
    def __init__(self, keyspaces: List[str], tables: Dict[str, List[str]]) -> None:
        self._keyspaces = keyspaces
        self._tables = tables
        self.authenticated = False
        self.closed = False

    def authenticate(self) -> bool:
        self.authenticated = True
        return True

    def get_keyspaces(self) -> List[_FakeKeyspace]:
        return [_FakeKeyspace(k) for k in self._keyspaces]

    def get_tables(self, keyspace_name: str) -> List[_FakeTable]:
        return [_FakeTable(t) for t in self._tables.get(keyspace_name, [])]

    def close(self) -> None:
        self.closed = True


def _config() -> Any:
    client = _FakeCassandraClient(
        ["analytics", "billing"], {"analytics": ["orders", "tmp_scratch"]}
    )
    return SimpleNamespace(
        get_client=lambda: client,
        keyspace_pattern=AllowDenyPattern(allow=[".*"]),
        table_pattern=AllowDenyPattern(allow=[".*"], deny=["^tmp_.*"]),
    )


def test_cassandra_lists_keyspaces_with_pattern_verdict() -> None:
    result = list_cassandra_children(_config(), [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["analytics"].kind == DatasetContainerSubTypes.KEYSPACE
    assert by_name["analytics"].pattern_field == "keyspace_pattern"
    assert by_name["analytics"].included is True


def test_cassandra_table_pattern_matches_the_fully_qualified_name() -> None:
    # cassandra.py's _generate_table matches table_pattern against
    # "<keyspace>.<table>" (e.g. "analytics.tmp_scratch"), not the bare table
    # name — so a deny anchored to the bare name ("^tmp_.*") never matches and
    # does NOT exclude the table; this is what real ingestion does too.
    result = list_cassandra_children(_config(), ["analytics"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].included is True
    assert by_name["tmp_scratch"].included is True

    # A deny anchored to the fully qualified name does exclude it.
    fqn_config = _config()
    fqn_config.table_pattern = AllowDenyPattern(
        allow=[".*"], deny=[r"^analytics\.tmp_scratch$"]
    )
    result = list_cassandra_children(fqn_config, ["analytics"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["tmp_scratch"].included is False
    assert by_name["tmp_scratch"].excluded_by == "table_pattern"
    assert by_name["orders"].included is True
