from typing import Any

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.aerospike_probe import list_aerospike_children
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from tests.unit.agent.pattern_hint_fixtures import config_with_hints

_SETS_INFO = (
    "sets\t"
    "ns=analytics:set=orders:objects=10:tombstones=0:truncate_lut=0:sindexes=0:index_populating=false;"
    "ns=analytics:set=tmp_scratch:objects=2:tombstones=0:truncate_lut=0:sindexes=0:index_populating=false;"
    "\n"
)


class _FakeAerospikeClient:
    def __init__(self, sets_info: str) -> None:
        self._sets_info = sets_info
        self.closed = False

    def info_random_node(self, command: str) -> str:
        assert command == "sets"
        return self._sets_info

    def close(self) -> None:
        self.closed = True


def _config(set_pattern: AllowDenyPattern) -> Any:
    client = _FakeAerospikeClient(_SETS_INFO)
    return config_with_hints(
        {"set_pattern": DatasetSubTypes.TABLE},
        get_client=lambda: client,
        namespace_pattern=AllowDenyPattern(allow=[".*"]),
        set_pattern=set_pattern,
    )


_DEFAULT_PATTERN = AllowDenyPattern(allow=[".*"], deny=["^tmp_.*"])


def test_aerospike_lists_namespaces_with_pattern_verdict() -> None:
    result = list_aerospike_children(_config(_DEFAULT_PATTERN), [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["analytics"].kind == DatasetContainerSubTypes.NAMESPACE
    assert by_name["analytics"].pattern_field == "namespace_pattern"
    assert by_name["analytics"].included is True


def test_aerospike_set_pattern_matches_the_fully_qualified_name() -> None:
    # aerospike.py's _get_namespace_workunits matches set_pattern against
    # "<namespace>.<set>" (e.g. "analytics.tmp_scratch"), not the bare set
    # name — so a deny anchored to the bare name ("^tmp_.*") never matches and
    # does NOT exclude the set; this is what real ingestion does too.
    result = list_aerospike_children(_config(_DEFAULT_PATTERN), ["analytics"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].included is True
    assert by_name["tmp_scratch"].included is True

    # A deny anchored to the fully qualified name does exclude it.
    fqn_anchored = AllowDenyPattern(allow=[".*"], deny=[r"^analytics\.tmp_scratch$"])
    result = list_aerospike_children(_config(fqn_anchored), ["analytics"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["tmp_scratch"].included is False
    assert by_name["tmp_scratch"].excluded_by == "set_pattern"
    assert by_name["orders"].included is True
