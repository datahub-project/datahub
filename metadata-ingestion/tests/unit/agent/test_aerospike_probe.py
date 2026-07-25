from types import SimpleNamespace
from typing import Any

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.aerospike_probe import list_aerospike_children
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)

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


def _config() -> Any:
    client = _FakeAerospikeClient(_SETS_INFO)
    return SimpleNamespace(
        get_client=lambda: client,
        namespace_pattern=AllowDenyPattern(allow=[".*"]),
        set_pattern=AllowDenyPattern(allow=[".*"], deny=["^tmp_.*"]),
    )


def test_aerospike_lists_namespaces_with_pattern_verdict() -> None:
    result = list_aerospike_children(_config(), [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["analytics"].kind == DatasetContainerSubTypes.NAMESPACE
    assert by_name["analytics"].pattern_field == "namespace_pattern"
    assert by_name["analytics"].included is True


def test_aerospike_lists_sets_reusing_set_pattern() -> None:
    result = list_aerospike_children(_config(), ["analytics"], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["orders"].kind == DatasetSubTypes.TABLE
    assert by_name["orders"].included is True
    # The connector's own set_pattern deny (^tmp_) is reused for the verdict.
    assert by_name["tmp_scratch"].included is False
    assert by_name["tmp_scratch"].excluded_by == "set_pattern"
