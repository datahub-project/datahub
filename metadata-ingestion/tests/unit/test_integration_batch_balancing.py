import pathlib
from typing import List

from tests.conftest import (
    _DEFAULT_TEST_WEIGHT,
    _bin_pack_groups,
    _build_class_weight_index,
    _connector_key,
    _ConnectorGroup,
    _item_weight,
    _nodeid_to_test_id,
)

INTEGRATION = pathlib.Path("tests/integration")


class FakeItem:
    """Minimal stand-in for a pytest item: only nodeid and path are read."""

    def __init__(self, nodeid: str, path: pathlib.Path) -> None:
        self.nodeid = nodeid
        self.path = path


def _group(key: str, n: int, weight_each: float = 1.0) -> _ConnectorGroup:
    items = [
        FakeItem(f"tests/integration/{key}/test_{key}.py::test_{i}", INTEGRATION / key)
        for i in range(n)
    ]
    return _ConnectorGroup(key=key, items=items, weight=n * weight_each)


def test_nodeid_to_test_id_with_and_without_class() -> None:
    assert (
        _nodeid_to_test_id("tests/integration/foo/test_x.py::TestA::test_a")
        == "tests.integration.foo.test_x.TestA::test_a"
    )
    assert (
        _nodeid_to_test_id("tests/integration/foo/test_x.py::test_b")
        == "tests.integration.foo.test_x::test_b"
    )
    # Parametrized ids keep the [N] suffix as part of the function name.
    assert (
        _nodeid_to_test_id("tests/integration/foo/test_x.py::TestA::test_a[0]")
        == "tests.integration.foo.test_x.TestA::test_a[0]"
    )


def test_build_class_weight_index_median_and_skip() -> None:
    weights = {
        "m.TestA::t1": 2.0,
        "m.TestA::t2": 4.0,
        "m.TestA::t3": 6.0,
        "orphan_no_class": 10.0,
    }
    index = _build_class_weight_index(weights)
    assert index == {"m.TestA": 4.0}


def test_item_weight_resolution_layers() -> None:
    item = FakeItem("m/mod.py::TestClass::exact", pathlib.Path("m/mod.py"))
    weights = {"m.mod.TestClass::exact": 7.5}
    assert _item_weight(item, weights, {}) == 7.5

    miss = FakeItem("m/mod.py::TestClass::other", pathlib.Path("m/mod.py"))
    assert _item_weight(miss, weights, {"m.mod.TestClass": 9.0}) == 9.0

    unknown = FakeItem("m/mod.py::NoClass::x", pathlib.Path("m/mod.py"))
    assert _item_weight(unknown, weights, {}) == _DEFAULT_TEST_WEIGHT


def test_connector_key_subdir_and_root() -> None:
    sub = FakeItem(
        "tests/integration/foo/test_a.py::test_a",
        INTEGRATION / "foo" / "test_a.py",
    )
    assert _connector_key(sub, INTEGRATION) == "foo"

    root = FakeItem(
        "tests/integration/test_top.py::test_t",
        INTEGRATION / "test_top.py",
    )
    assert _connector_key(root, INTEGRATION) == "__root__"


def test_bin_pack_groups_determinism_and_bin_count() -> None:
    groups = [
        _ConnectorGroup(key=f"k{i}", items=[], weight=w)
        for i, w in enumerate([50, 40, 30, 20, 10, 5, 5])
    ]
    first = _bin_pack_groups(groups, 6)
    second = _bin_pack_groups(groups, 6)
    assert len(first) == 6
    assert first == second


def test_bin_pack_groups_no_bin_exceeds_heaviest() -> None:
    weights = [50, 40, 30, 20, 10, 5, 5]
    groups = [
        _ConnectorGroup(key=f"k{i}", items=[], weight=w) for i, w in enumerate(weights)
    ]
    bins = _bin_pack_groups(groups, 6)
    assert max(sum(g.weight for g in b) for b in bins) <= 50


def test_end_to_end_no_drop_no_split_deterministic() -> None:
    groups: List[_ConnectorGroup] = [
        _group("alpha", 4, 3.0),
        _group("beta", 3, 5.0),
        _group("gamma", 5, 1.0),
        _group("delta", 2, 8.0),
        _group("epsilon", 4, 2.0),
        _group("zeta", 3, 4.0),
        _group("eta", 5, 1.5),
        _group("theta", 2, 6.0),
        _group("iota", 3, 3.0),
        _group("kappa", 4, 2.5),
        _group("lambda", 3, 4.5),
        _group("mu", 5, 1.0),
    ]
    bins = _bin_pack_groups(groups, 6)
    assert len(bins) == 6

    all_items = [item for g in groups for item in g.items]
    packed_items = [item for b in bins for g in b for item in g.items]
    # (a) nothing dropped or duplicated
    assert sorted(id(i) for i in packed_items) == sorted(id(i) for i in all_items)

    # (b) each connector's items live in exactly one bin
    key_to_bin: dict = {}
    for bin_index, bin_groups in enumerate(bins):
        for g in bin_groups:
            assert g.key not in key_to_bin, f"{g.key} split across bins"
            key_to_bin[g.key] = bin_index

    # (c) determinism
    assert _bin_pack_groups(groups, 6) == bins
