import pytest

from tests.conftest import (
    _UNIT_FILE_OVERHEAD_SECONDS,
    _assign_unit_shards,
    _parse_unit_shard,
    _unit_file_weights,
)


def test_parse_unit_shard() -> None:
    assert _parse_unit_shard("") is None
    assert _parse_unit_shard("2/4") == (2, 4)
    for bad in ("4/4", "1/0", "1-4", "a/b"):
        with pytest.raises(pytest.UsageError):
            _parse_unit_shard(bad)


def test_file_weights_attribute_class_tests_and_default_new_files() -> None:
    weights = {
        "tests.unit.a.test_x::test_one": 2.0,
        "tests.unit.a.test_x.TestThing::test_two": 3.0,
        # A sibling module whose name extends test_x must not be counted as test_x.
        "tests.unit.a.test_x_more::test_three": 7.0,
        "tests.unit.gone.test_removed::test_old": 100.0,
    }
    modules = [
        "tests.unit.a.test_x",
        "tests.unit.a.test_x_more",
        "tests.unit.b.test_new",
    ]

    result = _unit_file_weights(modules, weights)

    assert result["tests.unit.a.test_x"] == 5.0 + _UNIT_FILE_OVERHEAD_SECONDS
    assert result["tests.unit.a.test_x_more"] == 7.0 + _UNIT_FILE_OVERHEAD_SECONDS
    # Not in the weights file: median of the known modules (5 and 7).
    assert result["tests.unit.b.test_new"] == 6.0 + _UNIT_FILE_OVERHEAD_SECONDS


def test_assign_unit_shards_covers_every_module_once_and_balances() -> None:
    module_weights = {f"tests.unit.test_{i:03d}": float(i % 7 + 1) for i in range(200)}

    assignment = _assign_unit_shards(module_weights, 4)

    assert set(assignment) == set(module_weights)
    assert set(assignment.values()) == {0, 1, 2, 3}
    totals = [
        sum(w for m, w in module_weights.items() if assignment[m] == shard)
        for shard in range(4)
    ]
    assert max(totals) - min(totals) <= max(module_weights.values())
    # Every CI job computes the assignment independently, so it must be stable.
    assert _assign_unit_shards(dict(reversed(module_weights.items())), 4) == assignment
