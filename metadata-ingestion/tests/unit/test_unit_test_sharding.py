import pathlib
from typing import Dict, List

import pytest

import tests.conftest as root_conftest
from tests.conftest import (
    _UNIT_FILE_OVERHEAD_SECONDS,
    _UNIT_SHARD_ENV,
    _UNIT_SHARD_KEY,
    _assign_unit_shards,
    _build_unit_shard,
    _parse_unit_shard,
    _read_integration_weights,
    _unit_file_weights,
    _unit_shard,
)

_SHARDS = 4


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


def _real_shards(pytestconfig: pytest.Config) -> List[root_conftest._UnitShard]:
    root = pathlib.Path(pytestconfig.rootpath)
    python_files = list(pytestconfig.getini("python_files"))
    weights = _read_integration_weights()
    return [
        _build_unit_shard(index, _SHARDS, root, python_files, weights)
        for index in range(_SHARDS)
    ]


def test_every_real_unit_test_file_runs_in_exactly_one_shard(
    pytestconfig: pytest.Config,
) -> None:
    """What pytest_ignore_collect sees: real paths, the real python_files setting
    and the committed weights. A file owned by no shard would silently never run."""
    shards = _real_shards(pytestconfig)
    unit_dir = shards[0].unit_dir
    test_files = [p for p in unit_dir.rglob("*.py") if shards[0].is_test_file(p)]
    assert len(test_files) > 500, "unit test discovery looks broken"

    owners: Dict[pathlib.Path, List[int]] = {
        path: [s.index for s in shards if s.owns(path)] for path in test_files
    }
    misassigned = {p: o for p, o in owners.items() if len(o) != 1}
    assert not misassigned, misassigned
    assert {o[0] for o in owners.values()} == set(range(_SHARDS))


def test_non_test_files_are_left_to_pytest(pytestconfig: pytest.Config) -> None:
    shard = _real_shards(pytestconfig)[0]
    unit_dir = shard.unit_dir
    assert shard.owns(unit_dir / "conftest.py") is None
    assert shard.owns(unit_dir / "__init__.py") is None
    assert shard.owns(unit_dir.parent / "integration" / "test_anything.py") is None


def test_file_missing_from_assignment_runs_in_every_shard(
    pytestconfig: pytest.Config,
) -> None:
    """A file the assignment never saw is run everywhere rather than dropped."""
    shards = _real_shards(pytestconfig)
    new_file = shards[0].unit_dir / "brand_new" / "test_not_in_assignment.py"
    assert all(s.owns(new_file) is True for s in shards)


def test_unreadable_weights_fail_a_sharded_run(
    pytestconfig: pytest.Config,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    broken = tmp_path / "weights.json"
    broken.write_text("{not json")
    monkeypatch.setattr(root_conftest, "_INTEGRATION_WEIGHTS_FILE", broken)
    monkeypatch.setenv(_UNIT_SHARD_ENV, "1/4")
    # _unit_shard caches its result on the config; run uncached, then restore.
    had_shard = _UNIT_SHARD_KEY in pytestconfig.stash
    saved = pytestconfig.stash[_UNIT_SHARD_KEY] if had_shard else None
    if had_shard:
        del pytestconfig.stash[_UNIT_SHARD_KEY]
    try:
        with pytest.raises(pytest.UsageError, match="could not be read"):
            _unit_shard(pytestconfig)
    finally:
        if had_shard:
            pytestconfig.stash[_UNIT_SHARD_KEY] = saved
        elif _UNIT_SHARD_KEY in pytestconfig.stash:
            del pytestconfig.stash[_UNIT_SHARD_KEY]
