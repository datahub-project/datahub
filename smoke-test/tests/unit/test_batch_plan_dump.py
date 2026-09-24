"""CPU-only tests for the opt-in batch-plan dump plugin."""

from __future__ import annotations

from pathlib import Path

import pytest
from _pytest.mark.structures import Mark

from shard_pack import ModuleShard, pack_module_plans
from utilities.batch_plan_dump import (
    markexpr_filtered_items,
    resolve_dump_path,
    serialize_batch_plans,
)
from utilities.domains import Domain
from utilities.env_vars import get_smoke_dump_batch_plan

pytestmark = pytest.mark.domain(Domain.PLATFORM_INTERNAL)


def test_env_path_overrides_cli() -> None:
    assert resolve_dump_path("/tmp/cli.json", "/tmp/env.json") == "/tmp/env.json"
    assert resolve_dump_path("/tmp/cli.json", None) == "/tmp/cli.json"
    assert resolve_dump_path("/tmp/cli.json", "  ") == "/tmp/cli.json"
    assert resolve_dump_path(None, None) is None


def test_smoke_dump_batch_plan_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SMOKE_DUMP_BATCH_PLAN", raising=False)
    assert get_smoke_dump_batch_plan() is None
    monkeypatch.setenv("SMOKE_DUMP_BATCH_PLAN", "  /tmp/plan.json  ")
    assert get_smoke_dump_batch_plan() == "/tmp/plan.json"


def test_serialize_batch_plans_shape() -> None:
    shards = [
        ModuleShard("tests/a.py::TestA", 40.0, 0.0),
        ModuleShard("tests/b.py", 10.0, 5.0),
        ModuleShard("tests/c.py", 0.0, 20.0),
    ]
    plans = pack_module_plans(shards, batch_count=2, xdist_workers=3)
    payload = serialize_batch_plans(
        plans,
        batch_count=2,
        batch_number=0,
        xdist_workers=3,
        test_count=6,
        scope_test_counts={
            "tests/a.py::TestA": 3,
            "tests/b.py": 2,
            "tests/c.py": 1,
        },
        missing_weight_count=2,
        markexpr="p0",
        domains=["catalog"],
    )
    assert payload["batch_count"] == 2
    assert payload["batch_number"] == 0
    assert payload["xdist_workers"] == 3
    assert payload["markexpr"] == "p0"
    assert payload["domains"] == ["catalog"]
    assert payload["test_count"] == 6
    assert payload["scope_count"] == 3
    assert payload["missing_weight_count"] == 2
    assert len(payload["batches"]) == 2
    walls = [batch["predicted_wall"] for batch in payload["batches"]]
    assert payload["max_predicted_wall"] == pytest.approx(max(walls))
    assert payload["imbalance"] == pytest.approx(max(walls) - min(walls))
    for batch in payload["batches"]:
        counted = sum(
            {"tests/a.py::TestA": 3, "tests/b.py": 2, "tests/c.py": 1}[scope]
            for scope in batch["scopes"]
        )
        assert batch["test_count"] == counted
        assert batch["scope_count"] == len(batch["scopes"])


class _FakeItem:
    def __init__(self, nodeid: str, *mark_names: str) -> None:
        self.nodeid = nodeid
        self._marks = tuple(Mark(name, (), {}, _ispytest=True) for name in mark_names)

    def iter_markers(self) -> tuple[Mark, ...]:
        return self._marks


def test_markexpr_filtered_items_matches_pytest_m() -> None:
    p0 = _FakeItem("tests/a.py::test_p0", "p0", "domain")
    other = _FakeItem("tests/b.py::test_other", "domain")
    skipped = _FakeItem("tests/c.py::test_skip", "p0", "skip")
    items = [p0, other, skipped]

    assert markexpr_filtered_items("", items) == items
    assert markexpr_filtered_items("p0", items) == [p0, skipped]
    assert markexpr_filtered_items("p0 and not skip", items) == [p0]
    assert items == [p0, other, skipped]


def test_write_dump_creates_parent_dirs(tmp_path: Path) -> None:
    from utilities.batch_plan_dump import write_dump

    dest = tmp_path / "nested" / "plan.json"
    write_dump({"ok": True}, str(dest))
    assert dest.read_text().strip() == '{\n  "ok": true\n}'
