import json
from pathlib import Path

from lib.results import JsonlWriter, build_result_row, load_jsonl, write_summary


def test_jsonl_append_and_summary(tmp_path: Path) -> None:
    out = tmp_path / "run.jsonl"
    writer = JsonlWriter(out)
    row = build_result_row(
        run_id="run-1",
        metadata={"git": {"commit": "abc"}},
        persona="persona-admin",
        operation="getMe",
        metric_key="getMe@expect200",
        performance_profile="authz_allow",
        query_source="getMe",
        stress_target="test",
        membership_count=0,
        cache_phase="warm",
        harness={"warmup": 1, "iterations": 2, "execution_mode": "isolated"},
        samples_ms=[1.0, 2.0],
    )
    writer.append(row)
    writer.close()
    loaded = load_jsonl(out)
    assert len(loaded) == 1
    assert loaded[0]["schema_version"] == 2
    assert loaded[0]["metric_key"] == "getMe@expect200"
    assert loaded[0]["stats"]["max"] == 2.0
    summary = write_summary(out, loaded)
    data = json.loads(summary.read_text())
    assert len(data["results"]) == 1
    assert data["results"][0]["metric_key"] == "getMe@expect200"
