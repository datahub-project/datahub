import pathlib
from typing import Any, Dict

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource


def _make_source(**config_overrides: Any) -> DBTCoreSource:
    config: Dict[str, Any] = {
        "manifest_path": "unused/manifest.json",
        "target_platform": "postgres",
        "enable_meta_mapping": False,
    }
    config.update(config_overrides)
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-multi-project")
    ctx.graph = None
    return DBTCoreSource(DBTCoreConfig(**config), ctx)


def test_expand_glob_path_returns_sorted_local_matches(tmp_path: pathlib.Path) -> None:
    # Created out of order on purpose: expansion must not depend on creation order.
    for name in ["c.json", "a.json", "b.json"]:
        (tmp_path / name).write_text("{}")

    source = _make_source()
    expanded = source._expand_glob_path(f"{tmp_path}/*.json")

    assert expanded == [
        f"{tmp_path}/a.json",
        f"{tmp_path}/b.json",
        f"{tmp_path}/c.json",
    ]


def test_expand_glob_path_passes_through_non_glob_path() -> None:
    source = _make_source()
    assert source._expand_glob_path("s3://bucket/project/manifest.json") == [
        "s3://bucket/project/manifest.json"
    ]


def test_expand_run_results_paths_is_sorted(tmp_path: pathlib.Path) -> None:
    for name in ["run_results_z.json", "run_results_a.json"]:
        (tmp_path / name).write_text("{}")

    source = _make_source(run_results_paths=[f"{tmp_path}/run_results_*.json"])
    expanded = source._expand_run_results_paths()

    assert expanded == sorted(expanded)
    assert len(expanded) == 2
