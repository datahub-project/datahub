import pathlib
from typing import Any, Dict

import pytest

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


def test_expand_run_results_paths_preserves_config_order(
    tmp_path: pathlib.Path,
) -> None:
    # Two literal entries, declared newest-first. run_results files are appended
    # per node, so the caller's declared order must survive expansion.
    for name in ["run_results_a.json", "run_results_z.json"]:
        (tmp_path / name).write_text("{}")

    source = _make_source(
        run_results_paths=[
            f"{tmp_path}/run_results_z.json",
            f"{tmp_path}/run_results_a.json",
        ]
    )

    assert source._expand_run_results_paths() == [
        f"{tmp_path}/run_results_z.json",
        f"{tmp_path}/run_results_a.json",
    ]


def test_globbed_manifest_rejects_explicit_catalog_path() -> None:
    with pytest.raises(ValueError, match="catalog_path"):
        DBTCoreConfig(
            manifest_path="s3://bucket/*/manifest.json",
            catalog_path="s3://bucket/project_a/catalog.json",
            target_platform="postgres",
            aws_connection={"aws_region": "us-east-1"},
        )


def test_globbed_manifest_rejects_explicit_sources_path() -> None:
    with pytest.raises(ValueError, match="sources_path"):
        DBTCoreConfig(
            manifest_path="s3://bucket/*/manifest.json",
            sources_path="s3://bucket/project_a/sources.json",
            target_platform="postgres",
            aws_connection={"aws_region": "us-east-1"},
        )


def test_globbed_manifest_alone_is_accepted() -> None:
    config = DBTCoreConfig(
        manifest_path="s3://bucket/*/manifest.json",
        target_platform="postgres",
        aws_connection={"aws_region": "us-east-1"},
    )
    assert config.catalog_path is None


def test_non_glob_manifest_still_accepts_explicit_catalog_path() -> None:
    config = DBTCoreConfig(
        manifest_path="/tmp/project_a/manifest.json",
        catalog_path="/tmp/project_a/catalog.json",
        target_platform="postgres",
    )
    assert config.catalog_path == "/tmp/project_a/catalog.json"
