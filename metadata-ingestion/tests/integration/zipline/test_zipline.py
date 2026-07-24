import json
import pathlib
import shutil
import subprocess
import sys
from typing import Any, Dict, List, cast
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.zipline.report import ZiplineSourceReport
from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import (
    get_current_checkpoint_from_pipeline,
    mock_datahub_graph,  # noqa: F401  (pytest fixture)
    run_and_get_pipeline,
    validate_all_providers_have_committed_successfully,
)

FROZEN_TIME = "2023-06-01 07:00:00"
GMS_SERVER = "http://localhost:8080"

_OWNER_MAPPINGS = [
    {
        "team_name": "my_team",
        "datahub_owner_urn": "urn:li:corpGroup:my_team",
        "datahub_ownership_type": "TECHNICAL_OWNER",
    },
    {
        "team_name": "risk_team",
        "datahub_owner_urn": "urn:li:corpGroup:risk_team",
    },
]

_SOURCE_PLATFORM_MAP = {"data": "hive", "events": "kafka", "warehouse": "snowflake"}


def _resources_dir(pytestconfig: pytest.Config) -> pathlib.Path:
    return pytestconfig.rootpath / "tests/integration/zipline"


def _base_config(path: str, **overrides: Any) -> Dict[str, Any]:
    config: Dict[str, Any] = {
        "path": path,
        "source_platform_map": dict(_SOURCE_PLATFORM_MAP),
        "default_source_platform": "hive",
    }
    config.update(overrides)
    return config


def _build_pipeline(
    source_config: Dict[str, Any], output_path: pathlib.Path
) -> Pipeline:
    return Pipeline.create(
        {
            "run_id": "zipline-test",
            "source": {"type": "zipline", "config": source_config},
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )


def _run_pipeline(source_config: Dict[str, Any], output_path: pathlib.Path) -> Pipeline:
    pipeline = _build_pipeline(source_config, output_path)
    pipeline.run()
    pipeline.raise_from_status()
    return pipeline


def _run_pipeline_no_raise(
    source_config: Dict[str, Any], output_path: pathlib.Path
) -> Pipeline:
    pipeline = _build_pipeline(source_config, output_path)
    pipeline.run()
    return pipeline


def _write_json(path: pathlib.Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def _report(pipeline: Pipeline) -> ZiplineSourceReport:
    return cast(ZiplineSourceReport, pipeline.source.get_report())


def _records(output_path: pathlib.Path) -> List[Dict[str, Any]]:
    return json.loads(output_path.read_text())


def _aspect_names(records: List[Dict[str, Any]]) -> List[str]:
    names: List[str] = []
    for record in records:
        name = record.get("aspectName")
        if isinstance(name, str):
            names.append(name)
    return names


def _entity_urns_with_aspect(
    records: List[Dict[str, Any]], aspect_name: str
) -> List[str]:
    return [
        r["entityUrn"]
        for r in records
        if r.get("aspectName") == aspect_name and r.get("entityUrn")
    ]


def test_zipline_ingest(pytestconfig, tmp_path):
    """Full extraction: two teams, tags, owners, feature typing, and lineage."""
    test_resources_dir = _resources_dir(pytestconfig)
    output_path = tmp_path / "zipline_mces.json"

    pipeline = _run_pipeline(
        _base_config(
            str(test_resources_dir),
            enable_tag_extraction=True,
            enable_owner_extraction=True,
            owner_mappings=_OWNER_MAPPINGS,
        ),
        output_path,
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "zipline_mces_golden.json",
    )

    report = _report(pipeline)
    assert report.feature_tables_scanned == 2
    assert report.joins_scanned == 1
    assert report.staging_queries_scanned == 2
    # my_team + risk_team both emit a DataFlow via their jobs.
    assert report.teams_scanned == 2
    # Both staging queries have parseable SQL.
    assert report.sql_lineage_parsed == 2
    assert report.sql_lineage_failures == 0
    # Output-namespace tables are never in the source platform map, so they are
    # reported as unmapped (expected, not an error).
    assert "risk_features" in report.unmapped_source_namespaces


def test_zipline_ingest_defaults(pytestconfig, tmp_path):
    """With tag/owner extraction off (the defaults) no tag or ownership aspects
    are emitted, while feature tables are still produced."""
    test_resources_dir = _resources_dir(pytestconfig)
    output_path = tmp_path / "zipline_defaults.json"

    _run_pipeline(_base_config(str(test_resources_dir)), output_path)

    aspects = _aspect_names(_records(output_path))
    assert "mlFeatureTableProperties" in aspects
    assert "globalTags" not in aspects
    assert "ownership" not in aspects


def test_zipline_team_and_feature_filtering(pytestconfig, tmp_path):
    """team_pattern and feature_table_pattern drop the matching entities and are
    reflected in the report counters."""
    test_resources_dir = _resources_dir(pytestconfig)
    output_path = tmp_path / "zipline_filtered.json"

    pipeline = _run_pipeline(
        _base_config(
            str(test_resources_dir),
            team_pattern={"deny": ["risk_team"]},
            feature_table_pattern={"deny": ["my_team.user_features.v1"]},
        ),
        output_path,
    )

    report = _report(pipeline)
    # risk_team's group_by is filtered by team; my_team's single feature table is
    # filtered by feature_table_pattern -> nothing emitted through the mapper.
    assert report.feature_tables_scanned == 0
    assert report.filtered_teams == 1
    assert report.filtered_feature_tables == 1
    # risk_team's join/staging jobs are also team-filtered; only my_team remains.
    assert report.joins_scanned == 1
    assert report.staging_queries_scanned == 1
    # risk_team's staging query is team-filtered and counted (issue #7).
    assert report.filtered_staging_queries == 1

    urns = {r.get("entityUrn") for r in _records(output_path)}
    assert not any(urn and "risk_team" in urn for urn in urns)


def test_zipline_include_toggles(pytestconfig, tmp_path):
    """include_joins / include_staging_queries gate DataJob emission entirely."""
    test_resources_dir = _resources_dir(pytestconfig)
    output_path = tmp_path / "zipline_no_jobs.json"

    pipeline = _run_pipeline(
        _base_config(
            str(test_resources_dir),
            include_joins=False,
            include_staging_queries=False,
        ),
        output_path,
    )

    report = _report(pipeline)
    assert report.feature_tables_scanned == 2
    assert report.joins_scanned == 0
    assert report.staging_queries_scanned == 0

    aspects = _aspect_names(_records(output_path))
    assert "dataJobInfo" not in aspects
    assert "dataFlowInfo" not in aspects


def _stateful_config(path: str, output_path: pathlib.Path) -> Dict[str, Any]:
    return {
        "run_id": "zipline-stateful-run",
        "pipeline_name": "zipline-stateful-pipeline",
        "source": {
            "type": "zipline",
            "config": _base_config(
                path,
                stateful_ingestion={
                    "enabled": True,
                    "remove_stale_metadata": True,
                    "fail_safe_threshold": 100.0,
                    "state_provider": {
                        "type": "datahub",
                        "config": {"datahub_api": {"server": GMS_SERVER}},
                    },
                },
            ),
        },
        "sink": {"type": "file", "config": {"filename": str(output_path)}},
    }


def test_zipline_stateful_soft_deletes_removed_team(
    pytestconfig: pytest.Config,
    tmp_path: pathlib.Path,
    mock_datahub_graph: MagicMock,  # noqa: F811
) -> None:
    """Two-run stateful ingestion: when risk_team's compiled configs disappear,
    its entities are soft-deleted while my_team's are retained."""
    test_resources_dir = _resources_dir(pytestconfig)

    repo = tmp_path / "repo"
    shutil.copytree(test_resources_dir / "production", repo / "production")

    out1 = tmp_path / "run1.json"
    out2 = tmp_path / "run2.json"

    with patch(
        "datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider.DataHubGraph",
        mock_datahub_graph,
    ) as mock_checkpoint:
        mock_checkpoint.return_value = mock_datahub_graph

        pipeline1 = run_and_get_pipeline(_stateful_config(str(repo), out1))
        checkpoint1 = get_current_checkpoint_from_pipeline(pipeline1)

        # Drop the entire risk_team from the compiled repo before the second run.
        for subdir in ("group_bys", "joins", "staging_queries"):
            team_dir = repo / "production" / subdir / "risk_team"
            if team_dir.exists():
                shutil.rmtree(team_dir)

        pipeline2 = run_and_get_pipeline(_stateful_config(str(repo), out2))
        checkpoint2 = get_current_checkpoint_from_pipeline(pipeline2)

    assert checkpoint1 and checkpoint1.state
    assert checkpoint2 and checkpoint2.state
    validate_all_providers_have_committed_successfully(pipeline1, expected_providers=1)
    validate_all_providers_have_committed_successfully(pipeline2, expected_providers=1)

    state1 = checkpoint1.state
    state2 = checkpoint2.state
    assert isinstance(state1, GenericCheckpointState)
    assert isinstance(state2, GenericCheckpointState)

    difference = set(state1.get_urns_not_in(type="*", other_checkpoint_state=state2))
    assert difference, "expected risk_team URNs to leave the checkpoint"
    assert any("risk_team" in urn or "account_risk" in urn for urn in difference)
    assert not any("user_features" in urn for urn in difference)

    removed = _entity_urns_with_aspect(_records(out2), "status")
    removed_soft = [
        r["entityUrn"]
        for r in _records(out2)
        if r.get("aspectName") == "status"
        and (r.get("aspect", {}).get("json") or {}).get("removed") is True
    ]
    assert any(
        "account_risk" in urn or "risk_staging" in urn for urn in removed_soft
    ), removed
    assert not any("user_features" in urn for urn in removed_soft)


def test_zipline_missing_path_reports_failure(tmp_path):
    """A path with no compiled output produces a structured failure, not a crash."""
    output_path = tmp_path / "empty.json"
    empty_dir = tmp_path / "does_not_exist"

    pipeline = Pipeline.create(
        {
            "run_id": "zipline-missing",
            "source": {
                "type": "zipline",
                "config": {"path": str(empty_dir)},
            },
            "sink": {"type": "file", "config": {"filename": str(output_path)}},
        }
    )
    pipeline.run()

    report = _report(pipeline)
    assert report.failures
    assert report.feature_tables_scanned == 0


def test_zipline_path_without_compiled_output_fails(tmp_path):
    """Pointing at an existing dir that isn't compiled Zipline output (e.g. the
    Python config repo) must fail loudly rather than emit nothing silently."""
    decoy = tmp_path / "python_repo"
    (decoy / "src").mkdir(parents=True)
    output_path = tmp_path / "out.json"

    pipeline = _run_pipeline_no_raise(_base_config(str(decoy)), output_path)

    report = _report(pipeline)
    assert report.failures
    assert report.feature_tables_scanned == 0


def test_zipline_nameless_objects_warn_and_are_not_miscounted(tmp_path):
    """Compiled objects missing metaData.name are dropped with a warning, and a
    nameless Join/StagingQuery is NOT counted as scanned (issues #2 and #5)."""
    production = tmp_path / "production"
    _write_json(
        production / "group_bys" / "team_x" / "nameless",
        {"metaData": {"team": "team_x"}, "keyColumns": ["id"], "aggregations": []},
    )
    _write_json(
        production / "joins" / "team_x" / "nameless",
        {"metaData": {"team": "team_x"}, "joinParts": []},
    )
    _write_json(
        production / "staging_queries" / "team_x" / "nameless",
        {"metaData": {"team": "team_x"}, "query": "SELECT 1"},
    )
    output_path = tmp_path / "out.json"

    pipeline = _run_pipeline(_base_config(str(production)), output_path)

    report = _report(pipeline)
    assert report.feature_tables_scanned == 0
    # The misleading-counter bug: a nameless object emits nothing, so it must not
    # inflate the scanned counters.
    assert report.joins_scanned == 0
    assert report.staging_queries_scanned == 0
    # One warning per dropped object (GroupBy, Join, StagingQuery).
    warning_titles = [w.title for w in report.warnings]
    assert warning_titles.count("GroupBy missing name") == 1
    assert warning_titles.count("Join missing name") == 1
    assert warning_titles.count("StagingQuery missing name") == 1


def _compile_chronon_repo(repo: pathlib.Path) -> pathlib.Path:
    conf_files = [
        path.relative_to(repo).as_posix()
        for kind in ("group_bys", "joins", "staging_queries")
        for path in sorted((repo / kind).rglob("*.py"))
        if path.name != "__init__.py"
    ]
    for conf in conf_files:
        subprocess.run(
            [
                sys.executable,
                "-m",
                "ai.chronon.repo.compile",
                "--chronon_root",
                str(repo),
                "--conf",
                conf,
                "--force-overwrite",
                "-y",
            ],
            check=True,
            cwd=str(repo),
            capture_output=True,
        )
    return repo / "production"


def test_zipline_compiles_from_real_chronon_build(pytestconfig, tmp_path):
    """When chronon is installed, drive the real compiler over the checked-in DSL
    and confirm the connector reproduces the golden. This proves the committed
    `production/` fixtures are a faithful `compile.py` output, not hand-authored."""
    pytest.importorskip("ai.chronon")
    test_resources_dir = _resources_dir(pytestconfig)

    repo = tmp_path / "repo"
    shutil.copytree(test_resources_dir / "chronon_repo", repo)
    production = _compile_chronon_repo(repo)

    output_path = tmp_path / "compiled_mces.json"
    _run_pipeline(
        _base_config(
            str(production),
            enable_tag_extraction=True,
            enable_owner_extraction=True,
            owner_mappings=_OWNER_MAPPINGS,
        ),
        output_path,
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "zipline_mces_golden.json",
    )
