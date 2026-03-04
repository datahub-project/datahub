"""
Integration tests for the dlt DataHub connector.

Uses pre-baked schema YAML fixtures that mimic real dlt pipeline state output,
so no dlt installation or running service is required for the test suite.

To regenerate golden files after intentional changes:
    pytest tests/integration/dlt/test_dlt.py --update-golden-files
"""

from __future__ import annotations

import pathlib
from typing import Any, Dict

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2026-01-15 10:00:00+00:00"

RESOURCES_DIR = pathlib.Path(__file__).parent
TESTDATA_DIR = RESOURCES_DIR / "testdata"
GOLDEN_DIR = RESOURCES_DIR / "golden"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_dlt_pipeline(
    pipelines_dir: str,
    output_file: str,
    extra_config: Dict[str, Any] | None = None,
) -> None:
    """Run the dlt connector via Pipeline.create() and write output to a file."""
    config: Dict[str, Any] = {
        "pipelines_dir": pipelines_dir,
        "pipeline_pattern": {"allow": [".*"]},
        "include_lineage": True,
        "destination_platform_map": {
            "postgres": {
                "database": "chess",
                "platform_instance": None,
                "env": "DEV",
            }
        },
        "include_run_history": False,
        "env": "DEV",
    }
    if extra_config:
        config.update(extra_config)

    pipeline = Pipeline.create(
        {
            "run_id": "dlt-integration-test",
            "source": {
                "type": "dlt",
                "config": config,
            },
            "sink": {
                "type": "file",
                "config": {"filename": output_file},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@time_machine.travel(FROZEN_TIME)
@pytest.mark.integration
def test_dlt_ingest_golden(pytestconfig: pytest.Config, tmp_path: pathlib.Path) -> None:
    """
    dlt connector output matches the golden file.

    Validates:
    - One DataFlow emitted for chess_pipeline
    - DataJobs for all user tables (players_games, players_profiles, etc.)
    - Nested child table DataJob (players_games__opening) with parent_table property
    - dlt system tables (_dlt_loads, _dlt_version, _dlt_pipeline_state) NOT emitted
    - Outlet lineage URNs constructed for postgres destination
    """
    output_file = str(tmp_path / "dlt_output.json")

    _run_dlt_pipeline(
        pipelines_dir=str(TESTDATA_DIR),
        output_file=output_file,
    )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=GOLDEN_DIR / "dlt_golden.json",
        ignore_paths=[
            # pipelines_dir varies between machines
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['pipelines_dir'\]",
            r"root\[\d+\]\['aspect'\]\['json'\]\['customProperties'\]\['working_dir'\]",
        ],
    )


@time_machine.travel(FROZEN_TIME)
@pytest.mark.integration
def test_dlt_system_tables_excluded(tmp_path: pathlib.Path) -> None:
    """
    dlt system tables (_dlt_loads, _dlt_version, _dlt_pipeline_state) are
    never emitted as DataJob entities.
    """
    import json

    output_file = str(tmp_path / "dlt_output.json")
    _run_dlt_pipeline(
        pipelines_dir=str(TESTDATA_DIR),
        output_file=output_file,
    )

    with open(output_file) as f:
        records = json.load(f)

    # Collect all DataJob URNs from output
    datajob_urns = [
        r.get("entityUrn", "") for r in records if r.get("entityType") == "dataJob"
    ]

    system_tables = {"_dlt_loads", "_dlt_version", "_dlt_pipeline_state"}
    for system_table in system_tables:
        matching = [u for u in datajob_urns if system_table in u]
        assert not matching, (
            f"System table '{system_table}' should not be emitted as a DataJob, "
            f"but found URNs: {matching}"
        )


@time_machine.travel(FROZEN_TIME)
@pytest.mark.integration
def test_dlt_outlet_urns_match_postgres_format(tmp_path: pathlib.Path) -> None:
    """
    Outlet Dataset URNs emitted for postgres destination follow the format
    that the DataHub postgres connector produces: chess.chess_data.<table>.
    This validates lineage stitching will work when both connectors run.
    """
    import json

    output_file = str(tmp_path / "dlt_output.json")
    _run_dlt_pipeline(
        pipelines_dir=str(TESTDATA_DIR),
        output_file=output_file,
        extra_config={
            "destination_platform_map": {
                "postgres": {
                    "database": "chess",
                    "platform_instance": None,
                    "env": "DEV",
                }
            }
        },
    )

    with open(output_file) as f:
        records = json.load(f)

    # Collect all outlet dataset URNs from dataJobInputOutput aspects
    outlet_urns = []
    for record in records:
        aspect = record.get("aspect", {})
        aspect_json = aspect.get("json", {})
        output_datasets = aspect_json.get("outputDatasets", [])
        outlet_urns.extend(output_datasets)

    assert len(outlet_urns) > 0, "Expected at least one outlet Dataset URN"

    # All postgres outlets must follow chess.chess_data.<table> format
    for urn in outlet_urns:
        assert "dataPlatform:postgres" in urn, (
            f"Expected postgres platform in URN: {urn}"
        )
        assert "chess_data" in urn, (
            f"Expected chess_data (dlt dataset_name) in URN: {urn}"
        )
        assert "DEV" in urn, f"Expected DEV env in URN: {urn}"


@time_machine.travel(FROZEN_TIME)
@pytest.mark.integration
def test_dlt_nested_table_has_parent_property(tmp_path: pathlib.Path) -> None:
    """
    players_games__opening is a dlt nested child table.
    Its DataJob must carry parent_table='players_games' in customProperties.
    """
    import json

    output_file = str(tmp_path / "dlt_output.json")
    _run_dlt_pipeline(
        pipelines_dir=str(TESTDATA_DIR),
        output_file=output_file,
    )

    with open(output_file) as f:
        records = json.load(f)

    # Find aspects for the nested table DataJob
    nested_table = "players_games__opening"
    found_parent = False
    for record in records:
        urn = record.get("entityUrn", "")
        if nested_table not in urn:
            continue
        aspect = record.get("aspect", {})
        custom_props = aspect.get("json", {}).get("customProperties", {})
        if custom_props.get("parent_table") == "players_games":
            found_parent = True
            break

    assert found_parent, (
        f"'{nested_table}' DataJob must have parent_table='players_games' "
        "in customProperties"
    )


@time_machine.travel(FROZEN_TIME)
@pytest.mark.integration
def test_dlt_pipeline_pattern_filter(tmp_path: pathlib.Path) -> None:
    """
    When pipeline_pattern denies chess_pipeline, no DataFlow or DataJob is emitted.
    """
    import json

    output_file = str(tmp_path / "dlt_output.json")
    _run_dlt_pipeline(
        pipelines_dir=str(TESTDATA_DIR),
        output_file=output_file,
        extra_config={
            "pipeline_pattern": {"allow": [], "deny": ["chess_pipeline"]},
        },
    )

    with open(output_file) as f:
        records = json.load(f)

    flow_or_job = [r for r in records if r.get("entityType") in ("dataFlow", "dataJob")]
    assert len(flow_or_job) == 0, (
        f"Expected no DataFlow/DataJob when pipeline is filtered, got {len(flow_or_job)}"
    )
