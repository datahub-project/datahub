"""
Integration tests for the dlt DataHub connector.

Uses pre-baked schema YAML fixtures that mimic real dlt pipeline state output,
so no dlt installation or running service is required for the test suite.

To regenerate golden files after intentional changes:
    pytest tests/integration/dlt/test_dlt.py --update-golden-files
"""

from __future__ import annotations

import pathlib
from datetime import datetime, timezone
from typing import Any, Dict
from unittest.mock import patch

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.dlt.data_classes import DltLoadInfo
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

    # Configure a source_table_dataset_urns inlet for players_games so that
    # column-level lineage (fineGrainedLineages) is exercised in the golden file.
    # A single inlet + single outlet triggers the 1:1 CLL code path.
    _run_dlt_pipeline(
        pipelines_dir=str(TESTDATA_DIR),
        output_file=output_file,
        extra_config={
            "source_table_dataset_urns": {
                "chess_pipeline": {
                    "players_games": [
                        "urn:li:dataset:(urn:li:dataPlatform:chess_api,players_games,DEV)"
                    ]
                }
            }
        },
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

    # All postgres outlets must follow the 3-part chess.chess_data.<table> format.
    # The 3-part prefix (chess. = database, chess_data = dataset_name) confirms that
    # destination_platform_map.postgres.database is correctly prepended — this is the
    # critical lineage-stitching format that must match the postgres connector's URNs.
    for urn in outlet_urns:
        assert "dataPlatform:postgres" in urn, (
            f"Expected postgres platform in URN: {urn}"
        )
        assert "chess.chess_data." in urn, (
            f"Expected 3-part chess.chess_data.<table> format when database='chess' is "
            f"set in destination_platform_map, got: {urn}"
        )
        assert "DEV" in urn, f"Expected DEV env in URN: {urn}"


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


@pytest.mark.integration
def test_dlt_outlet_urns_fallback_when_destination_not_in_map(
    tmp_path: pathlib.Path,
) -> None:
    """
    When a pipeline's destination is not present in destination_platform_map,
    outlet URNs are constructed using dataset_name.table_name (2-part) rather
    than database.dataset_name.table_name (3-part), falling back to the
    connector's global env setting.
    """
    import json

    output_file = str(tmp_path / "dlt_output.json")
    _run_dlt_pipeline(
        pipelines_dir=str(TESTDATA_DIR),
        output_file=output_file,
        extra_config={"destination_platform_map": {}},
    )

    with open(output_file) as f:
        records = json.load(f)

    outlet_urns = []
    for record in records:
        aspect_json = record.get("aspect", {}).get("json", {})
        outlet_urns.extend(aspect_json.get("outputDatasets", []))

    assert len(outlet_urns) > 0, (
        "Expected outlet URNs even without destination_platform_map"
    )

    for urn in outlet_urns:
        # Fallback path: no database prefix → 2-part name (chess_data.table_name)
        assert "chess_data." in urn, (
            f"Expected 2-part dataset_name.table_name in fallback URN, got: {urn}"
        )
        # Must NOT contain the 3-part prefix (database.chess_data.table_name)
        assert "chess.chess_data" not in urn, (
            f"3-part URN should only appear when database is set in destination_platform_map, got: {urn}"
        )


@pytest.mark.integration
def test_dlt_include_lineage_false(tmp_path: pathlib.Path) -> None:
    """
    When include_lineage=False, all dataJobInputOutput aspects must have
    empty outputDatasets — no outlet URNs should be constructed.
    """
    import json

    output_file = str(tmp_path / "dlt_output.json")
    _run_dlt_pipeline(
        pipelines_dir=str(TESTDATA_DIR),
        output_file=output_file,
        extra_config={"include_lineage": False},
    )

    with open(output_file) as f:
        records = json.load(f)

    # Guard: confirm DataFlow/DataJob entities are present so the loop below is not vacuous.
    assert any(r.get("entityType") in ("dataFlow", "dataJob") for r in records), (
        "Expected DataFlow/DataJob entities to be present even when include_lineage=False"
    )

    for record in records:
        aspect_json = record.get("aspect", {}).get("json", {})
        output_datasets = aspect_json.get("outputDatasets", [])
        assert output_datasets == [], (
            f"Expected no outputDatasets when include_lineage=False, "
            f"but found {output_datasets} on {record.get('entityUrn')}"
        )


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


@time_machine.travel(FROZEN_TIME)
@pytest.mark.integration
def test_dlt_run_history_emits_dataprocess_instances(tmp_path: pathlib.Path) -> None:
    """
    When include_run_history=True and get_run_history returns load records,
    dataProcessInstance entities are emitted end-to-end through the full
    Pipeline.create() → file sink path.

    get_run_history is mocked so no destination credentials or live database
    are required. This tests the DPI serialization path, not the SQL query path
    (which is covered by DltClient.get_run_history's own error handling).
    """
    import json

    fake_loads = [
        DltLoadInfo(
            load_id="1000000001.0",
            schema_name="chess_pipeline",
            status=0,  # success
            inserted_at=datetime(2026, 1, 14, 10, 0, 0, tzinfo=timezone.utc),
            schema_version_hash="abc",
        ),
        DltLoadInfo(
            load_id="1000000002.0",
            schema_name="chess_pipeline",
            status=1,  # failure/in-progress
            inserted_at=datetime(2026, 1, 14, 11, 0, 0, tzinfo=timezone.utc),
            schema_version_hash="def",
        ),
    ]

    output_file = str(tmp_path / "dlt_dpi_output.json")

    with patch(
        "datahub.ingestion.source.dlt.dlt_client.DltClient.get_run_history",
        return_value=fake_loads,
    ):
        _run_dlt_pipeline(
            pipelines_dir=str(TESTDATA_DIR),
            output_file=output_file,
            extra_config={"include_run_history": True},
        )

    with open(output_file) as f:
        records = json.load(f)

    # DataProcessInstance entities must be present
    dpi_records = [r for r in records if r.get("entityType") == "dataProcessInstance"]
    assert len(dpi_records) > 0, (
        "Expected dataProcessInstance entities when include_run_history=True"
    )

    # Two loads → two distinct DPI URNs (URNs are hashed, not raw load_ids)
    dpi_urns = {r.get("entityUrn") for r in dpi_records}
    assert len(dpi_urns) == 2, (
        f"Expected 2 distinct DPI URNs (one per load), got {len(dpi_urns)}: {dpi_urns}"
    )

    # The two loads have different statuses (0=success, 1=failure).
    # Verify that two distinct run result types are emitted — if the status mapping
    # were broken and both loads produced the same result, this would catch it.
    run_results = set()
    for record in records:
        if record.get("entityType") != "dataProcessInstance":
            continue
        aspect = record.get("aspect", {}).get("json", {})
        result = aspect.get("result", {})
        if result:
            run_results.add(str(result.get("type", result)))

    assert len(run_results) == 2, (
        f"Expected 2 distinct run result types from the two loads (success + failure), "
        f"got {len(run_results)}: {run_results}. "
        f"This likely means the _DLT_LOAD_STATUS_MAP is not being applied correctly."
    )
