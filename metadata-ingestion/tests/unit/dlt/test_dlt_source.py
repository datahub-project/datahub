"""
Unit tests for the dlt DataHub connector.

Tests verify business logic using mocked dlt SDK calls — no pipeline execution,
no network calls, no real filesystem writes.

Standards compliance:
  - time-machine for time mocking (not freezegun)
  - pytest (not unittest.TestCase)
  - assert statements (not self.assertEqual)
  - All imports at top of file
  - No trivial tests (no testing config defaults or platform name)
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

import time_machine

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dlt.data_classes import (
    DltColumnInfo,
    DltLoadInfo,
    DltPipelineInfo,
    DltSchemaInfo,
    DltTableInfo,
)
from datahub.ingestion.source.dlt.dlt import DltSource
from datahub.ingestion.source.dlt.dlt_client import DltClient

# Frozen timestamp used by run history unit tests to ensure deterministic DPI timestamps.
FROZEN_TIME = "2026-02-24 12:00:00+00:00"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pipeline_info(
    pipeline_name: str = "chess_pipeline",
    destination: str = "postgres",
    dataset_name: str = "chess_data",
    tables: List[DltTableInfo] | None = None,
) -> DltPipelineInfo:
    if tables is None:
        tables = [
            DltTableInfo(
                table_name="players_games",
                write_disposition="append",
                parent_table=None,
                columns=[
                    DltColumnInfo("url", "string", True, False, False),
                    DltColumnInfo("time_class", "string", True, False, False),
                    DltColumnInfo("_dlt_id", "string", False, False, True),
                    DltColumnInfo("_dlt_load_id", "string", False, False, True),
                ],
                resource_name="players_games",
            ),
            DltTableInfo(
                table_name="players_games__opening",
                write_disposition="append",
                parent_table="players_games",
                columns=[
                    DltColumnInfo("eco", "string", True, False, False),
                    DltColumnInfo("name", "string", True, False, False),
                    DltColumnInfo("_dlt_id", "string", False, False, True),
                    DltColumnInfo("_dlt_parent_id", "string", False, False, True),
                ],
                resource_name=None,
            ),
        ]
    return DltPipelineInfo(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        working_dir=f"/tmp/test_pipelines/{pipeline_name}",
        pipelines_dir="/tmp/test_pipelines",
        schemas=[
            DltSchemaInfo(
                schema_name=pipeline_name,
                version=1,
                version_hash="abc123",
                tables=tables,
            )
        ],
    )


def _make_source(extra_config: dict | None = None) -> DltSource:
    config_dict = {
        "pipelines_dir": "/tmp/test_pipelines",
        "env": "DEV",
    }
    if extra_config:
        config_dict.update(extra_config)
    ctx = PipelineContext(run_id="test-run")
    return DltSource.create(config_dict, ctx)


# ---------------------------------------------------------------------------
# Test 1: Schema → DataJob mapping
# ---------------------------------------------------------------------------


def test_pipeline_schema_to_datajob_mapping() -> None:
    """Each table in the schema produces exactly one DataJob with correct flow_urn."""
    pipeline_info = _make_pipeline_info()
    source = _make_source()

    with (
        patch.object(
            source.client, "list_pipeline_names", return_value=["chess_pipeline"]
        ),
        patch.object(source.client, "get_pipeline_info", return_value=pipeline_info),
    ):
        workunits = list(source.get_workunits())

    # SDK V2 emits multiple MCPs per entity (one per aspect).
    # Deduplicate on entityUrn to count distinct DataJob entities.
    datajob_entity_urns = {
        wu.metadata.entityUrn
        for wu in workunits
        if hasattr(wu, "metadata")
        and wu.metadata is not None
        and hasattr(wu.metadata, "entityUrn")
        and wu.metadata.entityUrn is not None
        and "dataJob" in wu.metadata.entityUrn
    }

    # One DataJob per table in the schema (2 tables)
    assert len(datajob_entity_urns) == 2
    # DataJobs must reference the parent DataFlow
    for urn in datajob_entity_urns:
        assert "chess_pipeline" in urn


# ---------------------------------------------------------------------------
# Test 2: Nested table parent relationship
# ---------------------------------------------------------------------------


def test_nested_table_parent_relationship() -> None:
    """players_games__opening DataJob has parent_table in custom properties."""
    pipeline_info = _make_pipeline_info()
    source = _make_source()

    with (
        patch.object(
            source.client, "list_pipeline_names", return_value=["chess_pipeline"]
        ),
        patch.object(source.client, "get_pipeline_info", return_value=pipeline_info),
    ):
        workunits = list(source.get_workunits())

    # Find the workunit for the nested table
    nested_wus = [wu for wu in workunits if wu.id and "players_games__opening" in wu.id]
    assert len(nested_wus) > 0

    # At least one workunit for this job should carry dataJobInfo with custom_properties
    props_found = False
    for wu in nested_wus:
        if hasattr(wu, "metadata") and wu.metadata:
            aspect = getattr(wu.metadata, "aspect", None)
            if aspect and hasattr(aspect, "customProperties"):
                props = aspect.customProperties or {}
                if props.get("parent_table") == "players_games":
                    props_found = True
                    break
    # The nested table must have parent_table in its custom properties
    assert props_found, "players_games__opening must have parent_table='players_games'"


# ---------------------------------------------------------------------------
# Test 3: Postgres outlet URN construction
# ---------------------------------------------------------------------------


def test_destination_urn_construction_postgres() -> None:
    """Outlet Dataset URN for Postgres matches expected format for lineage stitching."""
    source = _make_source(
        extra_config={
            "include_lineage": True,
            "destination_platform_map": {
                "postgres": {"platform_instance": None, "env": "DEV"}
            },
        }
    )
    pipeline_info = _make_pipeline_info(
        destination="postgres", dataset_name="chess_data"
    )
    table = pipeline_info.schemas[0].tables[0]  # players_games

    outlets = source._build_outlet_urns(pipeline_info, table)

    assert len(outlets) == 1
    urn_str = str(outlets[0])
    assert "dataPlatform:postgres" in urn_str
    assert "chess_data.players_games" in urn_str
    assert "DEV" in urn_str


# ---------------------------------------------------------------------------
# Test 4: BigQuery outlet URN construction
# ---------------------------------------------------------------------------


def test_destination_urn_construction_bigquery() -> None:
    """Outlet Dataset URN for BigQuery includes project-level platform_instance."""
    source = _make_source(
        extra_config={
            "include_lineage": True,
            "destination_platform_map": {
                "bigquery": {"platform_instance": "my-gcp-project", "env": "PROD"}
            },
        }
    )
    pipeline_info = _make_pipeline_info(
        destination="bigquery", dataset_name="chess_data"
    )
    table = pipeline_info.schemas[0].tables[0]

    outlets = source._build_outlet_urns(pipeline_info, table)

    assert len(outlets) == 1
    urn_str = str(outlets[0])
    assert "dataPlatform:bigquery" in urn_str
    assert "chess_data.players_games" in urn_str
    assert "PROD" in urn_str


# ---------------------------------------------------------------------------
# Test 5: dlt system columns excluded from outlet URNs / not emitted as schema fields
# ---------------------------------------------------------------------------


def test_system_columns_excluded_from_column_level_lineage() -> None:
    """
    When a table contains only dlt system columns (_dlt_id, _dlt_load_id, etc.),
    _build_fine_grained_lineages must return [] — no CLL entries should be produced
    because system columns have no corresponding columns in the source system.

    This is the meaningful edge case: even with one inlet and one outlet, a table
    with no user columns produces no column-level lineage.
    """
    system_only_table = DltTableInfo(
        table_name="players_games",
        write_disposition="append",
        parent_table=None,
        columns=[
            DltColumnInfo("_dlt_id", "string", False, False, True),
            DltColumnInfo("_dlt_load_id", "string", False, False, True),
            DltColumnInfo("_dlt_parent_id", "string", False, False, True),
        ],
        resource_name=None,
    )
    source = _make_source(
        extra_config={
            "include_lineage": True,
            "destination_platform_map": {
                "postgres": {"platform_instance": None, "env": "DEV"}
            },
            "source_table_dataset_urns": {
                "chess_pipeline": {
                    "players_games": [
                        "urn:li:dataset:(urn:li:dataPlatform:postgres,src.players_games,DEV)"
                    ]
                }
            },
        }
    )
    pipeline_info = _make_pipeline_info(tables=[system_only_table])

    inlets = source._build_inlet_urns("chess_pipeline", "players_games")
    outlets = source._build_outlet_urns(pipeline_info, system_only_table)
    fgl = source._build_fine_grained_lineages(system_only_table, inlets, outlets)

    assert len(inlets) == 1, "Expected one inlet from source_table_dataset_urns"
    assert len(outlets) == 1, "Expected one outlet from destination_platform_map"
    assert fgl == [], (
        "Tables with only dlt system columns must produce no column-level lineage — "
        "system columns (_dlt_id, etc.) have no corresponding source columns"
    )


# ---------------------------------------------------------------------------
# Test 6: Run history emission — status=0 produces DataProcessInstance
# ---------------------------------------------------------------------------


@time_machine.travel(FROZEN_TIME)
def test_run_history_emits_dataprocess_instance() -> None:
    """
    When get_run_history returns a load with status=0, _emit_run_history
    must yield MetadataWorkUnit records containing DataProcessInstance aspects.
    A load with status=1 must still yield a DPI (marked FAILURE).
    """
    pipeline_info = _make_pipeline_info()
    source = _make_source(extra_config={"include_run_history": True})

    load_success = DltLoadInfo(
        load_id="1234567890.0",
        schema_name="chess_pipeline",
        status=0,
        inserted_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        schema_version_hash="abc",
    )
    load_failure = DltLoadInfo(
        load_id="1234567891.0",
        schema_name="chess_pipeline",
        status=1,
        inserted_at=datetime(2026, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
        schema_version_hash="def",
    )

    with patch.object(
        source.client, "get_run_history", return_value=[load_success, load_failure]
    ):
        workunits = list(source._emit_run_history(pipeline_info))

    # Both loads must produce workunits (generate_mcp + start_event + end_event per load)
    assert len(workunits) > 0

    # All workunits must reference DataProcessInstance entities
    entity_types = {
        wu.metadata.entityType
        for wu in workunits
        if hasattr(wu, "metadata")
        and wu.metadata is not None
        and hasattr(wu.metadata, "entityType")
        and wu.metadata.entityType is not None
    }
    assert "dataProcessInstance" in entity_types, (
        "Expected DataProcessInstance entities in run history workunits"
    )

    # Report counter must reflect both loads
    assert source.report.run_history_loaded == 2


# ---------------------------------------------------------------------------
# Test 7: Pipeline pattern filtering
# ---------------------------------------------------------------------------


def test_pipeline_pattern_filtering() -> None:
    """Pipelines not matching pipeline_pattern are excluded and reported."""
    source = _make_source(
        extra_config={
            "pipeline_pattern": {"allow": ["^prod_.*"], "deny": []},
        }
    )

    with patch.object(
        source.client,
        "list_pipeline_names",
        return_value=["prod_chess", "dev_chess", "test_pipeline"],
    ):
        get_pipeline_mock = MagicMock(return_value=_make_pipeline_info("prod_chess"))
        with patch.object(source.client, "get_pipeline_info", get_pipeline_mock):
            list(source.get_workunits())

    # Only prod_chess should have been processed
    assert get_pipeline_mock.call_count == 1
    assert get_pipeline_mock.call_args[0][0] == "prod_chess"

    # dev_chess and test_pipeline should be in the filtered list
    filtered = list(source.report.pipelines_filtered)
    assert "dev_chess" in filtered
    assert "test_pipeline" in filtered


# ---------------------------------------------------------------------------
# Test 8: Filesystem fallback when dlt package unavailable
# ---------------------------------------------------------------------------


def test_missing_dlt_package_falls_back_to_filesystem(tmp_path: Path) -> None:
    """
    DltClient uses filesystem YAML reading when dlt is not installed.
    Schema info is correctly extracted from the YAML file.
    """
    # Create a minimal schema YAML in the expected location
    pipeline_name = "test_pipeline"
    schemas_dir = tmp_path / pipeline_name / "schemas"
    schemas_dir.mkdir(parents=True)
    schema_yaml = schemas_dir / f"{pipeline_name}.schema.yaml"
    schema_yaml.write_text(
        """
name: test_pipeline
version: 1
version_hash: "xyz789"
tables:
  users:
    write_disposition: append
    columns:
      id:
        name: id
        data_type: bigint
        nullable: false
        primary_key: true
      name:
        name: name
        data_type: text
        nullable: true
"""
    )

    client = DltClient(pipelines_dir=str(tmp_path))

    with patch.object(client, "_dlt_available", False):
        info = client.get_pipeline_info(pipeline_name)

    assert info is not None
    assert info.pipeline_name == pipeline_name
    assert len(info.schemas) == 1
    assert info.schemas[0].schema_name == pipeline_name
    assert len(info.schemas[0].tables) == 1
    assert info.schemas[0].tables[0].table_name == "users"
    assert len(info.schemas[0].tables[0].columns) == 2


# ---------------------------------------------------------------------------
# Test 9: Write disposition in custom properties
# ---------------------------------------------------------------------------


def test_merge_write_disposition_emitted_as_custom_property() -> None:
    """write_disposition appears in DataJob custom properties."""
    merge_table = DltTableInfo(
        table_name="customers",
        write_disposition="merge",
        parent_table=None,
        columns=[DltColumnInfo("id", "bigint", False, True, False)],
        resource_name="customers",
    )
    pipeline_info = _make_pipeline_info(tables=[merge_table])
    source = _make_source()

    # Build a DataJob directly and check its custom properties

    dataflow = source._build_dataflow(pipeline_info)
    datajob = source._build_datajob(
        pipeline_info, merge_table, "customers", dataflow, inlets=[], outlets=[]
    )

    # Custom properties are accessible via the SDK V2 object
    found_write_disposition = False
    for mcp in datajob.as_mcps():
        aspect = getattr(mcp, "aspect", None)
        if aspect and hasattr(aspect, "customProperties"):
            props = aspect.customProperties or {}
            if props.get("write_disposition") == "merge":
                found_write_disposition = True
                break

    assert found_write_disposition, (
        "write_disposition='merge' must appear in customProperties"
    )


# ---------------------------------------------------------------------------
# Test 10: Column-level lineage for 1:1 sql_database pipelines
# ---------------------------------------------------------------------------


def test_column_level_lineage_emitted_for_single_inlet_outlet() -> None:
    """
    When there is exactly one inlet and one outlet, _build_fine_grained_lineages
    must emit one FineGrainedLineageClass per user column, mapping the same
    column name in the inlet dataset to the outlet dataset.
    dlt system columns (_dlt_id, _dlt_load_id, etc.) must be excluded.
    """
    from datahub.metadata.schema_classes import FineGrainedLineageClass

    source = _make_source(
        extra_config={
            "include_lineage": True,
            "destination_platform_map": {
                "postgres": {"platform_instance": None, "env": "DEV"}
            },
            "source_table_dataset_urns": {
                "chess_pipeline": {
                    "players_games": [
                        "urn:li:dataset:(urn:li:dataPlatform:postgres,src.public.players_games,DEV)"
                    ]
                }
            },
        }
    )

    table = DltTableInfo(
        table_name="players_games",
        write_disposition="append",
        parent_table=None,
        columns=[
            DltColumnInfo("url", "string", True, False, False),
            DltColumnInfo("time_class", "string", True, False, False),
            DltColumnInfo("_dlt_id", "string", False, False, True),
            DltColumnInfo("_dlt_load_id", "string", False, False, True),
        ],
        resource_name="players_games",
    )
    pipeline_info = _make_pipeline_info(tables=[table])

    inlets = source._build_inlet_urns("chess_pipeline", "players_games")
    outlets = source._build_outlet_urns(pipeline_info, table)
    fgl = source._build_fine_grained_lineages(table, inlets, outlets)

    assert len(inlets) == 1
    assert len(outlets) == 1
    # Two user columns → two FineGrainedLineage entries
    assert len(fgl) == 2

    def _extract_field_name(schema_field_urn: str) -> str:
        # Schema field URN format: urn:li:schemaField:(urn:li:dataset:(...),fieldPath)
        # The field name is the last comma-separated token before the closing paren.
        return schema_field_urn.rsplit(",", 1)[-1].rstrip(")")

    col_names = set()
    for entry in fgl:
        assert isinstance(entry, FineGrainedLineageClass)
        assert len(entry.upstreams) == 1
        assert len(entry.downstreams) == 1
        upstream_field = _extract_field_name(entry.upstreams[0])
        downstream_field = _extract_field_name(entry.downstreams[0])
        # Same column name must appear in both inlet and outlet field URNs
        assert upstream_field == downstream_field, (
            f"Expected same column name in upstream/downstream: {upstream_field} != {downstream_field}"
        )
        col_names.add(upstream_field)

    assert col_names == {"url", "time_class"}, (
        "Only user columns should appear in CLL — _dlt_id and _dlt_load_id must be excluded"
    )


# ---------------------------------------------------------------------------
# Test 11: Empty pipelines_dir emits no workunits
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Test 12: _emit_dpi_for_load maps status=0 → SUCCESS, others → FAILURE
# ---------------------------------------------------------------------------


@time_machine.travel(FROZEN_TIME)
def test_emit_dpi_for_load_status_mapping() -> None:
    """
    _emit_dpi_for_load must produce a DPI with status SUCCESS for status=0
    and FAILURE for status=1. This verifies the _DLT_LOAD_STATUS_MAP is
    actually applied in the serialization path, not just defined as a constant.
    """
    from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult

    pipeline_info = _make_pipeline_info()
    source = _make_source()

    def _get_result_from_load(status: int) -> str:
        """Run _emit_dpi_for_load and extract the run result string from the end event MCP."""
        load = DltLoadInfo(
            load_id=f"load_{status}",
            schema_name="chess_pipeline",
            status=status,
            inserted_at=datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc),
            schema_version_hash="abc",
        )
        workunits = list(source._emit_dpi_for_load(pipeline_info, load))
        # The end_event_mcp produces a dataProcessRunEvent with a result field
        for wu in workunits:
            if not (hasattr(wu, "metadata") and wu.metadata is not None):
                continue
            aspect = getattr(wu.metadata, "aspect", None)
            if aspect is None:
                continue
            result = getattr(aspect, "result", None)
            if result is not None:
                return str(result)
        return ""

    success_result = _get_result_from_load(0)
    failure_result = _get_result_from_load(1)

    assert "SUCCESS" in success_result.upper() or success_result == str(
        InstanceRunResult.SUCCESS
    ), f"Expected SUCCESS for status=0, got: {success_result}"
    assert "FAILURE" in failure_result.upper() or failure_result == str(
        InstanceRunResult.FAILURE
    ), f"Expected FAILURE for status=1, got: {failure_result}"


# ---------------------------------------------------------------------------
# Test 13: Empty pipelines_dir emits no workunits
# ---------------------------------------------------------------------------


def test_empty_pipeline_dir_emits_no_workunits() -> None:
    """An empty or missing pipelines_dir produces no workunits (no crash)."""
    source = _make_source(extra_config={"pipelines_dir": "/nonexistent/dir/xyz"})

    with patch.object(source.client, "list_pipeline_names", return_value=[]):
        workunits = list(source.get_workunits())

    # Should produce zero workunits (or only stale entity removal workunits)
    pipeline_workunits = [
        wu for wu in workunits if wu.id and ("dataFlow" in wu.id or "dataJob" in wu.id)
    ]
    assert len(pipeline_workunits) == 0
