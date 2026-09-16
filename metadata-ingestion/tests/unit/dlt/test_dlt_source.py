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

import pytest
import time_machine
from pydantic import ValidationError

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import SourceCapability
from datahub.ingestion.source.dlt.config import DltSourceConfig
from datahub.ingestion.source.dlt.data_classes import (
    DltColumnInfo,
    DltLoadInfo,
    DltLoadStatus,
    DltPipelineInfo,
    DltSchemaInfo,
    DltTableInfo,
)
from datahub.ingestion.source.dlt.dlt import DltSource
from datahub.ingestion.source.dlt.dlt_client import DltClient
from datahub.ingestion.source.dlt.dlt_report import DltSourceReport
from datahub.metadata.urns import DataFlowUrn

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
    # No database configured → 2-part path only; 3-part prefix must be absent
    assert "chess.chess_data" not in urn_str, (
        "Expected 2-part dataset_name.table URN when database= is not configured"
    )


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
        status=DltLoadStatus.LOADED,
        inserted_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        schema_version_hash="abc",
    )
    load_failure = DltLoadInfo(
        load_id="1234567891.0",
        schema_name="chess_pipeline",
        status=DltLoadStatus.UNKNOWN,
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
# Test 11: Run history — query failure (None return) increments error counter
# ---------------------------------------------------------------------------


def test_emit_run_history_with_query_failure_increments_error_count() -> None:
    """
    When get_run_history returns None (SQL exception or credential failure),
    _emit_run_history must:
    - Emit zero workunits
    - Call report_run_history_error() once
    - NOT call report_run_history_loaded()
    """
    pipeline_info = _make_pipeline_info()
    source = _make_source(extra_config={"include_run_history": True})

    with patch.object(source.client, "get_run_history", return_value=None):
        workunits = list(source._emit_run_history(pipeline_info))

    assert len(workunits) == 0, "Expected no workunits on hard failure"
    assert source.report.run_history_errors == 1, (
        "Expected run_history_errors to be incremented on hard failure"
    )
    assert source.report.run_history_loaded == 0, (
        "Expected run_history_loaded to remain 0 on hard failure"
    )


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

    report = DltSourceReport()
    client = DltClient(pipelines_dir=str(tmp_path), report=report)

    with patch.object(client, "_dlt_available", False):
        info = client.get_pipeline_info(pipeline_name)

    assert info is not None
    assert info.pipeline_name == pipeline_name
    assert len(info.schemas) == 1
    assert info.schemas[0].schema_name == pipeline_name
    assert len(info.schemas[0].tables) == 1
    assert info.schemas[0].tables[0].table_name == "users"
    assert len(info.schemas[0].tables[0].columns) == 2
    # Confirm no silent schema parse errors occurred during the filesystem fallback
    assert report.schema_read_errors == 0


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


def test_coerce_write_disposition_known_values_pass_through() -> None:
    """Guards against typing.get_args(DltWriteDisposition) returning an empty
    tuple — which would silently coerce every input (including legitimate
    values) to 'append' via the unknown-value fallback. The test_dlt golden
    flow uses _parse_table directly, bypassing _coerce_write_disposition, so
    this is the only direct test of the coercion contract.
    """
    from datahub.ingestion.source.dlt.dlt_client import _coerce_write_disposition

    # Known values must round-trip unchanged.
    assert _coerce_write_disposition("append") == "append"
    assert _coerce_write_disposition("replace") == "replace"
    assert _coerce_write_disposition("merge") == "merge"
    # Unknown / missing values fall back to the dlt default.
    assert _coerce_write_disposition("garbage") == "append"
    assert _coerce_write_disposition(None) == "append"
    assert _coerce_write_disposition(42) == "append"


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
        assert entry.upstreams is not None and entry.downstreams is not None
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
    flow_urn = DataFlowUrn.create_from_ids(
        orchestrator=source.platform,
        flow_id=pipeline_info.pipeline_name,
        env=source.config.env,
        platform_instance=source.config.platform_instance,
    )

    def _get_result_from_load(status: DltLoadStatus) -> str:
        """Run _emit_dpi_for_load and extract the run result string from the end event MCP."""
        load = DltLoadInfo(
            load_id=f"load_{int(status)}",
            schema_name="chess_pipeline",
            status=status,
            inserted_at=datetime(2026, 2, 24, 12, 0, 0, tzinfo=timezone.utc),
            schema_version_hash="abc",
        )
        workunits = list(source._emit_dpi_for_load(pipeline_info, load, flow_urn))
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

    success_result = _get_result_from_load(DltLoadStatus.LOADED)
    failure_result = _get_result_from_load(DltLoadStatus.UNKNOWN)

    assert "SUCCESS" in success_result.upper() or success_result == str(
        InstanceRunResult.SUCCESS
    ), f"Expected SUCCESS for status=LOADED, got: {success_result}"
    assert "FAILURE" in failure_result.upper() or failure_result == str(
        InstanceRunResult.FAILURE
    ), f"Expected FAILURE for status=UNKNOWN, got: {failure_result}"


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


# ---------------------------------------------------------------------------
# Test 14: test_connection — pipelines_dir validation (H6)
# ---------------------------------------------------------------------------


def test_test_connection_missing_pipelines_dir(tmp_path: Path) -> None:
    """test_connection fails when pipelines_dir does not exist."""
    nonexistent = tmp_path / "does-not-exist"
    report = DltSource.test_connection(
        {"pipelines_dir": str(nonexistent), "env": "DEV"}
    )
    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    # Failure reason should mention the missing path so the operator can fix it
    assert "does not exist" in (report.basic_connectivity.failure_reason or "")


def test_test_connection_pipelines_dir_is_a_file(tmp_path: Path) -> None:
    """test_connection fails when pipelines_dir points to a file, not a directory."""
    file_path = tmp_path / "not_a_dir"
    file_path.write_text("oops")
    report = DltSource.test_connection({"pipelines_dir": str(file_path), "env": "DEV"})
    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert "not a directory" in (report.basic_connectivity.failure_reason or "")


def test_test_connection_valid_dir_dlt_installed(tmp_path: Path) -> None:
    """Valid pipelines_dir + dlt installed → capable=True with no mitigation_message."""
    # Simulate dlt being importable (treat it as if installed even if it isn't).
    # Patch builtins.__import__ to return a stub for 'dlt'.
    real_import = __import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "dlt":
            return MagicMock(name="dlt-stub")
        return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

    with patch("builtins.__import__", side_effect=fake_import):
        report = DltSource.test_connection(
            {"pipelines_dir": str(tmp_path), "env": "DEV"}
        )
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    lineage_cap = report.capability_report.get(SourceCapability.LINEAGE_COARSE)
    assert lineage_cap is not None and lineage_cap.capable
    assert not lineage_cap.mitigation_message


def test_test_connection_valid_dir_dlt_missing(tmp_path: Path) -> None:
    """Valid pipelines_dir but dlt not installed → capable=True with mitigation_message."""
    real_import = __import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "dlt":
            raise ImportError("No module named 'dlt'")
        return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

    with patch("builtins.__import__", side_effect=fake_import):
        report = DltSource.test_connection(
            {"pipelines_dir": str(tmp_path), "env": "DEV"}
        )
    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    lineage_cap = report.capability_report.get(SourceCapability.LINEAGE_COARSE)
    assert lineage_cap is not None
    assert lineage_cap.capable  # filesystem fallback still works
    # mitigation_message should hint at installing dlt
    assert lineage_cap.mitigation_message is not None
    assert "dlt" in lineage_cap.mitigation_message.lower()


def test_test_connection_invalid_config_returns_failure() -> None:
    """A malformed config dict produces a non-capable basic_connectivity with the error type."""
    # Pass a wrong type for pipelines_dir to trigger pydantic validation
    report = DltSource.test_connection({"pipelines_dir": 123, "env": "DEV"})
    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    # Error message should include exception type so operator can distinguish
    # config error from filesystem issues.
    assert report.basic_connectivity.failure_reason is not None


# ---------------------------------------------------------------------------
# Test 15: SDK code path — _get_pipeline_info_via_sdk (H7)
# ---------------------------------------------------------------------------


def test_sdk_path_reads_pipeline_info_via_dlt_sdk(tmp_path: Path) -> None:
    """When dlt is available, get_pipeline_info uses the SDK and returns SDK-derived metadata."""
    pipeline_name = "chess_pipeline"

    # Build a fake dlt.Schema-like object whose .tables exposes a dict
    fake_schema = MagicMock()
    fake_schema.tables = {
        "users": {
            "write_disposition": "append",
            "columns": {
                "id": {"data_type": "bigint", "nullable": False, "primary_key": True},
                "name": {"data_type": "text", "nullable": True},
            },
        }
    }
    fake_schema.version = 3
    fake_schema.version_hash = "deadbeef"

    fake_pipeline = MagicMock()
    fake_pipeline.destination = MagicMock(destination_name="postgres")
    fake_pipeline.dataset_name = "public"
    fake_pipeline.working_dir = f"/tmp/wd/{pipeline_name}"
    fake_pipeline.schemas = {pipeline_name: fake_schema}

    fake_dlt = MagicMock()
    fake_dlt.pipeline.return_value = fake_pipeline

    report = DltSourceReport()
    client = DltClient(pipelines_dir=str(tmp_path), report=report)
    with (
        patch.object(client, "_dlt_available", True),
        patch.dict("sys.modules", {"dlt": fake_dlt}),
    ):
        info = client.get_pipeline_info(pipeline_name)

    assert info is not None
    assert info.destination == "postgres"
    assert info.dataset_name == "public"
    assert len(info.schemas) == 1
    assert info.schemas[0].schema_name == pipeline_name
    assert info.schemas[0].version == 3
    assert info.schemas[0].version_hash == "deadbeef"
    assert {t.table_name for t in info.schemas[0].tables} == {"users"}
    # No errors should be reported for the success path
    assert report.schema_read_errors == 0


def test_sdk_path_falls_back_to_filesystem_on_exception(tmp_path: Path) -> None:
    """SDK exception → falls back to filesystem AND emits report.warning + counter."""
    pipeline_name = "chess_pipeline"

    # Pre-populate a filesystem schema so the fallback has something to read
    schemas_dir = tmp_path / pipeline_name / "schemas"
    schemas_dir.mkdir(parents=True)
    (schemas_dir / f"{pipeline_name}.schema.yaml").write_text(
        "name: chess_pipeline\nversion: 1\nversion_hash: hash\ntables:\n"
        "  users:\n    write_disposition: append\n    columns:\n"
        "      id:\n        data_type: bigint\n        nullable: false\n"
    )

    # Make dlt.pipeline raise to trigger the except branch
    fake_dlt = MagicMock()
    fake_dlt.pipeline.side_effect = RuntimeError("simulated dlt SDK breakage")

    report = DltSourceReport()
    client = DltClient(pipelines_dir=str(tmp_path), report=report)
    with (
        patch.object(client, "_dlt_available", True),
        patch.dict("sys.modules", {"dlt": fake_dlt}),
    ):
        info = client.get_pipeline_info(pipeline_name)

    # Filesystem fallback succeeded
    assert info is not None
    assert len(info.schemas) == 1
    assert info.schemas[0].tables[0].table_name == "users"

    # Failure must be surfaced as a warning, not silently swallowed
    assert report.schema_read_errors == 1
    titles = [w.title for w in report.warnings]  # type: ignore[union-attr]
    assert any("dlt SDK attach failed" in (t or "") for t in titles), (
        f"Expected 'dlt SDK attach failed' warning, got titles: {titles}"
    )


# ---------------------------------------------------------------------------
# Test 16: _parse_load_row time-window filtering and protection (H8 + H2)
# ---------------------------------------------------------------------------


def test_parse_load_row_iso_string_inserted_at(tmp_path: Path) -> None:
    """ISO-string inserted_at values are parsed into tz-aware datetimes."""
    client = DltClient(pipelines_dir=str(tmp_path), report=DltSourceReport())
    row = ("load_42", "schema_a", 0, "2026-02-01T10:00:00+00:00", "vh")
    load = client._parse_load_row(row, start_time=None, end_time=None)
    assert load is not None
    assert load.inserted_at == datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc)


def test_parse_load_row_naive_datetime_coerced_to_utc(tmp_path: Path) -> None:
    """Naive datetimes are coerced to UTC so downstream comparisons don't fail."""
    client = DltClient(pipelines_dir=str(tmp_path), report=DltSourceReport())
    naive_ts = datetime(2026, 2, 1, 10, 0, 0)  # tzinfo is None
    row = ("load_42", "schema_a", 0, naive_ts, "vh")
    load = client._parse_load_row(row, start_time=None, end_time=None)
    assert load is not None
    assert load.inserted_at.tzinfo is not None


def test_parse_load_row_filters_outside_time_window(tmp_path: Path) -> None:
    """Rows older than start_time or newer than end_time are skipped (return None)."""
    client = DltClient(pipelines_dir=str(tmp_path), report=DltSourceReport())
    in_window = datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc)
    too_old = datetime(2025, 1, 1, tzinfo=timezone.utc)
    too_new = datetime(2027, 1, 1, tzinfo=timezone.utc)

    row_in = ("a", "s", 0, in_window, "vh")
    row_old = ("b", "s", 0, too_old, "vh")
    row_new = ("c", "s", 0, too_new, "vh")
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 12, 31, tzinfo=timezone.utc)

    assert client._parse_load_row(row_in, start_time=start, end_time=end) is not None
    assert client._parse_load_row(row_old, start_time=start, end_time=end) is None
    assert client._parse_load_row(row_new, start_time=start, end_time=end) is None


def test_parse_load_row_malformed_row_surfaces_warning(tmp_path: Path) -> None:
    """A short or unparseable row returns None and surfaces a warning to the report."""
    report = DltSourceReport()
    client = DltClient(pipelines_dir=str(tmp_path), report=report)
    short_row = ("only_two", "columns")  # 2 cols, expected 5

    load = client._parse_load_row(
        short_row, start_time=None, end_time=None, pipeline_name="chess_pipeline"
    )
    assert load is None
    assert report.malformed_run_history_rows == 1
    titles = [w.title for w in report.warnings]  # type: ignore[union-attr]
    assert any("Malformed _dlt_loads row" in (t or "") for t in titles), (
        f"Expected 'Malformed _dlt_loads row' warning, got titles: {titles}"
    )


def test_parse_load_row_unknown_status_boxed_into_unknown_enum(tmp_path: Path) -> None:
    """An int status that is not a known DltLoadStatus member is boxed into UNKNOWN."""
    client = DltClient(pipelines_dir=str(tmp_path), report=DltSourceReport())
    in_window = datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc)
    row = ("load", "schema", 99, in_window, "vh")  # status=99 is unknown
    load = client._parse_load_row(row, start_time=None, end_time=None)
    assert load is not None
    assert load.status == DltLoadStatus.UNKNOWN


# ---------------------------------------------------------------------------
# Test 17: URN field_validators reject malformed URNs at config-load time
# ---------------------------------------------------------------------------


def test_source_dataset_urns_validator_rejects_bad_urn() -> None:
    """A typo in source_dataset_urns must raise ValidationError before ingestion.

    Without this validator the bad URN would surface as a per-URN warning at
    workunit emission time and silently produce no upstream lineage.
    """
    with pytest.raises(ValidationError) as exc_info:
        DltSourceConfig.model_validate(
            {
                "pipelines_dir": "/tmp",
                "env": "DEV",
                "source_dataset_urns": {
                    "chess_pipeline": ["not-a-valid-urn"],
                },
            }
        )
    msg = str(exc_info.value)
    # Error must point the operator at the offending pipeline AND URN.
    assert "chess_pipeline" in msg
    assert "not-a-valid-urn" in msg


def test_source_table_dataset_urns_validator_rejects_bad_urn() -> None:
    """A typo nested in source_table_dataset_urns must include both pipeline and table names."""
    with pytest.raises(ValidationError) as exc_info:
        DltSourceConfig.model_validate(
            {
                "pipelines_dir": "/tmp",
                "env": "DEV",
                "source_table_dataset_urns": {
                    "chess_pipeline": {
                        "players_games": ["bogus-urn"],
                    }
                },
            }
        )
    msg = str(exc_info.value)
    assert "chess_pipeline" in msg
    assert "players_games" in msg
    assert "bogus-urn" in msg


def test_urn_validators_accept_valid_urns() -> None:
    """Well-formed Dataset URNs in both maps load successfully."""
    config = DltSourceConfig.model_validate(
        {
            "pipelines_dir": "/tmp",
            "env": "DEV",
            "source_dataset_urns": {
                "chess_pipeline": [
                    "urn:li:dataset:(urn:li:dataPlatform:chess_api,players_games,DEV)"
                ],
            },
            "source_table_dataset_urns": {
                "chess_pipeline": {
                    "players_games": [
                        "urn:li:dataset:(urn:li:dataPlatform:postgres,prod_db.public.players_games,PROD)"
                    ]
                }
            },
        }
    )
    # Round-tripped, both maps preserved.
    assert "chess_pipeline" in config.source_dataset_urns
    assert "players_games" in config.source_table_dataset_urns["chess_pipeline"]


# ---------------------------------------------------------------------------
# Test 18: DestinationPlatformConfig env validator rejects typos
# ---------------------------------------------------------------------------


def test_destination_platform_env_typo_rejected() -> None:
    """A typo'd env (e.g. 'PORD') must raise ValidationError so lineage URNs cannot silently mismatch."""
    with pytest.raises(ValidationError) as exc_info:
        DltSourceConfig.model_validate(
            {
                "pipelines_dir": "/tmp",
                "env": "DEV",
                "destination_platform_map": {
                    "postgres": {"env": "PORD"},  # typo
                },
            }
        )
    msg = str(exc_info.value)
    assert "PORD" in msg
    # Pydantic's location path must point the operator at the offending entry
    # so they don't have to grep through their config to find the broken key.
    assert "destination_platform_map" in msg or "postgres" in msg


def test_destination_platform_env_lowercased_input_is_normalized() -> None:
    """A correctly-named but lower-case env value is accepted and uppercased,
    mirroring the validator on EnvConfigMixin."""
    config = DltSourceConfig.model_validate(
        {
            "pipelines_dir": "/tmp",
            "env": "DEV",
            "destination_platform_map": {
                "postgres": {"env": "dev"},
            },
        }
    )
    assert config.destination_platform_map["postgres"].env == "DEV"
