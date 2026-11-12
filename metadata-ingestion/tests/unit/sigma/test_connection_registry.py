from dataclasses import dataclass
from typing import Optional
from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sigma.config import SigmaSourceConfig
from datahub.ingestion.source.sigma.connection_registry import (
    SIGMA_TYPE_TO_DATAHUB_PLATFORM_MAP,
    SigmaConnectionRegistry,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI


@dataclass
class _FakeReporter:
    connections_resolved: int = 0
    connections_unmappable_type: int = 0
    connections_skipped_missing_id: int = 0
    connections_duplicate_id: int = 0


def _build(
    raw: list, reporter: Optional[_FakeReporter] = None
) -> SigmaConnectionRegistry:
    if reporter is None:
        reporter = _FakeReporter()
    return SigmaConnectionRegistry.build(
        raw,
        reporter=reporter,
        type_to_platform_map=SIGMA_TYPE_TO_DATAHUB_PLATFORM_MAP,
    )


def test_registry_builds_from_full_metadata():
    raw = [
        {
            "connectionId": "conn-1",
            "name": "Prod Snowflake",
            "type": "snowflake",
            "host": "acme.snowflakecomputing.com",
            "account": "acme",
            "warehouse": "COMPUTE_WH",
        }
    ]
    registry = _build(raw)
    rec = registry.get("conn-1")
    assert rec is not None
    assert rec.datahub_platform == "snowflake"
    assert rec.host == "acme.snowflakecomputing.com"
    assert rec.account == "acme"
    assert rec.instance_hint == "COMPUTE_WH"
    assert rec.is_mappable is True


def test_registry_unmappable_type_keeps_record_unmappable():
    raw = [
        {
            "connectionId": "conn-3",
            "name": "Unknown Warehouse",
            "type": "oracle",
            "host": "oracle.example.com",
        }
    ]
    reporter = _FakeReporter()
    registry = _build(raw, reporter)
    rec = registry.get("conn-3")
    assert rec is not None
    assert rec.datahub_platform == ""
    assert rec.is_mappable is False
    assert reporter.connections_unmappable_type == 1


def test_registry_get_returns_none_for_unknown_id():
    registry = _build([])
    assert registry.get("nonexistent") is None


def test_registry_synapse_maps_to_mssql():
    # Azure Synapse (Dedicated SQL) is ingested as `mssql` in DataHub.
    raw = [
        {
            "connectionId": "conn-syn",
            "name": "Synapse",
            "type": "synapse",
            "host": "synapse.example.com",
        },
        {
            "connectionId": "conn-azsyn",
            "name": "Azure Synapse",
            "type": "azure_synapse",
            "host": "azsynapse.example.com",
        },
    ]
    registry = _build(raw)
    syn = registry.get("conn-syn")
    azsyn = registry.get("conn-azsyn")
    assert syn is not None and syn.datahub_platform == "mssql"
    assert azsyn is not None and azsyn.datahub_platform == "mssql"


def test_registry_skips_records_without_id():
    raw = [
        {"name": "no id", "type": "snowflake"},
        {"connectionId": "", "name": "empty id", "type": "snowflake"},
    ]
    reporter = _FakeReporter()
    registry = _build(raw, reporter)
    assert registry.by_id == {}
    assert reporter.connections_skipped_missing_id == 2
    assert reporter.connections_resolved == 0


def test_registry_counts_duplicate_ids():
    raw = [
        {
            "connectionId": "dup",
            "name": "First",
            "type": "snowflake",
            "host": "first.snowflakecomputing.com",
        },
        {
            "connectionId": "dup",
            "name": "Second",
            "type": "snowflake",
            "host": "second.snowflakecomputing.com",
        },
    ]
    reporter = _FakeReporter()
    registry = _build(raw, reporter)
    assert reporter.connections_duplicate_id == 1
    # Later record wins.
    rec = registry.get("dup")
    assert rec is not None and rec.host == "second.snowflakecomputing.com"


def test_registry_record_default_is_unmappable():
    # Records constructed outside build() default to untrusted.
    from datahub.ingestion.source.sigma.connection_registry import (
        SigmaConnectionRecord,
    )

    rec = SigmaConnectionRecord(
        connection_id="x",
        name="x",
        sigma_type="snowflake",
        datahub_platform="snowflake",
    )
    assert rec.is_mappable is False


def test_registry_build_handles_empty_input():
    reporter = _FakeReporter()
    registry = _build([], reporter)
    assert registry.by_id == {}
    assert reporter.connections_resolved == 0
    assert reporter.connections_unmappable_type == 0


def test_registry_build_increments_reporter_counters():
    raw = [
        {
            "connectionId": "c1",
            "name": "Snowflake mappable",
            "type": "snowflake",
            "host": "acme.snowflakecomputing.com",
        },
        {
            "connectionId": "c2",
            "name": "BigQuery mappable",
            "type": "bigquery",
        },
        {
            "connectionId": "c3",
            "name": "Unknown type",
            "type": "oracle",
        },
    ]
    reporter = _FakeReporter()
    _build(raw, reporter)
    assert reporter.connections_resolved == 2
    assert reporter.connections_unmappable_type == 1


def test_build_connection_registry_falls_back_on_exception():
    # If SigmaConnectionRegistry.build() raises (programming bug), SigmaSource
    # init must continue with an empty registry and surface a structured warning.
    config = SigmaSourceConfig(client_id="x", client_secret="y")
    ctx = PipelineContext(run_id="test")

    with (
        patch.object(SigmaAPI, "_generate_token"),
        patch.object(
            SigmaConnectionRegistry,
            "build",
            side_effect=RuntimeError("simulated bug in build()"),
        ),
    ):
        source = SigmaSource(config, ctx)

    assert source.connection_registry.by_id == {}
    warning_titles = [w.title for w in source.reporter.warnings]
    assert "Sigma Connection registry build failed" in warning_titles
