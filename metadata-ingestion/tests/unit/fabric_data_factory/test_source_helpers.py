"""Unit tests for Fabric Data Factory source helper functions.

Tests _parse_iso_to_millis and URL builders.
"""

from datahub.ingestion.source.fabric.data_factory.source import (
    FabricDataFactorySource,
    _parse_iso_to_millis,
)


class TestParseIsoToMillis:
    def test_iso_with_z_suffix(self) -> None:
        result = _parse_iso_to_millis("2024-01-15T10:00:00Z")
        # 2024-01-15T10:00:00 UTC = 1705312800 seconds
        assert result == 1705312800000

    def test_iso_with_utc_offset(self) -> None:
        result = _parse_iso_to_millis("2024-01-15T10:00:00+00:00")
        assert result == 1705312800000

    def test_iso_without_timezone_assumes_utc(self) -> None:
        result = _parse_iso_to_millis("2024-01-15T10:00:00")
        assert result == 1705312800000

    def test_iso_with_microseconds(self) -> None:
        result = _parse_iso_to_millis("2024-01-15T10:00:00.123456Z")
        assert result == 1705312800123

    def test_different_date(self) -> None:
        result = _parse_iso_to_millis("2023-06-01T00:00:00Z")
        assert result == 1685577600000


class TestGetPipelineUrl:
    def test_url_format(self) -> None:
        url = FabricDataFactorySource._get_pipeline_url("ws-123", "pl-456")
        assert url == "https://app.fabric.microsoft.com/groups/ws-123/pipelines/pl-456"


class TestGetPipelineRunUrl:
    def test_url_format(self) -> None:
        url = FabricDataFactorySource._get_pipeline_run_url(
            "ws-123", "pl-456", "run-789"
        )
        assert url == (
            "https://app.fabric.microsoft.com/workloads/data-pipeline"
            "/monitoring/workspaces/ws-123"
            "/pipelines/pl-456/run-789"
        )
