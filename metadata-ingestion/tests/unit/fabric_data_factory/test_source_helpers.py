"""Unit tests for Fabric Data Factory source helper functions.

Tests _parse_iso_to_millis, URL builders, and status mapping constants.
"""

from datahub.ingestion.source.fabric.data_factory.source import (
    PLATFORM,
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


class TestStatusMappings:
    """Verify status mapping dicts are populated and consistent."""

    def test_fabric_status_map_has_terminal_states(self) -> None:
        from datahub.ingestion.source.fabric.data_factory.source import (
            _FABRIC_STATUS_TO_RESULT,
        )

        assert "Completed" in _FABRIC_STATUS_TO_RESULT
        assert "Failed" in _FABRIC_STATUS_TO_RESULT
        assert "Cancelled" in _FABRIC_STATUS_TO_RESULT

    def test_activity_status_map_has_terminal_states(self) -> None:
        from datahub.ingestion.source.fabric.data_factory.source import (
            _ACTIVITY_STATUS_TO_RESULT,
        )

        assert "Succeeded" in _ACTIVITY_STATUS_TO_RESULT
        assert "Failed" in _ACTIVITY_STATUS_TO_RESULT
        assert "Cancelled" in _ACTIVITY_STATUS_TO_RESULT

    def test_activity_subtype_map_covers_copy_and_invoke(self) -> None:
        from datahub.ingestion.source.fabric.data_factory.source import (
            _ACTIVITY_SUBTYPE_MAP,
        )

        assert "Copy" in _ACTIVITY_SUBTYPE_MAP
        assert "InvokePipeline" in _ACTIVITY_SUBTYPE_MAP
        assert "Lookup" in _ACTIVITY_SUBTYPE_MAP

    def test_platform_constant(self) -> None:
        assert PLATFORM == "fabric-data-factory"
