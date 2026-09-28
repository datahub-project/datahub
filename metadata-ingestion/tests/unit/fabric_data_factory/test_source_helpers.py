"""Unit tests for Fabric Data Factory source helper functions.

Tests _parse_iso_to_millis, URL builders, and activity emission error isolation.
"""

from typing import Generator, Union, cast

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.fabric.common.models import FabricItem
from datahub.ingestion.source.fabric.common.utils import make_workspace_key
from datahub.ingestion.source.fabric.data_factory.models import PipelineActivity
from datahub.ingestion.source.fabric.data_factory.source import (
    FabricDataFactorySource,
    _parse_iso_to_millis,
)
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.entity import Entity


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


class TestEmitPipelineActivities:
    WORKSPACE_ID = "ws-1"
    PIPELINE_ID = "pl-1"

    def _emit(self) -> Generator[Union[MetadataWorkUnit, Entity], None, None]:
        source = FabricDataFactorySource.create(
            {
                "credential": {
                    "authentication_method": "service_principal",
                    "client_id": "test-client",
                    "client_secret": "test-secret",
                    "tenant_id": "test-tenant",
                },
            },
            PipelineContext(run_id="fabric-df-emit-activities"),
        )
        self.source = source
        source._pipeline_activities_cache[(self.WORKSPACE_ID, self.PIPELINE_ID)] = [
            PipelineActivity(name=name, type="Wait") for name in ("First", "Second")
        ]
        workspace_key = make_workspace_key(self.WORKSPACE_ID, None, "PROD")
        dataflow = DataFlow(
            platform="fabric-data-factory",
            name=f"{self.WORKSPACE_ID}.{self.PIPELINE_ID}",
            parent_container=workspace_key,
        )
        pipeline_item = FabricItem(
            id=self.PIPELINE_ID,
            name="etl",
            type="DataPipeline",
            workspace_id=self.WORKSPACE_ID,
        )
        return cast(
            Generator[Union[MetadataWorkUnit, Entity], None, None],
            source._emit_pipeline_activities(
                pipeline_item, dataflow, workspace_key, {}
            ),
        )

    def test_consumer_error_is_not_reported_as_activity_failure(self) -> None:
        """An error raised into the generator by its consumer must propagate."""
        emitted = self._emit()
        next(emitted)
        with pytest.raises(RuntimeError, match="sink failed"):
            emitted.throw(RuntimeError("sink failed"))
        assert len(self.source.report.warnings) == 0
