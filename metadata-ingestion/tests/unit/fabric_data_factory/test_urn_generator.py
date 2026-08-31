"""Unit tests for Fabric Data Factory URN generation functions.

Tests pipeline flow, activity job, and activity run URN generation
that is specific to the Data Factory connector.
"""

import pytest

from datahub.ingestion.source.fabric.common.urn_generator import (
    make_activity_job_id,
    make_activity_job_urn,
    make_activity_run_id,
    make_pipeline_flow_id,
    make_pipeline_flow_urn,
    make_pipeline_run_id,
)


class TestPipelineFlowUrn:
    def test_make_pipeline_flow_id(self) -> None:
        result = make_pipeline_flow_id("ws-123", "pl-456")
        assert result == "ws-123.pl-456"

    def test_make_pipeline_flow_urn_basic(self) -> None:
        urn = make_pipeline_flow_urn(
            workspace_id="ws-123",
            pipeline_id="pl-456",
            platform="fabric-data-factory",
            env="PROD",
        )
        assert str(urn) == ("urn:li:dataFlow:(fabric-data-factory,ws-123.pl-456,PROD)")

    def test_make_pipeline_flow_urn_with_platform_instance(self) -> None:
        urn = make_pipeline_flow_urn(
            workspace_id="ws-123",
            pipeline_id="pl-456",
            platform="fabric-data-factory",
            env="PROD",
            platform_instance="my-instance",
        )
        urn_str = str(urn)
        assert "fabric-data-factory" in urn_str
        assert "ws-123.pl-456" in urn_str

    def test_make_pipeline_flow_urn_dev_env(self) -> None:
        urn = make_pipeline_flow_urn(
            workspace_id="ws-123",
            pipeline_id="pl-456",
            platform="fabric-data-factory",
            env="DEV",
        )
        assert "DEV" in str(urn)


class TestActivityJobUrn:
    def test_make_activity_job_id_is_identity(self) -> None:
        assert make_activity_job_id("CopyData") == "CopyData"
        assert make_activity_job_id("My Activity Name") == "My Activity Name"

    def test_make_activity_job_urn(self) -> None:
        flow_urn = make_pipeline_flow_urn(
            workspace_id="ws-123",
            pipeline_id="pl-456",
            platform="fabric-data-factory",
            env="PROD",
        )
        job_urn = make_activity_job_urn("CopyData", flow_urn)
        urn_str = str(job_urn)
        assert "CopyData" in urn_str
        assert "ws-123.pl-456" in urn_str


class TestRunIds:
    def test_make_pipeline_run_id_is_identity(self) -> None:
        assert make_pipeline_run_id("run-abc-123") == "run-abc-123"

    def test_make_activity_run_id(self) -> None:
        result = make_activity_run_id("pipeline-run-1", "activity-run-2")
        assert result == "pipeline-run-1.activity-run-2"

    @pytest.mark.parametrize(
        "pipeline_run_id,activity_run_id,expected",
        [
            ("pr-1", "ar-1", "pr-1.ar-1"),
            ("abc", "def", "abc.def"),
            (
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
                "00000000-0000-0000-0000-000000000001.00000000-0000-0000-0000-000000000002",
            ),
        ],
    )
    def test_make_activity_run_id_parametrized(
        self, pipeline_run_id: str, activity_run_id: str, expected: str
    ) -> None:
        assert make_activity_run_id(pipeline_run_id, activity_run_id) == expected
