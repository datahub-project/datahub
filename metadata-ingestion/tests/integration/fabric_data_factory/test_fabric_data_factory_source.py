"""Integration tests for Fabric Data Factory source.

These tests use mocked client responses to verify the full ingestion pipeline
produces the expected metadata events.
"""

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.fabric.common.models import ItemJobStatus
from datahub.ingestion.source.fabric.data_factory.client import (
    FabricDataFactoryClient,
)
from datahub.testing import mce_helpers
from tests.integration.fabric_data_factory.test_factories import (
    ACTIVITY_RUN_ID_1,
    ACTIVITY_RUN_ID_2,
    CONNECTION_ID_LAKEHOUSE,
    CONNECTION_ID_SNOWFLAKE,
    FROZEN_TIME,
    LAKEHOUSE_ARTIFACT_ID,
    PIPELINE_ID_A,
    PIPELINE_ID_B,
    PIPELINE_RUN_ID_1,
    WORKSPACE_ID_1,
    WORKSPACE_ID_2,
    create_activity_run,
    create_connection_dict,
    create_copy_activity,
    create_external_dataset_settings,
    create_invoke_pipeline_activity,
    create_lakehouse_dataset_settings,
    create_lookup_activity,
    create_pipeline_item,
    create_pipeline_run,
    create_set_variable_activity,
    create_workspace,
)

GOLDEN_DIR = Path(__file__).parent / "golden"

SOURCE_CONFIG_BASE: Dict[str, Any] = {
    "credential": {
        "authentication_method": "service_principal",
        "client_id": "test-client",
        "client_secret": "test-secret",
        "tenant_id": "test-tenant",
    },
    "env": "PROD",
}


def _run_pipeline(
    output_file: str,
    source_config: Dict[str, Any],
    run_id: str = "fabric-data-factory-test",
) -> Pipeline:
    pipeline = Pipeline.create(
        {
            "run_id": run_id,
            "source": {
                "type": "fabric-data-factory",
                "config": {**SOURCE_CONFIG_BASE, **source_config},
            },
            "sink": {
                "type": "file",
                "config": {"filename": output_file},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    return pipeline


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_full_ingestion(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    """Comprehensive test: filtering, copy lineage, invoke pipeline, execution history.

    Setup:
    - 2 workspaces: prod-analytics (allowed) and dev-sandbox (filtered out)
    - 2 pipelines in prod: etl-daily (allowed) and test-validation (filtered out)
    - etl-daily has: Copy (Snowflake→Lakehouse), Lookup, InvokePipeline → child
    - Child pipeline (etl-child) has: Lookup + SetVariable with dependencies
    - Execution history: 1 pipeline run with 2 activity runs (success + failure)
    """
    output_file = str(tmp_path / "output.json")

    prod_workspace = create_workspace(WORKSPACE_ID_1, "prod-analytics")
    dev_workspace = create_workspace(WORKSPACE_ID_2, "dev-sandbox")

    etl_pipeline = create_pipeline_item(PIPELINE_ID_A, "etl-daily", WORKSPACE_ID_1)
    test_pipeline = create_pipeline_item(PIPELINE_ID_B, "etl-child", WORKSPACE_ID_1)

    connections = [
        create_connection_dict(
            CONNECTION_ID_SNOWFLAKE,
            "My Snowflake",
            "Snowflake",
            path="myaccount.snowflakecomputing.com;mydb",
        ),
        create_connection_dict(
            CONNECTION_ID_LAKEHOUSE,
            "My Lakehouse",
            "Lakehouse",
        ),
    ]

    etl_activities = [
        create_copy_activity(
            "CopySnowflakeToLakehouse",
            source_settings=create_external_dataset_settings(
                connection_id=CONNECTION_ID_SNOWFLAKE,
                schema="public",
                table="customers",
            ),
            sink_settings=create_lakehouse_dataset_settings(
                artifact_id=LAKEHOUSE_ARTIFACT_ID,
                workspace_id=WORKSPACE_ID_1,
                schema="dbo",
                table="customers",
            ),
        ),
        create_lookup_activity("LookupConfig", depends_on=["CopySnowflakeToLakehouse"]),
        create_invoke_pipeline_activity(
            "CallChildPipeline",
            child_pipeline_id=PIPELINE_ID_B,
            depends_on=["LookupConfig"],
        ),
    ]

    child_activities = [
        create_lookup_activity("ChildStep1"),
        create_set_variable_activity("ChildStep2", depends_on=["ChildStep1"]),
    ]

    pipeline_run = create_pipeline_run(
        run_id=PIPELINE_RUN_ID_1,
        item_id=PIPELINE_ID_A,
        workspace_id=WORKSPACE_ID_1,
        status=ItemJobStatus.COMPLETED,
        start_time="2024-01-15T10:00:00Z",
        end_time="2024-01-15T10:15:00Z",
        invoke_type="Manual",
    )

    activity_runs = [
        create_activity_run(
            activity_name="CopySnowflakeToLakehouse",
            activity_type="Copy",
            activity_run_id=ACTIVITY_RUN_ID_1,
            pipeline_run_id=PIPELINE_RUN_ID_1,
            status="Succeeded",
            start="2024-01-15T10:00:00Z",
            end="2024-01-15T10:10:00Z",
            duration_ms=600000,
        ),
        create_activity_run(
            activity_name="LookupConfig",
            activity_type="Lookup",
            activity_run_id=ACTIVITY_RUN_ID_2,
            pipeline_run_id=PIPELINE_RUN_ID_1,
            status="Failed",
            start="2024-01-15T10:10:00Z",
            end="2024-01-15T10:15:00Z",
            duration_ms=300000,
            error_message="Table not found",
        ),
    ]

    def items_side_effect(
        workspace_id: str, item_type: Optional[str] = None
    ) -> Iterator[Any]:
        if workspace_id == WORKSPACE_ID_1:
            return iter([etl_pipeline, test_pipeline])
        return iter([])

    def activities_side_effect(pipeline_id: str) -> List[Any]:
        if pipeline_id == PIPELINE_ID_A:
            return etl_activities
        if pipeline_id == PIPELINE_ID_B:
            return child_activities
        return []

    with (
        patch.object(
            FabricDataFactoryClient,
            "list_workspaces",
            return_value=iter([prod_workspace, dev_workspace]),
        ),
        patch.object(
            FabricDataFactoryClient,
            "list_items",
            side_effect=items_side_effect,
        ),
        patch.object(
            FabricDataFactoryClient,
            "list_connections",
            return_value=iter(connections),
        ),
        patch.object(
            FabricDataFactoryClient,
            "get_pipeline_activities",
            side_effect=activities_side_effect,
        ),
        patch.object(
            FabricDataFactoryClient,
            "get_pipeline_runs",
            return_value=[pipeline_run],
        ),
        patch.object(
            FabricDataFactoryClient,
            "query_activity_runs",
            return_value=activity_runs,
        ),
        patch.object(FabricDataFactoryClient, "close"),
    ):
        _run_pipeline(
            output_file,
            {
                "include_execution_history": True,
                "workspace_pattern": {"allow": ["prod-.*"]},
                "pipeline_pattern": {"allow": ["etl-.*"]},
            },
            run_id="full-ingestion-test",
        )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=str(GOLDEN_DIR / "test_full_ingestion_golden.json"),
    )


@freeze_time(FROZEN_TIME)
@pytest.mark.integration
def test_no_execution_history(pytestconfig: pytest.Config, tmp_path: Path) -> None:
    """Test that include_execution_history=False emits no DPI entities."""
    output_file = str(tmp_path / "output.json")

    workspace = create_workspace(WORKSPACE_ID_1, "Production Workspace")
    pipeline_item = create_pipeline_item(
        PIPELINE_ID_A, "Simple Pipeline", WORKSPACE_ID_1
    )

    activities = [
        create_lookup_activity("Step1"),
    ]

    with (
        patch.object(
            FabricDataFactoryClient,
            "list_workspaces",
            return_value=iter([workspace]),
        ),
        patch.object(
            FabricDataFactoryClient,
            "list_items",
            return_value=iter([pipeline_item]),
        ),
        patch.object(
            FabricDataFactoryClient,
            "list_connections",
            return_value=iter([]),
        ),
        patch.object(
            FabricDataFactoryClient,
            "get_pipeline_activities",
            return_value=activities,
        ),
        patch.object(
            FabricDataFactoryClient,
            "get_pipeline_runs",
            return_value=[],
        ),
        patch.object(FabricDataFactoryClient, "query_activity_runs", return_value=[]),
        patch.object(FabricDataFactoryClient, "close"),
    ):
        _run_pipeline(
            output_file,
            {"include_execution_history": False},
            run_id="no-execution-history-test",
        )

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_file,
        golden_path=str(GOLDEN_DIR / "test_no_execution_history_golden.json"),
    )
