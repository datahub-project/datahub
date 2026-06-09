from typing import Any, Dict, List, Optional, cast
from unittest import mock

from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.quicksight.quicksight import QuickSightSource
from datahub.ingestion.source.state.checkpoint import Checkpoint
from tests.test_helpers.state_helpers import (
    validate_all_providers_have_committed_successfully,
)

GMS_SERVER = "http://localhost:8080"
_ACCOUNT = "064369473231"

_CONFIG_METHOD = (
    "datahub.ingestion.source.quicksight.quicksight_config.QuickSightSourceConfig"
)
_CHECKPOINT_PROVIDER_GRAPH = (
    "datahub.ingestion.source.state_provider."
    "datahub_ingestion_checkpointing_provider.DataHubGraph"
)


def _quicksight_client(dashboard_ids: List[str]) -> mock.MagicMock:
    """Mocked boto3 QuickSight client returning the given dashboards (and nothing
    else) so a full pipeline run emits exactly one Dashboard entity per id."""
    client = mock.MagicMock()
    pages: Dict[str, List[Dict[str, Any]]] = {
        "list_namespaces": [{"Namespaces": [{"Name": "default"}]}],
        "list_dashboards": [
            {
                "DashboardSummaryList": [
                    {
                        "DashboardId": d,
                        "Name": d,
                        "Arn": f"arn:aws:quicksight:us-east-1:{_ACCOUNT}:dashboard/{d}",
                    }
                    for d in dashboard_ids
                ]
            }
        ],
    }

    def get_paginator(operation_name: str) -> mock.MagicMock:
        paginator = mock.MagicMock()
        # Default to a single empty page for unlisted operations.
        paginator.paginate.return_value = pages.get(operation_name, [{}])
        return paginator

    client.get_paginator.side_effect = get_paginator
    client.describe_dashboard.return_value = {"Dashboard": {"Version": {}}}
    return client


def _ingest(
    run_id: str,
    pipeline_name: str,
    tmp_path: Any,
    mock_datahub_graph: Any,
    dashboard_ids: List[str],
) -> Pipeline:
    sts_client = mock.MagicMock()
    sts_client.get_caller_identity.return_value = {"Account": _ACCOUNT}
    quicksight_client = _quicksight_client(dashboard_ids)

    config_dict = {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "source": {
            "type": "quicksight",
            "config": {
                "aws_region": "us-east-1",
                "aws_account_id": _ACCOUNT,
                # Keep the run focused on Dashboard entities — no extra API shapes.
                "extract_ownership": False,
                "extract_tags": False,
                "extract_dashboard_definitions": False,
                "extract_analysis_definitions": False,
                "stateful_ingestion": {
                    "enabled": True,
                    "state_provider": {
                        "type": "datahub",
                        "config": {"datahub_api": {"server": GMS_SERVER}},
                    },
                },
            },
        },
        "sink": {
            "type": "file",
            "config": {"filename": f"{tmp_path}/quicksight_mces_{run_id}.json"},
        },
    }

    with (
        mock.patch(
            f"{_CONFIG_METHOD}.get_quicksight_client",
            return_value=quicksight_client,
        ),
        mock.patch(
            f"{_CONFIG_METHOD}.get_sts_client",
            return_value=sts_client,
        ),
        mock.patch(_CHECKPOINT_PROVIDER_GRAPH, mock_datahub_graph) as mock_checkpoint,
    ):
        mock_checkpoint.return_value = mock_datahub_graph
        pipeline = Pipeline.create(config_dict)
        pipeline.run()
        pipeline.raise_from_status()
        return pipeline


def _checkpoints(
    pipeline: Pipeline,
) -> Dict[JobId, Optional[Checkpoint[Any]]]:
    source = cast(QuickSightSource, pipeline.source)
    return {
        job_id: source.state_provider.get_current_checkpoint(job_id)
        for job_id in source.state_provider._usecase_handlers
    }


def test_quicksight_stateful_ingestion_removes_stale_dashboard(
    tmp_path, mock_datahub_graph
):
    # Run 1 sees two dashboards; run 2 sees only one. Stale entity removal must
    # detect the dropped dashboard via the checkpoint state diff.
    pipeline1 = _ingest(
        "run1",
        "quicksight_test",
        tmp_path,
        mock_datahub_graph,
        ["dash-keep", "dash-remove"],
    )
    checkpoint1 = _checkpoints(pipeline1)
    assert checkpoint1
    for checkpoint in checkpoint1.values():
        assert checkpoint
        assert checkpoint.state

    pipeline2 = _ingest(
        "run2", "quicksight_test", tmp_path, mock_datahub_graph, ["dash-keep"]
    )
    checkpoint2 = _checkpoints(pipeline2)
    assert checkpoint2
    for checkpoint in checkpoint2.values():
        assert checkpoint
        assert checkpoint.state

    # Each run commits its checkpoint to the (mocked) state provider.
    validate_all_providers_have_committed_successfully(pipeline1, expected_providers=1)
    validate_all_providers_have_committed_successfully(pipeline2, expected_providers=1)

    # The dashboard present in run 1 but absent in run 2 is the stale one.
    for job_id in checkpoint1:
        state1 = checkpoint1[job_id].state  # type: ignore[union-attr]
        state2 = checkpoint2[job_id].state  # type: ignore[union-attr]
        stale = list(
            state1.get_urns_not_in(type="dashboard", other_checkpoint_state=state2)
        )
        assert stale == ["urn:li:dashboard:(quicksight,dash-remove)"]
