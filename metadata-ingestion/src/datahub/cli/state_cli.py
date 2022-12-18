import json
import logging
from datetime import datetime

import click
from click_default_group import DefaultGroup

from datahub.cli.cli_utils import get_url_and_token
from datahub.ingestion.api.ingestion_job_state_provider import IngestionJobStateProvider
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider import (
    DatahubIngestionCheckpointingProvider,
)
from datahub.metadata.schema_classes import DatahubIngestionCheckpointClass
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="urn")
def state() -> None:
    """Managed state stored in DataHub by stateful ingestion."""
    pass


@state.command()
@click.option("--pipeline-name", required=True, type=str)
@click.option("--platform", required=True, type=str)
@click.option("--platform-instance", required=True, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry
def inspect(pipeline_name: str, platform: str, platform_instance: str) -> None:
    """
    Get the latest stateful ingestion state for a given pipeline.
    Only works for state entity removal for now.
    """

    # Note that the platform-instance argument is not generated consistently,
    # and is not always equal to the platform_instance config.

    (url, token) = get_url_and_token()
    datahub_graph = DataHubGraph(DataHubGraphConfig(server=url, token=token))

    job_name = StaleEntityRemovalHandler.compute_job_id(platform)

    data_job_urn = IngestionJobStateProvider.get_data_job_urn(
        DatahubIngestionCheckpointingProvider.orchestrator_name,
        pipeline_name,
        job_name,
        platform_instance,
    )
    raw_checkpoint = datahub_graph.get_latest_timeseries_value(
        entity_urn=data_job_urn,
        filter_criteria_map={
            "pipelineName": pipeline_name,
            "platformInstanceId": platform_instance,
        },
        aspect_type=DatahubIngestionCheckpointClass,
    )

    if not raw_checkpoint:
        click.secho("No ingestion state found.", fg="red")
        exit(1)

    checkpoint = Checkpoint.create_from_checkpoint_aspect(
        job_name=job_name,
        checkpoint_aspect=raw_checkpoint,
        state_class=GenericCheckpointState,
    )
    assert checkpoint

    ts = datetime.utcfromtimestamp(raw_checkpoint.timestampMillis / 1000)
    logger.info(
        f"Found checkpoint with runId {checkpoint.run_id} and timestamp {ts.isoformat()}"
    )

    click.echo(json.dumps(checkpoint.state.urns, indent=2))
