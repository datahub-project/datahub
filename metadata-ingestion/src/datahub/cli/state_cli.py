import json
import logging
from typing import Optional

import click
from click_default_group import DefaultGroup

from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider import (
    DatahubIngestionCheckpointingProvider,
)
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
@click.option("--platform-instance", required=False, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def inspect(
    pipeline_name: str, platform: str, platform_instance: Optional[str]
) -> None:
    """
    Get the latest stateful ingestion state for a given pipeline.
    Only works for state entity removal for now.
    """

    # Note that the platform-instance argument is not generated consistently,
    # and is not always equal to the platform_instance config.

    datahub_graph = get_default_graph()
    checkpoint_provider = DatahubIngestionCheckpointingProvider(datahub_graph, "cli")

    job_name = StaleEntityRemovalHandler.compute_job_id(platform)

    raw_checkpoint = checkpoint_provider.get_latest_checkpoint(pipeline_name, job_name)
    if raw_checkpoint is None and platform_instance is not None:
        logger.info(
            "Failed to fetch state, but trying legacy URN format because platform_instance is provided."
        )
        raw_checkpoint = checkpoint_provider.get_latest_checkpoint(
            pipeline_name, job_name, platform_instance_id=platform_instance
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

    click.echo(json.dumps(checkpoint.state.urns, indent=2))
