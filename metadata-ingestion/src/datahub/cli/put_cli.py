import logging
from typing import Optional, Union

import click
from click_default_group import DefaultGroup

from datahub.cli.cli_utils import post_entity
from datahub.configuration.config_loader import load_config_file
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.metadata.schema_classes import (
    DataPlatformInfoClass as DataPlatformInfo,
    PlatformTypeClass,
    SystemMetadataClass,
)
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.urns.data_platform_urn import DataPlatformUrn
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="aspect")
def put() -> None:
    """A group of commands to put metadata in DataHub."""
    pass


@put.command(
    name="aspect",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option("--urn", required=True, type=str)
@click.option("-a", "--aspect", required=True, type=str)
@click.option("-d", "--aspect-data", required=True, type=str)
@click.option(
    "--run-id",
    type=str,
    required=False,
    help="Run ID into which we should log the aspect.",
)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def aspect(urn: str, aspect: str, aspect_data: str, run_id: Optional[str]) -> None:
    """Update a single aspect of an entity"""

    entity_type = guess_entity_type(urn)
    aspect_obj = load_config_file(
        aspect_data, allow_stdin=True, resolve_env_vars=False, process_directives=False
    )

    client = get_default_graph(ClientMode.CLI)

    system_metadata: Union[None, SystemMetadataClass] = None
    if run_id:
        system_metadata = SystemMetadataClass(runId=run_id)

    # TODO: Replace with client.emit, requires figuring out the correct subsclass of _Aspect to create from the data
    status = post_entity(
        client._session,
        client.config.server,
        urn=urn,
        aspect_name=aspect,
        entity_type=entity_type,
        aspect_value=aspect_obj,
        system_metadata=system_metadata,
    )
    click.secho(f"Update succeeded with status {status}", fg="green")


@put.command()
@click.pass_context
@upgrade.check_upgrade
@telemetry.with_telemetry()
@click.option(
    "--name",
    type=str,
    help="Platform name",
    required=True,
)
@click.option(
    "--display_name",
    type=str,
    help="Platform Display Name (human friendly)",
    required=False,
)
@click.option(
    "--logo",
    type=str,
    help="Logo URL that must be reachable from the DataHub UI.",
    required=True,
)
@click.option(
    "--run-id", type=str, help="Run ID into which we should log the platform."
)
def platform(
    ctx: click.Context, name: str, display_name: Optional[str], logo: str, run_id: str
) -> None:
    """
    Create or update a dataplatform entity in DataHub
    """

    if name.startswith(f"urn:li:{DataPlatformUrn.ENTITY_TYPE}"):
        platform_urn = DataPlatformUrn.from_string(name)
        platform_name = platform_urn.get_entity_id_as_string()
    else:
        platform_name = name.lower()
        platform_urn = DataPlatformUrn.create_from_id(platform_name)

    data_platform_info = DataPlatformInfo(
        name=name,
        type=PlatformTypeClass.OTHERS,
        datasetNameDelimiter=".",
        displayName=display_name or platform_name,
        logoUrl=logo,
    )
    datahub_graph = get_default_graph(ClientMode.CLI)
    mcp = MetadataChangeProposalWrapper(
        entityUrn=str(platform_urn),
        aspect=data_platform_info,
        systemMetadata=SystemMetadataClass(runId=run_id),
    )
    datahub_graph.emit(mcp)
    click.echo(
        f"✅ Successfully wrote data platform metadata for {platform_urn} to DataHub ({datahub_graph})"
    )
