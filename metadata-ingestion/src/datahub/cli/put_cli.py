import json
import logging
from typing import Any, Optional

import click
from click_default_group import DefaultGroup

from datahub.cli.cli_utils import get_url_and_token, post_entity
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, DataHubGraphConfig
from datahub.metadata.schema_classes import (
    DataPlatformInfoClass as DataPlatformInfo,
    PlatformTypeClass,
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
@click.pass_context
@upgrade.check_upgrade
@telemetry.with_telemetry
def aspect(ctx: Any, urn: str, aspect: str, aspect_data: str) -> None:
    """Update a single aspect of an entity"""

    entity_type = guess_entity_type(urn)
    with open(aspect_data) as fp:
        aspect_obj = json.load(fp)
        status = post_entity(
            urn=urn,
            aspect_name=aspect,
            entity_type=entity_type,
            aspect_value=aspect_obj,
        )
        click.secho(f"Update succeeded with status {status}", fg="green")


@put.command()
@click.pass_context
@upgrade.check_upgrade
@telemetry.with_telemetry
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
def platform(
    ctx: click.Context, name: str, display_name: Optional[str], logo: str
) -> None:
    """
    Create or update a dataplatform entity in DataHub
    """

    if name.startswith(f"urn:li:{DataPlatformUrn.ENTITY_TYPE}"):
        platform_urn = DataPlatformUrn.create_from_string(name)
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
    (url, token) = get_url_and_token()
    datahub_graph = DataHubGraph(DataHubGraphConfig(server=url, token=token))
    datahub_graph.emit(
        MetadataChangeProposalWrapper(
            entityUrn=str(platform_urn), aspect=data_platform_info
        )
    )
    click.echo(
        f"✅ Successfully wrote data platform metadata for {platform_urn} to DataHub ({datahub_graph})"
    )
