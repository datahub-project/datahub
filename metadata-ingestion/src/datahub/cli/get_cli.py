import json
import logging
from typing import Any, List, Optional

import click
from click_default_group import DefaultGroup

from datahub.cli.cli_utils import get_aspects_for_entity
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="urn")
def get() -> None:
    """A group of commands to get metadata from DataHub."""
    pass


@get.command()
@click.option("--urn", required=False, type=str)
@click.option("-a", "--aspect", required=False, multiple=True, type=str)
@click.option(
    "--details/--no-details",
    required=False,
    is_flag=True,
    default=False,
    help="Whether to print details from database which help in audit.",
)
@click.option(
    "--pretty-document",
    is_flag=True,
    default=False,
    help="Render a Context Document for human reading. "
    "Without this flag stdout remains machine-readable JSON.",
)
@click.pass_context
@upgrade.check_upgrade
def urn(ctx: Any, urn: Optional[str], aspect: List[str], details: bool, pretty_document: bool) -> None:
    """
    Get metadata for an entity with an optional list of aspects to project.
    This works for both versioned aspects and timeseries aspects. For timeseries aspects, it fetches the latest value.
    """
    # We're using ctx.args here so that we can support `datahub get urn:li:...`
    # in addition to the `--urn` variant.

    if urn is None:
        if not ctx.args:
            raise click.UsageError("Nothing for me to get. Maybe provide an urn?")
        urn = ctx.args[0]
        logger.debug(f"Using urn from args {urn}")

    client = get_default_graph(ClientMode.CLI)

    if aspect:
        # If aspects are specified, we need to do the existence check first.
        if not client.exists(urn):
            raise click.ClickException(f"urn {urn} not found")

    aspect_data = get_aspects_for_entity(
        session=client._session,
        gms_host=client.config.server,
        entity_urn=urn,
        aspects=aspect,
        typed=False,
        details=details,
    )

    if pretty_document and not urn.startswith("urn:li:document:"):
        raise click.UsageError(
            "--pretty-document requires a urn:li:document:* URN"
        )

    if pretty_document and aspect:
        raise click.UsageError(
            "--pretty-document cannot be combined with --aspect"
        )

    if not aspect:
        # If no aspects are specified and we only get a key aspect back, yield an error instead.
        if len(aspect_data) == 1 and "key" in next(iter(aspect_data)).lower():
            raise click.ClickException(f"urn {urn} not found")

    # If it's a Context Document and the user didn't request a specific aspect, render it nicely
    if pretty_document and "documentInfo" in aspect_data:
        doc_info = aspect_data["documentInfo"].get("value", aspect_data["documentInfo"]) if isinstance(aspect_data["documentInfo"], dict) else {}
        title = doc_info.get("title", "Untitled Document")
        contents = doc_info.get("contents", {}).get("text")
        if contents:
            click.echo(click.style(f"\n# {title}", fg="cyan", bold=True))
            click.echo("\n" + contents + "\n")
            click.echo(click.style("--- (Metadata Below) ---", fg="blue", bold=True))

    click.echo(
        json.dumps(
            aspect_data,
            sort_keys=True,
            indent=2,
        )
    )
