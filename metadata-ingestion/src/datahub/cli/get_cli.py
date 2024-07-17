import json
import logging
from typing import Any, List, Optional

import click
from click_default_group import DefaultGroup

from datahub.cli.cli_utils import get_aspects_for_entity
from datahub.telemetry import telemetry
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
@click.pass_context
@upgrade.check_upgrade
@telemetry.with_telemetry()
def urn(ctx: Any, urn: Optional[str], aspect: List[str], details: bool) -> None:
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
    click.echo(
        json.dumps(
            get_aspects_for_entity(
                entity_urn=urn, aspects=aspect, typed=False, details=details
            ),
            sort_keys=True,
            indent=2,
        )
    )
