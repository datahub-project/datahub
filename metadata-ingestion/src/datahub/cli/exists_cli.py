import json
import logging
from typing import Any, Optional

import click
from click_default_group import DefaultGroup

from datahub.ingestion.graph.client import get_default_graph
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="urn")
def exists() -> None:
    """A group of commands to check existence of entities in DataHub."""
    pass


@exists.command()
@click.option("--urn", required=False, type=str)
@click.pass_context
@upgrade.check_upgrade
@telemetry.with_telemetry()
def urn(ctx: Any, urn: Optional[str]) -> None:
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
    click.echo(json.dumps(get_default_graph().exists(urn)))
