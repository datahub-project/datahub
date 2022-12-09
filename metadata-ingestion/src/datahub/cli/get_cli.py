import json
import logging
from typing import Any, List, Optional

import click

from datahub.cli.cli_utils import get_aspects_for_entity
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.command(
    name="get",
    context_settings=dict(
        ignore_unknown_options=False,
        allow_extra_args=True,
    ),
)
@click.option("--urn", required=False, type=str)
@click.option("-a", "--aspect", required=False, multiple=True, type=str)
@click.pass_context
@upgrade.check_upgrade
@telemetry.with_telemetry
def get(ctx: Any, urn: Optional[str], aspect: List[str]) -> None:
    """
    Get metadata for an entity with an optional list of aspects to project.
    This works for both versioned aspects and timeseries aspects. For timeseries aspects, it fetches the latest value.
    """

    if urn is None:
        if not ctx.args:
            raise click.UsageError("Nothing for me to get. Maybe provide an urn?")
        urn = ctx.args[0]
        logger.debug(f"Using urn from args {urn}")
    click.echo(
        json.dumps(
            get_aspects_for_entity(entity_urn=urn, aspects=aspect, typed=False),
            sort_keys=True,
            indent=2,
        )
    )
