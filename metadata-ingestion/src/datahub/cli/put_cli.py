import json
import logging
from typing import Any

import click

from datahub.cli.cli_utils import post_entity
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)


@click.command(
    name="put",
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
def put(ctx: Any, urn: str, aspect: str, aspect_data: str) -> None:
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
