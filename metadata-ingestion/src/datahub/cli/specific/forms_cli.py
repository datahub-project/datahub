import json
import logging
from pathlib import Path

import click
from click_default_group import DefaultGroup

from datahub.api.entities.forms.forms import Forms
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="upsert")
def forms() -> None:
    """A group of commands to interact with forms in DataHub."""
    pass


@forms.command(
    name="upsert",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
@telemetry.with_telemetry()
def upsert(file: Path) -> None:
    """Upsert forms in DataHub."""

    Forms.create(str(file))


@forms.command(
    name="get",
)
@click.option("--urn", required=True, type=str)
@click.option("--to-file", required=False, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def get(urn: str, to_file: str) -> None:
    """Get form from DataHub"""
    with get_default_graph(ClientMode.CLI) as graph:
        if graph.exists(urn):
            form: Forms = Forms.from_datahub(graph=graph, urn=urn)
            click.secho(
                f"{json.dumps(form.dict(exclude_unset=True, exclude_none=True), indent=2)}"
            )
            if to_file:
                form.to_yaml(Path(to_file))
                click.secho(f"Form yaml written to {to_file}", fg="green")
        else:
            click.secho(f"Form {urn} does not exist")
