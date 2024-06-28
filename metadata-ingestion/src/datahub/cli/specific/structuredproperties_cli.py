import json
import logging
from pathlib import Path

import click
from click_default_group import DefaultGroup

from datahub.api.entities.structuredproperties.structuredproperties import (
    StructuredProperties,
)
from datahub.ingestion.graph.client import get_default_graph
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="upsert")
def properties() -> None:
    """A group of commands to interact with structured properties in DataHub."""
    pass


@properties.command(
    name="upsert",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
@telemetry.with_telemetry()
def upsert(file: Path) -> None:
    """Upsert structured properties in DataHub."""

    StructuredProperties.create(str(file))


@properties.command(
    name="get",
)
@click.option("--urn", required=True, type=str)
@click.option("--to-file", required=False, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def get(urn: str, to_file: str) -> None:
    """Get structured properties from DataHub"""
    urn = Urn.make_structured_property_urn(urn)

    with get_default_graph() as graph:
        if graph.exists(urn):
            structuredproperties: StructuredProperties = (
                StructuredProperties.from_datahub(graph=graph, urn=urn)
            )
            click.secho(
                f"{json.dumps(structuredproperties.dict(exclude_unset=True, exclude_none=True), indent=2)}"
            )
            if to_file:
                structuredproperties.to_yaml(Path(to_file))
                click.secho(
                    f"Structured property yaml written to {to_file}", fg="green"
                )
        else:
            click.secho(f"Structured property {urn} does not exist")
