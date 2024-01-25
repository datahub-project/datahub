import json
import logging
from pathlib import Path

import click
from click_default_group import DefaultGroup

from datahub.api.entities.dataset.dataset import Dataset
from datahub.ingestion.graph.client import get_default_graph
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="upsert")
def dataset() -> None:
    """A group of commands to interact with the Dataset entity in DataHub."""
    pass


@dataset.command(
    name="upsert",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
@telemetry.with_telemetry()
def upsert(file: Path) -> None:
    """Upsert attributes to a Dataset in DataHub."""

    with get_default_graph() as graph:
        for dataset in Dataset.from_yaml(str(file)):
            try:
                for mcp in dataset.generate_mcp():
                    graph.emit(mcp)
                click.secho(f"Update succeeded for urn {dataset.urn}.", fg="green")
            except Exception as e:
                click.secho(
                    f"Update failed for id {id}. due to {e}",
                    fg="red",
                )


@dataset.command(
    name="get",
)
@click.option("--urn", required=True, type=str)
@click.option("--to-file", required=False, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def get(urn: str, to_file: str) -> None:
    """Get a Dataset from DataHub"""

    if not urn.startswith("urn:li:dataset:"):
        urn = f"urn:li:dataset:{urn}"

    with get_default_graph() as graph:
        if graph.exists(urn):
            dataset: Dataset = Dataset.from_datahub(graph=graph, urn=urn)
            click.secho(
                f"{json.dumps(dataset.dict(exclude_unset=True, exclude_none=True), indent=2)}"
            )
            if to_file:
                dataset.to_yaml(Path(to_file))
                click.secho(f"Dataset yaml written to {to_file}", fg="green")
        else:
            click.secho(f"Dataset {urn} does not exist")
