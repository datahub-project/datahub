import logging
from typing import Optional

import click
from click_default_group import DefaultGroup

from datahub.api.entities.datacontract.datacontract import DataContract
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="upsert")
def datacontract() -> None:
    """A group of commands to interact with the DataContract entity in DataHub."""
    pass


@datacontract.command()
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
@telemetry.with_telemetry()
def upsert(file: str) -> None:
    """Upsert (create or update) a Data Contract in DataHub."""

    data_contract: DataContract = DataContract.from_yaml(file)
    urn = data_contract.urn

    with get_default_graph(ClientMode.CLI) as graph:
        if not graph.exists(data_contract.entity):
            raise ValueError(
                f"Cannot define a data contract for non-existent entity {data_contract.entity}"
            )

        try:
            for mcp in data_contract.generate_mcp():
                graph.emit(mcp)
            click.secho(f"Update succeeded for urn {urn}.", fg="green")
        except Exception as e:
            logger.exception(e)
            click.secho(
                f"Update failed for {urn}: {e}",
                fg="red",
            )


@datacontract.command()
@click.option(
    "--urn", required=False, type=str, help="The urn for the data contract to delete"
)
@click.option(
    "-f",
    "--file",
    required=False,
    type=click.Path(exists=True),
    help="The file containing the data contract definition",
)
@click.option("--hard/--soft", required=False, is_flag=True, default=False)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def delete(urn: Optional[str], file: Optional[str], hard: bool) -> None:
    """Delete a Data Contract in DataHub. Defaults to a soft-delete. Use --hard to completely erase metadata."""

    if not urn:
        if not file:
            raise click.UsageError(
                "Must provide either an urn or a file to delete a data contract"
            )

        data_contract = DataContract.from_yaml(file)
        urn = data_contract.urn

    with get_default_graph(ClientMode.CLI) as graph:
        if not graph.exists(urn):
            raise ValueError(f"Data Contract {urn} does not exist")

        graph.delete_entity(urn, hard=hard)
        click.secho(f"Data Contract {urn} deleted")
