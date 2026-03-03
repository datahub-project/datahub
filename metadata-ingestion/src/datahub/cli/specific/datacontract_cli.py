import logging
import warnings
from typing import Optional

import click
from click_default_group import DefaultGroup

from datahub.api.entities.datacontract.datacontract import DataContract
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="upsert")
def datacontract() -> None:
    """
    A group of commands to interact with the DataContract entity in DataHub.

    WARNING: This CLI is DEPRECATED and no longer supported.
    Please migrate to alternative data contract solutions.
    """
    # Issue deprecation warning
    warnings.warn(
        "The datacontract CLI is deprecated and no longer supported. "
        "Please migrate to alternative data contract solutions.",
        DeprecationWarning,
        stacklevel=2,
    )

    # Log deprecation message for runtime visibility
    logger.warning(
        "DEPRECATED: The datacontract CLI is no longer supported and will be removed in a future version. "
        "Please migrate to alternative data contract solutions."
    )

    # Display deprecation message to user
    click.secho(
        "⚠️  WARNING: This datacontract CLI is DEPRECATED and no longer supported.",
        fg="yellow",
        bold=True,
    )
    click.secho("Please migrate to alternative data contract solutions.", fg="yellow")


@datacontract.command()
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
def upsert(file: str) -> None:
    """
    Upsert (create or update) a Data Contract in DataHub.

    WARNING: This command is DEPRECATED and no longer supported.
    """

    click.secho(
        "⚠️  WARNING: The 'upsert' command is deprecated and no longer supported.",
        fg="yellow",
        bold=True,
    )

    logger.warning("DEPRECATED: datacontract upsert command is no longer supported")

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
def delete(urn: Optional[str], file: Optional[str], hard: bool) -> None:
    """
    Delete a Data Contract in DataHub. Defaults to a soft-delete. Use --hard to completely erase metadata.

    WARNING: This command is DEPRECATED and no longer supported.
    """

    click.secho(
        "⚠️  WARNING: The 'delete' command is deprecated and no longer supported.",
        fg="yellow",
        bold=True,
    )

    logger.warning("DEPRECATED: datacontract delete command is no longer supported")

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
