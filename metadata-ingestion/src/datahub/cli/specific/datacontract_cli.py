import logging
from typing import Optional

import click
from click_default_group import DefaultGroup

from datahub.api.entities.datacontract.datacontract import DataContract
from datahub.ingestion.graph.client import get_default_graph
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

    with get_default_graph() as graph:
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


'''
@datacontract.command(
    name="diff",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@click.option("--update", required=False, is_flag=True, default=False)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def diff(file: Path, update: bool) -> None:
    """Diff a Data Contract file with its twin in DataHub"""

    with get_default_graph() as emitter:
        id: Optional[str] = None
        try:
            data_product_local: DataProduct = DataProduct.from_yaml(file, emitter)
            id = data_product_local.id
            data_product_remote = DataProduct.from_datahub(
                emitter, data_product_local.urn
            )
            with NamedTemporaryFile(suffix=".yaml") as temp_fp:
                update_needed = data_product_remote.patch_yaml(
                    data_product_local,
                    Path(temp_fp.name),
                )
                if not update_needed:
                    click.secho(f"Update not needed for id {id}.", fg="green")
                else:
                    _print_diff(file, temp_fp.name)
                    if update:
                        # copy the temp file over to the main file
                        copyfile(temp_fp.name, file)
                        click.echo(f"Updated {file} successfully.")
                    else:
                        click.secho(f"Update needed for id {id}", fg="red")

        except Exception:
            raise
'''


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

    with get_default_graph() as graph:
        if not graph.exists(urn):
            raise ValueError(f"Data Contract {urn} does not exist")

        graph.delete_entity(urn, hard=hard)
        click.secho(f"Data Contract {urn} deleted")


'''
@datacontract.command()
@click.option("--urn", required=True, type=str)
@click.option("--to-file", required=False, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def get(urn: str, to_file: str) -> None:
    """Get a Data Contract from DataHub"""

    if not urn.startswith("urn:li:dataContract:"):
        urn = f"urn:li:dataContract:{urn}"

    with get_default_graph() as graph:
        # if graph.exists(urn):
        datacontract: DataContract = DataContract.from_datahub(graph=graph, urn=urn)
        click.secho(
            f"{json.dumps(datacontract.dict(exclude_unset=True, exclude_none=True), indent=2)}"
        )
        if to_file:
            datacontract.to_yaml(Path(to_file))
            click.secho(f"Data Contract yaml written to {to_file}", fg="green")
        # else:
        #     click.secho(f"Data Contract {urn} does not exist")
'''
