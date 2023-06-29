import difflib
import json
import logging
import os
import pathlib
import sys
from pathlib import Path
from shutil import copyfile
from tempfile import NamedTemporaryFile
from typing import Optional

import click
from click_default_group import DefaultGroup

from datahub.api.entities.dataproduct.dataproduct import DataProduct
from datahub.cli.specific.file_loader import load_file
from datahub.emitter.mce_builder import make_group_urn, make_user_urn
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import OwnerClass, OwnershipTypeClass
from datahub.specific.dataproduct import DataProductPatchBuilder
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


def _get_owner_urn(maybe_urn: str) -> str:
    if make_user_urn(maybe_urn) == maybe_urn or make_group_urn(maybe_urn) == maybe_urn:
        # already a valid identity in the right form
        return maybe_urn
    elif maybe_urn.startswith("urn:li:"):
        # this looks like an urn, but not a type we recognize
        raise Exception(
            f"Owner urn {maybe_urn} not recognized as one of the supported types (corpuser, corpGroup)"
        )
    else:
        # mint a user urn as the default
        return make_user_urn(maybe_urn)


def _abort_if_non_existent_urn(graph: DataHubGraph, urn: str, operation: str) -> None:
    try:
        parsed_urn: Urn = Urn.create_from_string(urn)
        entity_type = parsed_urn.get_type()
    except Exception:
        click.secho(f"Provided urn {urn} does not seem valid", fg="red")
        raise click.Abort()
    else:
        if not graph.exists(urn):
            click.secho(
                f"{entity_type.title()} {urn} does not exist. Will not {operation}.",
                fg="red",
            )
            raise click.Abort()


def _print_diff(orig_file, new_file):

    with open(orig_file) as fp:
        orig_lines = fp.readlines()
    with open(new_file) as fp:
        new_lines = fp.readlines()

    sys.stdout.writelines(
        difflib.unified_diff(orig_lines, new_lines, orig_file, new_file)
    )


@click.group(cls=DefaultGroup, default="upsert")
def dataproduct() -> None:
    """A group of commands to interact with the DataProduct entity in DataHub."""
    pass


def mutate(file: Path, validate_assets: bool, external_url: str, upsert: bool) -> None:
    """Update or Upsert a Data Product in DataHub"""

    config_dict = load_file(pathlib.Path(file))
    id = config_dict.get("id") if isinstance(config_dict, dict) else None
    with get_default_graph() as graph:
        data_product: DataProduct = DataProduct.from_yaml(file, graph)
        external_url_override = (
            external_url
            or os.getenv("DATAHUB_DATAPRODUCT_EXTERNAL_URL")
            or data_product.external_url
        )
        data_product.external_url = external_url_override
        if upsert and not graph.exists(data_product.urn):
            logger.info(f"Data Product {data_product.urn} does not exist, will create.")
            upsert = False

        if validate_assets and data_product.assets:
            missing_assets = []
            for asset in data_product.assets:
                try:
                    assert graph.exists(asset)
                except Exception as e:
                    logger.debug("Failed to validate existence", exc_info=e)
                    missing_assets.append(asset)
            if missing_assets:
                for a in missing_assets:
                    click.secho(f"Asset: {a} doesn't exist on DataHub", fg="red")
                click.secho(
                    "Aborting update due to the presence of missing assets in the yaml file. Turn off validation of assets using the --no-validate-assets option if you want to proceed.",
                    fg="red",
                )
                raise click.Abort()
        try:
            for mcp in data_product.generate_mcp(upsert=upsert):
                graph.emit(mcp)
            click.secho(f"Update succeeded for urn {data_product.urn}.", fg="green")
        except Exception as e:
            click.secho(
                f"Update failed for id {id}. due to {e}",
                fg="red",
            )


@dataproduct.command(
    name="update",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@click.option(
    "--validate-assets/--no-validate-assets", required=False, is_flag=True, default=True
)
@click.option("--external-url", required=False, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def update(file: Path, validate_assets: bool, external_url: str) -> None:
    """Create or Update a Data Product in DataHub. Use upsert if you want to apply partial updates."""

    mutate(file, validate_assets, external_url, upsert=False)


@dataproduct.command(
    name="upsert",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@click.option(
    "--validate-assets/--no-validate-assets", required=False, is_flag=True, default=True
)
@click.option("--external-url", required=False, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def upsert(file: Path, validate_assets: bool, external_url: str) -> None:
    """Upsert attributes to a Data Product in DataHub."""

    mutate(file, validate_assets, external_url, upsert=True)


@dataproduct.command(
    name="diff",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@click.option("--update", required=False, is_flag=True, default=False)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def diff(file: Path, update: bool) -> None:
    """Diff a Data Product file with its twin in DataHub"""

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


@dataproduct.command(
    name="delete",
)
@click.option(
    "--urn", required=False, type=str, help="The urn for the data product to delete"
)
@click.option(
    "-f",
    "--file",
    required=False,
    type=click.Path(exists=True),
    help="The file containing the data product definition",
)
@click.option("--hard/--soft", required=False, is_flag=True, default=False)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def delete(urn: str, file: Path, hard: bool) -> None:
    """Delete a Data Product in DataHub. Defaults to a soft-delete. Use --hard to completely erase metadata."""

    if not urn and not file:
        click.secho(
            "Must provide either an urn or a file to delete a data product", fg="red"
        )
        raise click.Abort()

    graph: DataHubGraph
    with get_default_graph() as graph:
        data_product_urn = (
            urn if urn.startswith("urn:li:dataProduct") else f"urn:li:dataProduct:{urn}"
        )
        if not urn:
            data_product: DataProduct = DataProduct.from_yaml(file, graph)
            data_product_urn = data_product.urn

        _abort_if_non_existent_urn(graph, data_product_urn, "delete")

        if hard:
            # we only delete references if this is a hard delete
            graph.delete_references_to_urn(data_product_urn)

        graph.delete_entity(data_product_urn, hard=hard)

        click.secho(f"Data Product {data_product_urn} deleted")


@dataproduct.command(
    name="get",
)
@click.option("--urn", required=True, type=str)
@click.option("--to-file", required=False, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def get(urn: str, to_file: str) -> None:
    """Get a Data Product from DataHub"""

    if not urn.startswith("urn:li:dataProduct:"):
        urn = f"urn:li:dataProduct:{urn}"

    with get_default_graph() as graph:
        if graph.exists(urn):
            dataproduct: DataProduct = DataProduct.from_datahub(graph=graph, id=urn)
            click.secho(
                f"{json.dumps(dataproduct.dict(exclude_unset=True, exclude_none=True), indent=2)}"
            )
            if to_file:
                dataproduct.to_yaml(Path(to_file))
                click.secho(f"Data Product yaml written to {to_file}", fg="green")
        else:
            click.secho(f"Data Product {urn} does not exist")


@dataproduct.command(
    name="set_description",
)
@click.option("--urn", required=True, type=str)
@click.option(
    "--description",
    required=False,
    type=str,
    help="Inline documentation for this data product",
)
@click.option(
    "--md-file",
    required=False,
    type=click.Path(exists=True),
    help="A markdown file that contains documentation for this data product",
)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def set_description(urn: str, description: str, md_file: Path) -> None:
    """Set description for a Data Product in DataHub"""

    if not urn.startswith("urn:li:dataProduct:"):
        urn = f"urn:li:dataProduct:{urn}"

    if description is None and not md_file:
        click.secho(
            "Need one of --description or --md-file provided to populate description field",
            fg="red",
        )
        raise click.Abort()

    if description and md_file:
        click.secho(
            "Need only one of --description or --md-file provided to populate description field. You provided both.",
            fg="red",
        )
        raise click.Abort()

    if md_file:
        logger.info(f"Opening file {md_file} for populating description")
        with open(md_file) as fp:
            description = fp.read()

    dataproduct_patcher: DataProductPatchBuilder = DataProduct.get_patch_builder(urn)
    dataproduct_patcher.set_description(description)
    with get_default_graph() as graph:
        _abort_if_non_existent_urn(graph, urn, "set description")
        for mcp in dataproduct_patcher.build():
            graph.emit(mcp)


@dataproduct.command(name="add_owner", help="Add an owner to a Data Product")
@click.option("--urn", required=True, type=str)
@click.option("--owner", required=True, type=str)
@click.option(
    "--owner-type",
    required=False,
    type=click.Choice(
        [
            OwnershipTypeClass.BUSINESS_OWNER,
            OwnershipTypeClass.TECHNICAL_OWNER,
            OwnershipTypeClass.DATA_STEWARD,
        ],
        case_sensitive=False,
    ),
    default=OwnershipTypeClass.TECHNICAL_OWNER,
)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def add_owner(urn: str, owner: str, owner_type: str) -> None:
    """Add owner for a Data Product in DataHub"""

    if not urn.startswith("urn:li:dataProduct:"):
        urn = f"urn:li:dataProduct:{urn}"
    dataproduct_patcher: DataProductPatchBuilder = DataProduct.get_patch_builder(urn)
    dataproduct_patcher.add_owner(
        owner=OwnerClass(owner=_get_owner_urn(owner), type=owner_type)
    )
    with get_default_graph() as graph:
        _abort_if_non_existent_urn(graph, urn, "add owners")
        for mcp in dataproduct_patcher.build():
            graph.emit(mcp)


@dataproduct.command(name="remove_owner", help="Remove an owner from a Data Product")
@click.option("--urn", required=True, type=str)
@click.argument("owner_urn", required=True, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def remove_owner(urn: str, owner_urn: str) -> None:
    """Remove owner for a Data Product in DataHub"""

    if not urn.startswith("urn:li:dataProduct:"):
        urn = f"urn:li:dataProduct:{urn}"
    dataproduct_patcher: DataProductPatchBuilder = DataProduct.get_patch_builder(urn)
    dataproduct_patcher.remove_owner(owner=_get_owner_urn(owner_urn))
    with get_default_graph() as graph:
        _abort_if_non_existent_urn(graph, urn, "remove owners")
        for mcp in dataproduct_patcher.build():
            print(json.dumps(mcp.to_obj()))
            graph.emit(mcp)


@dataproduct.command(name="add_asset", help="Add an asset to a Data Product")
@click.option("--urn", required=True, type=str)
@click.option("--asset", required=True, type=str)
@click.option(
    "--validate-assets/--no-validate-assets", required=False, is_flag=True, default=True
)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def add_asset(urn: str, asset: str, validate_assets: bool) -> None:
    """Add asset for a Data Product in DataHub"""

    if not urn.startswith("urn:li:dataProduct:"):
        urn = f"urn:li:dataProduct:{urn}"
    dataproduct_patcher: DataProductPatchBuilder = DataProduct.get_patch_builder(urn)
    dataproduct_patcher.add_asset(asset)
    with get_default_graph() as graph:
        _abort_if_non_existent_urn(graph, urn, "add assets")
        if validate_assets:
            _abort_if_non_existent_urn(
                graph,
                asset,
                "add assets. Use --no-validate-assets if you want to turn off validation",
            )
        for mcp in dataproduct_patcher.build():
            graph.emit(mcp)


@dataproduct.command(name="remove_asset", help="Add an asset to a Data Product")
@click.option("--urn", required=True, type=str)
@click.option("--asset", required=True, type=str)
@click.option(
    "--validate-assets/--no-validate-assets", required=False, is_flag=True, default=True
)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def remove_asset(urn: str, asset: str, validate_assets: bool) -> None:
    """Remove asset for a Data Product in DataHub"""

    if not urn.startswith("urn:li:dataProduct:"):
        urn = f"urn:li:dataProduct:{urn}"
    dataproduct_patcher: DataProductPatchBuilder = DataProduct.get_patch_builder(urn)
    dataproduct_patcher.remove_asset(asset)
    with get_default_graph() as graph:
        _abort_if_non_existent_urn(graph, urn, "remove assets")
        if validate_assets:
            _abort_if_non_existent_urn(
                graph,
                asset,
                "remove assets. Use --no-validate-assets if you want to turn off validation",
            )
        for mcp in dataproduct_patcher.build():
            graph.emit(mcp)
