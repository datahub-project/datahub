import json
import logging
import pathlib
from pathlib import Path
from typing import Optional

import click
from click_default_group import DefaultGroup

from datahub.api.entities.dataproduct.dataproduct import (
    DataProduct,
    DataProductGenerationConfig,
)
from datahub.cli.delete_cli import delete_one_urn_cmd, delete_references
from datahub.cli.specific.file_loader import load_file
from datahub.emitter.mce_builder import make_user_urn
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.schema_classes import OwnerClass, OwnershipTypeClass
from datahub.specific.dataproduct import DataProductPatchBuilder
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="upsert")
def dataproduct() -> None:
    """A group of commands to interact with the DataProduct entity in DataHub."""
    pass


@dataproduct.command(
    name="upsert",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@click.option(
    "--validate-assets/--no-validate-assets", required=False, is_flag=True, default=True
)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def upsert(file: Path, validate_assets: bool) -> None:
    """Create or Update a Data Product in DataHub"""

    config_dict = load_file(pathlib.Path(file))
    id = config_dict.get("id") if isinstance(config_dict, dict) else None
    with get_default_graph() as graph:
        data_product: DataProduct = DataProduct.from_yaml(file, graph)
        if validate_assets:
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
            for mcp in data_product.generate_mcp(
                generation_config=DataProductGenerationConfig(
                    validate_assets=validate_assets
                )
            ):
                graph.emit(mcp)
            click.secho(f"Update succeeded for urn {data_product.urn}.", fg="green")
        except Exception as e:
            click.secho(
                f"Update failed for id {id}. due to {e}",
                fg="red",
            )


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
            update_needed = data_product_remote.patch_yaml(
                file,
                data_product_local,
                Path(f"{str(file)}.update.yaml") if not update else file,
            )
            if not update_needed:
                click.secho(f"Update not needed for id {id}.", fg="green")
            else:
                click.secho(f"Update needed for id {id}", fg="red")

        except Exception:
            raise


@dataproduct.command(
    name="download",
)
@click.option("-f", "--file", required=True, type=click.Path())
@click.option("--urn", required=True, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def download(file: Path, urn: str) -> None:
    """Download a Data Product file from its twin in DataHub"""
    with get_default_graph() as graph:
        dataproduct: DataProduct = DataProduct.from_datahub(graph=graph, id=urn)
        dataproduct.to_yaml(file)


@dataproduct.command(
    name="delete",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
@telemetry.with_telemetry()
def delete(file: Path) -> None:
    """Delete a Data Product in DataHub"""

    with get_default_graph() as graph:
        data_product: DataProduct = DataProduct.from_yaml(file, graph)
        delete_references(data_product.urn)
        delete_one_urn_cmd(data_product.urn)
        click.secho(f"Data Product {data_product.urn} deleted")


@dataproduct.command(
    name="get",
)
@click.option("--urn", required=True, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def get(urn: str) -> None:
    """Get a Data Product from DataHub"""

    if not urn.startswith("urn:li:dataProduct:"):
        urn = f"urn:li:dataProduct:{urn}"

    with get_default_graph() as graph:
        if graph.exists(urn):
            result = graph.get_entity_raw(urn)
            click.secho(f"{json.dumps(result, indent=2)}", fg="green")
        else:
            click.secho(f"Data Product {urn} does not exist")


@dataproduct.command(
    name="set_description",
)
@click.option("--urn", required=True, type=str)
@click.argument("description", required=True, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def set_description(urn: str, description: str) -> None:
    """Set description for a Data Product in DataHub"""

    if not urn.startswith("urn:li:dataProduct:"):
        urn = f"urn:li:dataProduct:{urn}"
    dataproduct_patcher: DataProductPatchBuilder = DataProduct.get_patch_builder(urn)
    dataproduct_patcher.set_description(description)
    with get_default_graph() as graph:
        for mcp in dataproduct_patcher.build():
            graph.emit(mcp)


@dataproduct.command(
    name="add_owner",
)
@click.option("--urn", required=True, type=str)
@click.argument("owner_urn", required=True, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def add_owner(urn: str, owner_urn: str) -> None:
    """Add owner for a Data Product in DataHub"""

    if not urn.startswith("urn:li:dataProduct:"):
        urn = f"urn:li:dataProduct:{urn}"
    dataproduct_patcher: DataProductPatchBuilder = DataProduct.get_patch_builder(urn)
    dataproduct_patcher.add_owner(
        owner=OwnerClass(
            owner=make_user_urn(owner_urn), type=OwnershipTypeClass.TECHNICAL_OWNER
        )
    )
    with get_default_graph() as graph:
        for mcp in dataproduct_patcher.build():
            graph.emit(mcp)


@dataproduct.command(
    name="remove_owner",
)
@click.option("--urn", required=True, type=str)
@click.argument("owner_urn", required=True, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def remove_owner(urn: str, owner_urn: str) -> None:
    """Remove owner for a Data Product in DataHub"""

    if not urn.startswith("urn:li:dataProduct:"):
        urn = f"urn:li:dataProduct:{urn}"
    dataproduct_patcher: DataProductPatchBuilder = DataProduct.get_patch_builder(urn)
    dataproduct_patcher.remove_owner(owner=make_user_urn(owner_urn))
    with get_default_graph() as graph:
        for mcp in dataproduct_patcher.build():
            print(json.dumps(mcp.to_obj()))
            graph.emit(mcp)
