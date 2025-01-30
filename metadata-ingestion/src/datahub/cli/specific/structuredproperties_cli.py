import json
import logging
from pathlib import Path
from typing import Iterable

import click
from click_default_group import DefaultGroup
from ruamel.yaml import YAML

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

    with get_default_graph() as graph:
        StructuredProperties.create(str(file), graph)


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


@properties.command(
    name="list",
)
@click.option("--details/--no-details", is_flag=True, default=True)
@click.option("--to-file", required=False, type=str)
@telemetry.with_telemetry()
def list(details: bool, to_file: str) -> None:
    """List structured properties in DataHub"""

    def to_yaml_list(
        objects: Iterable[StructuredProperties],  # iterable of objects to dump
        file: Path,
    ) -> None:
        # if file exists, first we read it
        yaml = YAML(typ="rt")  # default, if not specfied, is 'rt' (round-trip)
        yaml.indent(mapping=2, sequence=4, offset=2)
        yaml.default_flow_style = False
        serialized_objects = []
        if file.exists():
            with open(file, "r") as fp:
                existing_objects = yaml.load(fp)  # this is a list of dicts
                existing_objects = [
                    StructuredProperties.parse_obj(obj) for obj in existing_objects
                ]
                objects = [obj for obj in objects]
                # do a positional update of the existing objects
                existing_urns = {obj.urn for obj in existing_objects}
                # existing_urns = {obj["urn"] if "urn" in obj else f"urn:li:structuredProperty:{obj['id']}" for obj in existing_objects}
                for i, obj in enumerate(existing_objects):
                    # existing_urn = obj["urn"] if "urn" in obj else f"urn:li:structuredProperty:{obj['id']}"
                    existing_urn = obj.urn
                    # breakpoint()
                    if existing_urn in {obj.urn for obj in objects}:
                        existing_objects[i] = next(
                            obj.dict(exclude_unset=True, exclude_none=True)
                            for obj in objects
                            if obj.urn == existing_urn
                        )
                new_objects = [
                    obj.dict(exclude_unset=True, exclude_none=True)
                    for obj in objects
                    if obj.urn not in existing_urns
                ]
                serialized_objects = existing_objects + new_objects
        else:
            serialized_objects = [
                obj.dict(exclude_unset=True, exclude_none=True) for obj in objects
            ]

        with open(file, "w") as fp:
            yaml.dump(serialized_objects, fp)

    with get_default_graph() as graph:
        if details:
            logger.info(
                "Listing structured properties with details. Use --no-details for urns only"
            )
            structuredproperties = StructuredProperties.list(graph)
            if to_file:
                to_yaml_list(structuredproperties, Path(to_file))
            else:
                for structuredproperty in structuredproperties:
                    click.secho(
                        f"{json.dumps(structuredproperty.dict(exclude_unset=True, exclude_none=True), indent=2)}"
                    )
        else:
            logger.info(
                "Listing structured property urns only, use --details for more information"
            )
            structured_property_urns = StructuredProperties.list_urns(graph)
            if to_file:
                with open(to_file, "w") as f:
                    for urn in structured_property_urns:
                        f.write(f"{urn}\n")
                click.secho(
                    f"Structured property urns written to {to_file}", fg="green"
                )
            else:
                for urn in structured_property_urns:
                    click.secho(f"{urn}")
