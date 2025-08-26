import logging
from typing import List

import click
from click_default_group import DefaultGroup
from tabulate import tabulate

from datahub.ingestion.graph.client import DataHubGraph, get_default_graph

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="query")
def relationships() -> None:
    """Query relationships metadata from DataHub.

    See `datahub relationships query`.

    """
    pass


@relationships.command(no_args_is_help=True)
@click.option(
    "--urn",
    required=True,
    type=str,
    help="URN of the entity to query relationships for",
)
@click.option(
    "--relationship-types",
    "-t",
    type=str,
    multiple=True,
    help="Relationship types to query",
)
def query(urn: str, relationship_types: List[str]) -> None:
    """Query relationships metadata from DataHub.

    URN is the URN of the entity to query relationships for.

    """
    # telemetry.emit_payload({"cli": "relationships", "command": "query"})
    with get_default_graph() as graph:
        incoming = graph.get_related_entities(
            urn,
            relationship_types=[t for t in relationship_types],
            direction=DataHubGraph.RelationshipDirection.INCOMING,
        )
        click.echo("Incoming relationships:")
        click.echo(tabulate(incoming, headers="keys", tablefmt="outline"))  # type: ignore
        click.echo("Outgoing relationships:")
        outgoing = graph.get_related_entities(
            urn,
            relationship_types=[t for t in relationship_types],
            direction=DataHubGraph.RelationshipDirection.OUTGOING,
        )
        click.echo(tabulate(outgoing, headers="keys", tablefmt="outline"))  # type: ignore
