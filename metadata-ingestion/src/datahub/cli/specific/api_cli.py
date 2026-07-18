import logging
import pathlib
from pathlib import Path
from typing import List, Optional

import click
from click_default_group import DefaultGroup

from datahub.api.entities.agent.api import Api, ApiParam
from datahub.cli.specific.file_loader import load_file
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)

_SUBTYPE_CHOICES = [
    "MCP_TOOL",
    "REST_ENDPOINT",
    "GRPC_METHOD",
    "GRAPHQL_FIELD",
    "FUNCTION",
    "AGENT",
    "UI_TOOL",
    "BUILTIN",
]


def _parse_param(raw: str) -> ApiParam:
    """Parse a ``NAME:TYPE[:required]`` flag value into an :class:`ApiParam`.

    ``order_id:string:required`` -> required param; ``order_id:string`` ->
    optional; ``order_id`` -> optional string.
    """
    parts = raw.split(":")
    name = parts[0].strip()
    if not name:
        raise click.BadParameter(f"parameter spec {raw!r} is missing a name")
    data_type = parts[1].strip() if len(parts) > 1 and parts[1].strip() else "string"
    required = len(parts) > 2 and parts[2].strip().lower() == "required"
    return ApiParam(name=name, data_type=data_type, required=required)


def _parse_return(raw: str) -> ApiParam:
    """Parse a ``NAME:TYPE`` flag value into an output :class:`ApiParam`."""
    parts = raw.split(":")
    name = parts[0].strip()
    if not name:
        raise click.BadParameter(f"returns spec {raw!r} is missing a name")
    data_type = parts[1].strip() if len(parts) > 1 and parts[1].strip() else "string"
    return ApiParam(name=name, data_type=data_type, required=True)


@click.group(cls=DefaultGroup, default="upsert", name="api")
def api() -> None:
    """A group of commands to register API entities in DataHub."""
    pass


@api.command(name="register")
@click.option("--id", "api_id", required=True, type=str, help="API id or urn.")
@click.option("--name", required=True, type=str, help="Display name of the API.")
@click.option(
    "--subtype",
    "subtypes",
    multiple=True,
    type=click.Choice(_SUBTYPE_CHOICES, case_sensitive=False),
    help="API subtype (repeatable). Defaults to MCP_TOOL.",
)
@click.option("--description", required=False, type=str)
@click.option(
    "--param",
    "params",
    multiple=True,
    type=str,
    metavar="NAME:TYPE[:required]",
    help="An input parameter (repeatable), e.g. order_id:string:required.",
)
@click.option(
    "--returns",
    "returns",
    multiple=True,
    type=str,
    metavar="NAME:TYPE",
    help="An output field (repeatable), e.g. order:object.",
)
@click.option(
    "--schema-definition",
    required=False,
    type=str,
    help="Opaque input/output schema (typically JSON Schema).",
)
@click.option("--external-url", required=False, type=str)
@upgrade.check_upgrade
def register(
    api_id: str,
    name: str,
    subtypes: tuple,
    description: Optional[str],
    params: tuple,
    returns: tuple,
    schema_definition: Optional[str],
    external_url: Optional[str],
) -> None:
    """Register an API in DataHub from command-line flags."""
    parameters: Optional[List[ApiParam]] = (
        [_parse_param(p) for p in params] if params else None
    )
    output_fields: Optional[List[ApiParam]] = (
        [_parse_return(r) for r in returns] if returns else None
    )
    resolved_subtypes = [s.upper() for s in subtypes] if subtypes else ["MCP_TOOL"]
    entity = Api(
        id=api_id,
        name=name,
        subtypes=resolved_subtypes,
        description=description,
        parameters=parameters,
        returns=output_fields,
        schema_definition=schema_definition,
        external_url=external_url,
    )
    with get_default_graph(ClientMode.CLI) as graph:
        urn = entity.emit(graph)
    click.secho(f"Registered API {urn}", fg="green")


@api.command(name="upsert")
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
def upsert(file: Path) -> None:
    """Upsert an API in DataHub from a YAML file."""
    config_dict = load_file(pathlib.Path(file))
    if not isinstance(config_dict, dict):
        raise click.BadParameter(f"{file} must contain a single API definition")
    entity = Api.parse_obj(config_dict)
    with get_default_graph(ClientMode.CLI) as graph:
        urn = entity.emit(graph)
    click.secho(f"Upserted API {urn}", fg="green")
