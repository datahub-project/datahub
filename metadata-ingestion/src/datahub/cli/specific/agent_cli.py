import logging
import pathlib
from pathlib import Path
from typing import Dict, List, Optional, Union

import click
from click_default_group import DefaultGroup

from datahub.api.entities.agent.agent import (
    Agent,
    AgentOwner,
    StructuredPropertyValue,
)
from datahub.cli.specific.file_loader import load_file
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)

# Structured-property qualified names carrying framework + version. The Agent
# class wraps these bare ids in urn:li:structuredProperty:<qualifiedName>. The
# definitions must already exist (see examples/agents/fx_risk_scoring_agent.py);
# this command assigns values but does not create the definitions.
FRAMEWORK_PROPERTY = "io.acryl.agent.framework"
VERSION_PROPERTY = "io.acryl.agent.version"


@click.group(cls=DefaultGroup, default="upsert", name="agent")
def agent() -> None:
    """A group of commands to register AI Agent entities in DataHub."""
    pass


@agent.command(name="register")
@click.option("--id", "agent_id", required=True, type=str, help="Agent id or urn.")
@click.option("--name", required=True, type=str, help="Display name of the agent.")
@click.option("--description", required=False, type=str)
@click.option(
    "--instructions",
    required=False,
    type=str,
    help="Custom base instructions injected into the system prompt.",
)
@click.option(
    "--framework",
    required=False,
    type=str,
    help=(
        "Agent framework (e.g. LangChain). Assigned to the "
        f"'{FRAMEWORK_PROPERTY}' structured property, which must already exist."
    ),
)
@click.option(
    "--version",
    "framework_version",
    required=False,
    type=str,
    help=(
        "Framework version. Assigned to the "
        f"'{VERSION_PROPERTY}' structured property, which must already exist."
    ),
)
@click.option(
    "--owner",
    "owners",
    multiple=True,
    type=str,
    help="An owner urn or bare user/group id (repeatable).",
)
@click.option(
    "--domain",
    required=False,
    type=str,
    help="Domain the agent belongs to (name or urn).",
)
@click.option(
    "--skill",
    "skills",
    multiple=True,
    type=str,
    help="An AgentSkill urn the agent adopts (repeatable).",
)
@click.option(
    "--tool",
    "tools",
    multiple=True,
    type=str,
    help="An API urn the agent invokes (repeatable).",
)
@click.option(
    "--model",
    "models",
    multiple=True,
    type=str,
    help="An mlModel urn the agent runs on (repeatable).",
)
@click.option(
    "--consumes-dataset",
    "consumes_datasets",
    multiple=True,
    type=str,
    help="A dataset urn the agent consumes, modeled as upstream (repeatable).",
)
@upgrade.check_upgrade
def register(
    agent_id: str,
    name: str,
    description: Optional[str],
    instructions: Optional[str],
    framework: Optional[str],
    framework_version: Optional[str],
    owners: tuple,
    domain: Optional[str],
    skills: tuple,
    tools: tuple,
    models: tuple,
    consumes_datasets: tuple,
) -> None:
    """Register an AI Agent in DataHub from command-line flags."""
    structured_properties: Dict[str, StructuredPropertyValue] = {}
    if framework:
        structured_properties[FRAMEWORK_PROPERTY] = framework
    if framework_version:
        structured_properties[VERSION_PROPERTY] = framework_version

    owner_list: Optional[List[Union[str, AgentOwner]]] = (
        list(owners) if owners else None
    )
    new_agent = Agent(
        id=agent_id,
        name=name,
        description=description,
        instructions=instructions,
        structured_properties=structured_properties or None,
        owners=owner_list,
        domain=domain,
        skills=list(skills) if skills else None,
        tools=list(tools) if tools else None,
        models=list(models) if models else None,
        consumes_datasets=list(consumes_datasets) if consumes_datasets else None,
    )
    with get_default_graph(ClientMode.CLI) as graph:
        urn = new_agent.emit(graph)
    click.secho(f"Registered AI Agent {urn}", fg="green")


@agent.command(name="upsert")
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
def upsert(file: Path) -> None:
    """Upsert an AI Agent in DataHub from a YAML file."""
    config_dict = load_file(pathlib.Path(file))
    if not isinstance(config_dict, dict):
        raise click.BadParameter(f"{file} must contain a single Agent definition")
    new_agent = Agent.parse_obj(config_dict)
    with get_default_graph(ClientMode.CLI) as graph:
        urn = new_agent.emit(graph)
    click.secho(f"Upserted AI Agent {urn}", fg="green")
