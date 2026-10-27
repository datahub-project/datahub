import logging
import pathlib
from pathlib import Path
from typing import List, Optional

import click
from click_default_group import DefaultGroup

from datahub.api.entities.agent.agent_skill import AgentSkill, SkillSourceRepository
from datahub.cli.specific.file_loader import load_file
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="upsert", name="agent-skill")
def agent_skill() -> None:
    """A group of commands to register AgentSkill entities in DataHub."""
    pass


@agent_skill.command(name="register")
@click.option("--id", "skill_id", required=True, type=str, help="Skill id or urn.")
@click.option("--name", required=True, type=str, help="Display name of the skill.")
@click.option("--description", required=False, type=str)
@click.option(
    "--instructions",
    required=False,
    type=str,
    help="Specialized prompts/context that define the skill.",
)
@click.option(
    "--source-repo",
    required=False,
    type=str,
    help="URL of the git repository containing the skill definition.",
)
@click.option(
    "--source-path",
    required=False,
    type=str,
    help="Path within the source repo to the skill definition.",
)
@click.option(
    "--requires-tool",
    "requires_tools",
    multiple=True,
    type=str,
    help="An API urn this skill requires (repeatable).",
)
@upgrade.check_upgrade
def register(
    skill_id: str,
    name: str,
    description: Optional[str],
    instructions: Optional[str],
    source_repo: Optional[str],
    source_path: Optional[str],
    requires_tools: tuple,
) -> None:
    """Register an AgentSkill in DataHub from command-line flags."""
    if source_path and not source_repo:
        raise click.BadParameter("--source-path requires --source-repo")
    source_repository: Optional[SkillSourceRepository] = (
        SkillSourceRepository(url=source_repo, path=source_path)
        if source_repo
        else None
    )
    required_tools: Optional[List[str]] = (
        list(requires_tools) if requires_tools else None
    )
    skill = AgentSkill(
        id=skill_id,
        name=name,
        description=description,
        instructions=instructions,
        source_repository=source_repository,
        required_tools=required_tools,
    )
    with get_default_graph(ClientMode.CLI) as graph:
        urn = skill.emit(graph)
    click.secho(f"Registered AgentSkill {urn}", fg="green")


@agent_skill.command(name="upsert")
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
def upsert(file: Path) -> None:
    """Upsert an AgentSkill in DataHub from a YAML file."""
    config_dict = load_file(pathlib.Path(file))
    if not isinstance(config_dict, dict):
        raise click.BadParameter(f"{file} must contain a single AgentSkill definition")
    skill = AgentSkill.parse_obj(config_dict)
    with get_default_graph(ClientMode.CLI) as graph:
        urn = skill.emit(graph)
    click.secho(f"Upserted AgentSkill {urn}", fg="green")
