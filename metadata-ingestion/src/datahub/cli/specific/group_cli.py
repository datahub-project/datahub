import logging
from pathlib import Path

import click
from click_default_group import DefaultGroup

from datahub.api.entities.corpgroup.corpgroup import (
    CorpGroup,
    CorpGroupGenerationConfig,
)
from datahub.cli.specific.file_loader import load_file
from datahub.ingestion.graph.client import get_default_graph
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="upsert")
def group() -> None:
    """A group of commands to interact with the Group entity in DataHub."""
    pass


@group.command(
    name="upsert",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@click.option(
    "--override-editable",
    required=False,
    is_flag=True,
    default=False,
    help="When set, writes to the editable section of the metadata graph, overwriting writes from the UI",
)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def upsert(file: Path, override_editable: bool) -> None:
    """Create or Update a Group with embedded Users"""

    config_dict = load_file(file)
    group_configs = config_dict if isinstance(config_dict, list) else [config_dict]
    with get_default_graph() as emitter:
        for group_config in group_configs:
            try:
                datahub_group = CorpGroup.parse_obj(config_dict)
                for mcp in datahub_group.generate_mcp(
                    generation_config=CorpGroupGenerationConfig(
                        override_editable=override_editable, datahub_graph=emitter
                    )
                ):
                    emitter.emit(mcp)
                click.secho(
                    f"Update succeeded for group {datahub_group.urn}.", fg="green"
                )
            except Exception as e:
                click.secho(
                    f"Update failed for id {group_config.get('id')}. due to {e}",
                    fg="red",
                )
