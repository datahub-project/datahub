import filecmp
import json
import logging
import os
import shutil
from pathlib import Path
from typing import List, Set, Tuple

import click
from click_default_group import DefaultGroup

from datahub.api.entities.dataset.dataset import Dataset, DatasetRetrievalConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.metadata.com.linkedin.pegasus2avro.common import Siblings
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="upsert")
def dataset() -> None:
    """A group of commands to interact with the Dataset entity in DataHub."""
    pass


@dataset.command(
    name="upsert",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@click.option(
    "-n", "--dry-run", type=bool, is_flag=True, default=False, help="Perform a dry run"
)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def upsert(file: Path, dry_run: bool) -> None:
    """Upsert attributes to a Dataset in DataHub."""
    # Call the sync command with to_datahub=True to perform the upsert operation
    ctx = click.get_current_context()
    ctx.invoke(sync, file=str(file), dry_run=dry_run, to_datahub=True)


@dataset.command(
    name="get",
)
@click.option("--urn", required=True, type=str)
@click.option("--to-file", required=False, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def get(urn: str, to_file: str) -> None:
    """Get a Dataset from DataHub"""

    if not urn.startswith("urn:li:dataset:"):
        urn = f"urn:li:dataset:{urn}"

    with get_default_graph(ClientMode.CLI) as graph:
        if graph.exists(urn):
            dataset: Dataset = Dataset.from_datahub(graph=graph, urn=urn)
            click.secho(
                f"{json.dumps(dataset.dict(exclude_unset=True, exclude_none=True), indent=2)}"
            )
            if to_file:
                dataset.to_yaml(Path(to_file))
                click.secho(f"Dataset yaml written to {to_file}", fg="green")
        else:
            click.secho(f"Dataset {urn} does not exist")


@dataset.command()
@click.option("--urn", required=True, type=str, help="URN of primary sibling")
@click.option(
    "--sibling-urns",
    required=True,
    type=str,
    help="URN of secondary sibling(s)",
    multiple=True,
)
@telemetry.with_telemetry()
def add_sibling(urn: str, sibling_urns: Tuple[str]) -> None:
    all_urns = set()
    all_urns.add(urn)
    for sibling_urn in sibling_urns:
        all_urns.add(sibling_urn)
    with get_default_graph(ClientMode.CLI) as graph:
        for _urn in all_urns:
            _emit_sibling(graph, urn, _urn, all_urns)


def _emit_sibling(
    graph: DataHubGraph, primary_urn: str, urn: str, all_urns: Set[str]
) -> None:
    siblings = _get_existing_siblings(graph, urn)
    for sibling_urn in all_urns:
        if sibling_urn != urn:
            siblings.add(sibling_urn)
    graph.emit(
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=Siblings(primary=primary_urn == urn, siblings=sorted(siblings)),
        )
    )


def _get_existing_siblings(graph: DataHubGraph, urn: str) -> Set[str]:
    existing = graph.get_aspect(urn, Siblings)
    if existing:
        return set(existing.siblings)
    else:
        return set()


@dataset.command(
    name="file",
)
@click.option("--lintCheck", required=False, is_flag=True)
@click.option("--lintFix", required=False, is_flag=True)
@click.argument("file", type=click.Path(exists=True))
@upgrade.check_upgrade
@telemetry.with_telemetry()
def file(lintcheck: bool, lintfix: bool, file: str) -> None:
    """Operate on a Dataset file"""

    if lintcheck or lintfix:
        import tempfile
        from pathlib import Path

        # Create a temporary file in a secure way
        # The file will be automatically deleted when the context manager exits
        with tempfile.NamedTemporaryFile(suffix=".yml", delete=False) as temp:
            temp_path = Path(temp.name)
            try:
                # Copy content to the temporary file
                shutil.copyfile(file, temp_path)

                # Run the linting
                datasets = Dataset.from_yaml(temp.name)
                for dataset in datasets:
                    dataset.to_yaml(temp_path)

                # Compare the files
                files_match = filecmp.cmp(file, temp_path)

                if files_match:
                    click.secho("No differences found", fg="green")
                else:
                    # Show diff for visibility
                    os.system(f"diff {file} {temp_path}")

                    if lintfix:
                        shutil.copyfile(temp_path, file)
                        click.secho(f"Fixed linting issues in {file}", fg="green")
                    else:
                        click.secho(
                            f"To fix these differences, run 'datahub dataset file --lintFix {file}'",
                            fg="yellow",
                        )
            finally:
                # Ensure the temporary file is removed
                if temp_path.exists():
                    temp_path.unlink()
    else:
        click.secho(
            "No operation specified. Choose from --lintCheck or --lintFix", fg="yellow"
        )


@dataset.command(
    name="sync",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@click.option("--to-datahub/--from-datahub", required=True, is_flag=True)
@click.option(
    "-n", "--dry-run", type=bool, is_flag=True, default=False, help="Perform a dry run"
)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def sync(file: str, to_datahub: bool, dry_run: bool) -> None:
    """Sync a Dataset file to/from DataHub"""

    dry_run_prefix = "[dry-run]: " if dry_run else ""  # prefix to use in messages

    failures: List[str] = []
    with get_default_graph(ClientMode.CLI) as graph:
        datasets = Dataset.from_yaml(file)
        for dataset in datasets:
            assert (
                dataset.urn is not None
            )  # Validator should have ensured this is filled. Tell mypy it's not None
            if to_datahub:
                missing_entity_references = [
                    entity_reference
                    for entity_reference in dataset.entity_references()
                    if not graph.exists(entity_reference)
                ]
                if missing_entity_references:
                    click.secho(
                        "\n\t- ".join(
                            [
                                f"{dry_run_prefix}Skipping Dataset {dataset.urn} due to missing entity references: "
                            ]
                            + missing_entity_references
                        ),
                        fg="red",
                    )
                    failures.append(dataset.urn)
                    continue
                try:
                    for mcp in dataset.generate_mcp():
                        if not dry_run:
                            graph.emit(mcp)
                    click.secho(
                        f"{dry_run_prefix}Update succeeded for urn {dataset.urn}.",
                        fg="green",
                    )
                except Exception as e:
                    click.secho(
                        f"{dry_run_prefix}Update failed for id {id}. due to {e}",
                        fg="red",
                    )
                    failures.append(dataset.urn)
            else:
                # Sync from DataHub
                if graph.exists(dataset.urn):
                    dataset_get_config = DatasetRetrievalConfig()
                    if dataset.downstreams:
                        dataset_get_config.include_downstreams = True
                    existing_dataset: Dataset = Dataset.from_datahub(
                        graph=graph, urn=dataset.urn, config=dataset_get_config
                    )
                    if not dry_run:
                        existing_dataset.to_yaml(Path(file))
                    else:
                        click.secho(f"{dry_run_prefix}Will update file {file}")
                else:
                    click.secho(f"{dry_run_prefix}Dataset {dataset.urn} does not exist")
                    failures.append(dataset.urn)
    if failures:
        click.secho(
            f"\n{dry_run_prefix}Failed to sync the following Datasets: {', '.join(failures)}",
            fg="red",
        )
        raise click.Abort()
