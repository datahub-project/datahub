import shutil
import tempfile

import click

from datahub import __package_name__
from datahub.cli.cli_utils import get_url_and_token
from datahub.cli.json_file import check_mce_file
from datahub.graph_consistency import check_data_platform
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.transformer.transform_registry import transform_registry


@click.group()
def check() -> None:
    """Helper commands for checking various aspects of DataHub."""
    pass


@check.command()
@click.argument("json-file", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--rewrite",
    default=False,
    is_flag=True,
    help="Rewrite the JSON file to it's canonical form.",
)
def metadata_file(json_file: str, rewrite: bool) -> None:
    """Check the schema of a metadata (MCE or MCP) JSON file."""

    if not rewrite:
        report = check_mce_file(json_file)
        click.echo(report)

    else:
        with tempfile.NamedTemporaryFile() as out_file:
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "file",
                        "config": {"filename": json_file},
                        "extractor": "generic",
                        "extractor_config": {"set_system_metadata": False},
                    },
                    "sink": {
                        "type": "file",
                        "config": {"filename": out_file.name},
                    },
                }
            )

            pipeline.run()
            pipeline.raise_from_status()

            shutil.copy(out_file.name, json_file)


@check.command()
@click.option(
    "--verbose",
    "-v",
    type=bool,
    is_flag=True,
    default=False,
    help="Include extra information for each plugin.",
)
def plugins(verbose: bool) -> None:
    """List the enabled ingestion plugins."""

    click.secho("Sources:", bold=True)
    click.echo(source_registry.summary(verbose=verbose))
    click.echo()
    click.secho("Sinks:", bold=True)
    click.echo(sink_registry.summary(verbose=verbose))
    click.echo()
    click.secho("Transformers:", bold=True)
    click.echo(transform_registry.summary(verbose=verbose, col_width=30))
    click.echo()
    if not verbose:
        click.echo("For details on why a plugin is disabled, rerun with '--verbose'")
    click.echo(
        f"If a plugin is disabled, try running: pip install '{__package_name__}[<plugin>]'"
    )


@check.command
def graph_consistency() -> None:
    gms_endpoint, gms_token = get_url_and_token()
    check_data_platform.check(gms_endpoint, gms_token)
