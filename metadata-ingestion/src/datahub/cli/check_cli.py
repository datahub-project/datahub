import click

from datahub import __package_name__
from datahub.cli.docker import docker_check_impl
from datahub.cli.json_file import check_mce_file
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.transformer.transform_registry import transform_registry


@click.group()
def check() -> None:
    """Helper commands for checking various aspects of DataHub."""
    pass


@check.command()
@click.argument("json-file", type=click.Path(exists=True, dir_okay=False))
def mce_file(json_file: str) -> None:
    """Check the schema of a MCE JSON file."""

    report = check_mce_file(json_file)
    click.echo(report)


@check.command()
def local_docker() -> None:
    """Check that the local Docker containers are healthy. (deprecated)"""
    click.secho("DeprecationWarning: use `datahub docker check` instead", fg="yellow")
    docker_check_impl()


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
