import sys

import click

from datahub import __package_name__
from datahub.check.docker import check_local_docker_containers
from datahub.check.json_file import check_mce_file
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry


@click.group()
def check() -> None:
    pass


@check.command()
@click.argument("json-file", type=click.Path(exists=True, dir_okay=False))
def mce_file(json_file: str) -> None:
    """Check the schema of a MCE JSON file"""

    report = check_mce_file(json_file)
    click.echo(report)


@check.command()
def local_docker() -> None:
    """Check that the local Docker containers are healthy"""

    issues = check_local_docker_containers()
    if not issues:
        click.secho("✔ No issues detected", fg="green")
    else:
        click.secho("The following issues were detected:", fg="bright_red")
        for issue in issues:
            click.echo(f"- {issue}")
        sys.exit(1)


@check.command()
@click.option(
    "--verbose",
    type=bool,
    is_flag=True,
    default=False,
    help="Include extra information for each plugin",
)
def plugins(verbose: bool) -> None:
    """Check the enabled ingestion plugins"""

    click.secho("Sources:", bold=True)
    click.echo(source_registry.summary(verbose=verbose))
    click.echo()
    click.secho("Sinks:", bold=True)
    click.echo(sink_registry.summary(verbose=verbose))
    click.echo()
    if not verbose:
        click.echo("For details on why a plugin is disabled, rerun with '--verbose'")
    click.echo(
        f"If a plugin is disabled, try running: pip install '{__package_name__}[<plugin>]'"
    )
