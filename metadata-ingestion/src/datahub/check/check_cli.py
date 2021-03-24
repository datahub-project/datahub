import sys
import click

from datahub.check.json_file import check_mce_file
from datahub.check.docker import check_local_docker_containers


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
        click.secho("All containers are healthy", fg="green", bold=True)
    else:
        click.secho("The following issues were detected:", fg="bright_red", bold=True)
        for issue in issues:
            click.echo(f"- {issue}")
        sys.exit(1)
