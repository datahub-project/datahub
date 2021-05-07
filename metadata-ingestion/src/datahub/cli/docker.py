import datetime
import subprocess
import sys
import tempfile
import time
from typing import List, NoReturn, Optional

import click

from datahub.cli.docker_check import check_local_docker_containers


@click.group()
def docker() -> None:
    pass


def _print_issue_list_and_exit(
    issues: List[str], header: str, footer: Optional[str] = None
) -> NoReturn:
    click.secho(header, fg="bright_red")
    for issue in issues:
        click.echo(f"- {issue}")
    if footer:
        click.echo()
        click.echo(footer)
    sys.exit(1)


def docker_check_impl() -> None:
    issues = check_local_docker_containers()
    if not issues:
        click.secho("✔ No issues detected", fg="green")
    else:
        _print_issue_list_and_exit(issues, "The following issues were detected:")


@docker.command()
def check() -> None:
    """Check that the Docker containers are healthy"""
    docker_check_impl()


@docker.command()
def quickstart() -> None:
    # Run pre-flight checks.
    issues = check_local_docker_containers(preflight_only=True)
    if issues:
        _print_issue_list_and_exit(issues, "Unable to run quickstart:")

    with tempfile.TemporaryDirectory() as workdir:
        # TODO pull file using requests
        workdir = "../docker"

        # Pull the latest containers.
        subprocess.run(["docker-compose", "pull"], cwd=workdir)

        start_time = datetime.datetime.now()
        sleep_interval = datetime.timedelta(seconds=2)
        up_interval = datetime.timedelta(minutes=1)
        up_attempts = 0
        while (datetime.datetime.now() - start_time) < datetime.timedelta(minutes=5):
            # Attempt to run docker-compose up every minute.
            if (datetime.datetime.now() - start_time) > up_attempts * up_interval:
                subprocess.run(
                    ["docker-compose", "-p", "datahub", "up", "-d"], cwd=workdir
                )
                up_attempts += 1

            # Check docker health every few seconds.
            issues = check_local_docker_containers()
            if not issues:
                break

            # Wait until next iteration.
            time.sleep(sleep_interval.total_seconds())
        else:
            # Falls through if the while loop doesn't exit via break.
            # TODO: save docker-compose logs somewhere
            _print_issue_list_and_exit(
                issues,
                header="Unable to run quickstart - the following issues were detected:",
                footer="If you think something went wrong, please file an issue at TODO",
            )

        # Handle success condition.
        click.secho("✔ DataHub is now running", fg="green")
        click.secho(
            "Ingest some demo data using `datahub docker ingest-sample-data`,\n"
            "or head to http://localhost:9002 to play around with the frontend.",
            fg="green",
        )


@docker.command()
def clean() -> None:
    # TODO this
    pass


@docker.command()
def ingest_sample_data() -> None:
    # TODO this
    pass
