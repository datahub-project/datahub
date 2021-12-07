import logging

import click

from datahub.cli import cli_utils

logger = logging.getLogger(__name__)

ELASTIC_MAX_PAGE_SIZE = 10000

RUNS_TABLE_COLUMNS = ["runId", "rows", "created at"]
RUN_TABLE_COLUMNS = ["urn", "aspect name", "created at"]


@click.command()
@click.option("--urn", required=True, type=str)
@click.option("-f", "--force", required=False, type=bool, default=False)
def delete(urn: str, force: bool) -> None:
    """Delete a provided URN from datahub"""
    if not force:
        click.confirm(
            "This will permanently delete data from DataHub. Do you want to continue?",
            abort=True,
        )

    payload_obj = {"urn": urn}

    urn, rows_affected = cli_utils.post_delete_endpoint(
        payload_obj, "/entities?action=delete"
    )

    if rows_affected == 0:
        click.echo(f"Nothing deleted for {urn}")
    else:
        click.echo(f"Successfully deleted {urn}. {rows_affected} rows deleted")
