import logging

import click

from datahub.cli.cli_utils import post_delete_endpoint

logger = logging.getLogger(__name__)

ELASTIC_MAX_PAGE_SIZE = 10000

RUNS_TABLE_COLUMNS = ["runId", "rows", "created at"]
RUN_TABLE_COLUMNS = ["urn", "aspect name", "created at"]


@click.command()
@click.option("--urn", required=True, type=str)
def delete(urn: str) -> None:
    """Delete a provided URN from datahub"""
    click.confirm(
        "This will permanently delete data from DataHub. Do you want to continue?",
        abort=True,
    )

    payload_obj = {"urn": urn}

    urn, rows_affected = post_delete_endpoint(payload_obj, "/entities?action=delete")

    click.echo(f"Successfully deleted {urn}. {rows_affected} rows deleted")
