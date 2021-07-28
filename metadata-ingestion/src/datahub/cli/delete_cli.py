import logging

import click
from tabulate import tabulate

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

    structured_rows, entities_affected, aspects_affected = post_delete_endpoint(
        payload_obj, "/entities?action=delete"
    )

    click.echo(
        f"showing first {len(structured_rows)} of {aspects_affected} aspects reverted by this run"
    )
    click.echo(tabulate(structured_rows, RUN_TABLE_COLUMNS, tablefmt="grid"))
