import json
import logging
import pathlib
import sys
from datetime import datetime

import click
from click_default_group import DefaultGroup
from pydantic import ValidationError
from tabulate import tabulate

import datahub as datahub_package
from datahub.cli.cli_utils import (
    CONDENSED_DATAHUB_CONFIG_PATH,
    get_session_and_host,
    post_rollback_endpoint,
)
from datahub.configuration.config_loader import load_config_file
from datahub.ingestion.run.pipeline import Pipeline

logger = logging.getLogger(__name__)

ELASTIC_MAX_PAGE_SIZE = 10000

RUNS_TABLE_COLUMNS = ["runId", "rows", "created at"]
RUN_TABLE_COLUMNS = ["urn", "aspect name", "created at"]


@click.group(cls=DefaultGroup)
def ingest() -> None:
    """Ingest metadata into DataHub."""
    pass


@ingest.command(default=True)
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    help="Config file in .toml or .yaml format.",
    required=True,
)
def run(config: str) -> None:
    """Ingest metadata into DataHub."""
    logger.debug("DataHub CLI version: %s", datahub_package.nice_version_name())

    config_file = pathlib.Path(config)
    pipeline_config = load_config_file(config_file)

    try:
        logger.debug(f"Using config: {pipeline_config}")
        pipeline = Pipeline.create(pipeline_config)
    except ValidationError as e:
        click.echo(e, err=True)
        sys.exit(1)

    logger.info("Starting metadata ingestion")
    pipeline.run()
    logger.info("Finished metadata ingestion")
    ret = pipeline.pretty_print_summary()
    sys.exit(ret)


def get_runs_url(gms_host: str) -> str:
    return f"{gms_host}/runs?action=rollback"


def parse_restli_response(response):
    response_json = response.json()

    if not isinstance(response_json, dict):
        click.echo(f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo()
        click.echo(response_json)
        exit()

    rows = response_json.get("value")
    if not isinstance(rows, list):
        click.echo(f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo()
        click.echo(response_json)
        exit()

    return rows


@ingest.command()
@click.argument("page_offset", type=int, default=0)
@click.argument("page_size", type=int, default=100)
def list_runs(page_offset: int, page_size: int) -> None:
    """List recent ingestion runs to datahub"""
    session, gms_host = get_session_and_host()

    url = f"{gms_host}/runs?action=list"

    payload_obj = {
        "pageOffset": page_offset,
        "pageSize": page_size,
    }

    payload = json.dumps(payload_obj)

    response = session.post(url, data=payload)

    rows = parse_restli_response(response)

    structured_rows = [
        [
            row.get("runId"),
            row.get("rows"),
            datetime.utcfromtimestamp(row.get("timestamp") / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        ]
        for row in rows
    ]

    click.echo(tabulate(structured_rows, RUNS_TABLE_COLUMNS, tablefmt="grid"))


@ingest.command()
@click.option("--run-id", required=True, type=str)
def show(run_id: str) -> None:
    """Describe a provided ingestion run to datahub"""
    payload_obj = {"runId": run_id, "dryRun": True}

    structured_rows, entities_affected, aspects_affected = post_rollback_endpoint(
        payload_obj, "/runs?action=rollback"
    )

    if aspects_affected >= ELASTIC_MAX_PAGE_SIZE:
        click.echo(
            f"this run created at least {entities_affected} new entities and updated at least {aspects_affected} aspects"
        )
    else:
        click.echo(
            f"this run created {entities_affected} new entities and updated {aspects_affected} aspects"
        )
    click.echo(
        "rolling back will delete the entities created and revert the updated aspects"
    )
    click.echo()
    click.echo(
        f"showing first {len(structured_rows)} of {aspects_affected} aspects touched by this run"
    )
    click.echo(tabulate(structured_rows, RUN_TABLE_COLUMNS, tablefmt="grid"))


@ingest.command()
@click.option("--run-id", required=True, type=str)
def rollback(run_id: str) -> None:
    """Rollback a provided ingestion run to datahub"""
    click.confirm(
        "This will permanently delete data from DataHub. Do you want to continue?",
        abort=True,
    )

    payload_obj = {"runId": run_id, "dryRun": False}
    structured_rows, entities_affected, aspects_affected = post_rollback_endpoint(
        payload_obj, "/runs?action=rollback"
    )

    click.echo(
        "rolling back deletes the entities created by a run and reverts the updated aspects"
    )
    click.echo(
        f"this rollback deleted {entities_affected} entities and rolled back {aspects_affected} aspects"
    )
    click.echo(
        f"showing first {len(structured_rows)} of {aspects_affected} aspects reverted by this run"
    )
    click.echo(tabulate(structured_rows, RUN_TABLE_COLUMNS, tablefmt="grid"))
