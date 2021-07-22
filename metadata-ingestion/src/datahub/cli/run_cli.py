import json
from datetime import datetime
from tabulate import tabulate
import requests
import yaml
import click
import os.path


ELASTIC_MAX_PAGE_SIZE = 10000
CONDENSED_DATAHUB_CONFIG_PATH = "~/.datahubenv"
DATAHUB_CONFIG_PATH = os.path.expanduser(CONDENSED_DATAHUB_CONFIG_PATH)

DEFAULT_DATAHUB_CONFIG = {
    'gms': {
        'server': 'http://localhost:8080',
        'token': '',
    }
}

RUNS_TABLE_COLUMNS = ["runId", "rows", "created at"]
RUN_TABLE_COLUMNS = ["urn", "aspect name", "created at"]


def get_runs_url(gms_host: str):
    return f"{gms_host}/runs?action=rollback"


@click.group()
def run() -> None:
    """View and delete ingestion runs."""
    pass


def parse_restli_response(response):
    response_json = response.json()

    if not isinstance(response_json, dict):
        click.echo(f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo()
        click.echo(response_json)
        exit()

    rows = response_json.get('value')
    if not isinstance(rows, list):
        click.echo(f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo()
        click.echo(response_json)
        exit()

    return rows


def parse_run_restli_response(response):
    response_json = response.json()

    if not isinstance(response_json, dict):
        click.echo(f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo()
        click.echo(response_json)
        exit()

    summary = response_json.get('value')
    if not isinstance(summary, dict):
        click.echo(f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo()
        click.echo(response_json)
        exit()

    return summary


def print_datahub_env_format_guide():
    click.secho(f"datahub config ({CONDENSED_DATAHUB_CONFIG_PATH}) is malformed.", bold=True)
    click.echo("see expected format below...")
    click.echo()
    click.echo("gms:")
    click.echo("  server: <gms host>")
    click.echo("  token: <optional gms token>")


def get_session_and_host():
    session = requests.Session()

    gms_host = "http://localhost:8080"
    gms_token = None
    if not os.path.isfile(DATAHUB_CONFIG_PATH):
        click.secho(f"No {CONDENSED_DATAHUB_CONFIG_PATH} file found, generating one for you...", bold=True)

        with open(DATAHUB_CONFIG_PATH, 'w+') as outfile:
            yaml.dump(DEFAULT_DATAHUB_CONFIG, outfile, default_flow_style=False)

    with open(DATAHUB_CONFIG_PATH, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
            if not isinstance(config, dict):
                print_datahub_env_format_guide()
                exit()

            gms_config = config.get('gms')
            if not isinstance(gms_config, dict) or gms_config.get('server') is None:
                print_datahub_env_format_guide()
                exit()

            gms_host = gms_config.get('server')
            gms_token = gms_config.get('token')
        except yaml.YAMLError as exc:
            click.secho(f"{DATAHUB_CONFIG_PATH} malformatted, error: {exc}", bold=True)

    session.headers.update(
        {
            "X-RestLi-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        }
    )
    if isinstance(gms_token, str) and len(gms_token) > 0:
        session.headers.update({"Authorization": f"Bearer {gms_token}"})

    return session, gms_host


@run.command()
@click.argument("page_offset", type=int, default=0)
@click.argument("page_size", type=int, default=100)
def ls(
        page_offset: int,
        page_size: int
) -> None:
    session, gms_host = get_session_and_host()

    url = f"{gms_host}/runs?action=list"

    payload_obj = {
        "pageOffset": page_offset,
        "pageSize": page_size,
    }

    payload = json.dumps(payload_obj)

    response = session.post(url, data=payload)

    rows = parse_restli_response(response)

    structured_rows = [[
        row.get('runId'),
        row.get("rows"),
        datetime.utcfromtimestamp(row.get("timestamp") / 1000).strftime('%Y-%m-%d %H:%M:%S')
    ] for row in rows]

    click.echo(tabulate(structured_rows, RUNS_TABLE_COLUMNS, tablefmt="grid"))


def post_run_endpoint(payload_obj: dict):
    session, gms_host = get_session_and_host()
    url = get_runs_url(gms_host)

    payload = json.dumps(payload_obj)

    response = session.post(url, payload)

    summary = parse_run_restli_response(response)
    rows = summary.get("aspectRowSummaries")
    entities_affected = summary.get("entitiesAffected")
    aspects_affected = summary.get("aspectsAffected")

    if len(rows) == 0:
        click.echo("No entities touched by this run. Double check your run id?")

    structured_rows = [[
        row.get("urn"),
        row.get("aspectName"),
        datetime.utcfromtimestamp(row.get("timestamp") / 1000).strftime('%Y-%m-%d %H:%M:%S')
    ] for row in rows]

    return structured_rows, entities_affected, aspects_affected


@run.command()
@click.argument("run_id", type=str)
def show(run_id: str) -> None:
    payload_obj = {
        "runId": run_id,
        "dryRun": True
    }
    structured_rows, entities_affected, aspects_affected = post_run_endpoint(payload_obj)

    if aspects_affected >= ELASTIC_MAX_PAGE_SIZE:
        click.echo(f"this run created at least {entities_affected} new entities and updated at least {aspects_affected} aspects")
    else:
        click.echo(f"this run created {entities_affected} new entities and updated {aspects_affected} aspects")
    click.echo(f"rolling back will delete the entities created and revert the updated aspects")
    click.echo()
    click.echo(f"showing first {len(structured_rows)} of {aspects_affected} aspects touched by this run")
    click.echo(tabulate(structured_rows, RUN_TABLE_COLUMNS, tablefmt="grid"))


@run.command()
@click.argument("run_id", type=str)
def rollback(run_id: str) -> None:
    payload_obj = {
        "runId": run_id,
        "dryRun": False
    }
    structured_rows, entities_affected, aspects_affected = post_run_endpoint(payload_obj)

    click.echo(f"rolling back deletes the entities created by a run and reverts the updated aspects")
    click.echo(f"this rollback deleted {entities_affected} entities and rolled back {aspects_affected} aspects")
    click.echo(f"showing first {len(structured_rows)} of {aspects_affected} aspects reverted by this run")
    click.echo(tabulate(structured_rows, RUN_TABLE_COLUMNS, tablefmt="grid"))
