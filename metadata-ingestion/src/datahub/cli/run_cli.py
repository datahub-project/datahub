import json
from datetime import datetime
from tabulate import tabulate
import requests
import yaml
import click
import os.path

CONDENSED_DATAHUB_CONFIG_PATH = "~/.datahubenv"
DATAHUB_CONFIG_PATH = os.path.expanduser(CONDENSED_DATAHUB_CONFIG_PATH)

DEFAULT_DATAHUB_CONFIG = {
    'gms': {
        'server': 'http://localhost:8080',
        'token': '',
    }
}

RUN_TABLE_COLUMNS = ["runId", "rows", "created at"]
SHOW_URNS_COLUMNS = ["URN"]


def get_runs_url(gms_host: str, rollback: bool):
    return f"{gms_host}/runs?action={'rollback' if rollback else 'ghost'}"


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
@click.argument("min_run_size", type=int, default=0)
@click.argument("max_run_size", type=int, default=-1)
@click.argument("page_offset", type=int, default=0)
@click.argument("page_size", type=int, default=100)
def ls(
        min_run_size: int,
        max_run_size: int,
        page_offset: int,
        page_size: int
) -> None:
    session, gms_host = get_session_and_host()

    url = f"{gms_host}/runs?action=list"

    payload_obj = {
        "minRunSize": min_run_size,
        "pageOffset": page_offset,
        "pageSize": page_size,
    }

    if max_run_size > 0:
        payload_obj['maxRunSize'] = max_run_size

    payload = json.dumps(payload_obj)

    response = session.post(url, data=payload)

    rows = parse_restli_response(response)

    structured_rows = [[
        row.get('runId'),
        row.get("rows"),
        datetime.utcfromtimestamp(row.get("timestamp") / 1000).strftime('%Y-%m-%d %H:%M:%S')
    ] for row in rows]

    click.echo(tabulate(structured_rows, RUN_TABLE_COLUMNS, tablefmt="grid"))


@run.command()
@click.argument("run_id", type=str)
@click.option('--show-aspects/--show-urns', default=False)
def show(
        run_id: str,
        show_aspects: bool
) -> None:
    session, gms_host = get_session_and_host()
    url = get_runs_url(gms_host, show_aspects)

    payload_obj = {
        "runId": run_id,
        "dryRun": True
    }
    payload = json.dumps(payload_obj)

    response = session.post(url, payload)

    rows = parse_restli_response(response)
    if len(rows) == 0:
        click.echo("No entities touched by this run. Double check your run id?")

    if show_aspects:
        click.echo(f"SHOWING FIRST {len(rows)} URNS TOUCHED BY THIS RUN")
        click.echo(tabulate(rows, SHOW_URNS_COLUMNS, tablefmt="grid"))

    click.echo(response.json())
