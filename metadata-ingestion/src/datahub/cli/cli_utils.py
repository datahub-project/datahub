import json
import os.path
import typing
from datetime import datetime

import click
import requests
import yaml

CONDENSED_DATAHUB_CONFIG_PATH = "~/.datahubenv"
DATAHUB_CONFIG_PATH = os.path.expanduser(CONDENSED_DATAHUB_CONFIG_PATH)

DEFAULT_DATAHUB_CONFIG = {
    "gms": {
        "server": "http://localhost:8080",
        "token": "",
    }
}


def print_datahub_env_format_guide():
    click.secho(
        f"datahub config ({CONDENSED_DATAHUB_CONFIG_PATH}) is malformed.", bold=True
    )
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
        click.secho(
            f"No {CONDENSED_DATAHUB_CONFIG_PATH} file found, generating one for you...",
            bold=True,
        )

        with open(DATAHUB_CONFIG_PATH, "w+") as outfile:
            yaml.dump(DEFAULT_DATAHUB_CONFIG, outfile, default_flow_style=False)

    with open(DATAHUB_CONFIG_PATH, "r") as stream:
        try:
            config = yaml.safe_load(stream)
            if not isinstance(config, dict):
                print_datahub_env_format_guide()
                exit()

            gms_config = config.get("gms")
            if not isinstance(gms_config, dict) or gms_config.get("server") is None:
                print_datahub_env_format_guide()
                exit()

            gms_host = gms_config.get("server", "")
            gms_token = gms_config.get("token")
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


def parse_run_restli_response(response):
    response_json = response.json()

    if not isinstance(response_json, dict):
        click.echo(f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo()
        click.echo(response_json)
        exit()

    summary = response_json.get("value")
    if not isinstance(summary, dict):
        click.echo(f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo()
        click.echo(response_json)
        exit()

    return summary


def post_delete_endpoint(
    payload_obj: dict,
    path: str,
) -> typing.Tuple[typing.List[typing.List[str]], int, int]:
    session, gms_host = get_session_and_host()
    url = gms_host + path

    payload = json.dumps(payload_obj)

    response = session.post(url, payload)

    summary = parse_run_restli_response(response)
    rows = summary.get("aspectRowSummaries")
    entities_affected = summary.get("entitiesAffected")
    aspects_affected = summary.get("aspectsAffected")

    if len(rows) == 0:
        click.echo("No entities touched by this run. Double check your run id?")

    structured_rows = [
        [
            row.get("urn"),
            row.get("aspectName"),
            datetime.utcfromtimestamp(row.get("timestamp") / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        ]
        for row in rows
    ]

    return structured_rows, entities_affected, aspects_affected
