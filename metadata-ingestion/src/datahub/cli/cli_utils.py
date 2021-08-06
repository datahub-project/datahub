import json
import os.path
import sys
import typing
from datetime import datetime
from typing import Optional

import click
import requests
import yaml
from pydantic import BaseModel, ValidationError

CONDENSED_DATAHUB_CONFIG_PATH = "~/.datahubenv"
DATAHUB_CONFIG_PATH = os.path.expanduser(CONDENSED_DATAHUB_CONFIG_PATH)

DEFAULT_DATAHUB_CONFIG = {
    "gms": {
        "server": "http://localhost:8080",
        "token": "",
    }
}


class GmsConfig(BaseModel):
    server: str
    token: Optional[str]


class DatahubConfig(BaseModel):
    gms: GmsConfig


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
            config_json = yaml.safe_load(stream)
            try:
                datahub_config = DatahubConfig(**config_json)
            except ValidationError as e:
                click.echo(
                    f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}"
                )
                click.echo(e, err=True)
                sys.exit(1)

            gms_config = datahub_config.gms

            gms_host = gms_config.server
            gms_token = gms_config.token
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


def post_rollback_endpoint(
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


def post_delete_endpoint(
    payload_obj: dict,
    path: str,
) -> typing.Tuple[str, int]:
    session, gms_host = get_session_and_host()
    url = gms_host + path

    payload = json.dumps(payload_obj)

    response = session.post(url, payload)

    summary = parse_run_restli_response(response)
    urn = summary.get("urn")
    rows_affected = summary.get("rows")

    return urn, rows_affected
