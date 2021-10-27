import json
import logging
import os.path
import sys
import typing
from datetime import datetime
from typing import List, Optional, Tuple

import click
import requests
import yaml
from pydantic import BaseModel, ValidationError

log = logging.getLogger(__name__)

DEFAULT_GMS_HOST = "http://localhost:8080"
CONDENSED_DATAHUB_CONFIG_PATH = "~/.datahubenv"
DATAHUB_CONFIG_PATH = os.path.expanduser(CONDENSED_DATAHUB_CONFIG_PATH)

ENV_SKIP_CONFIG = "DATAHUB_SKIP_CONFIG"
ENV_METADATA_HOST = "DATAHUB_GMS_HOST"
ENV_METADATA_TOKEN = "DATAHUB_GMS_TOKEN"


class GmsConfig(BaseModel):
    server: str
    token: Optional[str]


class DatahubConfig(BaseModel):
    gms: GmsConfig


def write_datahub_config(host: str, token: Optional[str]) -> None:
    config = {
        "gms": {
            "server": host,
            "token": token,
        }
    }
    with open(DATAHUB_CONFIG_PATH, "w+") as outfile:
        yaml.dump(config, outfile, default_flow_style=False)
    return None


def should_skip_config() -> bool:
    return os.getenv(ENV_SKIP_CONFIG, False) == "True"


def ensure_datahub_config() -> None:
    if not os.path.isfile(DATAHUB_CONFIG_PATH):
        click.secho(
            f"No {CONDENSED_DATAHUB_CONFIG_PATH} file found, generating one for you...",
            bold=True,
        )
        write_datahub_config(DEFAULT_GMS_HOST, None)


def get_details_from_config():
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
            return gms_host, gms_token
        except yaml.YAMLError as exc:
            click.secho(f"{DATAHUB_CONFIG_PATH} malformatted, error: {exc}", bold=True)
    return None, None


def get_details_from_env() -> Tuple[Optional[str], Optional[str]]:
    return os.environ.get(ENV_METADATA_HOST), os.environ.get(ENV_METADATA_TOKEN)


def first_non_null(ls: List[Optional[str]]) -> Optional[str]:
    return next((el for el in ls if el is not None and el.strip() != ""), None)


def get_session_and_host():
    session = requests.Session()

    gms_host_env, gms_token_env = get_details_from_env()
    if should_skip_config():
        gms_host = gms_host_env
        gms_token = gms_token_env
    else:
        ensure_datahub_config()
        gms_host_conf, gms_token_conf = get_details_from_config()
        gms_host = first_non_null([gms_host_env, gms_host_conf])
        gms_token = first_non_null([gms_token_env, gms_token_conf])

    if gms_host is None or gms_host.strip() == "":
        log.error(
            f"GMS Host is not set. Use datahub init command or set {ENV_METADATA_HOST} env var"
        )
        return None, None

    session.headers.update(
        {
            "X-RestLi-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        }
    )
    if isinstance(gms_token, str) and len(gms_token) > 0:
        session.headers.update(
            {"Authorization": f"Bearer {gms_token.format(**os.environ)}"}
        )

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
