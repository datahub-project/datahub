import json
import os
from typing import Dict

import click
from tabulate import tabulate

from datahub.cli.cli_utils import fixup_gms_url, generate_access_token
from datahub.cli.config_utils import load_client_config, persist_raw_datahub_config
from datahub.ingestion.graph.config import DatahubClientConfig

DATAHUB_SESSIONS_PATH = os.path.expanduser("~/.datahub/sessions.json")


def load_sessions() -> Dict[str, DatahubClientConfig]:
    if not os.path.exists(DATAHUB_SESSIONS_PATH):
        return {}
    with open(DATAHUB_SESSIONS_PATH, "r") as f:
        raw_sessions = json.load(f)
        return {
            name: DatahubClientConfig.parse_obj(config)
            for name, config in raw_sessions.items()
        }


def save_sessions(sessions: Dict[str, DatahubClientConfig]) -> None:
    os.makedirs(os.path.dirname(DATAHUB_SESSIONS_PATH), exist_ok=True)
    with open(DATAHUB_SESSIONS_PATH, "w") as f:
        json.dump(
            {name: config.dict() for name, config in sessions.items()}, f, indent=2
        )


@click.group()
def session() -> None:
    """Manage DataHub session profiles"""
    pass


@session.command()
@click.option(
    "--use-password",
    type=bool,
    is_flag=True,
    default=False,
    help="If passed then uses password to initialise token.",
)
def create(use_password: bool) -> None:
    """Create profile with which to connect to a DataHub instance"""

    sessions = load_sessions()

    click.echo(
        "Configure which datahub instance to connect to (https://your-instance.acryl.io/gms for Acryl hosted users)"
    )
    host = click.prompt(
        "Enter your DataHub host", type=str, default="http://localhost:8080"
    )
    host = fixup_gms_url(host)
    if use_password:
        username = click.prompt("Enter your DataHub username", type=str)
        password = click.prompt(
            "Enter your DataHub password",
            type=str,
        )
        _, token = generate_access_token(
            username=username, password=password, gms_url=host
        )
    else:
        token = click.prompt(
            "Enter your DataHub access token",
            type=str,
            default="",
        )

    profile_name = click.prompt("Enter name for profile", type=str)

    config = DatahubClientConfig(server=host, token=token)
    sessions[profile_name] = config
    save_sessions(sessions)
    click.echo(f"Created profile: {profile_name}")


@session.command()
def list() -> None:
    """List all session profiles"""
    sessions = load_sessions()
    if not sessions:
        click.echo("No profiles found")
        return

    headers = ["Profile", "URL"]
    table_data = [[name, config.server] for name, config in sessions.items()]
    click.echo(tabulate(table_data, headers=headers))


@session.command()
@click.argument("profile", type=str)
def delete(profile: str) -> None:
    """Delete a session profile"""
    sessions = load_sessions()
    if profile not in sessions:
        click.echo(f"Profile {profile} not found")
        return

    del sessions[profile]
    save_sessions(sessions)
    click.echo(f"Deleted profile: {profile}")


@session.command()
@click.argument("profile", type=str)
def use(profile: str) -> None:
    """Set the active session"""
    sessions = load_sessions()
    session = sessions.get(profile)
    if session:
        persist_raw_datahub_config(session.dict())
        click.echo(f"Using profile {profile}")
    else:
        click.echo(f"Profile {profile} not found")
        return


@session.command()
@click.option(
    "--profile",
    type=str,
    required=True,
    help="Name of profile under which to save the current datahubenv config",
)
def save(profile: str) -> None:
    """Save the current active datahubenv config as a session"""
    sessions = load_sessions()
    if profile in sessions:
        click.echo(
            f"Profile {profile} already exists, please make sure to use a unique profile name"
        )
        return

    config = load_client_config()
    sessions[profile] = config
    save_sessions(sessions)
    click.echo(f"Saved current datahubenv as profile: {profile}")
