import json
import copy
from math import log
import os
import click
import logging
from sqlalchemy import null
from tabulate import tabulate

from datahub.cli.cli_utils import fixup_gms_url, generate_access_token
from datahub.cli.config_utils import load_client_config, persist_raw_datahub_config
from datahub.ingestion.graph.config import DatahubClientConfig

logger = logging.getLogger(__name__)
DATAHUB_SESSIONS_PATH = os.path.expanduser("~/.datahub/sessions.json")

from typing import Optional, Type, Any
import click

def parse_value(value: str, expected_type: Optional[Type] = None) -> tuple[Any, Optional[str]]:
    
    if not value:
        return None, None
    if expected_type is None:
        return value, None
     
    try:
        if expected_type == str:
            return value, None
        elif expected_type == float:
            return float(value), None
        elif expected_type == bool:
            return bool(value), None
        elif expected_type == list:
            return [item.strip() for item in value.split(',') if item.strip()], None
        elif expected_type == dict:
            pairs = [pair.strip().split(':') for pair in value.split(',') if pair.strip()]
            return {k.strip(): v.strip() for k, v in pairs}, None
        else:
            return None, f"Unsupported type: {expected_type}"
    except Exception as e:
        return None, str(e)


def dynamic_prompt(prompt_text: str, expected_type: Optional[Type] = None, default: Optional[object] = None) -> Any:
    type_hint = {
        str: "text",
        float: "number", 
        bool: "boolean",
        list: "comma-separated values",
        dict: "key1:value1,key2:value2",
        None: "any value"
    }.get(expected_type, "value")
    
    if default is not None:    
       prompt_text += f" [{str(default)}]" 
    
    while True:
        if default is not None:
            value = click.prompt(prompt_text, default=default, show_default=False)
        else:
            value = click.prompt(prompt_text, default='', show_default=False) 
        result, error = parse_value(value, expected_type)
        
        if error and value:  # Only show error and retry if there was input
            click.echo(f"Error: {error}")
            retry = click.confirm("Would you like to try again?", default=True)
            if not retry:
                return None
        else:
            return result

def load_sessions() -> dict[str, DatahubClientConfig]:
    if not os.path.exists(DATAHUB_SESSIONS_PATH):
        return {}
    with open(DATAHUB_SESSIONS_PATH, "r") as f:
        raw_sessions = json.load(f)
        return {
            name: DatahubClientConfig.parse_obj(config)
            for name, config in raw_sessions.items()
        }


def save_sessions(sessions: dict[str, DatahubClientConfig]) -> None:
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

    # TODO if profile_name already exists, cancel the create, ask the customer to delete the old one or update instead
    config = DatahubClientConfig(server=host, token=token)
    sessions[profile_name] = config
    save_sessions(sessions)
    click.echo(f"Created profile: {profile_name}")


@session.command()
@click.option(
    "-p",
    "--profile",
    type=str,
    required=True,
    help="Name of profile to delete",
)
def update(profile: str) -> None:
    """Update a session profile"""
    sessions = load_sessions()
    if profile not in sessions:
        click.echo(f"Profile {profile} not found")
        return

    config: DatahubClientConfig = sessions[profile]
    config_copy: DatahubClientConfig = copy.deepcopy(config)
    for key, value in config_copy.dict().items():
        # Get type information for each field in DatahubClientConfig so we can generate
        # a dynamic, type-safe click prompt for it.
        field_type = config_copy.__fields__.get(key).type_
        new_value = dynamic_prompt(f"Enter new value for {key}", field_type, value)
        config_copy.__setattr__(key, new_value)

    sessions[profile] = config_copy
    save_sessions(sessions)
    click.echo(f"Updated profile: {profile}")
 

@session.command()
@click.option(
    "-p",
    "--profile",
    type=str,
    required=True,
    help="Name of profile to delete",
)
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
@click.option(
    "-p",
    "--profile",
    type=str,
    required=True,
    help="Name of profile to use",
)
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
    "-p",
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
