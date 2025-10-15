import logging
import pathlib
from pathlib import Path

import click
from click_default_group import DefaultGroup

from datahub.api.entities.corpuser.corpuser import CorpUser, CorpUserGenerationConfig
from datahub.cli.specific.file_loader import load_file
from datahub.configuration.common import OperationalError
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="upsert")
def user() -> None:
    """A group of commands to interact with the User entity in DataHub."""
    pass


@user.command(
    name="upsert",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@click.option(
    "--override-editable",
    required=False,
    default=False,
    is_flag=True,
    help="Use this flag to overwrite the information that is set via the UI",
)
@upgrade.check_upgrade
def upsert(file: Path, override_editable: bool) -> None:
    """Create or Update a User in DataHub"""

    config_dict = load_file(pathlib.Path(file))
    user_configs = config_dict if isinstance(config_dict, list) else [config_dict]
    with get_default_graph(ClientMode.CLI) as emitter:
        for user_config in user_configs:
            try:
                datahub_user: CorpUser = CorpUser.parse_obj(user_config)

                emitter.emit_all(
                    datahub_user.generate_mcp(
                        generation_config=CorpUserGenerationConfig(
                            override_editable=override_editable
                        )
                    )
                )
                click.secho(f"Update succeeded for urn {datahub_user.urn}.", fg="green")
            except Exception as e:
                click.secho(
                    f"Update failed for id {user_config.get('id')}. due to {e}",
                    fg="red",
                )


@user.command(name="add")
@click.option("--email", required=True, type=str, help="User's email address")
@click.option(
    "--display-name", required=True, type=str, help="User's full display name"
)
@click.option(
    "--password",
    is_flag=True,
    default=False,
    help="Prompt for password (hidden input)",
)
@click.option(
    "--role",
    required=False,
    type=click.Choice(
        ["Admin", "Editor", "Reader", "admin", "editor", "reader"], case_sensitive=False
    ),
    help="Optional role to assign (Admin, Editor, or Reader)",
)
@upgrade.check_upgrade
def add(email: str, display_name: str, password: bool, role: str) -> None:
    """Create a native DataHub user with email/password authentication"""

    if not password:
        click.secho(
            "Error: --password flag is required to prompt for password input",
            fg="red",
        )
        raise SystemExit(1)

    password_value = click.prompt(
        "Enter password", hide_input=True, confirmation_prompt=True
    )

    with get_default_graph(ClientMode.CLI) as graph:
        user_urn = f"urn:li:corpuser:{email}"

        if graph.exists(user_urn):
            click.secho(
                f"User with email {email} already exists (urn: {user_urn})", fg="yellow"
            )
            raise SystemExit(0)

        try:
            created_user_urn = graph.create_native_user(
                email=email,
                display_name=display_name,
                password=password_value,
                role=role,
            )

            if role:
                click.secho(
                    f"Successfully created user {email} with role {role.capitalize()} (URN: {created_user_urn})",
                    fg="green",
                )
            else:
                click.secho(
                    f"Successfully created user {email} (URN: {created_user_urn})",
                    fg="green",
                )
        except ValueError as e:
            click.secho(f"Error: {str(e)}", fg="red")
            raise SystemExit(1) from e
        except OperationalError as e:
            # OperationalError has message and info dict
            error_msg = e.message if hasattr(e, "message") else str(e.args[0])
            click.secho(f"Error: {error_msg}", fg="red")

            # Show additional error details if available
            if hasattr(e, "info") and e.info:
                logger.debug(f"Error details: {e.info}")
                if "status_code" in e.info:
                    click.secho(f"  HTTP Status: {e.info['status_code']}", fg="red")
                if "response_text" in e.info:
                    click.secho(
                        f"  Response: {e.info['response_text'][:200]}", fg="red"
                    )

            click.secho(
                "\nTip: Run with DATAHUB_DEBUG=1 environment variable for detailed logs",
                fg="yellow",
            )
            raise SystemExit(1) from e
        except Exception as e:
            click.secho(f"Unexpected error: {str(e)}", fg="red")
            logger.exception("Unexpected error during user creation")
            raise SystemExit(1) from e
