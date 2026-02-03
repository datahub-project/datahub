import logging

import click

logger = logging.getLogger(__name__)


@click.group()
def agent() -> None:
    """Helper commands for Creating and managing Agent on DataHub."""
    pass


@agent.group()
def create() -> None:
    """Create an agent on DataHub."""
    pass


@create.command(name="snowflake")
@click.option(
    "--sf-account",
    default=None,
    help="Snowflake account identifier (auto-detected if not provided)",
)
@click.option(
    "--sf-user",
    default=None,
    help="Snowflake user name (auto-detected if not provided)",
)
@click.option(
    "--sf-role",
    default=None,
    help="Snowflake role (auto-detected if not provided)",
)
@click.option(
    "--sf-warehouse",
    default=None,
    help="Snowflake warehouse name (auto-detected if not provided)",
)
@click.option(
    "--sf-database",
    default=None,
    help="Snowflake database name (auto-detected if not provided)",
)
@click.option(
    "--sf-schema",
    default=None,
    help="Snowflake schema name (auto-detected if not provided)",
)
@click.option("--datahub-url", required=True, help="DataHub instance URL")
@click.option("--datahub-token", required=True, help="DataHub Personal Access Token")
@click.option(
    "--agent-name", default="DATAHUB_SQL_AGENT", help="Agent name in Snowflake"
)
@click.option(
    "--agent-display-name",
    default="DataHub SQL Assistant",
    help="Agent display name in Snowflake UI",
)
@click.option(
    "--agent-color",
    default="blue",
    help="Agent color in Snowflake UI",
    type=click.Choice(["blue", "red", "green", "yellow", "purple", "orange"]),
)
@click.option(
    "--output-dir",
    default="./snowflake_setup",
    help="Output directory for generated SQL files",
)
@click.option(
    "--execute",
    is_flag=True,
    default=False,
    help="Connect to Snowflake and execute the SQL scripts directly",
)
@click.option(
    "--sf-password",
    help="Snowflake password (required if --execute is used with password authentication)",
)
@click.option(
    "--sf-authenticator",
    default="snowflake",
    type=click.Choice(["snowflake", "externalbrowser", "oauth"], case_sensitive=False),
    help="Authentication method: 'snowflake' (password), 'externalbrowser' (SSO), or 'oauth' (token-based). Default: snowflake",
)
@click.option(
    "--enable-mutations/--no-enable-mutations",
    default=True,
    help="Include mutation/write tools (tags, descriptions, owners, etc.). Default: enabled",
)
def create_snowflake(
    sf_account: str | None,
    sf_user: str | None,
    sf_role: str | None,
    sf_warehouse: str | None,
    sf_database: str | None,
    sf_schema: str | None,
    datahub_url: str,
    datahub_token: str,
    agent_name: str,
    agent_display_name: str,
    agent_color: str,
    output_dir: str,
    execute: bool,
    sf_password: str | None,
    sf_authenticator: str,
    enable_mutations: bool,
) -> None:
    """Create a Snowflake agent on DataHub.

    Snowflake connection parameters (account, user, role, warehouse, database, schema)
    will be auto-detected from your current session if not provided explicitly.

    Authentication methods:
    - snowflake (default): Standard password authentication
    - externalbrowser: SSO authentication via browser (no password required)
    - oauth: OAuth token-based authentication

    Examples:
        # Generate SQL files only (default)
        $ datahub agent create snowflake --datahub-url=... --datahub-token=...

        # Execute with password authentication
        $ datahub agent create snowflake --execute --sf-password=secret ...

        # Execute with SSO authentication (browser-based)
        $ datahub agent create snowflake --execute --sf-authenticator=externalbrowser ...
    """
    from datahub_agent_context.snowflake.snowflake import create_snowflake_agent

    ctx = click.get_current_context()
    ctx.invoke(
        create_snowflake_agent,
        sf_account=sf_account,
        sf_user=sf_user,
        sf_role=sf_role,
        sf_warehouse=sf_warehouse,
        sf_database=sf_database,
        sf_schema=sf_schema,
        datahub_url=datahub_url,
        datahub_token=datahub_token,
        agent_name=agent_name,
        agent_display_name=agent_display_name,
        agent_color=agent_color,
        output_dir=output_dir,
        execute=execute,
        sf_password=sf_password,
        sf_authenticator=sf_authenticator,
        enable_mutations=enable_mutations,
    )
