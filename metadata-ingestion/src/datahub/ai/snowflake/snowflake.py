import logging
from pathlib import Path
from typing import Any

import click

from datahub.ai.snowflake.generate_udfs import (
    generate_all_udfs,
    generate_datahub_udfs_sql,
)
from datahub.ai.snowflake.generators import (
    generate_configuration_sql,
    generate_cortex_agent_sql,
    generate_network_rules_sql,
    generate_stored_procedure_sql,
)

logger = logging.getLogger(__name__)


def extract_domain_from_url(datahub_url: str) -> str:
    """Extract domain from DataHub URL for network rules.

    Args:
        datahub_url: DataHub instance URL (e.g., https://example.datahubproject.io)

    Returns:
        Domain without protocol or path (e.g., example.datahubproject.io)

    Raises:
        ValueError: If the URL is invalid or missing required components
    """
    if not datahub_url or not isinstance(datahub_url, str):
        raise ValueError("DataHub URL must be a non-empty string")

    # Strip whitespace
    datahub_url = datahub_url.strip()

    # Check if URL has a protocol
    if not datahub_url.startswith(("http://", "https://")):
        raise ValueError(
            f"DataHub URL must start with http:// or https://, got: {datahub_url}"
        )

    # Extract domain
    domain = datahub_url.replace("https://", "").replace("http://", "")
    domain = domain.split("/")[0]

    # Validate domain is not empty and has valid characters
    if not domain:
        raise ValueError(f"Could not extract domain from URL: {datahub_url}")

    # Basic domain validation - must have at least one dot or be localhost
    if "." not in domain and not domain.startswith("localhost"):
        raise ValueError(
            f"Invalid domain format (must contain at least one dot or be localhost): {domain}"
        )

    return domain


def build_connection_params(
    sf_account: str | None,
    sf_user: str | None,
    sf_role: str | None,
    sf_warehouse: str | None,
    sf_password: str | None,
    sf_authenticator: str,
) -> dict[str, Any]:
    """Build Snowflake connection parameters based on authentication type.

    Args:
        sf_account: Snowflake account identifier
        sf_user: Snowflake user name
        sf_role: Snowflake role
        sf_warehouse: Snowflake warehouse name
        sf_password: Snowflake password (for password auth)
        sf_authenticator: Authentication method (snowflake, externalbrowser, oauth)

    Returns:
        Dictionary of connection parameters for snowflake.connector.connect()
    """
    connection_params: dict[str, Any] = {}

    if sf_user:
        connection_params["user"] = sf_user
    if sf_account:
        connection_params["account"] = sf_account
    if sf_role:
        connection_params["role"] = sf_role
    if sf_warehouse:
        connection_params["warehouse"] = sf_warehouse

    if sf_authenticator == "snowflake":
        if sf_password:
            connection_params["password"] = sf_password
    elif sf_authenticator == "externalbrowser":
        connection_params["authenticator"] = "externalbrowser"
    elif sf_authenticator == "oauth":
        connection_params["authenticator"] = "oauth"

    return connection_params


def generate_all_sql_scripts(
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
    enable_mutations: bool,
    execute_mode: bool,
) -> list[tuple[str, str]]:
    """Generate all SQL scripts for Snowflake agent setup.

    Args:
        sf_account: Snowflake account identifier
        sf_user: Snowflake user name
        sf_role: Snowflake role
        sf_warehouse: Snowflake warehouse name
        sf_database: Snowflake database name
        sf_schema: Snowflake schema name
        datahub_url: DataHub instance URL
        datahub_token: DataHub Personal Access Token
        agent_name: Agent name in Snowflake
        agent_display_name: Agent display name in Snowflake UI
        agent_color: Agent color in Snowflake UI
        enable_mutations: Include mutation/write tools
        execute_mode: Whether to include actual tokens in SQL (for execution)

    Returns:
        List of (script_name, script_content) tuples in execution order
    """
    config_sql = generate_configuration_sql(
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
        execute=execute_mode,
    )

    datahub_domain = extract_domain_from_url(datahub_url)
    network_rules_sql = generate_network_rules_sql(datahub_domain)

    datahub_udfs_sql = generate_datahub_udfs_sql(include_mutations=enable_mutations)

    stored_proc_sql = generate_stored_procedure_sql()

    cortex_agent_sql = generate_cortex_agent_sql(
        agent_name=agent_name,
        agent_display_name=agent_display_name,
        agent_color=agent_color,
        sf_warehouse=sf_warehouse,
        sf_database=sf_database,
        sf_schema=sf_schema,
        include_mutations=enable_mutations,
    )

    return [
        ("00_configuration.sql", config_sql),
        ("01_network_rules.sql", network_rules_sql),
        ("02_datahub_udfs.sql", datahub_udfs_sql),
        ("03_stored_procedure.sql", stored_proc_sql),
        ("04_cortex_agent.sql", cortex_agent_sql),
    ]


def write_sql_files_to_disk(
    output_path: Path,
    scripts: list[tuple[str, str]],
    enable_mutations: bool,
) -> None:
    """Write generated SQL scripts to disk.

    Args:
        output_path: Directory to write SQL files to
        scripts: List of (script_name, script_content) tuples
        enable_mutations: Whether mutations are enabled (for messaging)
    """
    for script_name, script_content in scripts:
        file_path = output_path / script_name
        file_path.write_text(script_content)

        if script_name == "02_datahub_udfs.sql":
            udf_count = len(generate_all_udfs(include_mutations=enable_mutations))
            mutation_note = " (read + write)" if enable_mutations else " (read-only)"
            click.echo(f"✓ Generated {file_path} - {udf_count} UDFs{mutation_note}")
        else:
            click.echo(f"✓ Generated {file_path}")


def execute_sql_scripts_in_snowflake(
    connection: Any,
    scripts: list[tuple[str, str]],
) -> bool:
    """Execute a list of SQL scripts in Snowflake in order.

    Args:
        connection: Snowflake connection object
        scripts: List of (script_name, script_content) tuples to execute

    Returns:
        True if all scripts executed successfully, False otherwise
    """
    all_success = True
    for script_name, script_content in scripts:
        click.echo(f"\n📝 Executing {script_name}...")
        success = execute_sql_in_snowflake(connection, script_content, script_name)
        if success:
            click.echo(f"  ✓ {script_name} completed successfully")
        else:
            click.echo(f"  ✗ {script_name} failed", err=True)
            all_success = False
            break

    return all_success


def execute_sql_in_snowflake(
    connection: Any,
    sql_content: str,
    script_name: str,
) -> bool:
    """
    Execute SQL content in Snowflake using execute_string.

    Args:
        connection: Snowflake connection object
        sql_content: SQL script content to execute (can contain multiple statements)
        script_name: Name of the script (for logging)

    Returns:
        True if successful, False otherwise
    """
    try:
        # Use execute_string to handle multi-statement SQL
        # This is the recommended way to execute multiple SQL statements
        # Returns a list of cursors, one per statement
        click.echo(f"  Executing {script_name}...")

        statement_count = 0

        # execute_string returns a list of cursors
        cursors = connection.execute_string(sql_content, remove_comments=True)

        for cursor in cursors:
            statement_count += 1

            # Fetch results if available
            if cursor.description:
                try:
                    results = cursor.fetchall()
                    if results:
                        click.echo(
                            f"    Statement {statement_count}: {len(results)} row(s) returned"
                        )
                except Exception as e:
                    # Some statements don't return results, that's okay
                    logger.debug(f"No results for statement {statement_count}: {e}")

            cursor.close()

        click.echo(f"  ✓ Executed {statement_count} statement(s) successfully")
        return True

    except Exception as e:
        click.echo(f"✗ Error executing {script_name}: {e}", err=True)
        logger.error(f"Error executing {script_name}: {e}")
        logger.error(f"Error executing {sql_content}")
        return False


def auto_detect_snowflake_params(
    connection: Any,
    sf_account: str | None,
    sf_user: str | None,
    sf_role: str | None,
    sf_warehouse: str | None,
    sf_database: str | None,
    sf_schema: str | None,
) -> tuple[str, str, str, str, str, str]:
    """Auto-detect Snowflake connection parameters from an active connection.

    Args:
        connection: Active Snowflake connection object
        sf_account: Account (auto-detected if None)
        sf_user: User (auto-detected if None)
        sf_role: Role (auto-detected if None)
        sf_warehouse: Warehouse (auto-detected if None)
        sf_database: Database (auto-detected if None)
        sf_schema: Schema (auto-detected if None)

    Returns:
        Tuple of (account, user, role, warehouse, database, schema)

    Raises:
        ValueError: If required parameters cannot be auto-detected
    """
    cursor = connection.cursor()
    try:
        if not sf_account:
            cursor.execute("SELECT CURRENT_ACCOUNT()")
            sf_account = cursor.fetchone()[0]
            click.echo(f"  Auto-detected account: {sf_account}")

        if not sf_user:
            cursor.execute("SELECT CURRENT_USER()")
            sf_user = cursor.fetchone()[0]
            click.echo(f"  Auto-detected user: {sf_user}")

        if not sf_role:
            cursor.execute("SELECT CURRENT_ROLE()")
            sf_role = cursor.fetchone()[0]
            click.echo(f"  Auto-detected role: {sf_role}")

        if not sf_warehouse:
            cursor.execute("SELECT CURRENT_WAREHOUSE()")
            result = cursor.fetchone()
            if result and result[0]:
                sf_warehouse = result[0]
                click.echo(f"  Auto-detected warehouse: {sf_warehouse}")
            else:
                raise ValueError(
                    "No warehouse is currently in use. Please specify --sf-warehouse"
                )

        if not sf_database:
            cursor.execute("SELECT CURRENT_DATABASE()")
            result = cursor.fetchone()
            if result and result[0]:
                sf_database = result[0]
                click.echo(f"  Auto-detected database: {sf_database}")
            else:
                raise ValueError(
                    "No database is currently in use. Please specify --sf-database"
                )

        if not sf_schema:
            cursor.execute("SELECT CURRENT_SCHEMA()")
            result = cursor.fetchone()
            if result and result[0]:
                sf_schema = result[0]
                click.echo(f"  Auto-detected schema: {sf_schema}")
            else:
                raise ValueError(
                    "No schema is currently in use. Please specify --sf-schema"
                )

        return sf_account, sf_user, sf_role, sf_warehouse, sf_database, sf_schema
    finally:
        cursor.close()


@click.command()
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
    "--sf-role", default=None, help="Snowflake role (auto-detected if not provided)"
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
def create_snowflake_agent(
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
    """Create a Snowflake agent on DataHub by generating SQL setup scripts.

    This command generates all necessary SQL files to set up a Snowflake Cortex Agent
    with DataHub integration using the datahub-agent-context package.
    """
    click.echo(
        "Generating Snowflake agent setup SQL files with datahub-agent-context..."
    )
    logger.info("Snowflake agent creation initiated")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate all SQL scripts for file output (without token exposure)
    scripts = generate_all_sql_scripts(
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
        enable_mutations=enable_mutations,
        execute_mode=False,
    )

    # Write scripts to disk
    write_sql_files_to_disk(output_path, scripts, enable_mutations)
    click.echo(f"\n✅ Snowflake agent setup files generated in: {output_path}")

    # Execute SQL scripts if --execute flag is set
    if execute:
        _execute_mode(
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
            sf_password=sf_password,
            sf_authenticator=sf_authenticator,
            enable_mutations=enable_mutations,
        )
    else:
        _show_manual_instructions()

    logger.info(f"Snowflake agent setup files generated in: {output_path}")


def _execute_mode(
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
    sf_password: str | None,
    sf_authenticator: str,
    enable_mutations: bool,
) -> None:
    """Execute SQL scripts directly in Snowflake."""
    if sf_authenticator == "snowflake" and not sf_password:
        click.echo(
            "\n✗ Error: --sf-password is required when using --execute with password authentication (--sf-authenticator=snowflake)",
            err=True,
        )
        logger.error("Password required for snowflake authenticator")
        return

    click.echo("\n🔄 Connecting to Snowflake and executing setup scripts...")
    if sf_authenticator == "externalbrowser":
        click.echo(
            "  Using SSO authentication - your browser will open for authentication..."
        )

    try:
        import snowflake.connector
    except ImportError:
        click.echo(
            "\n✗ Error: snowflake-connector-python package is not installed",
            err=True,
        )
        click.echo("Install it with: pip install snowflake-connector-python", err=True)
        logger.error("snowflake-connector-python not installed")
        return

    try:
        click.echo("  Connecting to Snowflake...")

        connection_params = build_connection_params(
            sf_account=sf_account,
            sf_user=sf_user,
            sf_role=sf_role,
            sf_warehouse=sf_warehouse,
            sf_password=sf_password,
            sf_authenticator=sf_authenticator,
        )

        if sf_authenticator == "oauth":
            click.echo(
                "  Note: OAuth authentication requires additional token configuration",
                err=True,
            )

        conn = snowflake.connector.connect(**connection_params)
        click.echo("  ✓ Connected successfully")

        # Auto-detect values from the Snowflake connection if not provided
        try:
            (
                sf_account,
                sf_user,
                sf_role,
                sf_warehouse,
                sf_database,
                sf_schema,
            ) = auto_detect_snowflake_params(
                conn,
                sf_account,
                sf_user,
                sf_role,
                sf_warehouse,
                sf_database,
                sf_schema,
            )
        except ValueError as e:
            click.echo(f"  ✗ Error: {e}", err=True)
            conn.close()
            return

        # Regenerate SQL with the detected values for execute mode (with actual token)
        scripts = generate_all_sql_scripts(
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
            enable_mutations=enable_mutations,
            execute_mode=True,
        )

        # Execute scripts in order
        all_success = execute_sql_scripts_in_snowflake(conn, scripts)

        conn.close()

        if all_success:
            click.echo("\n✅ All scripts executed successfully!")
            click.echo(
                f"\nYour DataHub agent '{agent_name}' is now ready to use in Snowflake Intelligence UI"
            )
        else:
            click.echo(
                "\n⚠️  Some scripts failed. Check the errors above and review the generated SQL files.",
                err=True,
            )

    except Exception as e:
        click.echo(f"\n✗ Error connecting to Snowflake: {e}", err=True)
        logger.error(f"Snowflake connection error: {e}")
        click.echo("\nYou can still manually run the generated SQL files in Snowflake.")


def _show_manual_instructions() -> None:
    """Show manual execution instructions for non-execute mode."""
    click.echo("\nNext steps:")
    click.echo("1. Review the generated SQL files")
    click.echo("2. Run them in order:")
    click.echo("   a. 00_configuration.sql - Set up configuration variables")
    click.echo(
        "   b. 01_network_rules.sql - Create network rules and access integration"
    )
    click.echo("   c. 02_datahub_udfs.sql - Create DataHub API UDFs")
    click.echo("   d. 03_stored_procedure.sql - Create SQL execution procedure")
    click.echo("   e. 04_cortex_agent.sql - Create the Cortex Agent")
    click.echo("3. Test your agent in Snowflake Intelligence UI")
    click.echo("\nNote: The UDFs use Snowflake secrets for secure credential storage.")
    click.echo(
        "\nTip: Use --execute flag to automatically run these scripts in Snowflake"
    )
    click.echo(
        "     For SSO authentication, use: --execute --sf-authenticator=externalbrowser"
    )


if __name__ == "__main__":
    create_snowflake_agent()
