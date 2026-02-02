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
        error_count = 0

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

        if error_count > 0:
            click.echo(f"  ⚠️  Completed with {error_count} error(s)", err=True)
            return False

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
    "--datahub-ips",
    default="('52.7.66.10', '44.217.146.124', '34.193.80.100')",
    help="DataHub IP addresses for network rule",
)
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
    datahub_ips: str,
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

    config_sql = generate_configuration_sql(
        sf_account=sf_account,
        sf_user=sf_user,
        sf_role=sf_role,
        sf_warehouse=sf_warehouse,
        sf_database=sf_database,
        sf_schema=sf_schema,
        datahub_url=datahub_url,
        datahub_token=datahub_token,
        datahub_ips=datahub_ips,
        agent_name=agent_name,
        agent_display_name=agent_display_name,
        agent_color=agent_color,
    )

    (output_path / "00_configuration.sql").write_text(config_sql)
    click.echo(f"✓ Generated {output_path / '00_configuration.sql'}")

    # Extract domain from DataHub URL for network rules
    datahub_domain = datahub_url.replace("https://", "").replace("http://", "")
    datahub_domain = datahub_domain.split("/")[0]

    network_rules_sql = generate_network_rules_sql(datahub_domain)
    (output_path / "01_network_rules.sql").write_text(network_rules_sql)
    click.echo(f"✓ Generated {output_path / '01_network_rules.sql'}")

    # Generate UDFs using datahub-agent-context package
    datahub_udfs_sql = generate_datahub_udfs_sql(include_mutations=enable_mutations)
    (output_path / "02_datahub_udfs.sql").write_text(datahub_udfs_sql)
    udf_count = len(generate_all_udfs(include_mutations=enable_mutations))
    mutation_note = " (read + write)" if enable_mutations else " (read-only)"
    click.echo(
        f"✓ Generated {output_path / '02_datahub_udfs.sql'} - {udf_count} UDFs{mutation_note}"
    )

    stored_proc_sql = generate_stored_procedure_sql()
    (output_path / "03_stored_procedure.sql").write_text(stored_proc_sql)
    click.echo(f"✓ Generated {output_path / '03_stored_procedure.sql'}")

    cortex_agent_sql = generate_cortex_agent_sql(
        agent_name=agent_name,
        agent_display_name=agent_display_name,
        agent_color=agent_color,
        sf_warehouse=sf_warehouse,
        sf_database=sf_database,
        sf_schema=sf_schema,
    )
    (output_path / "04_cortex_agent.sql").write_text(cortex_agent_sql)
    click.echo(f"✓ Generated {output_path / '04_cortex_agent.sql'}")

    click.echo(f"\n✅ Snowflake agent setup files generated in: {output_path}")

    # Execute SQL scripts if --execute flag is set
    if execute:
        # Validate authentication requirements
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
            click.echo(
                "Install it with: pip install snowflake-connector-python", err=True
            )
            logger.error("snowflake-connector-python not installed")
            return

        try:
            # Connect to Snowflake
            click.echo("  Connecting to Snowflake...")

            # Build connection parameters based on authenticator type
            # Only include non-None values in connection params
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
                # Standard password authentication
                if sf_password:
                    connection_params["password"] = sf_password
            elif sf_authenticator == "externalbrowser":
                # SSO via external browser
                connection_params["authenticator"] = "externalbrowser"
            elif sf_authenticator == "oauth":
                # OAuth token-based (token would need to be provided separately)
                connection_params["authenticator"] = "oauth"
                # Note: For OAuth, token would typically be passed via environment or separate parameter
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

            # Regenerate SQL with the detected values for execute mode
            config_sql = generate_configuration_sql(
                sf_account=sf_account,
                sf_user=sf_user,
                sf_role=sf_role,
                sf_warehouse=sf_warehouse,
                sf_database=sf_database,
                sf_schema=sf_schema,
                datahub_url=datahub_url,
                datahub_token=datahub_token,
                datahub_ips=datahub_ips,
                agent_name=agent_name,
                agent_display_name=agent_display_name,
                agent_color=agent_color,
            )

            # Extract domain from DataHub URL for network rules
            datahub_domain = (
                datahub_url.replace("https://", "").replace("http://", "").split("/")[0]
            )

            network_rules_sql = generate_network_rules_sql(datahub_domain)
            datahub_udfs_sql = generate_datahub_udfs_sql(
                include_mutations=enable_mutations
            )
            stored_proc_sql = generate_stored_procedure_sql()
            cortex_agent_sql = generate_cortex_agent_sql(
                agent_name=agent_name,
                agent_display_name=agent_display_name,
                agent_color=agent_color,
                sf_warehouse=sf_warehouse,
                sf_database=sf_database,
                sf_schema=sf_schema,
            )

            # Execute scripts in order
            scripts = [
                ("00_configuration.sql", config_sql),
                ("01_network_rules.sql", network_rules_sql),
                ("02_datahub_udfs.sql", datahub_udfs_sql),
                ("03_stored_procedure.sql", stored_proc_sql),
                ("04_cortex_agent.sql", cortex_agent_sql),
            ]

            all_success = True
            for script_name, script_content in scripts:
                click.echo(f"\n📝 Executing {script_name}...")
                success = execute_sql_in_snowflake(conn, script_content, script_name)
                if success:
                    click.echo(f"  ✓ {script_name} completed successfully")
                else:
                    click.echo(f"  ✗ {script_name} failed", err=True)
                    all_success = False
                    break

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
            click.echo(
                "\nYou can still manually run the generated SQL files in Snowflake."
            )

    else:
        # Non-execute mode - just show instructions
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
        click.echo(
            "\nNote: The UDFs use Snowflake secrets for secure credential storage."
        )
        click.echo(
            "\nTip: Use --execute flag to automatically run these scripts in Snowflake"
        )
        click.echo(
            "     For SSO authentication, use: --execute --sf-authenticator=externalbrowser"
        )

    logger.info(f"Snowflake agent setup files generated in: {output_path}")


if __name__ == "__main__":
    create_snowflake_agent()
