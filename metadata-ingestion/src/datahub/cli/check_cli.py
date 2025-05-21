import dataclasses
import json
import logging
import pathlib
import pprint
import shutil
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import click

from datahub._version import __package_name__
from datahub.cli.json_file import check_mce_file
from datahub.configuration import config_loader
from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.sink.sink_registry import sink_registry
from datahub.ingestion.source.source_registry import source_registry
from datahub.ingestion.transformer.transform_registry import transform_registry
from datahub.telemetry import telemetry
from datahub.utilities.file_backed_collections import (
    ConnectionWrapper,
    FileBackedDict,
)

logger = logging.getLogger(__name__)


@click.group()
def check() -> None:
    """Helper commands for checking various aspects of DataHub."""
    pass


@check.command()
@click.argument("json-file", type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--rewrite",
    default=False,
    is_flag=True,
    help="Rewrite the JSON file to it's canonical form.",
)
@click.option(
    "--unpack-mces", default=False, is_flag=True, help="Converts MCEs into MCPs"
)
@telemetry.with_telemetry()
def metadata_file(json_file: str, rewrite: bool, unpack_mces: bool) -> None:
    """Check the schema of a metadata (MCE or MCP) JSON file."""

    if not rewrite:
        report = check_mce_file(json_file)
        click.echo(report)

    else:
        with tempfile.NamedTemporaryFile() as out_file:
            pipeline = Pipeline.create(
                {
                    "source": {
                        "type": "file",
                        "config": {"filename": json_file},
                        "extractor": "generic",
                        "extractor_config": {
                            "unpack_mces_into_mcps": unpack_mces,
                        },
                    },
                    "flags": {"set_system_metadata": False},
                    "sink": {
                        "type": "file",
                        "config": {"filename": out_file.name},
                    },
                },
                report_to=None,
            )

            pipeline.run()
            pipeline.raise_from_status()

            shutil.copy(out_file.name, json_file)


@check.command(no_args_is_help=True)
@click.argument(
    "actual-file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
)
@click.argument(
    "expected-file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
)
@click.option(
    "--verbose",
    "-v",
    type=bool,
    default=False,
    help="Print full aspects that were changed, when comparing MCPs",
)
@click.option(
    "--ignore-path",
    multiple=True,
    type=str,
    default=(),
    help="[Advanced] Paths in the deepdiff object to ignore",
)
@telemetry.with_telemetry()
def metadata_diff(
    actual_file: str, expected_file: str, verbose: bool, ignore_path: List[str]
) -> None:
    """Compare two metadata (MCE or MCP) JSON files.

    To use this command, you must install the acryl-datahub[testing-utils] extra.

    Comparison is more sophisticated for files composed solely of MCPs.
    """
    from datahub.testing.compare_metadata_json import diff_metadata_json, load_json_file
    from datahub.testing.mcp_diff import MCPDiff

    actual = load_json_file(actual_file)
    expected = load_json_file(expected_file)

    diff = diff_metadata_json(output=actual, golden=expected, ignore_paths=ignore_path)
    if isinstance(diff, MCPDiff):
        click.echo(diff.pretty(verbose=verbose))
    else:
        click.echo(pprint.pformat(diff))


@check.command()
@click.option(
    "--verbose",
    "-v",
    type=bool,
    is_flag=True,
    default=False,
    help="Include extra information for each plugin.",
)
@click.option(
    "--source",
    type=str,
    default=None,
)
@telemetry.with_telemetry()
def plugins(source: Optional[str], verbose: bool) -> None:
    """List the enabled ingestion plugins."""

    if source:
        # Quick helper for one-off checks with full stack traces.
        source_registry.get(source)
        click.echo(f"Source {source} is enabled.")
        return

    click.secho("Sources:", bold=True)
    click.echo(source_registry.summary(verbose=verbose, col_width=25))
    click.echo()
    click.secho("Sinks:", bold=True)
    click.echo(sink_registry.summary(verbose=verbose))
    click.echo()
    click.secho("Transformers:", bold=True)
    click.echo(transform_registry.summary(verbose=verbose, col_width=30))
    click.echo()
    if not verbose:
        click.echo("For details on why a plugin is disabled, rerun with '--verbose'")
    click.echo(
        f"If a plugin is disabled, try running: pip install '{__package_name__}[<plugin>]'"
    )


@check.command()
@click.option(
    "--sql",
    type=str,
    required=True,
    help="The SQL query to parse",
)
@click.option(
    "--platform",
    type=str,
    required=True,
    help="The SQL dialect e.g. bigquery or snowflake",
)
def sql_format(sql: str, platform: str) -> None:
    """Parse a SQL query into an abstract syntax tree (AST)."""

    from datahub.sql_parsing.sqlglot_utils import try_format_query

    click.echo(try_format_query(sql, platform, raises=True))


@check.command()
@click.option(
    "--sql",
    type=str,
    help="The SQL query to parse",
)
@click.option(
    "--sql-file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="The SQL file to parse",
)
@click.option(
    "--platform",
    type=str,
    required=True,
    help="The SQL dialect e.g. bigquery or snowflake",
)
@click.option(
    "--platform-instance",
    type=str,
    help="The specific platform_instance the SQL query was run in",
)
@click.option(
    "--env",
    type=str,
    default=DEFAULT_ENV,
    help=f"The environment the SQL query was run in, defaults to {DEFAULT_ENV}",
)
@click.option(
    "--default-db",
    type=str,
    help="The default database to use for unqualified table names",
)
@click.option(
    "--default-schema",
    type=str,
    help="The default schema to use for unqualified table names",
)
@click.option(
    "--online/--offline",
    type=bool,
    is_flag=True,
    default=True,
    help="Run in offline mode and disable schema-aware parsing.",
)
@telemetry.with_telemetry()
def sql_lineage(
    sql: Optional[str],
    sql_file: Optional[str],
    platform: str,
    default_db: Optional[str],
    default_schema: Optional[str],
    platform_instance: Optional[str],
    env: str,
    online: bool,
) -> None:
    """Parse the lineage of a SQL query.

    In online mode (the default), we perform schema-aware parsing in order to generate column-level lineage.
    If offline mode is enabled or if the relevant tables are not in DataHub, this will be less accurate.
    """

    from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result

    if sql is None:
        if sql_file is None:
            raise click.UsageError("Either --sql or --sql-file must be provided")
        sql = pathlib.Path(sql_file).read_text()

    graph = None
    if online:
        graph = get_default_graph(ClientMode.CLI)

    lineage = create_lineage_sql_parsed_result(
        sql,
        graph=graph,
        platform=platform,
        platform_instance=platform_instance,
        env=env,
        default_db=default_db,
        default_schema=default_schema,
    )

    logger.debug("Sql parsing debug info: %s", lineage.debug_info)
    if lineage.debug_info.table_error:
        raise lineage.debug_info.table_error
    elif lineage.debug_info.error:
        logger.debug("Sql parsing error details", exc_info=lineage.debug_info.error)

    click.echo(lineage.json(indent=4))


@check.command()
@click.option(
    "--config",
    type=str,
    help="The DataHub recipe to load",
)
@click.option(
    "--pattern_key",
    type=str,
    help="The allow deny pattern key in the config -> source section of the recipe to validate against",
)
@click.option(
    "--input",
    type=str,
    help="the input to validate",
)
@telemetry.with_telemetry()
def test_allow_deny(config: str, input: str, pattern_key: str) -> None:
    """Test input string against AllowDeny pattern in a DataHub recipe.

    This command validates an input string against an AllowDeny pattern in a DataHub recipe.
    """

    pattern_dict: Optional[Dict] = None
    recipe_config_dict = config_loader.load_config_file(config)
    try:
        source_config = recipe_config_dict.get("source", {}).get("config", {})

        for key in pattern_key.split("."):
            if pattern_dict is None:
                pattern_dict = source_config.get(key)
            else:
                pattern_dict = pattern_dict.get(key)

        if pattern_dict is None:
            click.secho(f"{pattern_key} is not defined in the config", fg="red")
            exit(1)

        allow_deny_pattern = AllowDenyPattern.parse_obj(pattern_dict)
        if allow_deny_pattern.allowed(input):
            click.secho(f"✅ {input} is allowed by {pattern_key}", fg="green")
            exit(0)
        else:
            click.secho(f"❌{input} is denied by {pattern_key}", fg="red")
    except Exception as e:
        logger.error(f"Failed to validate pattern {pattern_dict} in path {pattern_key}")
        raise e


@check.command()
@click.option(
    "--config",
    type=str,
    help="The datahub recipe to load",
)
@click.option(
    "--path_spec_key",
    type=str,
    help="The path_specs key  in the config -> source section of the recipe to validate against",
)
@click.option(
    "--input",
    type=str,
    help="The input to validate",
)
@telemetry.with_telemetry()
def test_path_spec(config: str, input: str, path_spec_key: str) -> None:
    """Test input path string against PathSpec patterns in a DataHub recipe.

    This command validates an input path string against an PathSpec patterns in a DataHub recipe.
    """

    from datahub.ingestion.source.data_lake_common.path_spec import PathSpec

    pattern_dicts: Optional[Union[List[Dict], Dict]] = None
    recipe_config_dict = config_loader.load_config_file(config)
    try:
        source_config = recipe_config_dict.get("source", {}).get("config", {})

        for key in path_spec_key.split("."):
            if pattern_dicts is None:
                pattern_dicts = source_config.get(key)
            else:
                if isinstance(pattern_dicts, dict):
                    pattern_dicts = pattern_dicts.get(key)
        allowed = True

        if pattern_dicts is None:
            click.secho(f"{path_spec_key} is not defined in the config", fg="red")
            exit(1)

        if isinstance(pattern_dicts, dict):
            pattern_dicts = [pattern_dicts]

        for pattern_dict in pattern_dicts:
            path_spec_pattern = PathSpec.parse_obj(pattern_dict)
            if path_spec_pattern.allowed(input):
                click.echo(f"{input} is allowed by {path_spec_pattern}")
            else:
                allowed = False
                click.echo(f"{input} is denied by {path_spec_pattern}")

        if allowed:
            click.secho(f"✅ {input} is allowed by the path_specs", fg="green")
            exit(0)
        else:
            click.secho(f"❌{input} is denied by the path_specs", fg="red")

    except Exception as e:
        logger.error(
            f"Failed to validate pattern {pattern_dicts} in path {path_spec_key}"
        )
        raise e


def _jsonify(data: Any) -> Any:
    if dataclasses.is_dataclass(data):
        # dataclasses.asdict() is recursive. We're doing the recursion
        # manually here via _jsonify calls, so we can't use
        # dataclasses.asdict() here.
        return {
            f.name: _jsonify(getattr(data, f.name)) for f in dataclasses.fields(data)
        }
    elif isinstance(data, list):
        return [_jsonify(item) for item in data]
    elif isinstance(data, dict):
        return {_jsonify(k): _jsonify(v) for k, v in data.items()}
    elif isinstance(data, datetime):
        return data.isoformat()
    else:
        return data


@check.command()
@click.argument("db-file", type=click.Path(exists=True, dir_okay=False))
def extract_sql_agg_log(db_file: str) -> None:
    """Convert a sqlite db generated by the SqlParsingAggregator into a JSON."""

    if pathlib.Path(db_file).suffix != ".db":
        raise click.UsageError("DB file must be a sqlite db")

    output_dir = pathlib.Path(db_file).with_suffix("")
    output_dir.mkdir(exist_ok=True)

    shared_connection = ConnectionWrapper(pathlib.Path(db_file))

    tables: List[str] = [
        row[0]
        for row in shared_connection.execute(
            """\
SELECT
    name
FROM
    sqlite_schema
WHERE
    type ='table' AND
    name NOT LIKE 'sqlite_%';
""",
            parameters={},
        )
    ]
    logger.info(f"Extracting {len(tables)} tables from {db_file}: {tables}")

    for table in tables:
        table_output_path = output_dir / f"{table}.json"
        if table_output_path.exists():
            logger.info(f"Skipping {table_output_path} because it already exists")
            continue

        # Some of the tables might actually be FileBackedList. Because
        # the list is built on top of the FileBackedDict, we don't
        # need to distinguish between the two cases.

        table_data: FileBackedDict[Any] = FileBackedDict(
            shared_connection=shared_connection, tablename=table
        )

        data = {}
        with click.progressbar(
            table_data.items(), length=len(table_data), label=f"Extracting {table}"
        ) as items:
            for k, v in items:
                data[k] = _jsonify(v)

        with open(table_output_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        logger.info(f"Extracted {len(data)} entries to {table_output_path}")


@check.command()
def server_config() -> None:
    """Print the server config."""
    graph = get_default_graph(ClientMode.CLI)

    server_config = graph.get_server_config()

    click.echo(pprint.pformat(server_config))
