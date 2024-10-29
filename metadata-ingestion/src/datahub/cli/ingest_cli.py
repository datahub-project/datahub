import asyncio
import csv
import json
import logging
import os
import sys
import textwrap
from datetime import datetime
from typing import Optional

import click
import click_spinner
from click_default_group import DefaultGroup
from tabulate import tabulate

import datahub as datahub_package
from datahub.cli import cli_utils
from datahub.cli.config_utils import CONDENSED_DATAHUB_CONFIG_PATH
from datahub.configuration.common import ConfigModel, GraphError
from datahub.configuration.config_loader import load_config_file
from datahub.emitter.mce_builder import datahub_guid
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.run.connection import ConnectionManager
from datahub.ingestion.run.pipeline import Pipeline
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)

RUNS_TABLE_COLUMNS = ["runId", "rows", "created at"]
RUN_TABLE_COLUMNS = ["urn", "aspect name", "created at"]


@click.group(cls=DefaultGroup, default="run")
def ingest() -> None:
    """Ingest metadata into DataHub."""
    pass


@ingest.command()
@click.option(
    "-c",
    "--config",
    type=click.Path(dir_okay=False),
    help="Config file in .toml or .yaml format.",
    required=True,
)
@click.option(
    "-n",
    "--dry-run",
    type=bool,
    is_flag=True,
    default=False,
    help="Perform a dry run of the ingestion, essentially skipping writing to sink.",
)
@click.option(
    "--preview",
    type=bool,
    is_flag=True,
    default=False,
    help="Perform limited ingestion from the source to the sink to get a quick preview.",
)
@click.option(
    "--preview-workunits",
    type=int,
    default=10,
    help="The number of workunits to produce for preview.",
)
@click.option(
    "--strict-warnings/--no-strict-warnings",
    default=False,
    help="If enabled, ingestion runs with warnings will yield a non-zero error code",
)
@click.option(
    "--test-source-connection",
    type=bool,
    is_flag=True,
    default=False,
    help="When set, ingestion will only test the source connection details from the recipe",
)
@click.option(
    "--report-to",
    type=str,
    default="datahub",
    help="Provide an destination to send a structured report from the run. The default is 'datahub' and sends the report directly to the datahub server (using the sink configured in your recipe). Use the --no-default-report flag to turn off this default feature. Any other parameter passed to this argument is currently assumed to be a file that you want to write the report to. Supplements the reporting configuration in the recipe",
)
@click.option(
    "--no-default-report",
    type=bool,
    is_flag=True,
    default=False,
    help="Turn off default reporting of ingestion results to DataHub",
)
@click.option(
    "--no-spinner", type=bool, is_flag=True, default=False, help="Turn off spinner"
)
@click.option(
    "--no-progress",
    type=bool,
    is_flag=True,
    default=False,
    help="If enabled, mute intermediate progress ingestion reports",
)
@telemetry.with_telemetry(
    capture_kwargs=[
        "dry_run",
        "preview",
        "strict_warnings",
        "test_source_connection",
        "no_default_report",
        "no_spinner",
        "no_progress",
    ]
)
def run(
    config: str,
    dry_run: bool,
    preview: bool,
    strict_warnings: bool,
    preview_workunits: int,
    test_source_connection: bool,
    report_to: Optional[str],
    no_default_report: bool,
    no_spinner: bool,
    no_progress: bool,
) -> None:
    """Ingest metadata into DataHub."""

    async def run_pipeline_to_completion(pipeline: Pipeline) -> int:
        logger.info("Starting metadata ingestion")
        with click_spinner.spinner(disable=no_spinner or no_progress):
            try:
                pipeline.run()
            except Exception as e:
                logger.info(
                    f"Source ({pipeline.source_type}) report:\n{pipeline.source.get_report().as_string()}"
                )
                logger.info(
                    f"Sink ({pipeline.sink_type}) report:\n{pipeline.sink.get_report().as_string()}"
                )
                raise e
            else:
                logger.info("Finished metadata ingestion")
                pipeline.log_ingestion_stats()
                ret = pipeline.pretty_print_summary(warnings_as_failure=strict_warnings)
                return ret

    # main function begins
    logger.info("DataHub CLI version: %s", datahub_package.nice_version_name())

    pipeline_config = load_config_file(
        config,
        squirrel_original_config=True,
        squirrel_field="__raw_config",
        allow_stdin=True,
        allow_remote=True,
        process_directives=True,
        resolve_env_vars=True,
    )
    raw_pipeline_config = pipeline_config.pop("__raw_config")

    if test_source_connection:
        sys.exit(_test_source_connection(report_to, pipeline_config))

    if no_default_report:
        # The default is "datahub" reporting. The extra flag will disable it.
        report_to = None

    async def run_ingestion_and_check_upgrade() -> int:
        # TRICKY: We want to make sure that the Pipeline.create() call happens on the
        # same thread as the rest of the ingestion. As such, we must initialize the
        # pipeline inside the async function so that it happens on the same event
        # loop, and hence the same thread.

        # logger.debug(f"Using config: {pipeline_config}")
        pipeline = Pipeline.create(
            pipeline_config,
            dry_run=dry_run,
            preview_mode=preview,
            preview_workunits=preview_workunits,
            report_to=report_to,
            no_progress=no_progress,
            raw_config=raw_pipeline_config,
        )

        version_stats_future = asyncio.ensure_future(
            upgrade.retrieve_version_stats(pipeline.ctx.graph)
        )
        ingestion_future = asyncio.ensure_future(run_pipeline_to_completion(pipeline))
        ret = await ingestion_future

        # The main ingestion has completed. If it was successful, potentially show an upgrade nudge message.
        if ret == 0:
            try:
                # we check the other futures quickly on success
                version_stats = await asyncio.wait_for(version_stats_future, 0.5)
                upgrade.maybe_print_upgrade_message(version_stats=version_stats)
            except Exception as e:
                logger.debug(
                    f"timed out with {e} waiting for version stats to be computed... skipping ahead."
                )

        return ret

    loop = asyncio.get_event_loop()
    ret = loop.run_until_complete(run_ingestion_and_check_upgrade())
    if ret:
        sys.exit(ret)
    # don't raise SystemExit if there's no error


def _make_ingestion_urn(name: str) -> str:
    guid = datahub_guid(
        {
            "name": name,
        }
    )
    return f"urn:li:dataHubIngestionSource:deploy-{guid}"


class DeployOptions(ConfigModel):
    name: str
    schedule: Optional[str] = None
    time_zone: str = "UTC"
    cli_version: Optional[str] = None
    executor_id: str = "default"


@ingest.command()
@upgrade.check_upgrade
@telemetry.with_telemetry()
@click.option(
    "-n",
    "--name",
    type=str,
    help="Recipe Name",
)
@click.option(
    "-c",
    "--config",
    type=click.Path(dir_okay=False),
    help="Config file in .toml or .yaml format.",
    required=True,
)
@click.option(
    "--urn",
    type=str,
    help="Urn of recipe to update. If not specified here or in the recipe's pipeline_name, this will create a new ingestion source.",
    required=False,
)
@click.option(
    "--executor-id",
    type=str,
    default="default",
    help="Executor id to route execution requests to. Do not use this unless you have configured a custom executor.",
    required=False,
)
@click.option(
    "--cli-version",
    type=str,
    help="Provide a custom CLI version to use for ingestion. By default will use server default.",
    required=False,
    default=None,
)
@click.option(
    "--schedule",
    type=str,
    help="Cron definition for schedule. If none is provided, ingestion recipe will not be scheduled",
    required=False,
    default=None,
)
@click.option(
    "--time-zone",
    type=str,
    help="Timezone for the schedule in 'America/New_York' format. Uses UTC by default.",
    required=False,
    default="UTC",
)
def deploy(
    name: Optional[str],
    config: str,
    urn: Optional[str],
    executor_id: str,
    cli_version: Optional[str],
    schedule: Optional[str],
    time_zone: str,
) -> None:
    """
    Deploy an ingestion recipe to your DataHub instance.

    The urn of the ingestion source will be based on the name parameter in the format:
    urn:li:dataHubIngestionSource:<name>
    """

    datahub_graph = get_default_graph()

    pipeline_config = load_config_file(
        config,
        allow_stdin=True,
        allow_remote=True,
        resolve_env_vars=False,
    )

    deploy_options_raw = pipeline_config.pop("deployment", None)
    if deploy_options_raw is not None:
        deploy_options = DeployOptions.parse_obj(deploy_options_raw)

        if name:
            logger.info(f"Overriding deployment name {deploy_options.name} with {name}")
            deploy_options.name = name
    else:
        if not name:
            raise click.UsageError(
                "Either --name must be set or deployment_name specified in the config"
            )
        deploy_options = DeployOptions(name=name)

    # Use remaining CLI args to override deploy_options
    if schedule:
        deploy_options.schedule = schedule
    if time_zone:
        deploy_options.time_zone = time_zone
    if cli_version:
        deploy_options.cli_version = cli_version
    if executor_id:
        deploy_options.executor_id = executor_id

    logger.info(f"Using {repr(deploy_options)}")

    if not urn:
        # When urn/name is not specified, we will generate a unique urn based on the deployment name.
        urn = _make_ingestion_urn(deploy_options.name)
        logger.info(f"Using recipe urn: {urn}")

    # Invariant - at this point, both urn and deploy_options are set.

    variables: dict = {
        "urn": urn,
        "name": deploy_options.name,
        "type": pipeline_config["source"]["type"],
        "recipe": json.dumps(pipeline_config),
        "executorId": deploy_options.executor_id,
        "version": deploy_options.cli_version,
    }

    if deploy_options.schedule is not None:
        variables["schedule"] = {
            "interval": deploy_options.schedule,
            "timezone": deploy_options.time_zone,
        }

    # The updateIngestionSource endpoint can actually do upserts as well.
    graphql_query: str = textwrap.dedent(
        """
        mutation updateIngestionSource(
            $urn: String!,
            $name: String!,
            $type: String!,
            $schedule: UpdateIngestionSourceScheduleInput,
            $recipe: String!,
            $executorId: String!
            $version: String) {

            updateIngestionSource(urn: $urn, input: {
                name: $name,
                type: $type,
                schedule: $schedule,
                config: {
                    recipe: $recipe,
                    executorId: $executorId,
                    version: $version,
                }
            })
        }
        """
    )

    try:
        response = datahub_graph.execute_graphql(
            graphql_query, variables=variables, format_exception=False
        )
    except GraphError as graph_error:
        try:
            error = json.loads(str(graph_error).replace('"', '\\"').replace("'", '"'))
            click.secho(error[0]["message"], fg="red", err=True)
        except Exception:
            click.secho(
                f"Could not create ingestion source:\n{graph_error}", fg="red", err=True
            )
        sys.exit(1)

    click.echo(
        f"✅ Successfully wrote data ingestion source metadata for recipe {deploy_options.name}:"
    )
    click.echo(response)


def _test_source_connection(report_to: Optional[str], pipeline_config: dict) -> int:
    connection_report = None
    try:
        connection_report = ConnectionManager().test_source_connection(pipeline_config)
        logger.info(connection_report.as_json())
        if report_to and report_to != "datahub":
            with open(report_to, "w") as out_fp:
                out_fp.write(connection_report.as_json())
            logger.info(f"Wrote report successfully to {report_to}")
        return 0
    except Exception as e:
        logger.error(f"Failed to test connection due to {e}")
        if connection_report:
            logger.error(connection_report.as_json())
        return 1


def parse_restli_response(response):
    response_json = response.json()

    if not isinstance(response_json, dict):
        click.echo(f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo()
        click.echo(response_json)
        exit()

    rows = response_json.get("value")
    if not isinstance(rows, list):
        click.echo(f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo()
        click.echo(response_json)
        exit()

    return rows


@ingest.command()
@click.argument("path", type=click.Path(exists=True))
def mcps(path: str) -> None:
    """
    Ingest metadata from a mcp json file or directory of files.

    This requires that you've run `datahub init` to set up your config.
    """

    click.echo("Starting ingestion...")
    recipe: dict = {
        "source": {
            "type": "file",
            "config": {
                "path": path,
            },
        },
    }

    pipeline = Pipeline.create(recipe, report_to=None)
    pipeline.run()
    ret = pipeline.pretty_print_summary()
    sys.exit(ret)


@ingest.command()
@click.argument("page_offset", type=int, default=0)
@click.argument("page_size", type=int, default=100)
@click.option(
    "--include-soft-deletes",
    is_flag=True,
    default=False,
    help="If enabled, will list ingestion runs which have been soft deleted",
)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def list_runs(page_offset: int, page_size: int, include_soft_deletes: bool) -> None:
    """List recent ingestion runs to datahub"""

    client = get_default_graph()
    session = client._session
    gms_host = client.config.server

    url = f"{gms_host}/runs?action=list"

    payload_obj = {
        "pageOffset": page_offset,
        "pageSize": page_size,
        "includeSoft": include_soft_deletes,
    }

    payload = json.dumps(payload_obj)

    response = session.post(url, data=payload)

    rows = parse_restli_response(response)
    local_timezone = datetime.now().astimezone().tzinfo

    structured_rows = [
        [
            row.get("runId"),
            row.get("rows"),
            datetime.fromtimestamp(row.get("timestamp") / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            + f" ({local_timezone})",
        ]
        for row in rows
    ]

    click.echo(tabulate(structured_rows, RUNS_TABLE_COLUMNS, tablefmt="grid"))


@ingest.command()
@click.option("--run-id", required=True, type=str)
@click.option("--start", type=int, default=0)
@click.option("--count", type=int, default=100)
@click.option(
    "--include-soft-deletes",
    is_flag=True,
    default=False,
    help="If enabled, will include aspects that have been soft deleted",
)
@click.option("-a", "--show-aspect", required=False, is_flag=True)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def show(
    run_id: str, start: int, count: int, include_soft_deletes: bool, show_aspect: bool
) -> None:
    """Describe a provided ingestion run to datahub"""
    client = get_default_graph()
    session = client._session
    gms_host = client.config.server

    url = f"{gms_host}/runs?action=describe"

    payload_obj = {
        "runId": run_id,
        "start": start,
        "count": count,
        "includeSoft": include_soft_deletes,
        "includeAspect": show_aspect,
    }

    payload = json.dumps(payload_obj)

    response = session.post(url, data=payload)

    rows = parse_restli_response(response)
    if not show_aspect:
        click.echo(
            tabulate(
                cli_utils.format_aspect_summaries(rows),
                RUN_TABLE_COLUMNS,
                tablefmt="grid",
            )
        )
    else:
        for row in rows:
            click.echo(json.dumps(row, indent=4))


@ingest.command()
@click.option("--run-id", required=True, type=str)
@click.option("-f", "--force", required=False, is_flag=True)
@click.option("--dry-run", "-n", required=False, is_flag=True, default=False)
@click.option("--safe/--nuke", required=False, is_flag=True, default=True)
@click.option(
    "--report-dir",
    required=False,
    type=str,
    default="./rollback-reports",
    help="Path to directory where rollback reports will be saved to",
)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def rollback(
    run_id: str, force: bool, dry_run: bool, safe: bool, report_dir: str
) -> None:
    """Rollback a provided ingestion run to datahub"""
    client = get_default_graph()

    if not force and not dry_run:
        click.confirm(
            "This will permanently delete data from DataHub. Do you want to continue?",
            abort=True,
        )

    payload_obj = {"runId": run_id, "dryRun": dry_run, "safe": safe}
    (
        structured_rows,
        entities_affected,
        aspects_reverted,
        aspects_affected,
        unsafe_entity_count,
        unsafe_entities,
    ) = cli_utils.post_rollback_endpoint(
        client._session, client.config.server, payload_obj, "/runs?action=rollback"
    )

    click.echo(
        "Rolling back deletes the entities created by a run and reverts the updated aspects"
    )
    click.echo(
        f"This rollback {'will' if dry_run else ''} {'delete' if dry_run else 'deleted'} {entities_affected} entities and {'will roll' if dry_run else 'rolled'} back {aspects_reverted} aspects"
    )

    click.echo(
        f"showing first {len(structured_rows)} of {aspects_reverted} aspects {'that will be ' if dry_run else ''}reverted by this run"
    )
    click.echo(tabulate(structured_rows, RUN_TABLE_COLUMNS, tablefmt="grid"))

    if aspects_affected > 0:
        if safe:
            click.echo(
                f"WARNING: This rollback {'will hide' if dry_run else 'has hidden'} {aspects_affected} aspects related to {unsafe_entity_count} entities being rolled back that are not part ingestion run id."
            )
        else:
            click.echo(
                f"WARNING: This rollback {'will delete' if dry_run else 'has deleted'} {aspects_affected} aspects related to {unsafe_entity_count} entities being rolled back that are not part ingestion run id."
            )

    if unsafe_entity_count > 0:
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")

        try:
            folder_name = f"{report_dir}/{current_time}"

            ingestion_config_file_name = f"{folder_name}/config.json"
            os.makedirs(os.path.dirname(ingestion_config_file_name), exist_ok=True)
            with open(ingestion_config_file_name, "w") as file_handle:
                json.dump({"run_id": run_id}, file_handle)

            csv_file_name = f"{folder_name}/unsafe_entities.csv"
            with open(csv_file_name, "w") as file_handle:
                writer = csv.writer(file_handle)
                writer.writerow(["urn"])
                for row in unsafe_entities:
                    writer.writerow([row.get("urn")])

        except OSError as e:
            logger.exception(f"Unable to save rollback failure report: {e}")
            sys.exit(f"Unable to write reports to {report_dir}")
