import csv
import json
import logging
import os
import sys
import textwrap
from datetime import datetime
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from datahub.ingestion.recording.recorder import IngestionRecorder

import click
import click_spinner
from click_default_group import DefaultGroup
from tabulate import tabulate

from datahub._version import nice_version_name
from datahub.cli import cli_utils
from datahub.cli.config_utils import CONDENSED_DATAHUB_CONFIG_PATH, load_client_config
from datahub.configuration.common import GraphError
from datahub.configuration.config_loader import load_config_file
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.ingestion.run.connection import ConnectionManager
from datahub.ingestion.run.pipeline import Pipeline
from datahub.masking.bootstrap import initialize_secret_masking
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.ingest_utils import deploy_source_vars

logger = logging.getLogger(__name__)

INGEST_SRC_TABLE_COLUMNS = ["runId", "source", "startTime", "status", "URN"]
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
@click.option(
    "--record",
    type=bool,
    is_flag=True,
    default=False,
    help="Enable recording of ingestion run for debugging. "
    "Requires recording config in recipe or --record-password.",
)
@click.option(
    "--record-password",
    type=str,
    default=None,
    help="Password for encrypting the recording archive. "
    "Can also be set via DATAHUB_RECORDING_PASSWORD env var.",
)
@click.option(
    "--record-output-path",
    type=str,
    default=None,
    help="Path to save the recording archive (local path or S3 URL when s3_upload=true). "
    "Overrides recording.output_path in recipe. "
    "If not provided, uses INGESTION_ARTIFACT_DIR env var or temp directory.",
)
@click.option(
    "--no-s3-upload",
    type=bool,
    is_flag=True,
    default=False,
    help="Disable S3 upload of recording (for testing).",
)
@click.option(
    "--no-secret-redaction",
    type=bool,
    is_flag=True,
    default=False,
    help="Disable secret redaction in recordings (for local debugging). "
    "WARNING: Recording will contain actual secrets. Use with caution.",
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
        "record",
    ]
)
@upgrade.check_upgrade
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
    record: bool,
    record_password: Optional[str],
    record_output_path: Optional[str],
    no_s3_upload: bool,
    no_secret_redaction: bool,
) -> None:
    """Ingest metadata into DataHub."""

    # Initialize secret masking (before any logging)
    initialize_secret_masking()

    def run_pipeline_to_completion(pipeline: Pipeline) -> int:
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
    logger.info("DataHub CLI version: %s", nice_version_name())

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

    # Helper function to create and run the pipeline
    def create_and_run_pipeline() -> int:
        pipeline = Pipeline.create(
            pipeline_config,
            dry_run=dry_run,
            preview_mode=preview,
            preview_workunits=preview_workunits,
            report_to=report_to,
            no_progress=no_progress,
            raw_config=raw_pipeline_config,
        )
        return run_pipeline_to_completion(pipeline)

    # Handle recording if enabled (via --record flag or recording.enabled in recipe)
    # IMPORTANT: Pipeline.create() must happen INSIDE the recording context
    # so that SDK initialization (including auth) is captured by VCR.
    recording_enabled = record or pipeline_config.get("recording", {}).get(
        "enabled", False
    )
    if recording_enabled:
        recorder = _setup_recording(
            pipeline_config,
            record_password,
            record_output_path,
            no_s3_upload,
            no_secret_redaction,
            raw_pipeline_config,
        )
        with recorder:
            ret = create_and_run_pipeline()
    else:
        ret = create_and_run_pipeline()

    if ret:
        sys.exit(ret)
    # don't raise SystemExit if there's no error


@ingest.command()
@click.option(
    "-n",
    "--name",
    type=str,
    help="Recipe Name",
    required=False,
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
    help="Executor id to route execution requests to. Do not use this unless you have configured a custom executor.",
    required=False,
    default=None,
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
    default=None,
)
@click.option(
    "--debug", type=bool, help="Should we debug.", required=False, default=False
)
@click.option(
    "--extra-pip",
    type=str,
    help='Extra pip packages. e.g. ["memray"]',
    required=False,
    default=None,
)
@click.option(
    "--extra-env",
    type=str,
    help='Environment variables as comma-separated KEY=VALUE pairs. e.g. "VAR1=value1,VAR2=value2"',
    required=False,
    default=None,
)
@upgrade.check_upgrade
def deploy(
    name: Optional[str],
    config: str,
    urn: Optional[str],
    executor_id: Optional[str],
    cli_version: Optional[str],
    schedule: Optional[str],
    time_zone: Optional[str],
    extra_pip: Optional[str],
    extra_env: Optional[str],
    debug: bool = False,
) -> None:
    """
    Deploy an ingestion recipe to your DataHub instance.

    The urn of the ingestion source will be based on the name parameter in the format:
    urn:li:dataHubIngestionSource:<name>
    """

    datahub_graph = get_default_graph(ClientMode.CLI)

    variables = deploy_source_vars(
        name=name,
        config=config,
        urn=urn,
        executor_id=executor_id,
        cli_version=cli_version,
        schedule=schedule,
        time_zone=time_zone,
        extra_pip=extra_pip,
        debug=debug,
        extra_env=extra_env,
    )

    # The updateIngestionSource endpoint can actually do upserts as well.
    graphql_query: str = textwrap.dedent(
        """
        mutation updateIngestionSource($urn: String!, $input: UpdateIngestionSourceInput!) {
            updateIngestionSource(urn: $urn, input: $input)
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
        f"✅ Successfully wrote data ingestion source metadata for recipe {variables['input']['name']}:"
    )
    click.echo(response)


def _setup_recording(
    pipeline_config: dict,
    record_password: Optional[str],
    record_output_path: Optional[str],
    no_s3_upload: bool,
    no_secret_redaction: bool,
    raw_config: dict,
) -> "IngestionRecorder":
    """Setup recording for the ingestion run."""
    from datahub.ingestion.recording.config import (
        RecordingConfig,
        check_recording_dependencies,
        get_recording_password_from_env,
    )
    from datahub.ingestion.recording.recorder import IngestionRecorder

    # Check dependencies first
    check_recording_dependencies()

    # Build recording config from recipe, with CLI overrides
    recording_config_dict = pipeline_config.get("recording", {}).copy()

    # CLI password takes precedence, then env var, then recipe
    password = record_password or get_recording_password_from_env()
    if password:
        recording_config_dict["password"] = password

    # CLI --record-output-path flag overrides recipe
    if record_output_path:
        recording_config_dict["output_path"] = record_output_path

    # CLI --no-s3-upload flag overrides recipe
    if no_s3_upload:
        recording_config_dict["s3_upload"] = False

    # Ensure enabled is set (we're here because recording should be enabled)
    recording_config_dict["enabled"] = True

    # Validate config using pydantic model
    try:
        recording_config = RecordingConfig.model_validate(recording_config_dict)
    except Exception as e:
        click.secho(f"Error in recording configuration: {e}", fg="red", err=True)
        sys.exit(1)

    # Get password as string for recorder
    if not recording_config.password:
        click.secho(
            "Error: Recording password required. Provide via --record-password, "
            "DATAHUB_RECORDING_PASSWORD env var, or recipe recording.password.",
            fg="red",
            err=True,
        )
        sys.exit(1)
    password_str = recording_config.password.get_secret_value()

    # Get run_id from pipeline config or generate one
    run_id = pipeline_config.get("run_id")
    if not run_id:
        from datahub.ingestion.run.pipeline_config import _generate_run_id

        run_id = _generate_run_id(
            pipeline_config.get("source", {}).get("type", "unknown")
        )

    # Get source and sink types for metadata
    source_type = pipeline_config.get("source", {}).get("type")
    sink_type = pipeline_config.get("sink", {}).get("type", "datahub-rest")

    logger.info(f"Recording enabled for run_id: {run_id}")
    logger.info(f"S3 upload: {'enabled' if recording_config.s3_upload else 'disabled'}")
    if recording_config.output_path:
        logger.info(f"Output path: {recording_config.output_path}")
    if no_secret_redaction:
        logger.warning(
            "Secret redaction is DISABLED - recording will contain actual secrets. "
            "Use this only for local debugging and NEVER commit recordings to source control."
        )

    return IngestionRecorder(
        run_id=run_id,
        password=password_str,
        redact_secrets=not no_secret_redaction,
        recipe=raw_config,
        output_path=recording_config.output_path,
        s3_upload=recording_config.s3_upload,
        source_type=source_type,
        sink_type=sink_type,
    )


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
@click.argument(
    "path", type=click.Path(exists=False)
)  # exists=False since it only supports local filesystems
def mcps(path: str) -> None:
    """
    Ingest metadata from a mcp json file or directory of files.

    This requires that you've run `datahub init` to set up your config.
    """

    click.echo("Starting ingestion...")
    datahub_config = load_client_config()
    recipe: dict = {
        "source": {
            "type": "file",
            "config": {
                "path": path,
            },
        },
        "datahub_api": datahub_config,
    }

    pipeline = Pipeline.create(recipe, report_to=None)
    pipeline.run()
    ret = pipeline.pretty_print_summary()
    sys.exit(ret)


@ingest.command()
@click.argument("page_offset", type=int, default=0)
@click.argument("page_size", type=int, default=100)
@click.option("--urn", type=str, default=None, help="Filter by ingestion source URN.")
@click.option(
    "--source", type=str, default=None, help="Filter by ingestion source name."
)
@upgrade.check_upgrade
def list_source_runs(page_offset: int, page_size: int, urn: str, source: str) -> None:
    """
    List ingestion source runs with their details, optionally filtered by URN or source.
    Required the Manage Metadata Ingestion permission.
    """

    query = """
    query listIngestionRuns($input: ListIngestionSourcesInput!) {
      listIngestionSources(input: $input) {
        ingestionSources {
          urn
          name
          executions {
            executionRequests {
              id
              result {
                startTimeMs
                status
              }
            }
          }
        }
      }
    }
    """

    # filter by urn and/or source using CONTAINS
    filters = []
    if urn:
        filters.append({"field": "urn", "values": [urn], "condition": "CONTAIN"})
    if source:
        filters.append({"field": "name", "values": [source], "condition": "CONTAIN"})

    variables = {
        "input": {
            "start": page_offset,
            "count": page_size,
            "filters": filters,
        }
    }

    client = get_default_graph(ClientMode.CLI)
    session = client._session
    gms_host = client.config.server

    url = f"{gms_host}/api/graphql"
    try:
        response = session.post(url, json={"query": query, "variables": variables})
        response.raise_for_status()
    except Exception as e:
        click.echo(f"Error fetching data: {str(e)}")
        return

    try:
        data = response.json()
    except ValueError:
        click.echo("Failed to parse JSON response from server.")
        return

    if not data:
        click.echo("No response received from the server.")
        return
    if "errors" in data:
        click.echo("Errors in response:")
        for error in data["errors"]:
            click.echo(f"- {error.get('message', 'Unknown error')}")
        return

    # a lot of responses can be null if there's errors in the run
    ingestion_sources = (
        data.get("data", {}).get("listIngestionSources", {}).get("ingestionSources", [])
    )

    if not ingestion_sources:
        click.echo("No ingestion sources or executions found.")
        return

    rows = []
    for ingestion_source in ingestion_sources:
        urn = ingestion_source.get("urn", "N/A")
        name = ingestion_source.get("name", "N/A")

        executions = ingestion_source.get("executions", {}).get("executionRequests", [])

        for execution in executions:
            if execution is None:
                continue

            execution_id = execution.get("id", "N/A")
            result = execution.get("result") or {}
            status = result.get("status", "N/A")

            try:
                start_time = (
                    datetime.fromtimestamp(
                        result.get("startTimeMs", 0) / 1000
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    if status != "DUPLICATE" and result.get("startTimeMs") is not None
                    else "N/A"
                )
            except (TypeError, ValueError):
                start_time = "N/A"

            rows.append([execution_id, name, start_time, status, urn])

    if not rows:
        click.echo("No execution data found.")
        return

    click.echo(
        tabulate(
            rows,
            headers=INGEST_SRC_TABLE_COLUMNS,
            tablefmt="grid",
        )
    )


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
def list_runs(page_offset: int, page_size: int, include_soft_deletes: bool) -> None:
    """List recent ingestion runs to datahub"""

    client = get_default_graph(ClientMode.CLI)
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
def show(
    run_id: str, start: int, count: int, include_soft_deletes: bool, show_aspect: bool
) -> None:
    """Describe a provided ingestion run to datahub"""
    client = get_default_graph(ClientMode.CLI)
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
def rollback(
    run_id: str, force: bool, dry_run: bool, safe: bool, report_dir: str
) -> None:
    """Rollback a provided ingestion run to datahub"""
    client = get_default_graph(ClientMode.CLI)

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


@ingest.command()
@click.argument("archive_path", type=str)
@click.option(
    "--password",
    type=str,
    required=False,
    help="Password for decrypting the recording archive. "
    "Can also be set via DATAHUB_RECORDING_PASSWORD env var.",
)
@click.option(
    "--live-sink",
    type=bool,
    is_flag=True,
    default=False,
    help="If enabled, emit to real GMS server instead of using recorded responses.",
)
@click.option(
    "--server",
    type=str,
    default=None,
    help="GMS server URL when using --live-sink. Defaults to value in recorded recipe.",
)
@click.option(
    "--report-to",
    type=str,
    default=None,
    help="Path to write the report file.",
)
@click.option(
    "--no-spinner",
    type=bool,
    is_flag=True,
    default=False,
    help="Turn off spinner",
)
@click.option(
    "--use-responses-lib",
    type=bool,
    is_flag=True,
    default=False,
    help="Use responses library for HTTP replay instead of VCR.py. "
    "Useful for sources with known VCR compatibility issues (e.g., Looker).",
)
@telemetry.with_telemetry(capture_kwargs=["live_sink"])
@upgrade.check_upgrade
def replay(
    archive_path: str,
    password: Optional[str],
    live_sink: bool,
    server: Optional[str],
    report_to: Optional[str],
    no_spinner: bool,
    use_responses_lib: bool,
) -> None:
    """
    Replay a recorded ingestion run for debugging.

    archive_path can be a local file path or an S3 URL (s3://bucket/path/to/recording.zip).

    This command runs an ingestion using recorded HTTP and database responses,
    allowing offline debugging without network access.

    Examples:
        # Replay from local file
        datahub ingest replay ./recording.zip --password secret

        # Replay from S3
        datahub ingest replay s3://bucket/recordings/run-id.zip --password secret

        # Replay with live sink (emit to real GMS)
        datahub ingest replay ./recording.zip --password secret --live-sink
    """
    from datahub.ingestion.recording.config import (
        check_recording_dependencies,
        get_recording_password_from_env,
    )
    from datahub.ingestion.recording.replay import IngestionReplayer

    # Check dependencies
    try:
        check_recording_dependencies()
    except ImportError as e:
        click.secho(str(e), fg="red", err=True)
        sys.exit(1)

    # Get password
    password = password or get_recording_password_from_env()
    if not password:
        click.secho(
            "Error: Password required. Provide via --password or "
            "DATAHUB_RECORDING_PASSWORD env var.",
            fg="red",
            err=True,
        )
        sys.exit(1)

    logger.info("DataHub CLI version: %s", nice_version_name())
    logger.info(f"Replaying recording from: {archive_path}")
    logger.info(f"Mode: {'live sink' if live_sink else 'air-gapped'}")

    def run_replay_with_library(use_responses_lib: bool) -> int:
        """Run replay with specified HTTP library. Returns exit code."""
        with IngestionReplayer(
            archive_path=archive_path,
            password=password,
            live_sink=live_sink,
            gms_server=server,
            use_responses_library=use_responses_lib,
        ) as replayer:
            recipe = replayer.get_recipe()
            logger.info(f"Loaded recording: run_id={replayer.run_id}")

            # Create and run pipeline
            pipeline = Pipeline.create(recipe)

            logger.info("Starting replay...")
            with click_spinner.spinner(disable=no_spinner):
                try:
                    pipeline.run()
                except Exception as e:
                    logger.info(
                        f"Source ({pipeline.source_type}) report:\n"
                        f"{pipeline.source.get_report().as_string()}"
                    )
                    logger.info(
                        f"Sink ({pipeline.sink_type}) report:\n"
                        f"{pipeline.sink.get_report().as_string()}"
                    )
                    raise e
                else:
                    logger.info("Replay complete")
                    pipeline.log_ingestion_stats()
                    ret = pipeline.pretty_print_summary()

                    if report_to:
                        with open(report_to, "w") as f:
                            f.write(pipeline.source.get_report().as_string())

                    return ret or 0
        return 0

    if use_responses_lib:
        # User explicitly requested responses library
        ret = run_replay_with_library(use_responses_lib=True)
        if ret:
            sys.exit(ret)
    else:
        # Try VCR.py first, fallback to responses on failure
        try:
            ret = run_replay_with_library(use_responses_lib=False)
            if ret:
                # VCR.py replay had failures - retry with responses library
                # This handles cases where VCR patching causes issues (e.g., Looker SDK)
                logger.warning(
                    "VCR.py replay had failures. Retrying with responses library..."
                )
                ret = run_replay_with_library(use_responses_lib=True)
                if ret:
                    sys.exit(ret)
        except Exception as vcr_error:
            logger.warning(f"VCR.py replay failed with exception: {vcr_error}")
            logger.info("Retrying replay with responses library...")
            ret = run_replay_with_library(use_responses_lib=True)
            if ret:
                sys.exit(ret)
