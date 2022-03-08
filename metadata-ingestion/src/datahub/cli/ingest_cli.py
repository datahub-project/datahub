import fnmatch
import gc
import json
import logging
import pathlib
import sys
import tracemalloc
from datetime import datetime

import click
from click_default_group import DefaultGroup
from pydantic import ValidationError
from tabulate import tabulate

import datahub as datahub_package
from datahub.cli import cli_utils
from datahub.cli.cli_utils import (
    CONDENSED_DATAHUB_CONFIG_PATH,
    get_session_and_host,
    post_rollback_endpoint,
)
from datahub.configuration import SensitiveError
from datahub.configuration.config_loader import load_config_file
from datahub.ingestion.run.pipeline import Pipeline
from datahub.telemetry import telemetry

logger = logging.getLogger(__name__)

ELASTIC_MAX_PAGE_SIZE = 10000

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
    type=click.Path(exists=True, dir_okay=False),
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
    "--strict-warnings/--no-strict-warnings",
    default=False,
    help="If enabled, ingestion runs with warnings will yield a non-zero error code",
)
@click.option(
    "-ml",
    "--detect-memory-leaks",
    type=bool,
    is_flag=True,
    default=False,
    help="Run memory leak detection using tracemalloc.",
)
@telemetry.with_telemetry
def run(
    config: str,
    dry_run: bool,
    preview: bool,
    strict_warnings: bool,
    detect_memory_leaks: bool,
) -> None:
    """Ingest metadata into DataHub."""

    logger.info("DataHub CLI version: %s", datahub_package.nice_version_name())

    config_file = pathlib.Path(config)
    pipeline_config = load_config_file(config_file)

    try:
        logger.debug(f"Using config: {pipeline_config}")
        pipeline = Pipeline.create(pipeline_config, dry_run, preview)
    except ValidationError as e:
        click.echo(e, err=True)
        sys.exit(1)
    except Exception as e:
        # The pipeline_config may contain sensitive information, so we wrap the exception
        # in a SensitiveError to prevent detailed variable-level information from being logged.
        raise SensitiveError() from e

    if detect_memory_leaks:
        tracemalloc.start(25)
        if sys.version_info[0] >= 3 and sys.version_info[1] >= 9:
            tracemalloc.reset_peak()
        # snapshot_before_run = tracemalloc.take_snapshot()
        gc.set_debug(gc.DEBUG_LEAK)
    logger.info("Starting metadata ingestion")
    pipeline.run()
    logger.info("Finished metadata ingestion")
    ret = pipeline.pretty_print_summary(warnings_as_failure=strict_warnings)
    pipeline.log_ingestion_stats()

    def trace_has_file(trace: tracemalloc.Traceback, file_pattern: str) -> bool:
        for frame_index in range(0, len(trace)):
            cur_frame = trace[frame_index]
            if fnmatch.fnmatch(cur_frame.filename, file_pattern):
                return True
        return False

    if detect_memory_leaks:
        del pipeline
        logger.info(f"GC count before collect {gc.get_count()}")
        num_unreacheable_objects = gc.collect()
        logger.info(f"Number of unreachable objects = {num_unreacheable_objects}")
        logger.info(f"GC count after collect {gc.get_count()}")
        logger.info("Potentially leaking objects start")
        traces_seen = set()
        for obj in gc.garbage:
            obj_trace = tracemalloc.get_object_traceback(obj)
            if (obj_trace is not None) and (obj_trace not in traces_seen):
                traces_seen.add(obj_trace)
                if trace_has_file(obj_trace, "*lookml.py"):
                    referrers = gc.get_referrers(obj)
                    logger.info(
                        f"#Referrers:{len(referrers)}; Allocation Trace:\n\t"
                        + "\n\t".join(
                            obj_trace.format(limit=25, most_recent_first=True)
                        )
                    )
                    for ref_index, referrer in enumerate(referrers):
                        ref_trace = tracemalloc.get_object_traceback(referrer)
                        logger.info(
                            f"Referrer[{ref_index}] Object:{str(referrer)}, Trace:\n\t"
                            + "\n\t".join(
                                ref_trace.format(limit=5, most_recent_first=True)
                                if ref_trace
                                else []
                            )
                        )

        logger.info("Potentially leaking objects end")

        traced_memory_size, traced_memory_peak = tracemalloc.get_traced_memory()
        logger.info(
            f"Traced Memory: size={traced_memory_size}, peak={traced_memory_peak}"
        )
        # filter = tracemalloc.Filter(
        #    inclusive=True, filename_pattern="*datahub/ingestion*.py"
        # )
        snapshot_after_run = tracemalloc.take_snapshot()
        # snapshot_before_run = snapshot_before_run.filter_traces([filter])
        # snapshot_after_run = snapshot_after_run.filter_traces([filter])
        logger.info("After run stats")
        for stat in snapshot_after_run.statistics("lineno", cumulative=True):
            logger.info(
                f"size={stat.size}, count={stat.count}Trace=\n\t"
                + "\n\t".join(stat.traceback.format(limit=25, most_recent_first=True))
                + "\n\n"
            )
        # logger.info("Diff report")
        # for diff in snapshot_after_run.compare_to(snapshot_before_run, "lineno")[:10]:
        #    logger.info(f"{diff}\n{diff.traceback.format(limit=25, most_recent_first=True)}")
        # logger.info(diff)
        tracemalloc.stop()
    sys.exit(ret)


def get_runs_url(gms_host: str) -> str:
    return f"{gms_host}/runs?action=rollback"


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
@click.argument("page_offset", type=int, default=0)
@click.argument("page_size", type=int, default=100)
@telemetry.with_telemetry
def list_runs(page_offset: int, page_size: int) -> None:
    """List recent ingestion runs to datahub"""

    session, gms_host = get_session_and_host()

    url = f"{gms_host}/runs?action=list"

    payload_obj = {
        "pageOffset": page_offset,
        "pageSize": page_size,
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
@telemetry.with_telemetry
def show(run_id: str) -> None:
    """Describe a provided ingestion run to datahub"""

    payload_obj = {"runId": run_id, "dryRun": True}
    structured_rows, entities_affected, aspects_affected = post_rollback_endpoint(
        payload_obj, "/runs?action=rollback"
    )

    if aspects_affected >= ELASTIC_MAX_PAGE_SIZE:
        click.echo(
            f"this run created at least {entities_affected} new entities and updated at least {aspects_affected} aspects"
        )
    else:
        click.echo(
            f"this run created {entities_affected} new entities and updated {aspects_affected} aspects"
        )
    click.echo(
        "rolling back will delete the entities created and revert the updated aspects"
    )
    click.echo()
    click.echo(
        f"showing first {len(structured_rows)} of {aspects_affected} aspects touched by this run"
    )
    click.echo(tabulate(structured_rows, RUN_TABLE_COLUMNS, tablefmt="grid"))


@ingest.command()
@click.option("--run-id", required=True, type=str)
@click.option("-f", "--force", required=False, is_flag=True)
@click.option("--dry-run", "-n", required=False, is_flag=True, default=False)
@telemetry.with_telemetry
def rollback(run_id: str, force: bool, dry_run: bool) -> None:
    """Rollback a provided ingestion run to datahub"""

    cli_utils.test_connectivity_complain_exit("ingest")

    if not force and not dry_run:
        click.confirm(
            "This will permanently delete data from DataHub. Do you want to continue?",
            abort=True,
        )

    payload_obj = {"runId": run_id, "dryRun": dry_run}
    structured_rows, entities_affected, aspects_affected = post_rollback_endpoint(
        payload_obj, "/runs?action=rollback"
    )

    click.echo(
        "Rolling back deletes the entities created by a run and reverts the updated aspects"
    )
    click.echo(
        f"This rollback {'will' if dry_run else ''} {'delete' if dry_run else 'deleted'} {entities_affected} entities and {'will roll' if dry_run else 'rolled'} back {aspects_affected} aspects"
    )
    click.echo(
        f"showing first {len(structured_rows)} of {aspects_affected} aspects {'that will be' if dry_run else ''} reverted by this run"
    )
    click.echo(tabulate(structured_rows, RUN_TABLE_COLUMNS, tablefmt="grid"))
