import logging
from dataclasses import dataclass
from datetime import datetime
from random import choices
from typing import Optional

import click
import progressbar
from click_default_group import DefaultGroup
from tabulate import tabulate

from datahub.cli import cli_utils
from datahub.configuration.datetimes import ClickDatetime
from datahub.emitter.aspect import ASPECT_MAP, TIMESERIES_ASPECT_MAP
from datahub.ingestion.graph.client import (
    DataHubGraph,
    RemovedStatusFilter,
    get_default_graph,
)
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)

RUN_TABLE_COLUMNS = ["urn", "aspect name", "created at"]

UNKNOWN_NUM_RECORDS = -1


@click.group(cls=DefaultGroup, default="by-filter")
def delete() -> None:
    """Delete metadata from DataHub."""
    pass


@dataclass
class DeletionResult:
    num_records: int = 0
    num_timeseries_records: int = 0
    num_entities: int = 0

    def merge(self, another_result: "DeletionResult") -> None:
        self.num_records = (
            self.num_records + another_result.num_records
            if another_result.num_records != UNKNOWN_NUM_RECORDS
            else UNKNOWN_NUM_RECORDS
        )
        self.num_timeseries_records += another_result.num_timeseries_records
        self.num_entities += another_result.num_entities


@delete.command()
@click.option(
    "--registry-id", required=False, type=str, help="e.g. mycompany-dq-model:0.0.1"
)
@click.option(
    "--soft/--hard",
    required=False,
    is_flag=True,
    default=True,
    help="specifies soft/hard deletion",
)
@click.option("-n", "--dry-run", required=False, is_flag=True)
@telemetry.with_telemetry()
def by_registry(
    registry_id: str,
    soft: bool,
    dry_run: bool,
) -> None:
    """
    Delete all metadata written using the given registry id and version pair.
    """

    if soft and not dry_run:
        raise click.UsageError(
            "Soft-deleting with a registry-id is not yet supported. Try --dry-run to see what you will be deleting, before issuing a hard-delete using the --hard flag"
        )

    with PerfTimer() as timer:
        registry_delete = {"registryId": registry_id, "dryRun": dry_run, "soft": soft}
        (
            structured_rows,
            entities_affected,
            aspects_affected,
            unsafe_aspects,
            unsafe_entity_count,
            unsafe_entities,
        ) = cli_utils.post_rollback_endpoint(
            registry_delete, "/entities?action=deleteAll"
        )

    if not dry_run:
        message = "soft delete" if soft else "hard delete"
        click.echo(
            f"Took {timer.elapsed_seconds()} seconds to {message}"
            f" {aspects_affected} versioned rows"
            f" for {entities_affected} entities."
        )
    else:
        click.echo(
            f"{entities_affected} entities with {aspects_affected} rows will be affected. "
            f"Took {timer.elapsed_seconds()} seconds to evaluate."
        )
    if structured_rows:
        click.echo(tabulate(structured_rows, RUN_TABLE_COLUMNS, tablefmt="grid"))


@delete.command()
@click.option("--urn", required=False, type=str, help="the urn of the entity")
@click.option(
    "-a",
    "--aspect",
    # option with `_` is inconsistent with rest of CLI but kept for backward compatibility
    "--aspect_name",
    "--aspect-name",
    required=False,
    type=str,
    help="the aspect name associated with the entity",
)
@click.option(
    "-f", "--force", required=False, is_flag=True, help="force the delete if set"
)
@click.option(
    "--soft/--hard",
    required=False,
    is_flag=True,
    default=True,
    help="specifies soft/hard deletion",
)
@click.option(
    "-e", "--env", required=False, type=str, help="the environment of the entity"
)
@click.option(
    "-p", "--platform", required=False, type=str, help="the platform of the entity"
)
@click.option(
    # option with `_` is inconsistent with rest of CLI but kept for backward compatibility
    "--entity_type",
    "--entity-type",
    required=False,
    type=str,
    help="the entity type of the entity",
)
@click.option("--query", required=False, type=str)
@click.option(
    "--start-time",
    required=False,
    type=ClickDatetime(),
    help="the start time (only for timeseries aspects)",
)
@click.option(
    "--end-time",
    required=False,
    type=ClickDatetime(),
    help="the end time (only for timeseries aspects)",
)
@click.option("-n", "--dry-run", required=False, is_flag=True)
@click.option("--only-soft-deleted", required=False, is_flag=True, default=False)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def by_filter(
    urn: Optional[str],
    aspect: Optional[str],
    force: bool,
    soft: bool,
    env: Optional[str],
    platform: Optional[str],
    entity_type: Optional[str],
    query: Optional[str],
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    dry_run: bool,
    only_soft_deleted: bool,
) -> None:
    """Delete metadata from datahub using a single urn or a combination of filters"""

    # Check urn / filters.
    if urn:
        if platform or env or query:
            raise click.UsageError(
                "You cannot provide both an urn and a filter rule (platform / env / query)."
            )
    elif not urn and not (entity_type or platform or env or query):
        raise click.UsageError(
            "You must provide an urn or a filter (entity-type / platform / env / query) in order to delete entities."
        )
    elif query:
        logger.warning(
            "Using --query is an advanced feature and can easily delete unintended entities. Please use with caution."
        )
    elif env and not (platform or entity_type):
        logger.warning(
            f"Using --env without other filters will delete all metadata in the {env} environment. Please use with caution."
        )

    # Check soft / hard delete flags.
    # Note: aspect_name not None ==> hard delete,
    # but aspect_name is None could be either soft or hard delete
    if soft:
        if aspect:
            raise click.UsageError(
                "You cannot provide an aspect name when performing a soft delete. Use --hard to perform a hard delete."
            )

        if only_soft_deleted:
            raise click.UsageError(
                "You cannot provide --only-soft-deleted when performing a soft delete. Use --hard to perform a hard delete."
            )

        soft_delete_filter = RemovedStatusFilter.NOT_SOFT_DELETED
    else:
        # For hard deletes, we will always include the soft-deleted entities, and
        # can optionally filter to exclude non-soft-deleted entities.
        if only_soft_deleted:
            soft_delete_filter = RemovedStatusFilter.ONLY_SOFT_DELETED
        else:
            soft_delete_filter = RemovedStatusFilter.ALL

    # TODO: add some validation on entity_type

    # Check the aspect name.
    if aspect and aspect not in ASPECT_MAP:
        logger.info(f"Supported aspects: {list(sorted(ASPECT_MAP.keys()))}")
        raise click.UsageError(
            f"Unknown aspect {aspect}. Ensure the aspect is in the above list."
        )

    # Check that start/end time are set if and only if the aspect is a timeseries aspect.
    if aspect and aspect in TIMESERIES_ASPECT_MAP:
        if not start_time or not end_time:
            raise click.UsageError(
                "You must provide both --start-time and --end-time when deleting a timeseries aspect."
            )
    elif start_time or end_time:
        raise click.UsageError(
            "You can only provide --start-time and --end-time when deleting a timeseries aspect."
        )
    elif aspect:
        raise click.UsageError(
            "Aspect-specific deletion is only supported for timeseries aspects. Please delete the full entity or use a rollback instead."
        )

    if not force and not soft and not dry_run:
        click.confirm(
            "This will permanently delete data from DataHub. Do you want to continue?",
            abort=True,
        )

    graph = get_default_graph()
    logger.info(f"Using graph: {graph}")

    if urn:
        delete_by_urn = True
        urns = [urn]
    else:
        delete_by_urn = False
        urns = list(
            graph.get_urns_by_filter(
                entity_types=[entity_type] if entity_type else None,
                platform=platform,
                env=env,
                query=query,
                status=soft_delete_filter,
            )
        )
        if len(urns) == 0:
            click.echo(
                "Found no urns to delete. Maybe you want to change your filters to be something different?"
            )
            return

        logger.info(
            f"Filter matched {len(urns)} urn(s). Sample: {choices(urns, k=min(5, len(urns)))}"
        )
        # TODO: Display a breakdown of urns by entity type.

        if not force and not dry_run:
            click.confirm(
                f"This will delete {len(urns)} entities from DataHub. Do you want to continue?",
                abort=True,
            )

    urns_iter = urns
    if not delete_by_urn and not dry_run:
        urns_iter = progressbar.progressbar(urns, redirect_stdout=True)

    deletion_result = DeletionResult()
    with PerfTimer() as timer:
        for urn in urns_iter:
            one_result = _delete_one_urn(
                graph=graph,
                urn=urn,
                aspect_name=aspect,
                soft=soft,
                dry_run=dry_run,
                start_time=start_time,
                end_time=end_time,
            )
            deletion_result.merge(one_result)

    # Report out a summary of the deletion result.
    if not dry_run:
        if (
            deletion_result.num_entities == 0
            and deletion_result.num_records == 0
            and deletion_result.num_timeseries_records == 0
        ):
            click.echo("Nothing deleted")
        else:
            delete_type = "soft delete" if soft else "hard delete"
            click.echo(
                f"Took {timer.elapsed_seconds()} seconds to {delete_type}"
                f" {deletion_result.num_records} versioned rows"
                f" and {deletion_result.num_timeseries_records} timeseries aspect rows"
                f" for {deletion_result.num_entities} entities."
            )
    else:
        # dry run reporting
        click.echo(
            f"{deletion_result.num_entities} entities with "
            f"{deletion_result.num_records if deletion_result.num_records != UNKNOWN_NUM_RECORDS else 'unknown'} versioned rows "
            f"and {deletion_result.num_timeseries_records if deletion_result.num_timeseries_records != UNKNOWN_NUM_RECORDS else 'unknown'} timeseries aspects "
            f"will be affected. "
            f"Took {timer.elapsed_seconds()} seconds to evaluate."
        )


def _delete_one_urn(
    graph: DataHubGraph,
    urn: str,
    soft: bool = False,
    dry_run: bool = False,
    aspect_name: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    run_id: str = "__datahub-delete-cli",
) -> DeletionResult:

    rows_affected: int = 0
    ts_rows_affected: int = 0

    if (
        not soft
        and not aspect_name
        and guess_entity_type(urn)
        in {
            "tag",
            "corpuser",
            "corpGroup",
            "domain",
            # TODO build this out
        }
    ):
        references_count, related_aspects = graph._delete_references_to_urn(
            urn=urn,
            dry_run=dry_run,
        )

        if references_count > 0:
            if dry_run:
                logger.info(
                    f"[Dry-run] Would remove {references_count} references to {urn}"
                )

            logger.debug(
                f"Full list of references to {urn}\n"
                + tabulate(
                    [x.values() for x in related_aspects],
                    ["relationship", "entity", "aspect"],
                )
            )

            # TODO update deletion report

    if soft:
        # entity soft delete
        assert not aspect_name, "aspects cannot be soft deleted"

        if not dry_run:
            graph.soft_delete_entity(urn=urn, run_id=run_id)
        else:
            logger.info(f"[Dry-run] Would soft-delete {urn}")

        rows_affected = 1

    elif aspect_name and aspect_name in TIMESERIES_ASPECT_MAP:
        # hard delete timeseries aspect

        if not dry_run:
            ts_rows_affected = graph.hard_delete_timeseries_aspect(
                urn=urn,
                aspect_name=aspect_name,
                start_time=start_time,
                end_time=end_time,
            )
        else:
            logger.info(
                f"[Dry-run] Would hard-delete {urn} timeseries aspect {aspect_name}"
            )
            ts_rows_affected = UNKNOWN_NUM_RECORDS

    elif aspect_name:
        # hard delete non-timeseries aspect

        # TODO: The backend doesn't support this yet.
        raise NotImplementedError(
            "Delete by aspect is not supported yet for non-timeseries aspects. Please delete the full entity or use rollback instead."
        )
        # graph.hard_delete_aspect(urn=urn, aspect_name=aspect_name)
        # rows_affected = 1

    else:
        # full entity hard delete
        if not dry_run:
            rows_affected, ts_rows_affected = graph.hard_delete_entity(
                urn=urn,
            )
        else:
            logger.info(f"[Dry-run] Would hard-delete {urn}")
            rows_affected = UNKNOWN_NUM_RECORDS
            ts_rows_affected = UNKNOWN_NUM_RECORDS

    return DeletionResult(
        num_entities=1,
        num_records=rows_affected,
        num_timeseries_records=ts_rows_affected,
    )
