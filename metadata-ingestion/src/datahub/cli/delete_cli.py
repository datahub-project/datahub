import logging
import time
from dataclasses import dataclass
from datetime import datetime
from random import choices
from typing import Any, Dict, List, Optional, Tuple

import click
import progressbar
from requests import sessions
from tabulate import tabulate

from datahub.cli import cli_utils
from datahub.emitter import rest_emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    StatusClass,
    SystemMetadataClass,
)
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)

RUN_TABLE_COLUMNS = ["urn", "aspect name", "created at"]

UNKNOWN_NUM_RECORDS = -1


@dataclass
class DeletionResult:
    start_time: int = int(time.time() * 1000.0)
    end_time: int = 0
    num_records: int = 0
    num_timeseries_records: int = 0
    num_entities: int = 0
    sample_records: Optional[List[List[str]]] = None

    def start(self) -> None:
        self.start_time = int(time.time() * 1000.0)

    def end(self) -> None:
        self.end_time = int(time.time() * 1000.0)

    def merge(self, another_result: "DeletionResult") -> None:
        self.end_time = another_result.end_time
        self.num_records = (
            self.num_records + another_result.num_records
            if another_result.num_records != UNKNOWN_NUM_RECORDS
            else UNKNOWN_NUM_RECORDS
        )
        self.num_timeseries_records += another_result.num_timeseries_records
        self.num_entities += another_result.num_entities
        if another_result.sample_records:
            if not self.sample_records:
                self.sample_records = []
            self.sample_records.extend(another_result.sample_records)


@telemetry.with_telemetry
def delete_for_registry(
    registry_id: str,
    soft: bool,
    dry_run: bool,
) -> DeletionResult:
    deletion_result = DeletionResult()
    deletion_result.num_entities = 1
    deletion_result.num_records = UNKNOWN_NUM_RECORDS  # Default is unknown
    registry_delete = {"registryId": registry_id, "dryRun": dry_run, "soft": soft}
    (
        structured_rows,
        entities_affected,
        aspects_affected,
        unsafe_aspects,
        unsafe_entity_count,
        unsafe_entities,
    ) = cli_utils.post_rollback_endpoint(registry_delete, "/entities?action=deleteAll")
    deletion_result.num_entities = entities_affected
    deletion_result.num_records = aspects_affected
    deletion_result.sample_records = structured_rows
    deletion_result.end()
    return deletion_result


@click.command()
@click.option("--urn", required=False, type=str, help="the urn of the entity")
@click.option(
    "-a",
    "--aspect_name",
    required=False,
    type=str,
    help="the aspect name associated with the entity(only for timeseries aspects)",
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
    "--entity_type",
    required=False,
    type=str,
    default="dataset",
    help="the entity_type of the entity",
)
@click.option("--query", required=False, type=str)
@click.option(
    "--start-time",
    required=False,
    type=click.DateTime(),
    help="the start time(only for timeseries aspects)",
)
@click.option(
    "--end-time",
    required=False,
    type=click.DateTime(),
    help="the end time(only for timeseries aspects)",
)
@click.option("--registry-id", required=False, type=str)
@click.option("-n", "--dry-run", required=False, is_flag=True)
@click.option("--only-soft-deleted", required=False, is_flag=True, default=False)
@upgrade.check_upgrade
@telemetry.with_telemetry
def delete(
    urn: str,
    aspect_name: Optional[str],
    force: bool,
    soft: bool,
    env: str,
    platform: str,
    entity_type: str,
    query: str,
    start_time: Optional[datetime],
    end_time: Optional[datetime],
    registry_id: str,
    dry_run: bool,
    only_soft_deleted: bool,
) -> None:
    """Delete metadata from datahub using a single urn or a combination of filters"""

    cli_utils.test_connectivity_complain_exit("delete")
    # one of urn / platform / env / query must be provided
    if not urn and not platform and not env and not query and not registry_id:
        raise click.UsageError(
            "You must provide either an urn or a platform or an env or a query for me to delete anything"
        )

    include_removed: bool
    if soft:
        # For soft-delete include-removed does not make any sense
        include_removed = False
    else:
        # For hard-delete we always include the soft-deleted items
        include_removed = True

    # default query is set to "*" if not provided
    query = "*" if query is None else query

    if not force and not soft and not dry_run:
        click.confirm(
            "This will permanently delete data from DataHub. Do you want to continue?",
            abort=True,
        )

    if urn:
        # Single urn based delete
        session, host = cli_utils.get_session_and_host()
        entity_type = guess_entity_type(urn=urn)
        logger.info(f"DataHub configured with {host}")

        references_count, related_aspects = delete_references(
            urn, dry_run=True, cached_session_host=(session, host)
        )
        remove_references: bool = False

        if references_count > 0:
            print(
                f"This urn was referenced in {references_count} other aspects across your metadata graph:"
            )
            click.echo(
                tabulate(
                    [x.values() for x in related_aspects],
                    ["relationship", "entity", "aspect"],
                    tablefmt="grid",
                )
            )
            remove_references = click.confirm("Do you want to delete these references?")

        if remove_references:
            delete_references(urn, dry_run=False, cached_session_host=(session, host))

        deletion_result: DeletionResult = delete_one_urn_cmd(
            urn,
            aspect_name=aspect_name,
            soft=soft,
            dry_run=dry_run,
            entity_type=entity_type,
            start_time=start_time,
            end_time=end_time,
            cached_session_host=(session, host),
        )

        if not dry_run:
            if deletion_result.num_records == 0:
                click.echo(f"Nothing deleted for {urn}")
            else:
                click.echo(
                    f"Successfully deleted {urn}. {deletion_result.num_records} rows deleted"
                )

    elif registry_id:
        # Registry-id based delete
        if soft and not dry_run:
            raise click.UsageError(
                "Soft-deleting with a registry-id is not yet supported. Try --dry-run to see what you will be deleting, before issuing a hard-delete using the --hard flag"
            )
        deletion_result = delete_for_registry(
            registry_id=registry_id, soft=soft, dry_run=dry_run
        )
    else:
        # Filter based delete
        deletion_result = delete_with_filters(
            env=env,
            platform=platform,
            dry_run=dry_run,
            soft=soft,
            entity_type=entity_type,
            search_query=query,
            force=force,
            include_removed=include_removed,
            aspect_name=aspect_name,
            only_soft_deleted=only_soft_deleted,
        )

    if not dry_run:
        message = "soft delete" if soft else "hard delete"
        click.echo(
            f"Took {(deletion_result.end_time-deletion_result.start_time)/1000.0} seconds to {message}"
            f" {deletion_result.num_records} versioned rows"
            f" and {deletion_result.num_timeseries_records} timeseries aspect rows"
            f" for {deletion_result.num_entities} entities."
        )
    else:
        click.echo(
            f"{deletion_result.num_entities} entities with {deletion_result.num_records if deletion_result.num_records != UNKNOWN_NUM_RECORDS else 'unknown'} rows will be affected. Took {(deletion_result.end_time-deletion_result.start_time)/1000.0} seconds to evaluate."
        )
    if deletion_result.sample_records:
        click.echo(
            tabulate(deletion_result.sample_records, RUN_TABLE_COLUMNS, tablefmt="grid")
        )


def _get_current_time() -> int:
    return int(time.time() * 1000.0)


@telemetry.with_telemetry
def delete_with_filters(
    dry_run: bool,
    soft: bool,
    force: bool,
    include_removed: bool,
    aspect_name: Optional[str] = None,
    search_query: str = "*",
    entity_type: str = "dataset",
    env: Optional[str] = None,
    platform: Optional[str] = None,
    only_soft_deleted: Optional[bool] = False,
) -> DeletionResult:

    session, gms_host = cli_utils.get_session_and_host()
    token = cli_utils.get_token()

    logger.info(f"datahub configured with {gms_host}")
    emitter = rest_emitter.DatahubRestEmitter(gms_server=gms_host, token=token)
    batch_deletion_result = DeletionResult()

    urns: List[str] = []
    if not only_soft_deleted:
        urns = list(
            cli_utils.get_urns_by_filter(
                env=env,
                platform=platform,
                search_query=search_query,
                entity_type=entity_type,
                include_removed=False,
            )
        )

    soft_deleted_urns: List[str] = []
    if include_removed or only_soft_deleted:
        soft_deleted_urns = list(
            cli_utils.get_urns_by_filter(
                env=env,
                platform=platform,
                search_query=search_query,
                entity_type=entity_type,
                only_soft_deleted=True,
            )
        )

    final_message = ""
    if len(urns) > 0:
        final_message = f"{len(urns)} "
    if len(urns) > 0 and len(soft_deleted_urns) > 0:
        final_message += "and "
    if len(soft_deleted_urns) > 0:
        final_message = f"{len(soft_deleted_urns)} (soft-deleted) "

    logger.info(
        f"Filter matched {final_message} {entity_type} entities of {platform}. Sample: {choices(urns, k=min(5, len(urns)))}"
    )
    if len(urns) == 0 and len(soft_deleted_urns) == 0:
        click.echo(
            f"No urns to delete. Maybe you want to change entity_type={entity_type} or platform={platform} to be something different?"
        )
        return DeletionResult(end_time=int(time.time() * 1000.0))

    if not force and not dry_run:
        type_delete = "soft" if soft else "permanently"
        click.confirm(
            f"This will {type_delete} delete {len(urns)} entities. Are you sure?",
            abort=True,
        )

    if len(urns) > 0:
        for urn in progressbar.progressbar(urns, redirect_stdout=True):
            one_result = _delete_one_urn(
                urn,
                soft=soft,
                aspect_name=aspect_name,
                entity_type=entity_type,
                dry_run=dry_run,
                cached_session_host=(session, gms_host),
                cached_emitter=emitter,
            )
            batch_deletion_result.merge(one_result)

    if len(soft_deleted_urns) > 0 and not soft:
        click.echo("Starting to delete soft-deleted URNs")
        for urn in progressbar.progressbar(soft_deleted_urns, redirect_stdout=True):
            one_result = _delete_one_urn(
                urn,
                soft=soft,
                entity_type=entity_type,
                dry_run=dry_run,
                cached_session_host=(session, gms_host),
                cached_emitter=emitter,
                is_soft_deleted=True,
            )
            batch_deletion_result.merge(one_result)
    batch_deletion_result.end()

    return batch_deletion_result


def _delete_one_urn(
    urn: str,
    soft: bool = False,
    dry_run: bool = False,
    entity_type: str = "dataset",
    aspect_name: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    cached_session_host: Optional[Tuple[sessions.Session, str]] = None,
    cached_emitter: Optional[rest_emitter.DatahubRestEmitter] = None,
    run_id: str = "delete-run-id",
    deletion_timestamp: int = _get_current_time(),
    is_soft_deleted: Optional[bool] = None,
) -> DeletionResult:

    soft_delete_msg: str = ""
    if dry_run and is_soft_deleted:
        soft_delete_msg = "(soft-deleted)"

    deletion_result = DeletionResult()
    deletion_result.num_entities = 1
    deletion_result.num_records = UNKNOWN_NUM_RECORDS  # Default is unknown

    if soft:
        if aspect_name:
            raise click.UsageError(
                "Please provide --hard flag, as aspect values cannot be soft deleted."
            )
        # Add removed aspect
        if cached_emitter:
            emitter = cached_emitter
        else:
            _, gms_host = cli_utils.get_session_and_host()
            token = cli_utils.get_token()
            emitter = rest_emitter.DatahubRestEmitter(gms_server=gms_host, token=token)
        if not dry_run:
            emitter.emit_mcp(
                MetadataChangeProposalWrapper(
                    entityType=entity_type,
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=urn,
                    aspectName="status",
                    aspect=StatusClass(removed=True),
                    systemMetadata=SystemMetadataClass(
                        runId=run_id, lastObserved=deletion_timestamp
                    ),
                )
            )
        else:
            logger.info(f"[Dry-run] Would soft-delete {urn}")
    elif not dry_run:
        payload_obj: Dict[str, Any] = {"urn": urn}
        if aspect_name:
            payload_obj["aspectName"] = aspect_name
        if start_time:
            payload_obj["startTimeMillis"] = int(round(start_time.timestamp() * 1000))
        if end_time:
            payload_obj["endTimeMillis"] = int(round(end_time.timestamp() * 1000))
        rows_affected: int
        ts_rows_affected: int
        urn, rows_affected, ts_rows_affected = cli_utils.post_delete_endpoint(
            payload_obj,
            "/entities?action=delete",
            cached_session_host=cached_session_host,
        )
        deletion_result.num_records = rows_affected
        deletion_result.num_timeseries_records = ts_rows_affected
    else:
        logger.info(f"[Dry-run] Would hard-delete {urn} {soft_delete_msg}")
        deletion_result.num_records = (
            UNKNOWN_NUM_RECORDS  # since we don't know how many rows will be affected
        )

    deletion_result.end()
    return deletion_result


@telemetry.with_telemetry
def delete_one_urn_cmd(
    urn: str,
    aspect_name: Optional[str] = None,
    soft: bool = False,
    dry_run: bool = False,
    entity_type: str = "dataset",
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    cached_session_host: Optional[Tuple[sessions.Session, str]] = None,
    cached_emitter: Optional[rest_emitter.DatahubRestEmitter] = None,
) -> DeletionResult:
    """
    Wrapper around delete_one_urn because it is also called in a loop via delete_with_filters.

    This is a separate function that is called only when a single URN is deleted via the CLI.
    """

    return _delete_one_urn(
        urn,
        soft,
        dry_run,
        entity_type,
        aspect_name,
        start_time,
        end_time,
        cached_session_host,
        cached_emitter,
    )


def delete_references(
    urn: str,
    dry_run: bool = False,
    cached_session_host: Optional[Tuple[sessions.Session, str]] = None,
) -> Tuple[int, List[Dict]]:
    payload_obj = {"urn": urn, "dryRun": dry_run}
    return cli_utils.post_delete_references_endpoint(
        payload_obj,
        "/entities?action=deleteReferences",
        cached_session_host=cached_session_host,
    )
