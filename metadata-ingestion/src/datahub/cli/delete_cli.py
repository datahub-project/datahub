import logging
import time
from dataclasses import dataclass
from random import choices
from typing import Dict, List, Optional, Tuple

import click
import progressbar
from requests import sessions
from tabulate import tabulate

from datahub.cli import cli_utils
from datahub.cli.cli_utils import guess_entity_type
from datahub.emitter import rest_emitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    StatusClass,
    SystemMetadataClass,
)
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)

ELASTIC_MAX_PAGE_SIZE = 10000

RUNS_TABLE_COLUMNS = ["runId", "rows", "created at"]
RUN_TABLE_COLUMNS = ["urn", "aspect name", "created at"]

UNKNOWN_NUM_RECORDS = -1


@dataclass
class DeletionResult:
    start_time_millis: int = int(time.time() * 1000.0)
    end_time_millis: int = 0
    num_records: int = 0
    num_entities: int = 0
    sample_records: Optional[List[List[str]]] = None

    def start(self) -> None:
        self.start_time_millis = int(time.time() * 1000.0)

    def end(self) -> None:
        self.end_time_millis = int(time.time() * 1000.0)

    def merge(self, another_result: "DeletionResult") -> None:
        self.end_time_millis = another_result.end_time_millis
        self.num_records = (
            self.num_records + another_result.num_records
            if another_result.num_records != UNKNOWN_NUM_RECORDS
            else UNKNOWN_NUM_RECORDS
        )
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
@click.option("--urn", required=False, type=str)
@click.option("-f", "--force", required=False, is_flag=True)
@click.option("--soft/--hard", required=False, is_flag=True, default=True)
@click.option("-e", "--env", required=False, type=str)
@click.option("-p", "--platform", required=False, type=str)
@click.option("--entity_type", required=False, type=str, default="dataset")
@click.option("--query", required=False, type=str)
@click.option("--registry-id", required=False, type=str)
@click.option("-n", "--dry-run", required=False, is_flag=True)
@click.option("--include-removed", required=False, is_flag=True)
@upgrade.check_upgrade
@telemetry.with_telemetry
def delete(
    urn: str,
    force: bool,
    soft: bool,
    env: str,
    platform: str,
    entity_type: str,
    query: str,
    registry_id: str,
    dry_run: bool,
    include_removed: bool,
) -> None:
    """Delete metadata from datahub using a single urn or a combination of filters"""

    cli_utils.test_connectivity_complain_exit("delete")
    # one of urn / platform / env / query must be provided
    if not urn and not platform and not env and not query and not registry_id:
        raise click.UsageError(
            "You must provide either an urn or a platform or an env or a query for me to delete anything"
        )

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
            soft=soft,
            dry_run=dry_run,
            entity_type=entity_type,
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
        # log warn include_removed + hard is the only way to work
        if include_removed and soft:
            logger.warn(
                "A filtered delete including soft deleted entities is redundant, because it is a soft delete by default. Please use --include-removed in conjunction with --hard"
            )
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
        )

    if not dry_run:
        message = "soft delete" if soft else "hard delete"
        click.echo(
            f"Took {(deletion_result.end_time_millis-deletion_result.start_time_millis)/1000.0} seconds to {message} {deletion_result.num_records} rows for {deletion_result.num_entities} entities"
        )
    else:
        click.echo(
            f"{deletion_result.num_entities} entities with {deletion_result.num_records if deletion_result.num_records != UNKNOWN_NUM_RECORDS else 'unknown'} rows will be affected. Took {(deletion_result.end_time_millis-deletion_result.start_time_millis)/1000.0} seconds to evaluate."
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
    search_query: str = "*",
    entity_type: str = "dataset",
    env: Optional[str] = None,
    platform: Optional[str] = None,
) -> DeletionResult:

    session, gms_host = cli_utils.get_session_and_host()
    token = cli_utils.get_token()

    logger.info(f"datahub configured with {gms_host}")
    emitter = rest_emitter.DatahubRestEmitter(gms_server=gms_host, token=token)
    batch_deletion_result = DeletionResult()
    urns = [
        u
        for u in cli_utils.get_urns_by_filter(
            env=env,
            platform=platform,
            search_query=search_query,
            entity_type=entity_type,
            include_removed=include_removed,
        )
    ]
    logger.info(
        f"Filter matched {len(urns)} entities. Sample: {choices(urns, k=min(5, len(urns)))}"
    )
    if not force:
        click.confirm(
            f"This will delete {len(urns)} entities. Are you sure?", abort=True
        )

    for urn in progressbar.progressbar(urns, redirect_stdout=True):
        one_result = _delete_one_urn(
            urn,
            soft=soft,
            entity_type=entity_type,
            dry_run=dry_run,
            cached_session_host=(session, gms_host),
            cached_emitter=emitter,
        )
        batch_deletion_result.merge(one_result)
    batch_deletion_result.end()

    return batch_deletion_result


def _delete_one_urn(
    urn: str,
    soft: bool = False,
    dry_run: bool = False,
    entity_type: str = "dataset",
    cached_session_host: Optional[Tuple[sessions.Session, str]] = None,
    cached_emitter: Optional[rest_emitter.DatahubRestEmitter] = None,
    run_id: str = "delete-run-id",
    deletion_timestamp: int = _get_current_time(),
) -> DeletionResult:

    deletion_result = DeletionResult()
    deletion_result.num_entities = 1
    deletion_result.num_records = UNKNOWN_NUM_RECORDS  # Default is unknown

    if soft:
        # Add removed aspect
        if not cached_emitter:
            _, gms_host = cli_utils.get_session_and_host()
            token = cli_utils.get_token()
            emitter = rest_emitter.DatahubRestEmitter(gms_server=gms_host, token=token)
        else:
            emitter = cached_emitter
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
    else:
        if not dry_run:
            payload_obj = {"urn": urn}
            urn, rows_affected = cli_utils.post_delete_endpoint(
                payload_obj,
                "/entities?action=delete",
                cached_session_host=cached_session_host,
            )
            deletion_result.num_records = rows_affected
        else:
            logger.info(f"[Dry-run] Would hard-delete {urn}")
            deletion_result.num_records = UNKNOWN_NUM_RECORDS  # since we don't know how many rows will be affected

    deletion_result.end()
    return deletion_result


@telemetry.with_telemetry
def delete_one_urn_cmd(
    urn: str,
    soft: bool = False,
    dry_run: bool = False,
    entity_type: str = "dataset",
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
