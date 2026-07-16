"""DataHub CLI commands for migrating entities between platform instances."""

import json
import logging
import random
import uuid
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import click
import progressbar
from avrogen.dict_wrapper import DictWrapper

from datahub.cli import cli_utils, delete_cli, migration_utils
from datahub.cli.migration_utils import (
    ALL_ENTITY_TYPES,
    ENV_ENTITY_TYPES,
    ConflictStrategy,
    make_urn_builder,
    merge_entity,
)
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    BigQueryDatasetKey,
    DatabaseKey,
    ProjectIdKey,
    SchemaKey,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import (
    ClientMode,
    DataHubGraph,
    get_default_graph,
)
from datahub.metadata.schema_classes import (
    ContainerKeyClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    SystemMetadataClass,
)
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.urns.urn import Urn, guess_entity_type

log = logging.getLogger(__name__)


# --- Migration Report ---


class MigrationReport:
    def __init__(self, run_id: str, dry_run: bool, keep: bool) -> None:
        self.run_id = run_id
        self.dry_run = dry_run
        self.keep = keep
        self.num_events = 0
        self.entities_migrated: Dict[Tuple[str, str], int] = {}
        self.entities_created: Dict[Tuple[str, str], int] = {}
        self.entities_affected: Dict[Tuple[str, str], int] = {}
        self.conflicts_skipped: int = 0
        self.aspects_merged: int = 0
        self.entities_errored: List[Tuple[str, str]] = []

    def on_entity_migrated(self, urn: str, aspect: str) -> None:
        self.num_events += 1
        if (urn, aspect) not in self.entities_migrated:
            self.entities_migrated[(urn, aspect)] = 1

    def on_entity_create(self, urn: str, aspect: str) -> None:
        self.num_events += 1
        if (urn, aspect) not in self.entities_created:
            self.entities_created[(urn, aspect)] = 1

    def on_entity_affected(self, urn: str, aspect: str) -> None:
        self.num_events += 1
        if (urn, aspect) not in self.entities_affected:
            self.entities_affected[(urn, aspect)] = 1
        else:
            self.entities_affected[(urn, aspect)] += 1

    def _get_prefix(self) -> str:
        return "[Dry Run] " if self.dry_run else ""

    def __repr__(self) -> str:
        p = self._get_prefix()
        lines = [
            f"{p}Migration Report:",
            "--------------",
            f"{p}Migration Run Id: {self.run_id}",
            f"{p}Num entities created = {len(set(x[0] for x in self.entities_created))}",
            f"{p}Num entities affected = {len(set(x[0] for x in self.entities_affected))}",
            f"{p}Num entities {'kept' if self.keep else 'migrated'} = {len(set(x[0] for x in self.entities_migrated))}",
        ]
        if self.aspects_merged > 0:
            lines.append(f"{p}Aspects merged = {self.aspects_merged}")
        if self.conflicts_skipped > 0:
            lines.append(f"{p}Conflicts skipped = {self.conflicts_skipped}")
        if self.entities_errored:
            lines.append(f"{p}Entities errored = {len(self.entities_errored)}")
            for urn, err in self.entities_errored:
                lines.append(f"{p}  {urn}: {err}")
        lines.append(f"{p}Details:")
        lines.append(
            f"{p}New Entities Created: {set(x[0] for x in self.entities_created) or 'None'}"
        )
        lines.append(
            f"{p}External Entities Affected: {set(x[0] for x in self.entities_affected) or 'None'}"
        )
        lines.append(
            f"{p}Old Entities {'Kept' if self.keep else 'Migrated'} = {set(x[0] for x in self.entities_migrated) or 'None'}"
        )
        return "\n".join(lines)


@click.group()
@telemetry.with_telemetry()
def migrate() -> None:
    """Helper commands for migrating metadata within DataHub."""
    pass


def _parse_entity_types(
    entity_types_arg: Optional[str], force: bool = False, dry_run: bool = False
) -> List[str]:
    """Parse --entity-types CLI argument into a validated list of entity types."""
    if entity_types_arg is None:
        return list(ALL_ENTITY_TYPES)
    types = [t.strip() for t in entity_types_arg.split(",") if t.strip()]
    invalid = [t for t in types if t not in ALL_ENTITY_TYPES]
    if invalid:
        raise click.BadParameter(
            f"Unknown entity type(s): {', '.join(invalid)}. "
            f"Available: {', '.join(ALL_ENTITY_TYPES)}",
            param_hint="--entity-types",
        )
    _warn_dataflow_datajob_coupling(types, force=force, dry_run=dry_run)
    return types


def _warn_dataflow_datajob_coupling(
    types: List[str], force: bool = False, dry_run: bool = False
) -> None:
    """Warn if dataFlow and dataJob are not migrated together."""
    has_flow = "dataFlow" in types
    has_job = "dataJob" in types
    if has_flow and not has_job:
        click.echo(
            "\n⚠️  Warning: migrating dataFlow without dataJob. "
            "DataJob URNs embed their parent DataFlow URN — if you migrate "
            "flows without their jobs, the jobs will reference stale flow URNs."
        )
        if not force and not dry_run:
            click.confirm("Continue without dataJob?", abort=True)
    elif has_job and not has_flow:
        click.echo(
            "\n⚠️  Warning: migrating dataJob without dataFlow. "
            "DataJob URNs embed their parent DataFlow URN — migrating jobs "
            "without their parent flows may produce inconsistent URNs."
        )
        if not force and not dry_run:
            click.confirm("Continue without dataFlow?", abort=True)


# --- Core migration logic ---


def _migrate_single_entity(
    src_entity_urn: str,
    make_new_urn: Callable[[str], str],
    platform: str,
    target_instance: str,
    dry_run: bool,
    hard: bool,
    keep: bool,
    run_id: str,
    graph: DataHubGraph,
    on_conflict: Optional[ConflictStrategy],
    system_metadata: SystemMetadataClass,
    migration_report: MigrationReport,
) -> None:
    """Migrate a single entity URN to a new URN."""
    new_urn = make_new_urn(src_entity_urn)
    log.debug(f"Will migrate {src_entity_urn} to {new_urn}")

    # Check if target already exists (for merge mode)
    target_exists = False
    if on_conflict is not None:
        try:
            target_exists = graph.exists(new_urn)
        except Exception:
            target_exists = False

    if target_exists and on_conflict is not None:
        log.info(f"Target {new_urn} exists — merging aspects")
        merged, skipped = merge_entity(
            src_entity_urn, new_urn, on_conflict, graph, dry_run
        )
        migration_report.aspects_merged += merged
        migration_report.conflicts_skipped += skipped
    else:
        for mcp in migration_utils.clone_aspect(
            src_entity_urn,
            aspect_names=migration_utils.all_aspects,
            dst_urn=new_urn,
            run_id=run_id,
        ):
            if not dry_run:
                graph.emit_mcp(mcp)
            migration_report.on_entity_create(mcp.entityUrn, mcp.aspectName)  # type: ignore

    # Always emit dataPlatformInstance
    if not dry_run:
        graph.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=new_urn,
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(platform),
                    instance=make_dataplatform_instance_urn(platform, target_instance),
                ),
                systemMetadata=system_metadata,
            )
        )
    migration_report.on_entity_create(new_urn, "dataPlatformInstance")

    # Update incoming relationships
    for relationship in migration_utils.get_incoming_relationships(src_entity_urn):
        target_urn = relationship.urn
        entity_type = guess_entity_type(target_urn)
        relationship_type = relationship.relationship_type
        aspect_name = migration_utils.get_aspect_name_from_relationship(
            relationship_type, entity_type
        )
        aspect_map = cli_utils.get_aspects_for_entity(
            graph._session,
            graph.config.server,
            target_urn,
            aspects=[aspect_name],
            typed=True,
        )
        if aspect_name in aspect_map:
            aspect = aspect_map[aspect_name]
            assert isinstance(aspect, DictWrapper)
            aspect = migration_utils.modify_urn_list_for_aspect(
                aspect_name, aspect, relationship_type, src_entity_urn, new_urn
            )
            mcp = MetadataChangeProposalWrapper(entityUrn=target_urn, aspect=aspect)
            if not dry_run:
                graph.emit_mcp(mcp)
            migration_report.on_entity_affected(mcp.entityUrn, mcp.aspectName)  # type: ignore
        else:
            log.debug(f"Didn't find aspect {aspect_name} for urn {target_urn}")

    if not dry_run and not keep:
        log.info(f"will {'hard' if hard else 'soft'} delete {src_entity_urn}")
        delete_cli._delete_one_urn(graph, src_entity_urn, soft=not hard, run_id=run_id)
    migration_report.on_entity_migrated(src_entity_urn, "status")  # type: ignore


def _migrate_entities(
    urns_to_migrate: List[str],
    make_new_urn: Callable[[str], str],
    platform: str,
    target_instance: str,
    dry_run: bool,
    force: bool,
    hard: bool,
    keep: bool,
    run_id: str,
    graph: DataHubGraph,
    on_conflict: Optional[ConflictStrategy] = None,
    skip_on_error: bool = False,
) -> MigrationReport:
    """Migrate a list of entity URNs to new URNs, updating relationships."""
    migration_report = MigrationReport(run_id, dry_run, keep)
    system_metadata = SystemMetadataClass(runId=run_id)

    if not force and not dry_run:
        sampled_urns = random.sample(urns_to_migrate, k=min(10, len(urns_to_migrate)))
        sampled_new_urns = [make_new_urn(u) for u in sampled_urns]
        click.echo(f"Will migrate {len(urns_to_migrate)} urns such as {sampled_urns}")
        click.echo(f"New urns will look like {sampled_new_urns}")
        click.confirm("Ok to proceed?", abort=True)

    for src_entity_urn in progressbar.progressbar(
        urns_to_migrate, redirect_stdout=True
    ):
        try:
            _migrate_single_entity(
                src_entity_urn=src_entity_urn,
                make_new_urn=make_new_urn,
                platform=platform,
                target_instance=target_instance,
                dry_run=dry_run,
                hard=hard,
                keep=keep,
                run_id=run_id,
                graph=graph,
                on_conflict=on_conflict,
                system_metadata=system_metadata,
                migration_report=migration_report,
            )
        except Exception as e:
            if skip_on_error:
                log.warning(f"Error migrating {src_entity_urn}, skipping: {e}")
                migration_report.entities_errored.append((src_entity_urn, str(e)))
            else:
                click.echo(
                    f"\nError migrating {src_entity_urn}: {e}\n"
                    "Hint: use --skip-on-error to skip problematic entities "
                    "and continue with the rest."
                )
                raise

    return migration_report


def _migrate_containers(
    env: str,
    platform: str,
    target_instance: str,
    should_migrate: Callable[[Dict[str, str]], bool],
    dry_run: bool,
    hard: bool,
    keep: bool,
    rest_emitter: DataHubGraph,
) -> None:
    """Migrate containers matching a filter to a new platform instance."""
    run_id: str = f"container-migrate-{uuid.uuid4()}"
    migration_report = MigrationReport(run_id, dry_run, keep)

    container_id_map: Dict[str, str] = {}
    containers = _get_containers_for_migration(env)
    skipped_count = 0
    for container in progressbar.progressbar(containers, redirect_stdout=True):
        subType = container["aspects"]["subTypes"]["value"]["typeNames"][0]
        customProperties = container["aspects"]["containerProperties"]["value"][
            "customProperties"
        ]
        if not should_migrate(customProperties):
            log.debug(
                f"{container['urn']} does not match filter criteria, skipping.. "
                f"{customProperties}"
            )
            skipped_count += 1
            continue

        try:
            newKey: Union[SchemaKey, DatabaseKey, ProjectIdKey, BigQueryDatasetKey]
            if subType == "Schema":
                newKey = SchemaKey.model_validate(customProperties)
            elif subType == "Database":
                newKey = DatabaseKey.model_validate(customProperties)
            elif subType == "Project":
                newKey = ProjectIdKey.model_validate(customProperties)
            elif subType == "Dataset":
                newKey = BigQueryDatasetKey.model_validate(customProperties)
            else:
                log.warning(f"Invalid subtype {subType}. Skipping")
                continue
        except Exception as e:
            log.warning(f"Unable to map {customProperties} to key due to exception {e}")
            continue

        newKey.instance = target_instance

        src_urn = container["urn"]
        dst_urn = f"urn:li:container:{newKey.guid()}"
        container_id_map[src_urn] = dst_urn

        for mcp in migration_utils.clone_aspect(
            src_urn,
            aspect_names=migration_utils.all_aspects,
            dst_urn=dst_urn,
            run_id=run_id,
        ):
            migration_report.on_entity_create(mcp.entityUrn, mcp.aspectName)  # type: ignore
            assert mcp.aspect
            if mcp.aspectName == "containerProperties":
                assert isinstance(mcp.aspect, ContainerPropertiesClass)
                mcp.aspect.customProperties = newKey.model_dump(
                    by_alias=True, exclude_none=True
                )
            elif mcp.aspectName == "containerKey":
                assert isinstance(mcp.aspect, ContainerKeyClass)
                mcp.aspect.guid = newKey.guid()
            if not dry_run:
                rest_emitter.emit_mcp(mcp)
                migration_report.on_entity_affected(mcp.entityUrn, mcp.aspectName)  # type: ignore

        _process_container_relationships(
            container_id_map, dry_run, src_urn, dst_urn, migration_report, rest_emitter
        )

        if not dry_run and not keep:
            log.info(f"will {'hard' if hard else 'soft'} delete {src_urn}")
            delete_cli._delete_one_urn(
                rest_emitter, src_urn, soft=not hard, run_id=run_id
            )
        migration_report.on_entity_migrated(src_urn, "status")  # type: ignore

    if skipped_count > 0:
        click.echo(
            f"Skipped {skipped_count} containers that didn't match filter criteria. "
            "These containers may not have the expected platform/instance properties set."
        )
    click.echo(f"{migration_report}")


def _get_containers_for_migration(env: str) -> List[Any]:
    client = get_default_graph(ClientMode.CLI)
    containers_to_migrate = list(
        client.get_urns_by_filter(entity_types=["container"], env=env)
    )
    containers = []
    increment = 20
    for i in range(0, len(containers_to_migrate), increment):
        for container in _batch_get_ids(
            client, containers_to_migrate[i : i + increment]
        ):
            log.debug(container)
            containers.append(container)
    return containers


def _batch_get_ids(client: DataHubGraph, ids: List[str]) -> Iterable[Dict]:
    session = client._session
    url = client.config.server + "/entitiesV2"
    ids_to_get = [Urn.url_encode(id) for id in ids]
    response = session.get(f"{url}?ids=List({','.join(ids_to_get)})")

    if response.status_code == 200:
        assert response._content
        results = json.loads(response._content)
        num_entities = len(results["results"])
        entities_yielded = 0
        for x in results["results"].values():
            entities_yielded += 1
            yield x
        assert entities_yielded == num_entities
    else:
        log.error(f"Failed to execute batch get with {str(response.content)}")
        response.raise_for_status()


def _process_container_relationships(
    container_id_map: Dict[str, str],
    dry_run: bool,
    src_urn: str,
    dst_urn: str,
    migration_report: MigrationReport,
    rest_emitter: DatahubRestEmitter,
) -> None:
    client = get_default_graph(ClientMode.CLI)
    for relationship in migration_utils.get_incoming_relationships(urn=src_urn):
        target_urn: str = relationship.urn
        if target_urn in container_id_map:
            target_urn = container_id_map[target_urn]

        entity_type = guess_entity_type(target_urn)
        relationship_type = relationship.relationship_type
        aspect_name = migration_utils.get_aspect_name_from_relationship(
            relationship_type, entity_type
        )
        aspect_map = cli_utils.get_aspects_for_entity(
            client._session,
            client.config.server,
            target_urn,
            aspects=[aspect_name],
            typed=True,
        )
        if aspect_name in aspect_map:
            aspect = aspect_map[aspect_name]
            assert isinstance(aspect, DictWrapper)
            aspect = migration_utils.modify_urn_list_for_aspect(
                aspect_name, aspect, relationship_type, src_urn, dst_urn
            )
            mcp = MetadataChangeProposalWrapper(entityUrn=target_urn, aspect=aspect)
            if not dry_run:
                rest_emitter.emit_mcp(mcp)
            migration_report.on_entity_affected(mcp.entityUrn, mcp.aspectName)  # type: ignore
        else:
            log.debug(f"Didn't find aspect {aspect_name} for urn {target_urn}")


# --- CLI Commands ---


@migrate.command()
@click.option("--platform", type=str, required=True)
@click.option("--instance", type=str, required=True)
@click.option("--dry-run", "-n", type=bool, is_flag=True, default=False)
@click.option("--env", type=str, default=DEFAULT_ENV)
@click.option("-F", "--force", type=bool, is_flag=True, default=False)
@click.option(
    "--hard",
    type=bool,
    is_flag=True,
    default=False,
    help="Hard-delete previous entities instead of soft-delete.",
)
@click.option(
    "--keep",
    type=bool,
    is_flag=True,
    default=False,
    help="Do not delete previous entities.",
)
@click.option(
    "--on-conflict",
    type=click.Choice(["overwrite", "patch", "prompt"]),
    default="overwrite",
    help="How to handle existing target entities.",
)
@click.option(
    "--skip-on-error",
    type=bool,
    is_flag=True,
    default=False,
    help="Skip entities that cause errors instead of aborting.",
)
@click.option(
    "--entity-types",
    type=str,
    default=None,
    help=(
        "Comma-separated list of entity types to migrate. "
        f"Available: {','.join(ALL_ENTITY_TYPES)}. "
        "Default: all types."
    ),
)
@telemetry.with_telemetry()
@upgrade.check_upgrade
def dataplatform2instance(
    instance: str,
    platform: str,
    dry_run: bool,
    env: str,
    force: bool,
    hard: bool,
    keep: bool,
    on_conflict: str,
    skip_on_error: bool,
    entity_types: Optional[str],
) -> None:
    """Migrate entities from one dataplatform to a dataplatform instance."""
    click.echo(
        f"Starting migration: platform:{platform}, instance={instance}, "
        f"force={force}, dry-run={dry_run}"
    )
    run_id = f"migrate-{uuid.uuid4()}"
    graph = get_default_graph(ClientMode.CLI)
    conflict = ConflictStrategy(on_conflict)

    entity_types_to_migrate = _parse_entity_types(
        entity_types, force=force, dry_run=dry_run
    )
    click.echo(
        f"This command will migrate {', '.join(t.upper() for t in entity_types_to_migrate)} "
        "and CONTAINERS."
    )

    for entity_type in entity_types_to_migrate:
        urns_to_migrate: List[str] = []
        for src_urn in graph.get_urns_by_filter(
            platform=platform,
            env=env if entity_type in ENV_ENTITY_TYPES else None,
            entity_types=[entity_type],
        ):
            response = graph.get_aspects_for_entity(
                entity_urn=src_urn,
                aspects=["dataPlatformInstance"],
                aspect_types=[DataPlatformInstanceClass],
            )
            if "dataPlatformInstance" in response:
                assert isinstance(
                    response["dataPlatformInstance"], DataPlatformInstanceClass
                )
                if response["dataPlatformInstance"].instance:
                    log.debug(f"{src_urn} already has instance, skipping")
                    continue
            urns_to_migrate.append(src_urn)

        if not urns_to_migrate:
            click.echo(f"No {entity_type} entities found without instance, skipping.")
            continue

        click.echo(f"Found {len(urns_to_migrate)} {entity_type} entities to migrate.")
        report = _migrate_entities(
            urns_to_migrate=urns_to_migrate,
            make_new_urn=make_urn_builder(entity_type, new_instance=instance),
            platform=platform,
            target_instance=instance,
            dry_run=dry_run,
            force=force,
            hard=hard,
            keep=keep,
            run_id=f"{run_id}-{entity_type}",
            graph=graph,
            on_conflict=conflict,
            skip_on_error=skip_on_error,
        )
        click.echo(f"{report}")

    _migrate_containers(
        env=env,
        platform=platform,
        target_instance=instance,
        should_migrate=lambda props: (
            (env is None or props.get("instance") == env)
            and (platform is None or props.get("platform") == platform)
        ),
        dry_run=dry_run,
        hard=hard,
        keep=keep,
        rest_emitter=graph,
    )


@migrate.command()
@click.option("--platform", type=str, required=True)
@click.option("--old-instance", type=str, required=True)
@click.option("--new-instance", type=str, required=True)
@click.option("--dry-run", "-n", type=bool, is_flag=True, default=False)
@click.option("--env", type=str, default=DEFAULT_ENV)
@click.option("-F", "--force", type=bool, is_flag=True, default=False)
@click.option(
    "--hard",
    type=bool,
    is_flag=True,
    default=False,
    help="Hard-delete previous entities instead of soft-delete.",
)
@click.option(
    "--keep",
    type=bool,
    is_flag=True,
    default=False,
    help="Do not delete previous entities.",
)
@click.option(
    "--on-conflict",
    type=click.Choice(["overwrite", "patch", "prompt"]),
    default="patch",
    help="How to handle existing target entities.",
)
@click.option(
    "--skip-on-error",
    type=bool,
    is_flag=True,
    default=False,
    help="Skip entities that cause errors instead of aborting.",
)
@click.option(
    "--entity-types",
    type=str,
    default=None,
    help=(
        "Comma-separated list of entity types to migrate. "
        f"Available: {','.join(ALL_ENTITY_TYPES)}. "
        "Default: all types."
    ),
)
@telemetry.with_telemetry()
@upgrade.check_upgrade
def instance2instance(
    platform: str,
    old_instance: str,
    new_instance: str,
    dry_run: bool,
    env: str,
    force: bool,
    hard: bool,
    keep: bool,
    on_conflict: str,
    skip_on_error: bool,
    entity_types: Optional[str],
) -> None:
    """Migrate entities from one platform instance to another."""
    conflict = ConflictStrategy(on_conflict)
    entity_types_to_migrate = _parse_entity_types(
        entity_types, force=force, dry_run=dry_run
    )
    click.echo(
        f"Starting migration: platform:{platform}, "
        f"old-instance={old_instance}, new-instance={new_instance}, "
        f"force={force}, dry-run={dry_run}, on-conflict={conflict.value}"
    )
    click.echo(
        f"This command will migrate {', '.join(t.upper() for t in entity_types_to_migrate)} "
        "and CONTAINERS."
    )
    run_id = f"migrate-i2i-{uuid.uuid4()}"
    graph = get_default_graph(ClientMode.CLI)

    for entity_type in entity_types_to_migrate:
        urns = list(
            graph.get_urns_by_filter(
                platform=platform,
                platform_instance=old_instance,
                env=env if entity_type in ENV_ENTITY_TYPES else None,
                entity_types=[entity_type],
            )
        )
        if not urns:
            click.echo(f"No {entity_type} entities found, skipping.")
            continue

        click.echo(f"Found {len(urns)} {entity_type} entities to migrate.")
        report = _migrate_entities(
            urns_to_migrate=urns,
            make_new_urn=make_urn_builder(
                entity_type,
                new_instance=new_instance,
                old_instance=old_instance,
            ),
            platform=platform,
            target_instance=new_instance,
            dry_run=dry_run,
            force=force,
            hard=hard,
            keep=keep,
            run_id=f"{run_id}-{entity_type}",
            graph=graph,
            on_conflict=conflict,
            skip_on_error=skip_on_error,
        )
        click.echo(f"{report}")

    _migrate_containers(
        env=env,
        platform=platform,
        target_instance=new_instance,
        should_migrate=lambda props: (
            props.get("platform") == platform and props.get("instance") == old_instance
        ),
        dry_run=dry_run,
        hard=hard,
        keep=keep,
        rest_emitter=graph,
    )
