"""DataHub CLI commands for migrating entities between platform instances."""

import json
import logging
import random
import uuid
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import click
import progressbar

from datahub.cli import delete_cli, migration_utils
from datahub.cli.migration_utils import ALL_ENTITY_TYPES
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
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
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
)
from datahub.migration import engine
from datahub.migration.fetch import fetch_instance_urns, fetch_platform_urns
from datahub.migration.models import (
    ConflictStrategy,
    MigrationOptions,
    MigrationPair,
    MigrationReport,
)
from datahub.migration.transform import make_urn_builder, pairs_from_transform
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade
from datahub.utilities.urns.urn import Urn, guess_entity_type

log = logging.getLogger(__name__)


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


def _run_migration(
    graph: DataHubGraph,
    pairs: List[MigrationPair],
    options: MigrationOptions,
) -> MigrationReport:
    """Drive the migration engine over a fixed list of pairs, with a progress bar.

    Adds the CLI-only error hint that the engine (which is UI-agnostic) omits.
    """
    try:
        return engine.migrate_pairs(
            graph,
            progressbar.progressbar(pairs, redirect_stdout=True),
            options,
        )
    except Exception as e:
        click.echo(
            f"\nError during migration: {e}\n"
            "Hint: use --skip-on-error to skip problematic entities "
            "and continue with the rest."
        )
        raise


def _run_entity_migration(
    graph: DataHubGraph,
    *,
    urns: List[str],
    transform: Callable[[str], str],
    platform: str,
    target_instance: str,
    options: MigrationOptions,
    force: bool,
) -> MigrationReport:
    """Confirm, then migrate a single entity type via fetch → transform → engine."""
    if not force and not options.dry_run:
        sampled_urns = random.sample(urns, k=min(10, len(urns)))
        sampled_new_urns = [transform(u) for u in sampled_urns]
        click.echo(f"Will migrate {len(urns)} urns such as {sampled_urns}")
        click.echo(f"New urns will look like {sampled_new_urns}")
        click.confirm("Ok to proceed?", abort=True)

    # A platform instance cannot be recovered from the target URN, so the target
    # instance aspect is built here from the known platform/instance and attached
    # to every pair.
    instance_aspect = DataPlatformInstanceClass(
        platform=make_data_platform_urn(platform),
        instance=make_dataplatform_instance_urn(platform, target_instance),
    )
    pairs = list(
        pairs_from_transform(urns, transform, data_platform_instance=instance_aspect)
    )
    return _run_migration(graph, pairs, options)


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
            aspect_names=migration_utils.get_migratable_aspect_names("container"),
            dst_urn=dst_urn,
            run_id=run_id,
        ):
            migration_report.on_entity_create(mcp.entityUrn, mcp.aspectName)  # type: ignore
            assert mcp.aspect
            # The key aspect (containerKey) is not cloned — GMS derives it from
            # the new URN's guid. We only need to reseat customProperties on the
            # cloned containerProperties.
            if mcp.aspectName == "containerProperties":
                assert isinstance(mcp.aspect, ContainerPropertiesClass)
                mcp.aspect.customProperties = newKey.model_dump(
                    by_alias=True, exclude_none=True
                )
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
    rewrite_urn = migration_utils.make_self_urn_rewriter(src_urn, dst_urn)
    seen_targets = set()
    for relationship in migration_utils.get_incoming_relationships(urn=src_urn):
        target_urn: str = relationship.urn
        if target_urn in container_id_map:
            target_urn = container_id_map[target_urn]
        if target_urn in seen_targets:
            continue
        seen_targets.add(target_urn)
        for mcp in migration_utils.rewrite_incoming_references(
            client, target_urn, rewrite_urn
        ):
            if not dry_run:
                rest_emitter.emit_mcp(mcp)
            migration_report.on_entity_affected(mcp.entityUrn, mcp.aspectName)  # type: ignore


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
    type=click.Choice(["overwrite", "patch", "prompt", "preserve"]),
    default="overwrite",
    help=(
        "How to handle existing target entities: overwrite (replace target "
        "values), patch (only add new data), prompt (ask per conflict), or "
        "preserve (leave the existing target untouched, but still repoint "
        "references to it and delete the source)."
    ),
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
    """Migrate entities from one dataplatform to a dataplatform instance.

    Migrates every entity of the selected types (datasets, charts, dashboards,
    dataflows, datajobs, and containers) together with ALL of their aspects —
    the aspect list is sourced from the entity registry, so user-authored
    metadata (ownership, tags, terms, documentation, structured properties, ...)
    is carried over, not just a fixed subset. Timeseries aspects (usage, profiles)
    and system-managed aspects (browse paths, incidents summary) are not migrated.
    """
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
        urns_to_migrate = list(
            fetch_platform_urns(
                graph, platform=platform, env=env, entity_type=entity_type
            )
        )
        if not urns_to_migrate:
            click.echo(f"No {entity_type} entities found without instance, skipping.")
            continue

        click.echo(f"Found {len(urns_to_migrate)} {entity_type} entities to migrate.")
        options = MigrationOptions(
            run_id=f"{run_id}-{entity_type}",
            dry_run=dry_run,
            hard=hard,
            keep=keep,
            on_conflict=conflict,
            skip_on_error=skip_on_error,
        )
        report = _run_entity_migration(
            graph,
            urns=urns_to_migrate,
            transform=make_urn_builder(entity_type, new_instance=instance),
            platform=platform,
            target_instance=instance,
            options=options,
            force=force,
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
    type=click.Choice(["overwrite", "patch", "prompt", "preserve"]),
    default="patch",
    help=(
        "How to handle existing target entities: overwrite (replace target "
        "values), patch (only add new data), prompt (ask per conflict), or "
        "preserve (leave the existing target untouched, but still repoint "
        "references to it and delete the source)."
    ),
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
    """Migrate entities from one platform instance to another.

    Migrates every entity of the selected types (datasets, charts, dashboards,
    dataflows, datajobs, and containers) together with ALL of their aspects —
    the aspect list is sourced from the entity registry, so user-authored
    metadata (ownership, tags, terms, documentation, structured properties, ...)
    is carried over, not just a fixed subset. Timeseries aspects (usage, profiles)
    and system-managed aspects (browse paths, incidents summary) are not migrated.
    """
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
            fetch_instance_urns(
                graph,
                platform=platform,
                old_instance=old_instance,
                env=env,
                entity_type=entity_type,
            )
        )
        if not urns:
            click.echo(f"No {entity_type} entities found, skipping.")
            continue

        click.echo(f"Found {len(urns)} {entity_type} entities to migrate.")
        options = MigrationOptions(
            run_id=f"{run_id}-{entity_type}",
            dry_run=dry_run,
            hard=hard,
            keep=keep,
            on_conflict=conflict,
            skip_on_error=skip_on_error,
        )
        report = _run_entity_migration(
            graph,
            urns=urns,
            transform=make_urn_builder(
                entity_type,
                new_instance=new_instance,
                old_instance=old_instance,
            ),
            platform=platform,
            target_instance=new_instance,
            options=options,
            force=force,
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


def _validate_mapping_pair(source: str, target: str) -> None:
    """Validate a single source → target URN pair from a mapping file."""
    for label, urn in (("source", source), ("target", target)):
        if not isinstance(urn, str) or not urn.startswith("urn:li:"):
            raise click.BadParameter(
                f"Invalid {label} URN: {urn!r}", param_hint="--mapping-file"
            )
    if source == target:
        raise click.BadParameter(
            f"source and target must differ (got identical URN {source}); an "
            f"identity mapping would delete the source.",
            param_hint="--mapping-file",
        )
    src_type = guess_entity_type(source)
    tgt_type = guess_entity_type(target)
    if src_type != tgt_type:
        raise click.BadParameter(
            f"source and target must be the same entity type: "
            f"{source} ({src_type}) != {target} ({tgt_type})",
            param_hint="--mapping-file",
        )


def _load_mapping_pairs(mapping_file: str) -> List[MigrationPair]:
    """Parse a JSON mapping file into validated :class:`MigrationPair` objects.

    Accepts either a list of ``{"source": ..., "target": ...}`` objects or a flat
    ``{source: target}`` object.
    """
    with open(mapping_file) as f:
        data = json.load(f)

    raw_pairs: List[Tuple[str, str]] = []
    if isinstance(data, dict):
        raw_pairs = list(data.items())
    elif isinstance(data, list):
        for i, item in enumerate(data):
            if (
                not isinstance(item, dict)
                or "source" not in item
                or "target" not in item
            ):
                raise click.BadParameter(
                    f"Entry {i} must be an object with 'source' and 'target' keys.",
                    param_hint="--mapping-file",
                )
            raw_pairs.append((item["source"], item["target"]))
    else:
        raise click.BadParameter(
            "Mapping file must be a JSON object or a list of {source, target} objects.",
            param_hint="--mapping-file",
        )

    if not raw_pairs:
        raise click.BadParameter("Mapping file is empty.", param_hint="--mapping-file")

    pairs: List[MigrationPair] = []
    for source, target in raw_pairs:
        _validate_mapping_pair(source, target)
        # No dataPlatformInstance is stamped: a platform instance cannot be
        # recovered from the target URN, and the caller controls the exact target,
        # so we do not guess one. Use instance2instance for instance-aware moves.
        pairs.append(MigrationPair(source_urn=source, target_urn=target))
    return pairs


@migrate.command(name="urns-mapping")
@click.option(
    "--mapping-file",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
    help=(
        "Path to a JSON file of source → target URN pairs. Either a list of "
        '{"source": "urn:...", "target": "urn:..."} objects or a flat '
        '{"urn:src": "urn:tgt"} object.'
    ),
)
@click.option("--dry-run", "-n", type=bool, is_flag=True, default=False)
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
    type=click.Choice(["overwrite", "patch", "prompt", "preserve"]),
    default="overwrite",
    help=(
        "How to handle existing target entities: overwrite (replace target "
        "values), patch (only add new data), prompt (ask per conflict), or "
        "preserve (leave the existing target untouched, but still repoint "
        "references to it and delete the source)."
    ),
)
@click.option(
    "--skip-on-error",
    type=bool,
    is_flag=True,
    default=False,
    help="Skip entities that cause errors instead of aborting.",
)
@telemetry.with_telemetry()
@upgrade.check_upgrade
def urns_mapping(
    mapping_file: str,
    dry_run: bool,
    force: bool,
    hard: bool,
    keep: bool,
    on_conflict: str,
    skip_on_error: bool,
) -> None:
    """Migrate entities using an explicit source → target URN mapping.

    Migrates each source entity (with ALL of its registry-defined aspects) to the
    target URN you specify, repointing references and deleting the source — the
    same engine that backs the instance-migration commands, but with the target
    URNs supplied directly instead of derived from a strategy.

    Source and target of each pair must be the same entity type. Unlike the
    instance-migration commands this does not stamp a dataPlatformInstance and does
    not migrate containers or enforce dataFlow/dataJob parent consistency — the
    caller owns the exact mapping.
    """
    pairs = _load_mapping_pairs(mapping_file)
    conflict = ConflictStrategy(on_conflict)
    graph = get_default_graph(ClientMode.CLI)
    click.echo(
        f"Starting URN-mapping migration of {len(pairs)} pair(s): "
        f"force={force}, dry-run={dry_run}, on-conflict={conflict.value}"
    )

    if not force and not dry_run:
        sample = [(p.source_urn, p.target_urn) for p in pairs[:10]]
        click.echo(f"Will migrate {len(pairs)} urns such as {sample}")
        click.confirm("Ok to proceed?", abort=True)

    options = MigrationOptions(
        run_id=f"migrate-urns-{uuid.uuid4()}",
        dry_run=dry_run,
        hard=hard,
        keep=keep,
        on_conflict=conflict,
        skip_on_error=skip_on_error,
    )
    report = _run_migration(graph, pairs, options)
    click.echo(f"{report}")
