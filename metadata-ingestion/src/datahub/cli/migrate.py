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
from datahub.cli.snowflake_semantic_view_migration import (
    SEMANTIC_VIEW_SUBTYPE,
    MigrationDirection,
    discover_semantic_model_urns,
    discover_semantic_view_dataset_urns,
    filter_by_semantic_view_subtype,
    run_migration,
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
    The progress bar advances after each pair is fully processed (cloned,
    repointed, deleted), so it reflects real work rather than just iteration.
    """
    bar = progressbar.ProgressBar(max_value=len(pairs), redirect_stdout=True)
    bar.start()
    done = [0]

    def _advance(_pair: MigrationPair) -> None:
        done[0] += 1
        bar.update(done[0])

    try:
        return engine.migrate_pairs(graph, pairs, options, on_pair_done=_advance)
    except Exception as e:
        click.echo(
            f"\nError during migration: {e}\n"
            "Hint: use --skip-on-error to skip problematic entities "
            "and continue with the rest."
        )
        raise
    finally:
        bar.finish()


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
    report_file: Optional[str] = None,
) -> None:
    """Migrate containers matching a filter to a new platform instance."""
    run_id: str = f"container-migrate-{uuid.uuid4()}"
    migration_report = MigrationReport(run_id, dry_run, keep)
    if report_file:
        migration_report.open_report_file(report_file)

    container_id_map: Dict[str, str] = {}
    containers = _get_containers_for_migration(env)
    skipped_count = 0
    try:
        for container in progressbar.progressbar(containers, redirect_stdout=True):
            aspects = container.get("aspects") or {}
            container_props = (aspects.get("containerProperties") or {}).get("value")
            if not container_props:
                log.debug(
                    f"{container['urn']} missing containerProperties aspect, skipping"
                )
                skipped_count += 1
                continue
            customProperties = container_props.get("customProperties") or {}
            if not should_migrate(customProperties):
                log.debug(
                    f"{container['urn']} does not match filter criteria, skipping.. "
                    f"{customProperties}"
                )
                skipped_count += 1
                continue

            type_names = ((aspects.get("subTypes") or {}).get("value") or {}).get(
                "typeNames"
            ) or []
            if not type_names:
                # Showcase / partial containers can lack subTypes; cannot choose a key type.
                log.warning(
                    f"{container['urn']} missing subTypes aspect, skipping container migration"
                )
                skipped_count += 1
                continue
            subType = type_names[0]

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
                log.warning(
                    f"Unable to map {customProperties} to key due to exception {e}"
                )
                continue

            newKey.instance = target_instance

            src_urn = container["urn"]
            dst_urn = f"urn:li:container:{newKey.guid()}"
            container_id_map[src_urn] = dst_urn
            migration_report.set_current_pair(src_urn, dst_urn)

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

            # dataPlatformInstance is excluded from the clone (it is instance-bound
            # and would otherwise carry the *old* instance) and GMS does not derive
            # it, so re-emit a fresh one for the new instance — mirroring the entity
            # engine's _emit_target_instance. Without it the migrated container has
            # no instance aspect and drops out of instance-scoped search and filters.
            if not dry_run:
                rest_emitter.emit_mcp(
                    MetadataChangeProposalWrapper(
                        entityUrn=dst_urn,
                        aspect=DataPlatformInstanceClass(
                            platform=make_data_platform_urn(platform),
                            instance=make_dataplatform_instance_urn(
                                platform, target_instance
                            ),
                        ),
                    )
                )
            migration_report.on_entity_create(dst_urn, "dataPlatformInstance")

            _process_container_relationships(
                container_id_map,
                dry_run,
                src_urn,
                dst_urn,
                migration_report,
                rest_emitter,
            )

            if not dry_run and not keep:
                log.info(f"will {'hard' if hard else 'soft'} delete {src_urn}")
                delete_cli._delete_one_urn(
                    rest_emitter, src_urn, soft=not hard, run_id=run_id
                )
            migration_report.on_entity_migrated(src_urn, "COMPLETED")  # type: ignore
    finally:
        migration_report.close_report_file()
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
@click.option(
    "--checkpoint-file",
    type=click.Path(dir_okay=False),
    default=None,
    help=(
        "Resumable migrations: already-migrated source URNs are read from "
        "this file and skipped; newly migrated URNs are appended on success. "
        "Created automatically on first write."
    ),
)
@click.option(
    "--migration-report",
    type=click.Path(dir_okay=False),
    default=None,
    help=(
        "Write per-entity migration activity to a file (TSV). "
        "Columns: action, source_urn, target_urn, aspect. "
        "For 'affected' lines: action, referrer_urn, target_urn, aspect."
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
    checkpoint_file: Optional[str],
    migration_report: Optional[str],
) -> None:
    """Migrate entities from one dataplatform to a dataplatform instance.

    Migrates every entity of the selected types (datasets, charts, dashboards,
    dataflows, datajobs, and containers) together with ALL of their aspects —
    the aspect list is sourced from the entity registry, so user-authored
    metadata (ownership, tags, terms, documentation, structured properties, ...)
    is carried over, not just a fixed subset. Timeseries aspects (usage, profiles)
    and system-managed aspects (browse paths, incidents summary) are not migrated.
    schemaField entities are not migrated; column-level metadata on the dataset's
    editableSchemaMetadata aspect is carried over.

    See ``datahub migrate urns-mapping --help`` for details on --on-conflict
    and nested-URN limitations (dataFlow/dataJob, schemaField).
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
            checkpoint_file=checkpoint_file,
            report_file=migration_report,
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
        report_file=migration_report,
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
@click.option(
    "--checkpoint-file",
    type=click.Path(dir_okay=False),
    default=None,
    help=(
        "Resumable migrations: already-migrated source URNs are read from "
        "this file and skipped; newly migrated URNs are appended on success. "
        "Created automatically on first write."
    ),
)
@click.option(
    "--migration-report",
    type=click.Path(dir_okay=False),
    default=None,
    help=(
        "Write per-entity migration activity to a file (TSV). "
        "Columns: action, source_urn, target_urn, aspect. "
        "For 'affected' lines: action, referrer_urn, target_urn, aspect."
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
    checkpoint_file: Optional[str],
    migration_report: Optional[str],
) -> None:
    """Migrate entities from one platform instance to another.

    Migrates every entity of the selected types (datasets, charts, dashboards,
    dataflows, datajobs, and containers) together with ALL of their aspects —
    the aspect list is sourced from the entity registry, so user-authored
    metadata (ownership, tags, terms, documentation, structured properties, ...)
    is carried over, not just a fixed subset. Timeseries aspects (usage, profiles)
    and system-managed aspects (browse paths, incidents summary) are not migrated.
    schemaField entities are not migrated; column-level metadata on the dataset's
    editableSchemaMetadata aspect is carried over.

    See ``datahub migrate urns-mapping --help`` for details on --on-conflict
    and nested-URN limitations (dataFlow/dataJob, schemaField).
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
            checkpoint_file=checkpoint_file,
            report_file=migration_report,
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
        report_file=migration_report,
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


def _extract_pair(item: Dict[str, str], label: str) -> Tuple[str, str]:
    """Extract (source, target) from a dict, accepting both key variants."""
    source = item.get("source") or item.get("source_urn")
    target = item.get("target") or item.get("target_urn")
    if not source or not target:
        raise click.BadParameter(
            f"{label} must have 'source'/'source_urn' and 'target'/'target_urn' keys.",
            param_hint="--mapping-file",
        )
    return (source, target)


def _is_source_target_object(data: Dict[str, str]) -> bool:
    """Check if a dict uses source/target or source_urn/target_urn keys."""
    return bool(
        ("source" in data or "source_urn" in data)
        and ("target" in data or "target_urn" in data)
    )


def _load_mapping_pairs(mapping_file: str) -> List[MigrationPair]:
    """Parse a JSON or JSONL mapping file into validated :class:`MigrationPair` objects.

    Supported formats:

    1. **JSONL** — one ``{"source": ..., "target": ...}`` per line.
    2. **JSON array** — ``[{"source": ..., "target": ...}, ...]``.
    3. **Single JSON object** — ``{"source": ..., "target": ...}``.
    4. **Flat JSON object** — ``{"<source_urn>": "<target_urn>", ...}``.

    Keys ``source_urn`` / ``target_urn`` are accepted as aliases for
    ``source`` / ``target``.
    """
    raw_pairs: List[Tuple[str, str]] = []

    with open(mapping_file) as f:
        content = f.read().strip()

    # Try parsing as a single JSON document first.
    lines = content.splitlines()
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        # Not valid as a single JSON document — try JSONL.
        if len(lines) < 2:
            raise
        data = None

    if data is None:
        # JSONL: parse each non-empty line independently.
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as e:
                raise click.BadParameter(
                    f"Line {i + 1} is not valid JSON: {e}",
                    param_hint="--mapping-file",
                ) from e
            if not isinstance(item, dict):
                raise click.BadParameter(
                    f"Line {i + 1} must be a JSON object with 'source' and 'target' keys.",
                    param_hint="--mapping-file",
                )
            raw_pairs.append(_extract_pair(item, f"Line {i + 1}"))
    elif isinstance(data, dict):
        if _is_source_target_object(data):
            raw_pairs = [_extract_pair(data, "Root object")]
        else:
            raw_pairs = list(data.items())
    elif isinstance(data, list):
        for i, item in enumerate(data):
            if not isinstance(item, dict):
                raise click.BadParameter(
                    f"Entry {i} must be an object with 'source' and 'target' keys.",
                    param_hint="--mapping-file",
                )
            raw_pairs.append(_extract_pair(item, f"Entry {i}"))
    else:
        raise click.BadParameter(
            "Mapping file must be a JSON object, array, or JSONL.",
            param_hint="--mapping-file",
        )

    if not raw_pairs:
        raise click.BadParameter("Mapping file is empty.", param_hint="--mapping-file")

    # Each URN must appear at most once on each side. A repeated source would
    # migrate (and, without --keep, delete) the same entity more than once — the
    # second pass reads an already-deleted source. A repeated target would collapse
    # several sources into one entity, dropping all but one source's metadata (and,
    # under preserve, deleting those sources anyway). Both are user errors.
    for label, urns in (
        ("source", [source for source, _ in raw_pairs]),
        ("target", [target for _, target in raw_pairs]),
    ):
        duplicates = sorted({u for u in urns if urns.count(u) > 1})
        if duplicates:
            raise click.BadParameter(
                f"Duplicate {label} URN(s) in mapping: {duplicates}. Each {label} "
                f"may appear at most once.",
                param_hint="--mapping-file",
            )

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
@click.option(
    "--checkpoint-file",
    type=click.Path(dir_okay=False),
    default=None,
    help=(
        "Resumable migrations: already-migrated source URNs are read from "
        "this file and skipped; newly migrated URNs are appended on success. "
        "Created automatically on first write."
    ),
)
@click.option(
    "--migration-report",
    type=click.Path(dir_okay=False),
    default=None,
    help=(
        "Write per-entity migration activity to a file (TSV). "
        "Columns: action, source_urn, target_urn, aspect. "
        "For 'affected' lines: action, referrer_urn, target_urn, aspect."
    ),
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
    checkpoint_file: Optional[str],
    migration_report: Optional[str],
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

    \b
    Conflict resolution (--on-conflict)
    ------------------------------------
    When the target entity already exists, --on-conflict controls how each
    aspect is merged. The strategy is applied per aspect and varies by
    aspect category:

    \b
      Always merged (additive):   ownership, tags, glossary terms, lineage.
                                  Source items are unioned into the target
                                  regardless of --on-conflict.
      Strategy-dependent (scalar): schema, editable schema, view properties,
                                  and any other non-additive registry aspect.
                                  "overwrite" replaces the target value;
                                  "patch" keeps the target value when present.
      Mixed:                      datasetProperties, editableDatasetProperties.
                                  customProperties keys are always merged;
                                  description follows the chosen strategy.
      Always overwritten:         containerProperties.
      Never overwritten:          status (the target's soft-delete state is
                                  authoritative), container (the target's
                                  parent container is preserved; for p2i/i2i
                                  the container migration repoints it).
      Preserve:                   skips all merge work — the target is left
                                  untouched, but incoming references are still
                                  repointed and the source is deleted.

    \b
    Examples
    --------
    Conservative (safest): keep source entities and preserve existing targets:

      datahub migrate urns-mapping --mapping-file map.json --keep --on-conflict preserve --migration-report report.tsv

    This clones aspects to targets that don't exist yet, leaves existing targets
    untouched, repoints incoming references, and does NOT delete the sources.
    Useful for a dry-run-like trial where you can inspect results before
    re-running without --keep.

    \b
    Limitations — nested URNs
    -------------------------
    Some entity URNs embed another entity's URN (e.g. a dataJob URN embeds
    its parent dataFlow URN). The migration engine does NOT automatically
    derive child URN mappings from parent URN mappings:

    \b
      dataFlow / dataJob:  When migrating a dataFlow, you must also provide
                           mappings for its child dataJob URNs. Otherwise the
                           dataJob key aspects may become inconsistent.
      schemaField:         Column-level metadata stored on the dataset's
                           editableSchemaMetadata aspect IS migrated. However,
                           schemaField entities (which carry their own tags,
                           terms, and documentation) are NOT migrated by any
                           migration command. These are typically re-created
                           by the next ingestion run.
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
        checkpoint_file=checkpoint_file,
        report_file=migration_report,
    )
    report = _run_migration(graph, pairs, options)
    click.echo(f"{report}")


def _read_urns_from_file(path: str) -> List[str]:
    with open(path) as f:
        stripped = (line.strip() for line in f)
        return [line for line in stripped if line and not line.startswith("#")]


# NOTE: this command deliberately does NOT use the shared migration engine
# (fetch → transform → migrate) that backs dataplatform2instance / instance2instance /
# urns-mapping. Those perform a URN *move*: clone every registry aspect to the new
# URN, repoint references, and delete the source. This command is a governance
# *copy* with different semantics — it copies only a subset of aspects (ownership,
# domains, tags, terms, docs, ...), fans column tags/terms out onto metric /
# schemaField URNs, writes to a destination that may not exist yet, and never
# deletes the source or rewrites lineage. Forcing it onto the generic engine would
# change its behavior, so the specialized copy logic lives in
# snowflake_semantic_view_migration.run_migration instead.
@migrate.command(name="snowflake-semantic-views")
@click.option(
    "--direction",
    type=click.Choice([d.value for d in MigrationDirection]),
    required=True,
    help="dataset-to-sm migrates legacy 'Semantic View' datasets to semanticModel "
    "entities (flag OFF->ON). sm-to-dataset migrates semanticModel entities back "
    "to legacy datasets (flag ON->OFF).",
)
@click.option(
    "--env",
    type=str,
    default=DEFAULT_ENV,
    help="Env for the dataset side of the mapping. Required to reconstruct dataset "
    "urns for sm-to-dataset; also used to filter discovery for dataset-to-sm. "
    "Note: semanticModel URNs omit env, so PROD and DEV views with the same "
    "db.schema.view map to one semanticModel and would overwrite each other's "
    "governance — migrate one env at a time.",
)
@click.option(
    "--platform-instance",
    type=str,
    default=None,
    help="Platform instance used by the Snowflake ingestion recipe, if any. Must "
    "match the recipe exactly or urn mapping will be wrong. Also filters discovery.",
)
@click.option(
    "--convert-urns-to-lowercase/--no-convert-urns-to-lowercase",
    default=True,
    help="Must match the Snowflake recipe's convert_urns_to_lowercase setting "
    "(connector default: true).",
)
@click.option(
    "--urn",
    "urns",
    type=str,
    multiple=True,
    help="Explicit source urn(s) to migrate. Can be passed multiple times. If "
    "neither --urn nor --urn-file is given, entities are discovered via search.",
)
@click.option(
    "--urn-file",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Path to a file with one source urn per line. Blank lines and '#' "
    "comment lines are ignored.",
)
@click.option(
    "--include-soft-deleted/--exclude-soft-deleted",
    default=False,
    help="Include soft-deleted entities when discovering sources (default: false). "
    "After flag flip + stateful ingest, sources are often soft-deleted — pass "
    "--include-soft-deleted deliberately so stale unrelated deletes are not "
    "migrated by accident. Soft-deleted sources are marked in the report.",
)
@click.option("--dry-run", "-n", type=bool, is_flag=True, default=False)
@click.option(
    "-F",
    "--force",
    type=bool,
    is_flag=True,
    default=False,
    help="Skip the 'Semantic View' subtype check (dataset-to-sm) and skip the "
    "confirmation prompt.",
)
@click.option(
    "--report-inbound-refs",
    type=bool,
    is_flag=True,
    default=False,
    help="List DownstreamOf/Consumes/etc relationships pointing at the source urn "
    "that this command does not repoint.",
)
@telemetry.with_telemetry()
@upgrade.check_upgrade
def snowflake_semantic_views(
    direction: str,
    env: str,
    platform_instance: Optional[str],
    convert_urns_to_lowercase: bool,
    urns: Tuple[str, ...],
    urn_file: Optional[str],
    include_soft_deleted: bool,
    dry_run: bool,
    force: bool,
    report_inbound_refs: bool,
) -> None:
    """Copy governance between legacy Snowflake "Semantic View" datasets and
    semanticModel/metric entities.

    Source must exist; destination URNs may not (aspects are written so a later
    Snowflake ingest with emit_semantic_model_entities can fill structural
    aspects). Typical order: migrate governance, then ingest.

    Copies entity-level ownership, domains, tags, glossary terms, institutional
    memory, structured properties, documentation, deprecation, and applications.
    Also fans out column tags/terms (from editableSchemaMetadata and non-synthetic
    schemaMetadata tags) onto metric / schemaField URNs. Does NOT touch lineage,
    policies, data products, or soft/hard-delete.

    Re-running is safe: destination entity aspects are overwritten (last-write-wins);
    editableSchemaMetadata field entries are merged (descriptions preserved; tags
    unioned by URN).
    """
    migration_direction = MigrationDirection(direction)
    graph = get_default_graph(ClientMode.CLI)

    urns_to_process: List[str] = list(urns)
    if urn_file:
        urns_to_process.extend(_read_urns_from_file(urn_file))
    # Preserve order while dropping duplicates from --urn / --urn-file.
    urns_to_process = list(dict.fromkeys(urns_to_process))
    used_discovery = not bool(urns_to_process)

    subtype_skipped: List[str] = []
    if urns_to_process:
        if migration_direction == MigrationDirection.DATASET_TO_SM:
            urns_to_process, subtype_skipped = filter_by_semantic_view_subtype(
                graph, urns_to_process, force
            )
    else:
        if migration_direction == MigrationDirection.DATASET_TO_SM:
            urns_to_process = discover_semantic_view_dataset_urns(
                graph,
                env=env,
                platform_instance=platform_instance,
                include_soft_deleted=include_soft_deleted,
            )
        else:
            urns_to_process = discover_semantic_model_urns(
                graph,
                platform_instance=platform_instance,
                include_soft_deleted=include_soft_deleted,
            )

    if not urns_to_process:
        if subtype_skipped:
            click.echo(
                f"No entities found to migrate: all {len(subtype_skipped)} provided "
                f"urn(s) lack the '{SEMANTIC_VIEW_SUBTYPE}' subtype. Pass --force to "
                "bypass this check."
            )
            return
        if used_discovery and not include_soft_deleted:
            if migration_direction == MigrationDirection.DATASET_TO_SM:
                soft_only = discover_semantic_view_dataset_urns(
                    graph,
                    env=env,
                    platform_instance=platform_instance,
                    only_soft_deleted=True,
                )
            else:
                soft_only = discover_semantic_model_urns(
                    graph,
                    platform_instance=platform_instance,
                    only_soft_deleted=True,
                )
            if soft_only:
                click.echo(
                    f"No live entities found to migrate, but found "
                    f"{len(soft_only)} soft-deleted "
                    f"{'Semantic View dataset' if migration_direction == MigrationDirection.DATASET_TO_SM else 'semanticModel'}"
                    f"{'s' if len(soft_only) != 1 else ''}. "
                    "Did ingest already run? Re-run with --include-soft-deleted "
                    "after reviewing that set."
                )
                return
        click.echo("No entities found to migrate.")
        return

    click.echo(
        f"Found {len(urns_to_process)} entities to migrate ({direction})."
        + (
            f" Skipped {len(subtype_skipped)} without '{SEMANTIC_VIEW_SUBTYPE}' subtype."
            if subtype_skipped
            else ""
        )
    )
    if not force and not dry_run:
        sample = urns_to_process[:5]
        click.echo(f"Will migrate urns such as: {sample}")
        click.confirm("Ok to proceed?", abort=True)

    report = run_migration(
        graph=graph,
        direction=migration_direction,
        urns=urns_to_process,
        platform_instance=platform_instance,
        convert_urns_to_lowercase=convert_urns_to_lowercase,
        env=env,
        dry_run=dry_run,
        report_inbound_refs=report_inbound_refs,
        subtype_skipped=subtype_skipped,
    )
    click.echo(f"{report}")
