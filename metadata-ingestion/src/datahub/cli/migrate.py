import logging
import random
import uuid
from typing import Any, Dict, List, Tuple, Union

import click
import progressbar
from avrogen.dict_wrapper import DictWrapper

from datahub.cli import cli_utils, delete_cli, migration_utils
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    dataset_urn_to_key,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    BigQueryDatasetKey,
    DatabaseKey,
    ProjectIdKey,
    SchemaKey,
)
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ContainerKeyClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    SystemMetadataClass,
)
from datahub.telemetry import telemetry

log = logging.getLogger(__name__)


class MigrationReport:
    def __init__(self, run_id: str, dry_run: bool, keep: bool) -> None:
        self.run_id = run_id
        self.dry_run = dry_run
        self.keep = keep
        self.num_events = 0
        self.entities_migrated: Dict[Tuple[str, str], int] = {}
        self.entities_created: Dict[Tuple[str, str], int] = {}
        self.entities_affected: Dict[Tuple[str, str], int] = {}

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
            self.entities_affected[(urn, aspect)] = (
                self.entities_affected[(urn, aspect)] + 1
            )

    def _get_prefix(self) -> str:
        return "[Dry Run] " if self.dry_run else ""

    def __repr__(self) -> str:
        repr = f"{self._get_prefix()}Migration Report:\n--------------\n"
        repr += f"{self._get_prefix()}Migration Run Id: {self.run_id}\n"
        repr += f"{self._get_prefix()}Num entities created = {len(set([x[0] for x in self.entities_created.keys()]))}\n"
        repr += f"{self._get_prefix()}Num entities affected = {len(set([x[0] for x in self.entities_affected.keys()]))}\n"
        repr += f"{self._get_prefix()}Num entities {'kept' if self.keep else 'migrated'} = {len(set([x[0] for x in self.entities_migrated.keys()]))}\n"
        repr += f"{self._get_prefix()}Details:\n"
        repr += f"{self._get_prefix()}New Entities Created: {set([x[0] for x in self.entities_created.keys()]) or 'None'}\n"
        repr += f"{self._get_prefix()}External Entities Affected: {set([x[0] for x in self.entities_affected.keys()]) or 'None'}\n"
        repr += f"{self._get_prefix()}Old Entities {'Kept' if self.keep else 'Migrated'} = {set([x[0] for x in self.entities_migrated.keys()]) or 'None'}\n"
        return repr


@click.group()
@telemetry.with_telemetry()
def migrate() -> None:
    """Helper commands for migrating metadata within DataHub."""
    pass


def _get_type_from_urn(urn: str) -> str:
    return urn.split(":")[2]


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
    help="When enabled will hard-delete the previous entities that are being migrated. When not set, will perform a soft-delete.",
)
@click.option(
    "--keep",
    type=bool,
    is_flag=True,
    default=False,
    help="When enabled, will not delete (hard/soft) the previous entities.",
)
@telemetry.with_telemetry()
def dataplatform2instance(
    instance: str,
    platform: str,
    dry_run: bool,
    env: str,
    force: bool,
    hard: bool,
    keep: bool,
) -> None:
    """Migrate entities from one dataplatform to a dataplatform instance."""
    dataplatform2instance_func(instance, platform, dry_run, env, force, hard, keep)


def dataplatform2instance_func(
    instance: str,
    platform: str,
    dry_run: bool,
    env: str,
    force: bool,
    hard: bool,
    keep: bool,
) -> None:
    click.echo(
        f"Starting migration: platform:{platform}, instance={instance}, force={force}, dry-run={dry_run}"
    )
    run_id: str = f"migrate-{uuid.uuid4()}"
    migration_report = MigrationReport(run_id, dry_run, keep)
    system_metadata = SystemMetadataClass(runId=run_id)

    if not dry_run:
        rest_emitter = DatahubRestEmitter(
            gms_server=cli_utils.get_session_and_host()[1]
        )

    urns_to_migrate = []

    # we first calculate all the urns we will be migrating
    for src_entity_urn in cli_utils.get_urns_by_filter(platform=platform, env=env):
        key = dataset_urn_to_key(src_entity_urn)
        assert key
        # Does this urn already have a platform instance associated with it?
        response = cli_utils.get_aspects_for_entity(
            entity_urn=src_entity_urn, aspects=["dataPlatformInstance"], typed=True
        )
        if "dataPlatformInstance" in response:
            assert isinstance(
                response["dataPlatformInstance"], DataPlatformInstanceClass
            )
            data_platform_instance: DataPlatformInstanceClass = response[
                "dataPlatformInstance"
            ]
            if data_platform_instance.instance:
                log.debug("This is already an instance-specific urn, will skip")
                continue
            else:
                log.debug(
                    f"{src_entity_urn} is not an instance specific urn. {response}"
                )
                urns_to_migrate.append(src_entity_urn)

    if not force and not dry_run:
        # get a confirmation from the operator before proceeding if this is not a dry run
        sampled_urns_to_migrate = random.choices(
            urns_to_migrate, k=min(10, len(urns_to_migrate))
        )
        sampled_new_urns: List[str] = [
            make_dataset_urn_with_platform_instance(
                platform=key.platform,
                name=key.name,
                platform_instance=instance,
                env=str(key.origin),
            )
            for key in [dataset_urn_to_key(x) for x in sampled_urns_to_migrate]
            if key
        ]
        click.echo(
            f"Will migrate {len(urns_to_migrate)} urns such as {random.choices(urns_to_migrate, k=min(10, len(urns_to_migrate)))}"
        )
        click.echo(f"New urns will look like {sampled_new_urns}")
        click.confirm("Ok to proceed?", abort=True)

    for src_entity_urn in progressbar.progressbar(
        urns_to_migrate, redirect_stdout=True
    ):
        key = dataset_urn_to_key(src_entity_urn)
        assert key
        new_urn = make_dataset_urn_with_platform_instance(
            platform=key.platform,
            name=key.name,
            platform_instance=instance,
            env=str(key.origin),
        )
        log.debug(f"Will migrate {src_entity_urn} to {new_urn}")
        relationships = migration_utils.get_incoming_relationships(src_entity_urn)

        for mcp in migration_utils.clone_aspect(
            src_entity_urn,
            aspect_names=migration_utils.all_aspects,
            dst_urn=new_urn,
            dry_run=dry_run,
            run_id=run_id,
        ):
            if not dry_run:
                rest_emitter.emit_mcp(mcp)
            migration_report.on_entity_create(mcp.entityUrn, mcp.aspectName)  # type: ignore

        if not dry_run:
            rest_emitter.emit_mcp(
                MetadataChangeProposalWrapper(
                    entityUrn=new_urn,
                    aspect=DataPlatformInstanceClass(
                        platform=make_data_platform_urn(platform),
                        instance=make_dataplatform_instance_urn(platform, instance),
                    ),
                    systemMetadata=system_metadata,
                )
            )
        migration_report.on_entity_create(new_urn, "dataPlatformInstance")

        for relationship in relationships:
            target_urn = relationship["entity"]
            entity_type = _get_type_from_urn(target_urn)
            relationshipType = relationship["type"]
            aspect_name = migration_utils.get_aspect_name_from_relationship(
                relationshipType, entity_type
            )
            aspect_map = cli_utils.get_aspects_for_entity(
                target_urn, aspects=[aspect_name], typed=True
            )
            if aspect_name in aspect_map:
                aspect = aspect_map[aspect_name]
                assert isinstance(aspect, DictWrapper)
                aspect = migration_utils.modify_urn_list_for_aspect(
                    aspect_name, aspect, relationshipType, src_entity_urn, new_urn
                )
                # use mcpw
                mcp = MetadataChangeProposalWrapper(
                    entityUrn=target_urn,
                    aspect=aspect,
                )
                if not dry_run:
                    rest_emitter.emit_mcp(mcp)
                migration_report.on_entity_affected(mcp.entityUrn, mcp.aspectName)  # type: ignore
            else:
                log.debug(f"Didn't find aspect {aspect_name} for urn {target_urn}")

        if not dry_run and not keep:
            log.info(f"will {'hard' if hard else 'soft'} delete {src_entity_urn}")
            delete_cli._delete_one_urn(src_entity_urn, soft=not hard, run_id=run_id)
        migration_report.on_entity_migrated(src_entity_urn, "status")  # type: ignore

    click.echo(f"{migration_report}")
    migrate_containers(
        dry_run=dry_run,
        env=env,
        hard=hard,
        instance=instance,
        platform=platform,
        keep=keep,
        rest_emitter=rest_emitter,
    )


def migrate_containers(
    dry_run: bool,
    env: str,
    platform: str,
    hard: bool,
    instance: str,
    keep: bool,
    rest_emitter: DatahubRestEmitter,
) -> None:
    run_id: str = f"container-migrate-{uuid.uuid4()}"
    migration_report = MigrationReport(run_id, dry_run, keep)

    # Find container ids need to be migrated
    container_id_map: Dict[str, str] = {}
    # Get all the containers need to be migrated
    containers = get_containers_for_migration(env)
    for container in progressbar.progressbar(containers, redirect_stdout=True):
        # Generate new container key
        subType = container["aspects"]["subTypes"]["value"]["typeNames"][0]
        customProperties = container["aspects"]["containerProperties"]["value"][
            "customProperties"
        ]
        if (env is not None and customProperties["instance"] != env) or (
            platform is not None and customProperties["platform"] != platform
        ):
            log.debug(
                f"{container['urn']} does not match filter criteria, skipping.. {customProperties} {env} {platform}"
            )
            continue

        try:
            newKey: Union[SchemaKey, DatabaseKey, ProjectIdKey, BigQueryDatasetKey]
            if subType == "Schema":
                newKey = SchemaKey.parse_obj(customProperties)
            elif subType == "Database":
                newKey = DatabaseKey.parse_obj(customProperties)
            elif subType == "Project":
                newKey = ProjectIdKey.parse_obj(customProperties)
            elif subType == "Dataset":
                newKey = BigQueryDatasetKey.parse_obj(customProperties)
            else:
                log.warning(f"Invalid subtype {subType}. Skipping")
                continue
        except Exception as e:
            log.warning(f"Unable to map {customProperties} to key due to exception {e}")
            continue

        newKey.instance = instance

        log.debug(
            f"Container key migration: {container['urn']} -> urn:li:container:{newKey.guid()}"
        )

        src_urn = container["urn"]
        dst_urn = f"urn:li:container:{newKey.guid()}"
        container_id_map[src_urn] = dst_urn

        # Clone aspects of container with the new urn
        for mcp in migration_utils.clone_aspect(
            src_urn,
            aspect_names=migration_utils.all_aspects,
            dst_urn=dst_urn,
            dry_run=dry_run,
            run_id=run_id,
        ):
            migration_report.on_entity_create(mcp.entityUrn, mcp.aspectName)  # type: ignore
            assert mcp.aspect
            # Update containerProperties to reflect the new key
            if mcp.aspectName == "containerProperties":
                assert isinstance(mcp.aspect, ContainerPropertiesClass)
                containerProperties: ContainerPropertiesClass = mcp.aspect
                containerProperties.customProperties = newKey.dict(
                    by_alias=True, exclude_none=True
                )
                mcp.aspect = containerProperties
            elif mcp.aspectName == "containerKey":
                assert isinstance(mcp.aspect, ContainerKeyClass)
                containerKey: ContainerKeyClass = mcp.aspect
                containerKey.guid = newKey.guid()
                mcp.aspect = containerKey
            if not dry_run:
                rest_emitter.emit_mcp(mcp)
                migration_report.on_entity_affected(mcp.entityUrn, mcp.aspectName)  # type: ignore

        process_container_relationships(
            container_id_map=container_id_map,
            dry_run=dry_run,
            src_urn=src_urn,
            dst_urn=dst_urn,
            migration_report=migration_report,
            rest_emitter=rest_emitter,
        )

        if not dry_run and not keep:
            log.info(f"will {'hard' if hard else 'soft'} delete {src_urn}")
            delete_cli._delete_one_urn(src_urn, soft=not hard, run_id=run_id)
        migration_report.on_entity_migrated(src_urn, "status")  # type: ignore

    click.echo(f"{migration_report}")


def get_containers_for_migration(env: str) -> List[Any]:
    containers_to_migrate = list(cli_utils.get_container_ids_by_filter(env=env))
    containers = []

    increment = 20
    for i in range(0, len(containers_to_migrate), increment):
        for container in cli_utils.batch_get_ids(
            containers_to_migrate[i : i + increment]
        ):
            log.debug(container)
            containers.append(container)

    return containers


def process_container_relationships(
    container_id_map: Dict[str, str],
    dry_run: bool,
    src_urn: str,
    dst_urn: str,
    migration_report: MigrationReport,
    rest_emitter: DatahubRestEmitter,
) -> None:
    relationships = migration_utils.get_incoming_relationships(urn=src_urn)
    for relationship in relationships:
        log.debug(f"Incoming Relationship: {relationship}")
        target_urn = relationship["entity"]

        # We should use the new id if we already migrated it
        if target_urn in container_id_map:
            target_urn = container_id_map.get(target_urn)

        entity_type = _get_type_from_urn(target_urn)
        relationshipType = relationship["type"]
        aspect_name = migration_utils.get_aspect_name_from_relationship(
            relationshipType, entity_type
        )
        aspect_map = cli_utils.get_aspects_for_entity(
            target_urn, aspects=[aspect_name], typed=True
        )
        if aspect_name in aspect_map:
            aspect = aspect_map[aspect_name]
            assert isinstance(aspect, DictWrapper)
            aspect = migration_utils.modify_urn_list_for_aspect(
                aspect_name, aspect, relationshipType, src_urn, dst_urn
            )
            # use mcpw
            mcp = MetadataChangeProposalWrapper(
                entityUrn=target_urn,
                aspect=aspect,
            )

            if not dry_run:
                rest_emitter.emit_mcp(mcp)
            migration_report.on_entity_affected(mcp.entityUrn, mcp.aspectName)  # type: ignore
        else:
            log.debug(f"Didn't find aspect {aspect_name} for urn {target_urn}")
