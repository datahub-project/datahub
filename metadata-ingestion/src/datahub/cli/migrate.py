import logging
import random
import uuid
from typing import Dict, List, Tuple

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
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
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
@telemetry.with_telemetry
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
@telemetry.with_telemetry
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

    all_aspects = [
        "schemaMetadata",
        "datasetProperties",
        "viewProperties",
        "subTypes",
        "editableDatasetProperties",
        "ownership",
        "datasetDeprecation",
        "institutionalMemory",
        "editableSchemaMetadata",
        "globalTags",
        "glossaryTerms",
        "upstreamLineage",
        "datasetUpstreamLineage",
        "status",
    ]

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
        relationships = migration_utils.get_incoming_relationships_dataset(
            src_entity_urn
        )

        for mcp in migration_utils.clone_aspect(
            src_entity_urn,
            aspect_names=all_aspects,
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
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=new_urn,
                    aspectName="dataPlatformInstance",
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
            aspect_name = (
                migration_utils.get_aspect_name_from_relationship_type_and_entity(
                    relationshipType, entity_type
                )
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
                    entityType=entity_type,
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=target_urn,
                    aspectName=aspect_name,
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

    print(f"{migration_report}")
