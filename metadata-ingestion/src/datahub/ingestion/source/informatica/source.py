import logging
from typing import Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.informatica.client import InformaticaClient
from datahub.ingestion.source.informatica.config import (
    CONNECTION_TYPE_MAP,
    ORCHESTRATOR,
    InformaticaSourceConfig,
)
from datahub.ingestion.source.informatica.models import (
    IdmcConnection,
    IdmcMapping,
    IdmcMappingTask,
    IdmcObject,
    InformaticaSourceReport,
    LineageTable,
    MappingLineageInfo,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import (
    CorpUserUrn,
    DataFlowUrn,
    DataJobUrn,
    DatasetUrn,
)

logger = logging.getLogger(__name__)

PLATFORM = "informatica"
PLATFORM_URN = make_data_platform_urn(PLATFORM)


class InformaticaProjectKey(ContainerKey):
    project_name: str


class InformaticaFolderKey(ContainerKey):
    project_name: str
    folder_name: str


@platform_name("Informatica")
@config_class(InformaticaSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Projects and folders as containers")
@capability(SourceCapability.LINEAGE_COARSE, "Table-level lineage via v3 Export API")
@capability(SourceCapability.DELETION_DETECTION, "Via stateful ingestion")
@capability(SourceCapability.OWNERSHIP, "From IDMC object createdBy/updatedBy")
class InformaticaSource(StatefulIngestionSourceBase):
    """DataHub ingestion source for Informatica Cloud (IDMC).

    Extracts projects, folders, taskflows (DataFlow), mapping tasks and mappings
    (DataJob), and table-level lineage from source/target connections.
    """

    config: InformaticaSourceConfig
    report: InformaticaSourceReport
    platform: str = PLATFORM

    def __init__(self, config: InformaticaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = InformaticaSourceReport()
        self.client = InformaticaClient(config, self.report)
        self._connections: Dict[str, IdmcConnection] = {}
        self._v2_mappings_by_guid: Dict[str, IdmcMapping] = {}
        self._project_objects: Dict[str, IdmcObject] = {}
        self._folder_objects: Dict[str, IdmcObject] = {}

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "InformaticaSource":
        config = InformaticaSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_report(self) -> InformaticaSourceReport:
        return self.report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.client.login()
        yield from self._extract_containers()
        yield from self._extract_taskflows()
        yield from self._extract_mappings_and_tasks()
        if self.config.extract_lineage:
            yield from self._extract_lineage()

    def _extract_containers(self) -> Iterable[MetadataWorkUnit]:
        """Extract IDMC projects and folders as DataHub containers."""
        for tag in self._tag_filters_or_none():
            for project in self.client.list_objects("Project", tag=tag):
                if not self.config.project_pattern.allowed(project.name):
                    self.report.projects_filtered += 1
                    continue
                self.report.projects_scanned += 1
                self._project_objects[project.id] = project
                yield from self._emit_project_container(project)
        for tag in self._tag_filters_or_none():
            for folder in self.client.list_objects("Folder", tag=tag):
                if not self.config.folder_pattern.allowed(folder.name):
                    self.report.folders_filtered += 1
                    continue
                self.report.folders_scanned += 1
                self._folder_objects[folder.id] = folder
                yield from self._emit_folder_container(folder)

    def _emit_project_container(
        self, project: IdmcObject
    ) -> Iterable[MetadataWorkUnit]:
        key = InformaticaProjectKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            project_name=project.name,
        )
        yield from gen_containers(
            container_key=key,
            name=project.name,
            description=project.description or None,
            sub_types=["Project"],
            owner_urn=self._make_owner_urn(project.updated_by or project.created_by),
        )

    def _emit_folder_container(self, folder: IdmcObject) -> Iterable[MetadataWorkUnit]:
        parent_project_name = self._resolve_parent_project(folder)
        parent_key: Optional[ContainerKey] = None
        if parent_project_name:
            parent_key = InformaticaProjectKey(
                platform=PLATFORM,
                instance=self.config.platform_instance,
                env=self.config.env,
                project_name=parent_project_name,
            )
        key = InformaticaFolderKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            project_name=parent_project_name or "__root__",
            folder_name=folder.name,
        )
        yield from gen_containers(
            container_key=key,
            name=folder.name,
            description=folder.description or None,
            sub_types=["Folder"],
            owner_urn=self._make_owner_urn(folder.updated_by or folder.created_by),
            parent_container_key=parent_key,
        )

    def _resolve_parent_project(self, obj: IdmcObject) -> str:
        """Try to extract the parent project name from the object's path."""
        return self._resolve_parent_project_from_path(obj.path)

    @staticmethod
    def _resolve_parent_project_from_path(path: str) -> str:
        """Extract the project name from an IDMC path like '/Explore/{ProjectName}/...'."""
        parts = [p for p in path.split("/") if p and p != "Explore"]
        return parts[0] if parts else ""

    def _extract_taskflows(self) -> Iterable[MetadataWorkUnit]:
        """Extract IDMC taskflows as DataFlow entities."""
        for tag in self._tag_filters_or_none():
            for tf in self.client.list_objects("TASKFLOW", tag=tag):
                if self._is_bundle_object(tf):
                    continue
                if not self.config.taskflow_pattern.allowed(tf.name):
                    self.report.taskflows_filtered += 1
                    continue
                self.report.taskflows_scanned += 1
                yield from self._emit_taskflow(tf)

    def _emit_taskflow(self, tf: IdmcObject) -> Iterable[MetadataWorkUnit]:
        flow_urn = str(
            DataFlowUrn.create_from_ids(
                orchestrator=ORCHESTRATOR,
                flow_id=tf.id,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(
                name=tf.name,
                description=tf.description or None,
                customProperties={
                    "path": tf.path,
                    "createdBy": tf.created_by,
                    "updatedBy": tf.updated_by,
                    "createTime": tf.create_time,
                    "updateTime": tf.update_time,
                },
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()
        if self.config.platform_instance:
            yield MetadataChangeProposalWrapper(
                entityUrn=flow_urn,
                aspect=DataPlatformInstanceClass(
                    platform=PLATFORM_URN,
                    instance=make_dataplatform_instance_urn(
                        PLATFORM, self.config.platform_instance
                    ),
                ),
            ).as_workunit()
        if self.config.extract_ownership:
            yield from self._emit_ownership(flow_urn, tf.updated_by or tf.created_by)

    def _extract_mappings_and_tasks(self) -> Iterable[MetadataWorkUnit]:
        """Extract mappings and mapping tasks as DataJob entities."""
        # v2 mapping cache keyed by v3 GUID, for cross-referencing below.
        for m in self.client.list_mappings():
            if m.asset_frs_guid:
                self._v2_mappings_by_guid[m.asset_frs_guid] = m
        for tag in self._tag_filters_or_none():
            for obj in self.client.list_objects("DTEMPLATE", tag=tag):
                if self._is_bundle_object(obj):
                    continue
                if not self.config.mapping_pattern.allowed(obj.name):
                    self.report.mappings_filtered += 1
                    continue
                self.report.mappings_scanned += 1
                yield from self._emit_mapping(
                    obj, self._v2_mappings_by_guid.get(obj.id)
                )
        try:
            for mt in self.client.list_mapping_tasks():
                self.report.mapping_tasks_scanned += 1
                yield from self._emit_mapping_task(mt)
        except Exception:
            logger.warning("Failed to fetch mapping tasks", exc_info=True)

    def _emit_mapping(
        self,
        obj: IdmcObject,
        v2_mapping: Optional[IdmcMapping],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit a mapping as a DataJob under a synthetic per-project DataFlow."""
        flow_urn = str(
            DataFlowUrn.create_from_ids(
                orchestrator=ORCHESTRATOR,
                flow_id=f"project:{self._resolve_parent_project(obj) or 'default'}",
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )
        )
        job_urn = str(DataJobUrn.create_from_ids(data_flow_urn=flow_urn, job_id=obj.id))
        custom_props: Dict[str, str] = {
            "path": obj.path,
            "objectType": "DTEMPLATE",
            "v3Id": obj.id,
        }
        if v2_mapping:
            custom_props["v2Id"] = v2_mapping.v2_id
            custom_props["valid"] = str(v2_mapping.valid)
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=obj.name,
                description=obj.description or None,
                type="COMMAND",
                customProperties=custom_props,
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=SubTypesClass(typeNames=["Mapping"]),
        ).as_workunit()
        if self.config.platform_instance:
            yield MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataPlatformInstanceClass(
                    platform=PLATFORM_URN,
                    instance=make_dataplatform_instance_urn(
                        PLATFORM, self.config.platform_instance
                    ),
                ),
            ).as_workunit()
        if self.config.extract_ownership:
            yield from self._emit_ownership(job_urn, obj.updated_by or obj.created_by)

    def _emit_mapping_task(self, mt: IdmcMappingTask) -> Iterable[MetadataWorkUnit]:
        """Emit a mapping task as a DataJob."""
        flow_urn = str(
            DataFlowUrn.create_from_ids(
                orchestrator=ORCHESTRATOR,
                flow_id=f"mttask_flow:{mt.v2_id}",
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )
        )
        job_urn = str(
            DataJobUrn.create_from_ids(data_flow_urn=flow_urn, job_id=mt.v2_id)
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(
                name=mt.name,
                description=mt.description or None,
                type="COMMAND",
                customProperties={
                    "objectType": "MTT",
                    "v2Id": mt.v2_id,
                    "mappingId": mt.mapping_id,
                    "mappingName": mt.mapping_name,
                },
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=SubTypesClass(typeNames=["Mapping Task"]),
        ).as_workunit()

    def _extract_lineage(self) -> Iterable[MetadataWorkUnit]:
        """Extract table-level lineage from mapping definitions via v3 export."""
        self._load_connections()
        mapping_ids = [
            obj.id
            for obj in self._iter_mapping_objects()
            if not self._is_bundle_object(obj)
            and self.config.mapping_pattern.allowed(obj.name)
        ]
        if not mapping_ids:
            logger.info("No mappings to extract lineage for.")
            return
        for batch_start in range(0, len(mapping_ids), self.config.export_batch_size):
            batch = mapping_ids[
                batch_start : batch_start + self.config.export_batch_size
            ]
            try:
                job_id = self.client.submit_export_job(batch)
                status = self.client.wait_for_export(job_id)
                if status.state != "SUCCESSFUL":
                    logger.warning(
                        "Export job %s ended with state %s: %s",
                        job_id,
                        status.state,
                        status.message,
                    )
                    continue
                for lineage_info in self.client.download_and_parse_export(job_id):
                    yield from self._emit_lineage(lineage_info)
            except Exception:
                logger.warning(
                    "Failed to process export batch starting at index %d",
                    batch_start,
                    exc_info=True,
                )

    def _load_connections(self) -> None:
        """Load all connections into cache, keyed by federatedId and id."""
        if self._connections:
            return
        try:
            for conn in self.client.list_connections():
                if conn.federated_id:
                    self._connections[conn.federated_id] = conn
                self._connections[conn.id] = conn
        except Exception:
            logger.warning("Failed to load connections", exc_info=True)

    def _resolve_connection_platform(self, connection: IdmcConnection) -> Optional[str]:
        """Resolve an IDMC connection to a DataHub platform name.

        Priority: manual override → connParams["Connection Type"] → base type.
        """
        override = self.config.connection_type_overrides.get(connection.id)
        if override:
            return override
        if connection.conn_type and connection.conn_type in CONNECTION_TYPE_MAP:
            return CONNECTION_TYPE_MAP[connection.conn_type]
        if connection.base_type and connection.base_type in CONNECTION_TYPE_MAP:
            return CONNECTION_TYPE_MAP[connection.base_type]
        return None

    def _emit_lineage(
        self, lineage_info: MappingLineageInfo
    ) -> Iterable[MetadataWorkUnit]:
        """Emit dataJobInputOutput and upstreamLineage for a mapping's lineage."""
        input_urns: List[str] = [
            urn
            for src in lineage_info.source_tables
            if (urn := self._resolve_table_to_dataset_urn(src)) is not None
        ]
        output_urns: List[str] = [
            urn
            for tgt in lineage_info.target_tables
            if (urn := self._resolve_table_to_dataset_urn(tgt)) is not None
        ]
        if not input_urns and not output_urns:
            return
        project_name = self._resolve_parent_project_from_path(lineage_info.mapping_name)
        flow_urn = str(
            DataFlowUrn.create_from_ids(
                orchestrator=ORCHESTRATOR,
                flow_id=f"project:{project_name or 'default'}",
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )
        )
        job_urn = str(
            DataJobUrn.create_from_ids(
                data_flow_urn=flow_urn, job_id=lineage_info.mapping_id
            )
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=input_urns,
                outputDatasets=output_urns,
            ),
        ).as_workunit()
        self.report.lineage_edges_emitted += len(input_urns) + len(output_urns)
        for tgt_urn in output_urns:
            yield MetadataChangeProposalWrapper(
                entityUrn=tgt_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=src_urn,
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        )
                        for src_urn in input_urns
                    ]
                ),
            ).as_workunit()

    def _resolve_table_to_dataset_urn(self, table: LineageTable) -> Optional[str]:
        """Resolve a LineageTable to a DataHub dataset URN."""
        conn = self._connections.get(table.connection_federated_id)
        if not conn:
            self.report.report_connection_unresolved(
                table.connection_federated_id,
                "unknown",
                f"Connection not found for table {table.table_name}",
            )
            return None
        platform = self._resolve_connection_platform(conn)
        if not platform:
            self.report.report_connection_unresolved(
                conn.id,
                conn.name,
                f"Unmapped connection type: {conn.conn_type or conn.base_type}",
            )
            return None
        self.report.connections_resolved += 1
        # Dataset name: [database.][schema.]table — empty parts are skipped.
        parts: List[str] = []
        if conn.database:
            parts.append(conn.database)
        if table.schema_name:
            parts.append(table.schema_name)
        elif conn.schema:
            parts.append(conn.schema)
        parts.append(table.table_name)
        return str(
            DatasetUrn.create_from_ids(
                platform_id=platform,
                table_name=".".join(parts),
                env=self.config.env,
            )
        )

    def _iter_mapping_objects(self) -> Iterable[IdmcObject]:
        """Re-iterate mapping objects from the v3 API for lineage extraction."""
        for tag in self._tag_filters_or_none():
            yield from self.client.list_objects("DTEMPLATE", tag=tag)

    def _tag_filters_or_none(self) -> List[Optional[str]]:
        """Return tag filters or [None] to indicate no tag filtering."""
        if self.config.tag_filter_names:
            return list(self.config.tag_filter_names)  # type: ignore[arg-type]
        return [None]

    @staticmethod
    def _is_bundle_object(obj: IdmcObject) -> bool:
        return obj.path.startswith("Add-On Bundles/") or obj.updated_by in (
            "bundle-license-notifier",
        )

    @staticmethod
    def _make_owner_urn(user_identifier: str) -> Optional[str]:
        if not user_identifier:
            return None
        return str(CorpUserUrn(user_identifier))

    def _emit_ownership(
        self, entity_urn: str, owner: str
    ) -> Iterable[MetadataWorkUnit]:
        if not owner:
            return
        yield MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=str(CorpUserUrn(owner)),
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
            ),
        ).as_workunit()
