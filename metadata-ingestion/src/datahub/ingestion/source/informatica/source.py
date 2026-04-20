import logging
from typing import Dict, Iterable, List, Optional, Union

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
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
    InformaticaSourceConfig,
)
from datahub.ingestion.source.informatica.models import (
    ExportJobState,
    IdmcConnection,
    IdmcMapping,
    IdmcMappingTask,
    IdmcObject,
    IdmcObjectType,
    InformaticaApiError,
    InformaticaLoginError,
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
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import CorpUserUrn, DataFlowUrn, DataJobUrn, DatasetUrn
from datahub.sdk.container import Container
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)

PLATFORM = "informatica"
ORPHAN_PROJECT_SENTINEL = "__root__"


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

    Emits IDMC Projects and Folders as Containers; Taskflows and per-project
    mapping flows as DataFlows; Mappings and Mapping Tasks as DataJobs; and
    table-level lineage resolved via the v3 Export API.
    """

    config: InformaticaSourceConfig
    report: InformaticaSourceReport
    platform: str = PLATFORM

    def __init__(self, config: InformaticaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = InformaticaSourceReport()
        self.client = InformaticaClient(config, self.report)
        # Connections are looked up by federated_id during lineage resolution,
        # and by id for the override-by-id config lookup. Separate dicts prevent
        # one namespace from silently shadowing the other.
        self._connections_by_fed_id: Dict[str, IdmcConnection] = {}
        self._connections_by_id: Dict[str, IdmcConnection] = {}
        self._v2_mappings_by_guid: Dict[str, IdmcMapping] = {}
        self._project_objects: Dict[str, IdmcObject] = {}
        self._folder_objects: Dict[str, IdmcObject] = {}
        # Cache of DTEMPLATE IDs collected during mapping extraction, reused by
        # the lineage phase to avoid a second full v3 pass.
        self._mapping_ids: List[str] = []
        # Map v3 mapping GUID → parent project name, populated during mapping
        # extraction and looked up during lineage emission so both phases
        # resolve to the same per-project DataFlow URN.
        self._mapping_project: Dict[str, str] = {}
        # Track synthetic DataFlows we've already emitted so each is emitted once
        # even when multiple DataJobs reference it.
        self._emitted_flow_ids: set[str] = set()

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

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        try:
            self.client.login()
        except InformaticaLoginError as e:
            self.report.failure(
                title="IDMC login failed",
                message="Cannot authenticate to Informatica Cloud; aborting ingestion. Check login_url, username, and password.",
                context=self.config.login_url,
                exc=e,
            )
            return
        yield from self._extract_containers()
        yield from self._extract_taskflows()
        yield from self._extract_mappings_and_tasks()
        if self.config.extract_lineage:
            yield from self._extract_lineage()

    # ------------------------------------------------------------------ containers

    def _extract_containers(self) -> Iterable[Entity]:
        try:
            for project in self._iter_with_tags("Project"):
                if not self.config.project_pattern.allowed(project.name):
                    self.report.projects_filtered += 1
                    continue
                self.report.projects_scanned += 1
                self._project_objects[project.id] = project
                yield self._make_project_container(project)
        except Exception as e:
            self.report.warning(
                title="Failed to list IDMC projects",
                message="Container hierarchy will be incomplete.",
                context="/public/core/v3/objects?type=Project",
                exc=e,
            )
        try:
            for folder in self._iter_with_tags("Folder"):
                if not self.config.folder_pattern.allowed(folder.name):
                    self.report.folders_filtered += 1
                    continue
                self.report.folders_scanned += 1
                self._folder_objects[folder.id] = folder
                yield self._make_folder_container(folder)
        except Exception as e:
            self.report.warning(
                title="Failed to list IDMC folders",
                message="Container hierarchy will be incomplete.",
                context="/public/core/v3/objects?type=Folder",
                exc=e,
            )

    def _make_project_container(self, project: IdmcObject) -> Container:
        return Container(
            container_key=self._project_key(project.name),
            display_name=project.name,
            description=project.description,
            subtype="Project",
            owners=self._owner_list(project.updated_by or project.created_by),
        )

    def _make_folder_container(self, folder: IdmcObject) -> Container:
        parent_project_name = self._resolve_parent_project(folder)
        parent_key: Optional[ContainerKey] = (
            self._project_key(parent_project_name) if parent_project_name else None
        )
        key = InformaticaFolderKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            project_name=parent_project_name or ORPHAN_PROJECT_SENTINEL,
            folder_name=folder.name,
        )
        return Container(
            container_key=key,
            display_name=folder.name,
            description=folder.description,
            subtype="Folder",
            owners=self._owner_list(folder.updated_by or folder.created_by),
            parent_container=parent_key,
        )

    def _project_key(self, project_name: str) -> InformaticaProjectKey:
        return InformaticaProjectKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            project_name=project_name,
        )

    def _resolve_parent_project(self, obj: IdmcObject) -> str:
        return self._resolve_parent_project_from_path(obj.path)

    @staticmethod
    def _resolve_parent_project_from_path(path: str) -> str:
        """Extract the project name from an IDMC path like '/Explore/{ProjectName}/...'."""
        parts = [p for p in path.split("/") if p and p != "Explore"]
        return parts[0] if parts else ""

    # ------------------------------------------------------------------ taskflows

    def _extract_taskflows(self) -> Iterable[Entity]:
        try:
            for tf in self._iter_with_tags("TASKFLOW"):
                if self._is_bundle_object(tf):
                    continue
                if not self.config.taskflow_pattern.allowed(tf.name):
                    self.report.taskflows_filtered += 1
                    continue
                self.report.taskflows_scanned += 1
                yield self._make_taskflow(tf)
        except Exception as e:
            self.report.warning(
                title="Failed to list IDMC taskflows",
                message="Taskflow DataFlow entities will be missing.",
                context="/public/core/v3/objects?type=TASKFLOW",
                exc=e,
            )

    def _make_taskflow(self, tf: IdmcObject) -> DataFlow:
        return DataFlow(
            platform=PLATFORM,
            name=f"taskflow:{tf.id}",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=tf.name,
            description=tf.description,
            custom_properties=self._taskflow_custom_props(tf),
            owners=self._owner_list(tf.updated_by or tf.created_by),
            subtype="Taskflow",
        )

    @staticmethod
    def _taskflow_custom_props(tf: IdmcObject) -> Dict[str, str]:
        return {
            k: v
            for k, v in (
                ("path", tf.path),
                ("createdBy", tf.created_by or ""),
                ("updatedBy", tf.updated_by or ""),
                ("createTime", tf.create_time or ""),
                ("updateTime", tf.update_time or ""),
            )
            if v
        }

    # ------------------------------------------------------------------ mappings

    def _extract_mappings_and_tasks(self) -> Iterable[Entity]:
        # Cache v2 mapping → GUID so we can look up v2 metadata (valid, v2_id) for v3 objects.
        try:
            for m in self.client.list_mappings():
                if m.asset_frs_guid:
                    self._v2_mappings_by_guid[m.asset_frs_guid] = m
        except Exception as e:
            self.report.warning(
                title="Failed to list IDMC v2 mappings",
                message="v2 mapping attributes (valid flag, v2_id) will be missing from DataJob custom properties.",
                context="/api/v2/mapping",
                exc=e,
            )

        try:
            for obj in self._iter_with_tags("DTEMPLATE"):
                if self._is_bundle_object(obj):
                    continue
                if not self.config.mapping_pattern.allowed(obj.name):
                    self.report.mappings_filtered += 1
                    continue
                self.report.mappings_scanned += 1
                self._mapping_ids.append(obj.id)
                project_name = self._resolve_parent_project(obj)
                self._mapping_project[obj.id] = project_name
                project_flow = self._project_flow(project_name)
                project_flow_urn = str(project_flow.urn)
                if project_flow_urn not in self._emitted_flow_ids:
                    self._emitted_flow_ids.add(project_flow_urn)
                    yield project_flow
                yield self._make_mapping_datajob(
                    obj,
                    project_flow,
                    self._v2_mappings_by_guid.get(obj.id),
                )
        except Exception as e:
            self.report.warning(
                title="Failed to list IDMC mappings",
                message="Mappings and their lineage will be missing.",
                context="/public/core/v3/objects?type=DTEMPLATE",
                exc=e,
            )

        try:
            for mt in self.client.list_mapping_tasks():
                self.report.mapping_tasks_scanned += 1
                task_flow = self._mapping_task_flow(mt)
                task_flow_urn = str(task_flow.urn)
                if task_flow_urn not in self._emitted_flow_ids:
                    self._emitted_flow_ids.add(task_flow_urn)
                    yield task_flow
                yield self._make_mapping_task_datajob(mt, task_flow)
        except Exception as e:
            self.report.warning(
                title="Failed to list IDMC mapping tasks",
                message="Mapping Tasks will be missing from ingestion output.",
                context="/api/v2/mttask",
                exc=e,
            )

    def _make_mapping_datajob(
        self,
        obj: IdmcObject,
        flow: DataFlow,
        v2_mapping: Optional[IdmcMapping],
    ) -> DataJob:
        custom_props: Dict[str, str] = {
            "path": obj.path,
            "objectType": "DTEMPLATE",
            "v3Id": obj.id,
        }
        if v2_mapping:
            custom_props["v2Id"] = v2_mapping.v2_id
            custom_props["valid"] = "true" if v2_mapping.valid else "false"
        return DataJob(
            name=obj.id,
            flow=flow,
            display_name=obj.name,
            description=obj.description,
            custom_properties=custom_props,
            subtype="Mapping",
            owners=self._owner_list(obj.updated_by or obj.created_by),
        )

    def _make_mapping_task_datajob(
        self, mt: IdmcMappingTask, flow: DataFlow
    ) -> DataJob:
        custom_props: Dict[str, str] = {
            "objectType": "MTT",
            "v2Id": mt.v2_id,
            "mappingId": mt.mapping_id,
            "mappingName": mt.mapping_name,
        }
        return DataJob(
            name=mt.v2_id,
            flow=flow,
            display_name=mt.name,
            description=mt.description,
            custom_properties=custom_props,
            subtype="Mapping Task",
            owners=self._owner_list(mt.updated_by or mt.created_by),
        )

    def _mapping_task_flow(self, mt: IdmcMappingTask) -> DataFlow:
        return DataFlow(
            platform=PLATFORM,
            name=f"mttask_flow:{mt.v2_id}",
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=mt.name,
            subtype="Mapping Task Flow",
        )

    def _project_flow(self, project_name: str) -> DataFlow:
        """Return the synthetic per-project DataFlow shared by lineage and mapping emission."""
        return DataFlow(
            platform=PLATFORM,
            name=self._project_flow_id(project_name),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=f"Mappings in {project_name}" if project_name else "Mappings",
            subtype="Project Mapping Flow",
        )

    @staticmethod
    def _project_flow_id(project_name: str) -> str:
        return f"project:{project_name or 'default'}"

    # ------------------------------------------------------------------ lineage

    def _extract_lineage(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self._load_connections()
        if not self._mapping_ids:
            logger.info("No mappings to extract lineage for.")
            return
        batch_size = self.config.export_batch_size
        for batch_start in range(0, len(self._mapping_ids), batch_size):
            batch = self._mapping_ids[batch_start : batch_start + batch_size]
            try:
                yield from self._process_export_batch(batch, batch_start)
            except (InformaticaApiError, InformaticaLoginError) as e:
                self.report.warning(
                    title="IDMC export batch failed",
                    message="Lineage for this batch will be missing.",
                    context=f"batch_start={batch_start}, size={len(batch)}",
                    exc=e,
                )
                self.report.export_jobs_failed.append(f"batch@{batch_start}: {e}")
            except Exception as e:
                self.report.warning(
                    title="Unexpected error processing export batch",
                    message="Lineage for this batch will be missing.",
                    context=f"batch_start={batch_start}, size={len(batch)}",
                    exc=e,
                )

    def _process_export_batch(
        self, batch: List[str], batch_start: int
    ) -> Iterable[MetadataWorkUnit]:
        job_id = self.client.submit_export_job(batch)
        status = self.client.wait_for_export(job_id)
        if status.state != ExportJobState.SUCCESSFUL:
            self.report.warning(
                title="IDMC export job did not succeed",
                message="Lineage for this batch will be missing.",
                context=f"job_id={job_id}, state={status.state.value}, batch_start={batch_start}",
            )
            return
        for lineage_info in self.client.download_and_parse_export(job_id, batch):
            yield from self._emit_lineage(lineage_info)

    def _load_connections(self) -> None:
        """Load all connections into cache, keyed by federatedId and id (separate dicts)."""
        if self._connections_by_id or self._connections_by_fed_id:
            return
        try:
            for conn in self.client.list_connections():
                if conn.federated_id:
                    self._connections_by_fed_id[conn.federated_id] = conn
                self._connections_by_id[conn.id] = conn
        except Exception as e:
            self.report.failure(
                title="Failed to load IDMC connections",
                message="Lineage resolution will be degraded; upstream/downstream datasets cannot be resolved.",
                context="/api/v2/connection",
                exc=e,
            )

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
        """Emit dataJobInputOutput for the mapping job and upstreamLineage for each target."""
        if not lineage_info.mapping_id:
            self.report.warning(
                title="IDMC lineage missing mapping id",
                message="Could not align mapping export entry to a submitted v3 GUID; skipping lineage emission.",
                context=f"mapping_name={lineage_info.mapping_name}",
            )
            return
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
        project_name = self._project_from_mapping_id(lineage_info.mapping_id)
        job_urn = str(
            DataJobUrn.create_from_ids(
                data_flow_urn=str(
                    DataFlowUrn.create_from_ids(
                        orchestrator=PLATFORM,
                        flow_id=self._project_flow_id(project_name),
                        env=self.config.env,
                        platform_instance=self.config.platform_instance,
                    )
                ),
                job_id=lineage_info.mapping_id,
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

    def _project_from_mapping_id(self, mapping_id: str) -> str:
        return self._mapping_project.get(mapping_id, "")

    def _resolve_table_to_dataset_urn(self, table: LineageTable) -> Optional[str]:
        conn = self._connections_by_fed_id.get(table.connection_federated_id)
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

    # ------------------------------------------------------------------ helpers

    def _iter_with_tags(self, object_type: IdmcObjectType) -> Iterable[IdmcObject]:
        """Iterate v3 objects of a type, honoring tag filters and de-duplicating by id."""
        seen: set[str] = set()
        for tag in self._tag_filters_or_none():
            for obj in self.client.list_objects(object_type, tag=tag):
                if obj.id in seen:
                    continue
                seen.add(obj.id)
                yield obj

    def _tag_filters_or_none(self) -> List[Optional[str]]:
        if self.config.tag_filter_names:
            return list(self.config.tag_filter_names)  # type: ignore[arg-type]
        return [None]

    @staticmethod
    def _is_bundle_object(obj: IdmcObject) -> bool:
        return obj.path.startswith("Add-On Bundles/") or obj.updated_by == (
            "bundle-license-notifier"
        )

    def _owner_list(
        self, user_identifier: Optional[str]
    ) -> Optional[List[CorpUserUrn]]:
        if not user_identifier or not self.config.extract_ownership:
            return None
        return [CorpUserUrn(user_identifier)]
