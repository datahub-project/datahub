import functools
import logging
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import requests

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
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import CorpUserUrn, DataJobUrn, DatasetUrn
from datahub.sdk.container import Container
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)

PLATFORM = "informatica"
ORPHAN_PROJECT_SENTINEL = "__root__"
# Shared between extraction and lineage phases so DataJob URNs match.
MAPPING_JOB_ID = "transform"

T = TypeVar("T")


class InformaticaProjectKey(ContainerKey):
    project_name: str


class InformaticaFolderKey(InformaticaProjectKey):
    # Inheriting from ProjectKey lets ContainerKey.parent_key() walk
    # Folder → Project automatically for BrowsePathsV2 assembly.
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
        # Lineage looks up by federated_id; config overrides look up by id.
        self._connections_by_fed_id: Dict[str, IdmcConnection] = {}
        self._connections_by_id: Dict[str, IdmcConnection] = {}
        self._v2_mappings_by_guid: Dict[str, IdmcMapping] = {}
        # Collected during mapping extraction so lineage phase avoids a second v3 pass.
        self._mapping_ids: List[str] = []
        # v3 mapping GUID → DataFlow URN; lineage phase reuses this for DataJob URNs.
        self._mapping_flow_urns: Dict[str, str] = {}
        # v2 mapping ID → IDMC path, so mapping tasks can inherit their
        # parent mapping's browse path when their own is absent.
        self._v2_id_to_path: Dict[str, str] = {}
        # Track emitted containers so filtered segments fall back to plain-name
        # browse-path entries instead of dangling URN references.
        self._emitted_project_names: Set[str] = set()
        self._emitted_folder_keys: Set[Tuple[str, str]] = set()

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

    def _extract_containers(self) -> Iterable[Entity]:
        projects = self._safe_list(
            lambda: self._iter_with_tags("Project"),
            title="Failed to list IDMC projects",
            message="Container hierarchy will be incomplete.",
            context="/public/core/v3/objects?type=Project",
        )
        for project in projects:
            if not self.config.project_pattern.allowed(project.name):
                self.report.projects_filtered += 1
                self.report.report_filtered(
                    "pattern", "Project", f"name={project.name!r} path={project.path}"
                )
                continue
            self.report.projects_scanned += 1
            self._emitted_project_names.add(project.name)
            logger.debug(
                "Emitting Project container: name=%s path=%s id=%s",
                project.name,
                project.path,
                project.id,
            )
            yield self._make_project_container(project)

        folders = self._safe_list(
            lambda: self._iter_with_tags("Folder"),
            title="Failed to list IDMC folders",
            message="Container hierarchy will be incomplete.",
            context="/public/core/v3/objects?type=Folder",
        )
        for folder in folders:
            try:
                if self._is_project_masquerading_as_folder(folder):
                    self.report.report_filtered(
                        "project-level",
                        "Folder",
                        f"name={folder.name!r} path={folder.path}",
                    )
                    continue
                if not self.config.folder_pattern.allowed(folder.name):
                    self.report.folders_filtered += 1
                    self.report.report_filtered(
                        "pattern",
                        "Folder",
                        f"name={folder.name!r} path={folder.path}",
                    )
                    continue
                self.report.folders_scanned += 1
                parent_project = (
                    self._resolve_parent_project(folder) or ORPHAN_PROJECT_SENTINEL
                )
                self._emitted_folder_keys.add((parent_project, folder.name))
                logger.debug(
                    "Emitting Folder container: name=%s path=%s parent_project=%s",
                    folder.name,
                    folder.path,
                    parent_project,
                )
                yield self._make_folder_container(folder)
            except Exception as e:
                self.report.warning(
                    title="Failed to process IDMC folder",
                    message="This folder will be skipped.",
                    context=f"id={folder.id}, name={folder.name!r}",
                    exc=e,
                )

    def _safe_list(
        self,
        iterable_factory: Callable[[], Iterable[T]],
        *,
        title: str,
        message: str,
        context: str,
    ) -> Iterator[T]:
        """Iterate an IDMC listing call, converting enumeration failures to
        source-report warnings instead of aborting the run.

        Takes a zero-arg factory (not the iterable directly) so that
        exceptions raised at *call time* — e.g. a non-generator client
        method like ``list_mapping_tasks`` that HTTP-fails on invocation —
        are caught the same as iteration-time failures. Per-item failures
        should still be handled inline by the caller since they're usually
        type-specific.
        """
        try:
            yield from iterable_factory()
        except Exception as e:
            self.report.warning(title=title, message=message, context=context, exc=e)

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
        key = self._folder_key(
            parent_project_name or ORPHAN_PROJECT_SENTINEL, folder.name
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

    def _excluded_by_folder_filter(self, path: str) -> bool:
        """True when the entity's containing folder is rejected by folder_pattern.

        Cascades the folder filter to entities inside a filtered folder —
        users expect ``folder_pattern`` to also exclude mappings/tasks living
        inside folders that don't match. Entities at the project root
        (2-segment path) aren't subject to folder filtering.
        """
        parts = self._parse_idmc_path(path)
        if len(parts) < 3:
            return False
        folder_name = parts[1]
        return not self.config.folder_pattern.allowed(folder_name)

    @staticmethod
    def _parse_idmc_path(path: str) -> List[str]:
        """Split an IDMC path into its meaningful segments.

        Drops empty elements and the leading ``Explore`` token so callers
        work with ``[Project, Folder, Asset]`` uniformly whether the raw
        path arrived as ``/Explore/P/F/A`` or ``P/F/A``.
        """
        return [p for p in path.split("/") if p and p != "Explore"]

    @staticmethod
    def _resolve_parent_project_from_path(path: str) -> str:
        """Extract the project name from an IDMC path like '/Explore/{ProjectName}/...'."""
        parts = InformaticaSource._parse_idmc_path(path)
        return parts[0] if parts else ""

    def _browse_path_entries(self, path: str) -> List[BrowsePathEntryClass]:
        """Return BrowsePathsV2 ancestor entries for an IDMC path.

        Emits the Project (and Folder, when present) as container URN entries so
        the entity nests under its IDMC project/folder containers in the UI.
        For '/Explore/Sales/ETL/my_entity' → [ProjectURN(Sales), FolderURN(ETL)].

        When a path segment corresponds to a container we did NOT emit (filtered
        out, or not a real IDMC entity), a plain-name entry is used instead so
        the UI renders the folder's human-readable name rather than a ghost URN.
        """
        parts = self._parse_idmc_path(path)
        ancestors = parts[:-1] if len(parts) > 1 else []
        if not ancestors:
            return []
        project_name = ancestors[0]
        entries: List[BrowsePathEntryClass] = [
            self._container_or_name_entry(
                project_name in self._emitted_project_names,
                self._project_key(project_name).as_urn(),
                project_name,
            )
        ]
        if len(ancestors) >= 2:
            folder_name = ancestors[1]
            entries.append(
                self._container_or_name_entry(
                    (project_name, folder_name) in self._emitted_folder_keys,
                    self._folder_key(project_name, folder_name).as_urn(),
                    folder_name,
                )
            )
            for extra in ancestors[2:]:
                entries.append(BrowsePathEntryClass(id=extra))
        return entries

    @staticmethod
    def _container_or_name_entry(
        emitted: bool, urn: str, name: str
    ) -> BrowsePathEntryClass:
        """Container URN entry when emitted; plain-name entry otherwise.

        Plain-name entries avoid ghost URNs in the UI navigate tree when the
        container for a path segment isn't part of this ingestion run.
        """
        if emitted:
            return BrowsePathEntryClass(id=urn, urn=urn)
        return BrowsePathEntryClass(id=name)

    def _folder_key(self, project_name: str, folder_name: str) -> InformaticaFolderKey:
        return InformaticaFolderKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            project_name=project_name,
            folder_name=folder_name,
        )

    def _container_parent_key(self, path: str) -> Optional[ContainerKey]:
        """Return the deepest emitted-container key for an IDMC path.

        Used with ``flow._set_container`` so DataFlows carry both
        ``ContainerClass.container`` and ``BrowsePathsV2``. DataHub's
        ``browseV2`` needs the ``container`` pointer to group entities at
        the project root consistently with entities inside a folder.
        Returns None when no ancestor container was emitted.
        """
        parts = self._parse_idmc_path(path)
        if len(parts) < 2:
            return None
        project_name = parts[0]
        if project_name not in self._emitted_project_names:
            return None
        if len(parts) >= 3 and (project_name, parts[1]) in self._emitted_folder_keys:
            return self._folder_key(project_name, parts[1])
        return self._project_key(project_name)

    def _extract_taskflows(self) -> Iterable[Entity]:
        taskflows = self._safe_list(
            lambda: self._iter_with_tags("TASKFLOW"),
            title="Failed to list IDMC taskflows",
            message="Taskflow DataFlow entities will be missing.",
            context="/public/core/v3/objects?type=TASKFLOW",
        )
        for tf in taskflows:
            try:
                if self._excluded_by_folder_filter(tf.path):
                    self.report.taskflows_filtered += 1
                    self.report.report_filtered(
                        "folder-excluded",
                        "TASKFLOW",
                        f"name={tf.name!r} path={tf.path}",
                    )
                    continue
                if not self.config.taskflow_pattern.allowed(tf.name):
                    self.report.taskflows_filtered += 1
                    self.report.report_filtered(
                        "pattern",
                        "TASKFLOW",
                        f"name={tf.name!r} path={tf.path}",
                    )
                    continue
                self.report.taskflows_scanned += 1
                logger.debug(
                    "Emitting Taskflow: name=%s path=%s id=%s",
                    tf.name,
                    tf.path,
                    tf.id,
                )
                yield self._make_taskflow(tf)
            except Exception as e:
                self.report.warning(
                    title="Failed to process IDMC taskflow",
                    message="This taskflow will be skipped.",
                    context=f"id={tf.id}, name={tf.name!r}",
                    exc=e,
                )

    def _make_taskflow(self, tf: IdmcObject) -> DataFlow:
        display = tf.name or tf.id
        flow = DataFlow(
            platform=PLATFORM,
            # GUID flow_id avoids ``.`` or ``/`` which the UI splits into hierarchy.
            name=tf.id or display,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=display,
            description=tf.description,
            custom_properties=self._taskflow_custom_props(tf),
            owners=self._owner_list(tf.updated_by or tf.created_by),
            subtype="Taskflow",
        )
        parent_key = self._container_parent_key(tf.path)
        if parent_key is not None:
            flow._set_container(parent_key)
        browse_entries = self._browse_path_entries(tf.path)
        if browse_entries:
            flow._set_aspect(BrowsePathsV2Class(path=browse_entries))
        return flow

    @staticmethod
    def _taskflow_custom_props(tf: IdmcObject) -> Dict[str, str]:
        return {
            k: v
            for k, v in (
                ("idmcId", tf.id),
                ("path", tf.path),
                ("createdBy", tf.created_by or ""),
                ("updatedBy", tf.updated_by or ""),
                ("createTime", tf.create_time or ""),
                ("updateTime", tf.update_time or ""),
            )
            if v
        }

    def _extract_mappings_and_tasks(self) -> Iterable[Entity]:
        # Cache v2 mappings by GUID for v3-object metadata lookup (valid flag, v2_id).
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

        # DTEMPLATE = mapping, DMAPPLET = CDI-native mapplet, MAPPLET = imported
        # from PowerCenter. Both mapplet shapes coexist, so we query each and dedup.
        mapping_types: List[Tuple[IdmcObjectType, str]] = [
            ("DTEMPLATE", "Mapping"),
            ("MAPPLET", "Mapplet"),
            ("DMAPPLET", "Mapplet"),
        ]
        seen_mapping_ids: Set[str] = set()
        mapplets_from_v3 = 0
        for obj_type, default_subtype in mapping_types:
            items = self._safe_list(
                functools.partial(self._iter_with_tags, obj_type),
                title="Failed to list IDMC mappings",
                message=f"{obj_type} objects will be missing from ingestion output.",
                context=f"/public/core/v3/objects?type={obj_type}",
            )
            for obj in items:
                subtype = self._mapping_subtype_for(obj, default_subtype)
                if not self._accept_mapping_object(obj, obj_type, seen_mapping_ids):
                    continue
                if subtype == "Mapplet":
                    mapplets_from_v3 += 1
                yield from self._emit_mapping(obj, subtype)

        if mapplets_from_v3 == 0:
            # Some pods don't expose mapplets via v3; fall back to the v2 endpoint.
            try:
                fallback_mapplets = list(self.client.list_mapplets_v2())
            except Exception as e:
                self.report.warning(
                    title="v2 mapplet fallback raised an exception",
                    message="Mapplets may be missing from this run.",
                    context="/api/v2/mapplet (and alternatives)",
                    exc=e,
                )
                fallback_mapplets = []
            if fallback_mapplets:
                for obj in fallback_mapplets:
                    if not self._accept_mapping_object(
                        obj, "MAPPLET_V2", seen_mapping_ids
                    ):
                        continue
                    yield from self._emit_mapping(obj, "Mapplet")
            else:
                self.report.info(
                    title="No mapplets found on this IDMC pod",
                    message=(
                        "v3 MAPPLET-family queries returned 0 items and no "
                        "v2 mapplet endpoint responded."
                    ),
                    context="mapplet-ingest",
                )

        mapping_tasks = self._safe_list(
            self.client.list_mapping_tasks,
            title="Failed to list IDMC mapping tasks",
            message="Mapping Tasks will be missing from ingestion output.",
            context="/public/core/v3/objects?type=MTT",
        )
        for mt in mapping_tasks:
            if self._excluded_by_folder_filter(mt.path):
                self.report.report_filtered(
                    "folder-excluded",
                    "MTT",
                    f"name={mt.name!r} path={mt.path}",
                )
                continue
            self.report.mapping_tasks_scanned += 1
            logger.debug(
                "Emitting MappingTask: name=%s path=%s mapping_id=%s",
                mt.name,
                mt.path or "<empty>",
                mt.mapping_id or "<empty>",
            )
            if not mt.path:
                self.report.report_filtered(
                    "mtt-empty-path",
                    "MTT",
                    f"name={mt.name!r} falling back to parent mapping={mt.mapping_id!r}",
                )
            yield self._make_mapping_task(mt)

    def _mapping_subtype_for(self, obj: IdmcObject, default_subtype: str) -> str:
        # Some pods return mapplets under the DTEMPLATE query with
        # ``documentType=MAPPLET``; trust documentType over the query type.
        return "Mapplet" if obj.object_type == "MAPPLET" else default_subtype

    def _accept_mapping_object(
        self, obj: IdmcObject, source_type: str, seen_mapping_ids: Set[str]
    ) -> bool:
        """Apply dedup + folder + pattern filters; record rejections in the report."""
        if obj.id in seen_mapping_ids:
            self.report.report_filtered("dup", source_type, obj.path)
            return False
        if self._excluded_by_folder_filter(obj.path):
            self.report.mappings_filtered += 1
            self.report.report_filtered(
                "folder-excluded",
                source_type,
                f"name={obj.name!r} path={obj.path}",
            )
            return False
        if not self.config.mapping_pattern.allowed(obj.name):
            self.report.mappings_filtered += 1
            self.report.report_filtered(
                "pattern",
                source_type,
                f"name={obj.name!r} path={obj.path}",
            )
            return False
        seen_mapping_ids.add(obj.id)
        return True

    def _emit_mapping(self, obj: IdmcObject, subtype: str) -> Iterable[Entity]:
        """Update internal caches, then yield the DataFlow/DataJob pair."""
        self.report.mappings_scanned += 1
        logger.debug(
            "Emitting %s: name=%s path=%s id=%s",
            subtype,
            obj.name,
            obj.path,
            obj.id,
        )
        self._mapping_ids.append(obj.id)
        v2_mapping = self._v2_mappings_by_guid.get(obj.id)
        if v2_mapping:
            self._v2_id_to_path[v2_mapping.v2_id] = obj.path
        yield from self._emit_mapping_entities(obj, v2_mapping, subtype)

    def _emit_mapping_entities(
        self,
        obj: IdmcObject,
        v2_mapping: Optional[IdmcMapping],
        subtype: str,
    ) -> Iterable[Entity]:
        # Each mapping pairs a DataFlow with an inner ``transform`` DataJob —
        # only DataJobs can carry the ``dataJobInputOutput`` lineage aspect.
        custom_props: Dict[str, str] = {
            "path": obj.path,
            "objectType": obj.object_type or "DTEMPLATE",
            "v3Id": obj.id,
        }
        if v2_mapping:
            custom_props["v2Id"] = v2_mapping.v2_id
            custom_props["valid"] = "true" if v2_mapping.valid else "false"

        flow = DataFlow(
            platform=PLATFORM,
            # GUID flow_id avoids ``.`` or ``/`` which the UI splits into hierarchy.
            name=obj.id,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=obj.name,
            description=obj.description,
            subtype=subtype,
            custom_properties=custom_props,
            owners=self._owner_list(obj.updated_by or obj.created_by),
        )
        parent_key = self._container_parent_key(obj.path)
        if parent_key is not None:
            flow._set_container(parent_key)
        browse_entries = self._browse_path_entries(obj.path)
        if browse_entries:
            flow._set_aspect(BrowsePathsV2Class(path=browse_entries))
        self._mapping_flow_urns[obj.id] = str(flow.urn)
        yield flow

        # display_name prefixes the mapping name so jobs stay distinguishable
        # from every other mapping's constant-named ``transform`` in lineage views.
        job = DataJob(
            name=MAPPING_JOB_ID,
            flow=flow,
            description=obj.description,
            display_name=f"{obj.name}.transform",
            custom_properties=custom_props,
            subtype=f"{subtype} Logic",
            owners=self._owner_list(obj.updated_by or obj.created_by),
        )
        # Override the SDK-built path so the DataFlow entry's ``id`` is the
        # display name — SDK default uses the GUID flow_id, which renders raw.
        job_browse_entries = [
            *browse_entries,
            BrowsePathEntryClass(id=obj.name, urn=str(flow.urn)),
        ]
        job._set_aspect(BrowsePathsV2Class(path=job_browse_entries))
        yield job

    def _make_mapping_task(self, mt: IdmcMappingTask) -> DataFlow:
        # Browse path falls back to the parent mapping's when the task
        # itself has no location field.
        custom_props: Dict[str, str] = {
            k: v
            for k, v in (
                ("objectType", "MTT"),
                ("idmcId", mt.v2_id),
                ("path", mt.path),
                ("v2Id", mt.v2_id),
                ("mappingId", mt.mapping_id),
                ("mappingName", mt.mapping_name),
                ("createdBy", mt.created_by or ""),
                ("updatedBy", mt.updated_by or ""),
                ("createTime", mt.create_time or ""),
                ("updateTime", mt.update_time or ""),
            )
            if v
        }
        flow = DataFlow(
            platform=PLATFORM,
            # v2_id: unique per task, no separators the UI would split.
            name=mt.v2_id or mt.name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=mt.name,
            description=mt.description,
            custom_properties=custom_props,
            owners=self._owner_list(mt.updated_by or mt.created_by),
            subtype="Mapping Task",
        )
        parent_path = mt.path or self._v2_id_to_path.get(mt.mapping_id, "")
        parent_key = self._container_parent_key(parent_path)
        if parent_key is not None:
            flow._set_container(parent_key)
        browse_entries = self._mapping_task_browse_entries(mt)
        if browse_entries:
            flow._set_aspect(BrowsePathsV2Class(path=browse_entries))
        return flow

    def _mapping_task_browse_entries(
        self, mt: IdmcMappingTask
    ) -> List[BrowsePathEntryClass]:
        if mt.path:
            return self._browse_path_entries(mt.path)
        mapping_path = self._v2_id_to_path.get(mt.mapping_id)
        if mapping_path:
            return self._browse_path_entries(mapping_path)
        return []

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
            except (
                InformaticaApiError,
                InformaticaLoginError,
                requests.RequestException,
            ) as e:
                # Per-entry parse/zip failures are reported deeper in the client;
                # only expected IDMC/network errors are warned here so unexpected
                # ones still propagate with a stack trace.
                self.report.warning(
                    title="IDMC export batch failed",
                    message="Lineage for this batch will be missing.",
                    context=f"batch_start={batch_start}, size={len(batch)}",
                    exc=e,
                )
                self.report.export_jobs_failed.append(f"batch@{batch_start}: {e}")

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

        Priority (first hit wins):
          1. ``connection_type_overrides[id]`` — per-connection user override
          2. ``connection_type_platform_map[conn_type]`` — recipe-level map
          3. Built-in ``CONNECTION_TYPE_MAP[conn_type]``
          4. ``connection_type_platform_map[base_type]``
          5. Built-in ``CONNECTION_TYPE_MAP[base_type]``
          6. Substring inference from the connection's display name (for
             IDMC orgs that return ``conn_type=''`` ``base_type='TOOLKIT'``
             on marketplace connectors).

        ``conn_type`` is ``connParams["Connection Type"]`` (specific);
        ``base_type`` is the generic ``type`` field (often ``TOOLKIT``).
        """
        override = self.config.connection_type_overrides.get(connection.id)
        if override:
            return override
        user_map = self.config.connection_type_platform_map
        if connection.conn_type:
            if connection.conn_type in user_map:
                return user_map[connection.conn_type]
            if connection.conn_type in CONNECTION_TYPE_MAP:
                return CONNECTION_TYPE_MAP[connection.conn_type]
        if connection.base_type:
            if connection.base_type in user_map:
                return user_map[connection.base_type]
            if connection.base_type in CONNECTION_TYPE_MAP:
                return CONNECTION_TYPE_MAP[connection.base_type]
        inferred = self._platform_from_connection_name(connection.name)
        if inferred:
            logger.info(
                "Inferred platform=%s for connection id=%s name=%r via "
                "name-substring fallback (conn_type=%r base_type=%r)",
                inferred,
                connection.id,
                connection.name,
                connection.conn_type,
                connection.base_type,
            )
            return inferred
        return None

    @staticmethod
    def _platform_from_connection_name(name: str) -> Optional[str]:
        """Best-effort platform inference from a connection's display name.

        Scans CONNECTION_TYPE_MAP keys for any that appear as a
        case-insensitive substring of the name (with non-alphanumerics
        stripped). Returns the platform for the *longest* matching key so
        more-specific identifiers win over shorter ones.
        """
        if not name:
            return None
        haystack = "".join(c for c in name.lower() if c.isalnum())
        best_needle_len = 0
        best_platform: Optional[str] = None
        for key, platform in CONNECTION_TYPE_MAP.items():
            needle = "".join(c for c in key.lower() if c.isalnum())
            if needle and needle in haystack:
                if len(needle) > best_needle_len:
                    best_needle_len = len(needle)
                    best_platform = platform
        return best_platform

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
        flow_urn = self._mapping_flow_urns.get(lineage_info.mapping_id)
        if not flow_urn:
            self.report.warning(
                title="IDMC lineage references unknown mapping",
                message="Export returned a mapping id we didn't emit; skipping lineage for it.",
                context=f"mapping_id={lineage_info.mapping_id} name={lineage_info.mapping_name}",
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
        job_urn = str(
            DataJobUrn.create_from_ids(
                data_flow_urn=flow_urn,
                job_id=MAPPING_JOB_ID,
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
                (
                    f"Unmapped connection: conn_type={conn.conn_type!r} "
                    f"base_type={conn.base_type!r}. Add "
                    f"connection_type_platform_map: {{{conn.conn_type or conn.base_type!r}: "
                    f"'<platform>'}} to the recipe."
                ),
            )
            return None
        self.report.connections_resolved += 1
        parts: List[str] = []
        if conn.database:
            parts.append(conn.database)
        if table.schema_name:
            parts.append(table.schema_name)
        elif conn.db_schema:
            parts.append(conn.db_schema)
        parts.append(table.table_name)
        return str(
            DatasetUrn.create_from_ids(
                platform_id=platform,
                table_name=".".join(parts),
                env=self.config.env,
            )
        )

    def _iter_with_tags(self, object_type: IdmcObjectType) -> Iterable[IdmcObject]:
        # Centralized tag-filter + bundle-skip + dedup so every entity type
        # gets the same treatment. Drops are recorded to the report.
        seen: set[str] = set()
        for tag in self._tag_filters_or_none(object_type):
            for obj in self.client.list_objects(object_type, tag=tag):
                if obj.id in seen:
                    self.report.report_filtered("dup", object_type, obj.path)
                    continue
                seen.add(obj.id)
                if self._is_bundle_object(obj):
                    self.report.report_filtered("bundle", object_type, obj.path)
                    logger.debug(
                        "Skipping bundle %s: id=%s path=%s",
                        object_type,
                        obj.id,
                        obj.path,
                    )
                    continue
                yield obj

    def _tag_filters_or_none(self, object_type: IdmcObjectType) -> List[Optional[str]]:
        # IDMC v3 rejects ``tag==`` on Project/Folder with HTTP 400;
        # tags only apply to assets like TASKFLOW and DTEMPLATE.
        if object_type in ("Project", "Folder"):
            return [None]
        if self.config.tag_filter_names:
            tags: List[Optional[str]] = list(self.config.tag_filter_names)
            return tags
        return [None]

    @staticmethod
    def _is_project_masquerading_as_folder(obj: IdmcObject) -> bool:
        # IDMC's ``type=Folder`` query sometimes returns project roots
        # (path ``/Explore/<name>``) — emitting those would duplicate the Project.
        return len(InformaticaSource._parse_idmc_path(obj.path)) <= 1

    @staticmethod
    def _is_bundle_object(obj: IdmcObject) -> bool:
        # Add-On Bundles live as a sibling of the user's projects and must be
        # skipped. Match any path segment rather than a fixed prefix since IDMC
        # returns ``/Explore/Add-On Bundles/...``, ``/Add-On Bundles/...``, or
        # the legacy ``Add-On Bundles/...`` form interchangeably.
        if obj.updated_by == "bundle-license-notifier":
            return True
        if obj.name == "Add-On Bundles":
            return True
        return any(seg == "Add-On Bundles" for seg in obj.path.split("/") if seg)

    def _owner_list(
        self, user_identifier: Optional[str]
    ) -> Optional[List[CorpUserUrn]]:
        if not user_identifier or not self.config.extract_ownership:
            return None
        return [CorpUserUrn(user_identifier)]
