import logging
import re
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
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
from pydantic import ValidationError

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
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    TestableSource,
    TestConnectionReport,
)
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
    TaskflowStep,
    V2Id,
    V3Guid,
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
    OwnershipTypeClass,
    StatusClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import CorpUserUrn, DatasetUrn, TagUrn
from datahub.sdk.container import Container
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)

PLATFORM = "informatica"
ORPHAN_PROJECT_SENTINEL = "__root__"
# Shared between extraction and lineage phases so DataJob URNs match.
MAPPING_JOB_ID = "transform"

_UNSAFE_FLOW_ID_CHARS = re.compile(r"[^\w-]+")

T = TypeVar("T")


def _safe_flow_id(name: str, fallback: str) -> str:
    """URN-safe flow_id from a user-facing IDMC name.

    DataHub's Navigate tree renders DataFlow nodes by the URN flow_id
    segment (not ``dataFlowInfo.name``), and visually splits on ``.``
    and ``/`` — so we sanitize those out. Falls back to ``fallback``
    when the result is empty so we never emit an MCP with an empty URN.
    """
    if not name:
        return fallback
    sanitized = _UNSAFE_FLOW_ID_CHARS.sub("_", name).strip("_")
    return sanitized or fallback


@dataclass
class OrchestrateState:
    """Per-orchestrate state accumulated across the lineage phase.

    ``output_datasets`` fills from the last MT's outputs (via the
    ``_orchestrate_by_last_mt`` reverse index) before emission.
    """

    last_mt_urn: str
    mt_urns_in_order: List[str]
    output_datasets: Set[str] = field(default_factory=set)


@dataclass
class TaskflowStepWalkResult:
    step_order: List[str]
    mt_urns_in_order: List[str]
    step_summary_parts: List[str]


class InformaticaProjectKey(ContainerKey):
    project_name: str


class InformaticaFolderKey(InformaticaProjectKey):
    # Inherit from ProjectKey so ContainerKey.parent_key() walks
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
@capability(SourceCapability.TAGS, "IDMC object tags emitted as DataHub GlobalTags")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class InformaticaSource(StatefulIngestionSourceBase, TestableSource):
    """DataHub ingestion source for Informatica Cloud (IDMC).

    Emits IDMC Projects and Folders as Containers; Taskflows and Mapping
    Tasks as DataFlows (each MT with an inner ``transform`` DataJob that
    carries table-level lineage resolved from the referenced Mapping). Plain
    Mappings and Mapplets are intentionally not emitted — only runnable MTs
    surface in DataHub.
    """

    config: InformaticaSourceConfig
    report: InformaticaSourceReport
    platform: str = PLATFORM

    def __init__(self, config: InformaticaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = InformaticaSourceReport()
        self.client = InformaticaClient(config, self.report)
        # Lineage lookup by federated_id; config overrides by id.
        self._connections_by_fed_id: Dict[str, IdmcConnection] = {}
        self._connections_by_id: Dict[str, IdmcConnection] = {}
        # Two indexes into v2 /api/v2/mapping — MT→Mapping refs use v2 ids,
        # export batches use v3 GUIDs.
        self._v2_mappings_by_guid: Dict[V3Guid, IdmcMapping] = {}
        self._v2_mappings_by_v2_id: Dict[V2Id, IdmcMapping] = {}
        self._mapping_ids: List[V3Guid] = []
        # v3 GUID → MT DataJob URNs that run that mapping (fan-out).
        self._mapping_v3_to_mt_job_urns: Dict[V3Guid, List[str]] = {}
        self._mt_v2_id_to_job_urn: Dict[V2Id, str] = {}
        # MT DataJob URN → its step-order predecessors in any Taskflow.
        # Ordered + deduplicated so the same predecessor isn't added twice.
        self._mt_predecessors: Dict[str, List[str]] = {}
        # Orchestrate sits at the end of a Taskflow's chain:
        # ``input_ds → MT1 → … → MTn → orchestrate → out_ds``.
        self._orchestrate_state: Dict[str, OrchestrateState] = {}
        # Reverse index so lineage emission bubbles the last MT's outputs
        # up to any Taskflow's orchestrate.
        self._orchestrate_by_last_mt: Dict[str, List[str]] = {}
        # Containers we've emitted — filtered segments fall back to
        # plain-name browse entries instead of dangling URNs.
        self._emitted_project_names: Set[str] = set()
        self._emitted_folder_keys: Set[Tuple[str, str]] = set()
        # External datasets (Snowflake, Oracle, etc.) referenced by
        # lineage need a minimal Status aspect to register as existing
        # entities; otherwise ``searchAcrossLineage`` filters them out.
        self._stubbed_external_dataset_urns: Set[str] = set()

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "InformaticaSource":
        config = InformaticaSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Validate login credentials against IDMC before a full ingestion run."""
        try:
            config = InformaticaSourceConfig.model_validate(config_dict)
        except (ValidationError, ValueError, TypeError) as e:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Failed to parse config: {e}",
            )
        client = InformaticaClient(config, InformaticaSourceReport())
        try:
            client.login()
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(capable=True)
            )
        except InformaticaLoginError as e:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False,
                    failure_reason=str(e),
                )
            )
        except Exception as e:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Unexpected error during login: {e}",
            )
        finally:
            client.close()

    def get_report(self) -> InformaticaSourceReport:
        return self.report

    def close(self) -> None:
        self.client.close()
        super().close()

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
        # MTs emit before Taskflows so step resolution sees real MT URNs.
        yield from self._extract_mappings_and_tasks()
        yield from self._extract_taskflows()
        self._summarize_taskflow_steps()
        if self.config.extract_lineage:
            yield from self._extract_lineage()

    def _summarize_taskflow_steps(self) -> None:
        """Surface step-DAG coverage as a report warning when incomplete
        so missing ``Asset - export`` privilege is actionable from the
        ingestion report JSON without scanning logs.
        """
        total = self.report.taskflows_scanned
        with_steps = self.report.taskflows_with_steps
        if total == 0 or with_steps == total:
            return
        missing = total - with_steps
        self.report.warning(
            title="Taskflow step DAG missing for some Taskflows",
            message=(
                f"Resolved {with_steps}/{total} Taskflow step DAGs; "
                f"{missing} Taskflow(s) emitted as DataFlow-only. "
                "Common cause: service account lacks 'Asset - export' "
                "privilege. Grant it to enable MT chaining via inputDatajobs."
            ),
            context=f"taskflows_scanned={total}, taskflows_with_steps={with_steps}",
        )

    def _extract_containers(self) -> Iterable[Entity]:
        # Projects emit before Folders so folder parent_container edges
        # can check ``_emitted_project_names`` and skip orphans.
        yield from self._emit_projects()
        yield from self._emit_folders()

    def _emit_projects(self) -> Iterable[Entity]:
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

    def _emit_folders(self) -> Iterable[Entity]:
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
                parent_project = self._resolve_parent_project(folder)
                if parent_project and parent_project not in self._emitted_project_names:
                    # Parent was filtered out upstream; skipping keeps the UI
                    # tree clean instead of surfacing orphan folders.
                    self.report.folders_filtered += 1
                    self.report.report_filtered(
                        "parent-project-filtered",
                        "Folder",
                        f"name={folder.name!r} path={folder.path}",
                    )
                    continue
                self.report.folders_scanned += 1
                parent_project = parent_project or ORPHAN_PROJECT_SENTINEL
                self._emitted_folder_keys.add((parent_project, folder.name))
                logger.debug(
                    "Emitting Folder container: name=%s path=%s parent_project=%s",
                    folder.name,
                    folder.path,
                    parent_project,
                )
                yield self._make_folder_container(folder)
            except (
                InformaticaApiError,
                InformaticaLoginError,
                requests.RequestException,
                ValueError,
                KeyError,
            ) as e:
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
        """Wrap an IDMC listing call, converting enumeration failures to
        report warnings. Takes a zero-arg factory so call-time failures
        (non-generator clients that HTTP-fail on invocation) are caught
        the same as iteration-time failures.
        """
        try:
            yield from iterable_factory()
        except (
            InformaticaApiError,
            InformaticaLoginError,
            requests.RequestException,
            ValueError,
            KeyError,
        ) as e:
            self.report.warning(title=title, message=message, context=context, exc=e)

    def _make_project_container(self, project: IdmcObject) -> Container:
        return Container(
            container_key=self._project_key(project.name),
            display_name=project.name,
            description=project.description,
            subtype="Project",
            owners=self._owner_list(project.created_by, project.updated_by),
            tags=self._tag_list(project.tags),
        )

    def _make_folder_container(self, folder: IdmcObject) -> Container:
        parent_project_name = self._resolve_parent_project(folder)
        # Only reference the project when it was actually emitted; otherwise
        # the folder would dangle with parent_container → a ghost URN.
        parent_key: Optional[ContainerKey] = (
            self._project_key(parent_project_name)
            if parent_project_name
            and parent_project_name in self._emitted_project_names
            else None
        )
        key = self._folder_key(
            parent_project_name or ORPHAN_PROJECT_SENTINEL, folder.name
        )
        return Container(
            container_key=key,
            display_name=folder.name,
            description=folder.description,
            subtype="Folder",
            owners=self._owner_list(folder.created_by, folder.updated_by),
            parent_container=parent_key,
            tags=self._tag_list(folder.tags),
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
        # Two-pass: collect filtered Taskflows, batch-prefetch their step
        # definitions in one v3 Export job, then emit using the warmed cache.
        accepted: List[IdmcObject] = []
        taskflows = self._safe_list(
            lambda: self._iter_with_tags("TASKFLOW"),
            title="Failed to list IDMC taskflows",
            message="Taskflow DataFlow entities will be missing.",
            context="/public/core/v3/objects?type=TASKFLOW",
        )
        for tf in taskflows:
            if self._excluded_by_folder_filter(tf.path):
                self.report.taskflows_filtered += 1
                self.report.report_filtered(
                    "folder-excluded",
                    "TASKFLOW",
                    f"name={tf.name!r} path={tf.path}",
                )
                continue
            if not self.config.taskflow_pattern.allowed(
                f"{tf.path}/{tf.name}" if tf.path else tf.name
            ):
                self.report.taskflows_filtered += 1
                self.report.report_filtered(
                    "pattern",
                    "TASKFLOW",
                    f"name={tf.name!r} path={tf.path}",
                )
                continue
            accepted.append(tf)

        if accepted:
            guids = [tf.id for tf in accepted if tf.id]
            self.client.prefetch_taskflow_definitions(guids)

        for tf in accepted:
            try:
                self.report.taskflows_scanned += 1
                logger.debug(
                    "Emitting Taskflow: name=%s path=%s id=%s",
                    tf.name,
                    tf.path,
                    tf.id,
                )
                flow = self._make_taskflow(tf)
                yield flow
                yield from self._emit_taskflow_steps(tf, flow)
            except (
                InformaticaApiError,
                InformaticaLoginError,
                requests.RequestException,
                ValueError,
                KeyError,
            ) as e:
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
            name=_safe_flow_id(tf.name, fallback=tf.id or display),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=display,
            description=tf.description,
            custom_properties=self._taskflow_custom_props(tf),
            owners=self._owner_list(tf.created_by, tf.updated_by),
            subtype="Taskflow",
            tags=self._tag_list(tf.tags),
        )
        self._attach_container_and_browse_path(flow, tf.path)
        return flow

    def _attach_container_and_browse_path(
        self, flow: DataFlow, path: str
    ) -> List[BrowsePathEntryClass]:
        """Wire a DataFlow to its parent Container + BrowsePathsV2.

        Returns the entries so callers that build deeper paths (e.g. MT
        DataJob appends the MT name) can extend them.
        """
        parent_key = self._container_parent_key(path)
        if parent_key is not None:
            flow._set_container(parent_key)
        browse_entries = self._browse_path_entries(path) if path else []
        if browse_entries:
            flow._set_aspect(BrowsePathsV2Class(path=browse_entries))
        return browse_entries

    @staticmethod
    def _audit_props(obj: IdmcObject, **extra: str) -> Dict[str, str]:
        """Common ``customProperties`` (v3 id, type, path, audit fields)
        shared across Taskflow / MT emission. Empty strings are stripped.
        """
        props: Dict[str, str] = {
            "v3Id": obj.id,
            "objectType": obj.object_type or "",
            "path": obj.path,
            "createdBy": obj.created_by or "",
            "updatedBy": obj.updated_by or "",
            "createTime": obj.create_time.isoformat() if obj.create_time else "",
            "updateTime": obj.update_time.isoformat() if obj.update_time else "",
        }
        props.update(extra)
        return {k: v for k, v in props.items() if v}

    @staticmethod
    def _taskflow_custom_props(tf: IdmcObject) -> Dict[str, str]:
        return InformaticaSource._audit_props(
            tf, objectType=tf.object_type or "TASKFLOW"
        )

    @staticmethod
    def _mt_custom_props(
        mt: IdmcMappingTask, mapping_name: str, mapping_v3_guid: str
    ) -> Dict[str, str]:
        # Historical quirk: the ``v3Id`` key carries the v2 id (MTT v3
        # GUIDs contain separators the UI would split). Kept for back-compat
        # with previously-emitted entities.
        return {
            k: v
            for k, v in {
                "v3Id": mt.v2_id,
                "objectType": "MTT",
                "path": mt.path,
                "mappingId": mt.mapping_id or "",
                "mappingName": mapping_name,
                "mappingV3Id": mapping_v3_guid,
                "createdBy": mt.created_by or "",
                "updatedBy": mt.updated_by or "",
                "createTime": mt.create_time.isoformat() if mt.create_time else "",
                "updateTime": mt.update_time.isoformat() if mt.update_time else "",
            }.items()
            if v
        }

    def _emit_taskflow_steps(self, tf: IdmcObject, flow: DataFlow) -> Iterable[Entity]:
        """Emit a chained-MT orchestration graph for a Taskflow.

        The resulting Taskflow lineage reads as
        ``input_ds → MT1 → … → MTn → orchestrate → out_ds``. Step-order
        edges between MTs go into ``_mt_predecessors`` (merged into each
        MT's ``inputDatajobs`` during the lineage phase); a single
        ``orchestrate`` DataJob is emitted per Taskflow at the end.

        Non-data steps (command / decision / …) are summarized in
        ``customProperties`` but don't participate in the chain.
        Silently no-ops when the definition isn't available.
        """
        definition = self.client.get_taskflow_definition(
            taskflow_name=tf.name, taskflow_v3_guid=tf.id
        )
        if definition is None or not definition.steps:
            return
        walk = self._walk_taskflow_steps(definition.steps)
        if not walk.mt_urns_in_order:
            return
        self.report.taskflows_with_steps += 1
        yield self._emit_orchestrate_datajob(tf, flow, walk)

    def _walk_taskflow_steps(self, steps: List[TaskflowStep]) -> TaskflowStepWalkResult:
        """Resolve step task refs and register predecessor edges in
        ``_mt_predecessors``. Pure — no DataJob emission.
        """
        step_order: List[str] = []
        step_summary_parts: List[str] = []
        mt_urns_in_order: List[str] = []
        prev_mt_urn: Optional[str] = None
        for step in steps:
            step_order.append(step.step_id)
            mt_urn = self._resolve_task_ref(step)
            if mt_urn is None:
                if step.step_type != "data":
                    step_summary_parts.append(f"{step.step_name} ({step.step_type})")
                continue
            if mt_urn not in mt_urns_in_order:
                mt_urns_in_order.append(mt_urn)
            step_summary_parts.append(
                f"{step.step_name} → {step.task_ref_name or step.task_ref_id}"
            )
            # Register prev_mt → mt_urn. Dedup so an MT appearing in two
            # Taskflows with identical ordering doesn't get duplicate edges.
            if prev_mt_urn and prev_mt_urn != mt_urn:
                predecessors = self._mt_predecessors.setdefault(mt_urn, [])
                if prev_mt_urn not in predecessors:
                    predecessors.append(prev_mt_urn)
            prev_mt_urn = mt_urn
        return TaskflowStepWalkResult(
            step_order=step_order,
            mt_urns_in_order=mt_urns_in_order,
            step_summary_parts=step_summary_parts,
        )

    def _emit_orchestrate_datajob(
        self, tf: IdmcObject, flow: DataFlow, walk: TaskflowStepWalkResult
    ) -> DataJob:
        """Emit the orchestrate DataJob and register its URN for the
        lineage phase to bubble last-MT outputs onto.
        """
        last_mt_urn = walk.mt_urns_in_order[-1]
        custom_props: Dict[str, str] = {
            "mappingTaskCount": str(len(walk.mt_urns_in_order)),
            "stepOrder": " → ".join(walk.step_order),
        }
        if walk.step_summary_parts:
            custom_props["stepSummary"] = " | ".join(walk.step_summary_parts)
        job = DataJob(
            name="orchestrate",
            flow=flow,
            display_name=f"{tf.name}.orchestrate",
            subtype="Taskflow Orchestration",
            description=(
                f"Runs {len(walk.mt_urns_in_order)} Mapping Task(s) in order: "
                f"{' → '.join(walk.step_summary_parts)}"
                if walk.step_summary_parts
                else None
            ),
            custom_properties=custom_props,
        )
        # inputDatajobs = [last MT]; outputDatasets is filled during the
        # lineage phase so Taskflow Lineage ends with ``… → orch → out_ds``.
        job._set_aspect(
            DataJobInputOutputClass(
                inputDatasets=[],
                outputDatasets=[],
                inputDatajobs=[last_mt_urn],
            )
        )
        orchestrate_urn = str(job.urn)
        self._orchestrate_state[orchestrate_urn] = OrchestrateState(
            last_mt_urn=last_mt_urn,
            mt_urns_in_order=list(walk.mt_urns_in_order),
        )
        self._orchestrate_by_last_mt.setdefault(last_mt_urn, []).append(orchestrate_urn)
        return job

    def _resolve_task_ref(self, step: TaskflowStep) -> Optional[str]:
        """Resolve a step task reference to its MT's DataJob URN.

        Prefers v2 id over name. ``None`` if the step references an MT
        that wasn't emitted (e.g. filtered by pattern).
        """
        if step.task_ref_id:
            return self._mt_v2_id_to_job_urn.get(V2Id(step.task_ref_id))
        return None

    def _extract_mappings_and_tasks(self) -> Iterable[Entity]:
        """MT-centric emission.

        Only Mapping Tasks are emitted as first-class entities; the v2
        mapping cache is still loaded so ``mt.mapping_id`` → v3 GUID
        translation works for the lineage export batch. Mappings without
        an MT and Mapplets are never emitted.
        """
        # Cache v2 mappings by both v3 GUID and v2 id — lineage export
        # keys by GUID, the /mttask enrichment keys by v2 id.
        try:
            for m in self.client.list_mappings():
                if m.asset_frs_guid:
                    self._v2_mappings_by_guid[m.asset_frs_guid] = m
                if m.v2_id:
                    self._v2_mappings_by_v2_id[m.v2_id] = m
        except (
            InformaticaApiError,
            InformaticaLoginError,
            requests.RequestException,
        ) as e:
            self.report.warning(
                title="Failed to list IDMC v2 mappings",
                message=(
                    "Mapping Tasks may be missing mapping references and "
                    "lineage may not resolve to the right mapping."
                ),
                context="/api/v2/mapping",
                exc=e,
            )

        mapping_tasks = self._safe_list(
            self.client.list_mapping_tasks,
            title="Failed to list IDMC mapping tasks",
            message="Mapping Tasks will be missing from ingestion output.",
            context="/public/core/v3/objects?type=MTT",
        )
        try:
            for mt in mapping_tasks:
                if self._excluded_by_folder_filter(mt.path):
                    self.report.report_filtered(
                        "folder-excluded",
                        "MTT",
                        f"name={mt.name!r} path={mt.path}",
                    )
                    continue
                if not self.config.mapping_task_pattern.allowed(
                    f"{mt.path}/{mt.name}" if mt.path else mt.name
                ):
                    self.report.mapping_tasks_filtered += 1
                    self.report.report_filtered(
                        "pattern",
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
                        f"name={mt.name!r} mapping={mt.mapping_id!r}",
                    )
                yield from self._emit_mapping_task(mt)
        finally:
            self._v2_mappings_by_guid.clear()
            self._v2_mappings_by_v2_id.clear()

    def _emit_mapping_task(self, mt: IdmcMappingTask) -> Iterable[Entity]:
        """Emit a Mapping Task as DataFlow + inner ``transform`` DataJob.

        The inner DataJob is the lineage attachment point — each MT
        referencing a given Mapping receives the same tables (fan-out).
        """
        # v2 mapping_id → v3 GUID so the lineage phase can match export
        # output back to this MT's DataJob.
        v2_mapping = (
            self._v2_mappings_by_v2_id.get(mt.mapping_id) if mt.mapping_id else None
        )
        mapping_v3_guid: V3Guid = (
            v2_mapping.asset_frs_guid if v2_mapping else V3Guid("")
        )
        mapping_name = mt.mapping_name or (v2_mapping.name if v2_mapping else "")
        custom_props = self._mt_custom_props(mt, mapping_name, mapping_v3_guid)
        task_tags = self._tag_list(mt.tags)
        if not mt.v2_id:
            logger.warning(
                "Mapping Task %r has no v2_id; falling back to name for URN, "
                "which may collide with same-named MTs in other folders.",
                mt.name,
            )
        flow = DataFlow(
            platform=PLATFORM,
            name=_safe_flow_id(
                mt.v2_id,
                fallback=f"{mt.path}/{mt.name}" if mt.path else mt.name,
            ),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=mt.name,
            description=mt.description,
            custom_properties=custom_props,
            owners=self._owner_list(mt.created_by, mt.updated_by),
            subtype="Mapping Task",
            tags=task_tags,
        )
        browse_entries = self._attach_container_and_browse_path(flow, mt.path)
        yield flow
        yield self._make_mt_transform_datajob(
            mt, flow, custom_props, task_tags, browse_entries, mapping_v3_guid
        )

    def _make_mt_transform_datajob(
        self,
        mt: IdmcMappingTask,
        flow: DataFlow,
        custom_props: Dict[str, str],
        task_tags: Optional[List[TagUrn]],
        browse_entries: List[BrowsePathEntryClass],
        mapping_v3_guid: V3Guid,
    ) -> DataJob:
        """Build the MT's inner ``transform`` DataJob and register its
        URN in the indexes the lineage + Taskflow-step phases consume.
        """
        job = DataJob(
            name=MAPPING_JOB_ID,
            flow=flow,
            description=mt.description,
            display_name=f"{mt.name}.transform",
            custom_properties=custom_props,
            subtype="Task Logic",
            owners=self._owner_list(mt.created_by, mt.updated_by),
            tags=task_tags,
        )
        job._set_aspect(
            BrowsePathsV2Class(
                path=[
                    *browse_entries,
                    BrowsePathEntryClass(id=mt.name, urn=str(flow.urn)),
                ]
            )
        )
        job_urn = str(job.urn)
        if mapping_v3_guid:
            self._mapping_v3_to_mt_job_urns.setdefault(mapping_v3_guid, []).append(
                job_urn
            )
            if mapping_v3_guid not in self._mapping_ids:
                self._mapping_ids.append(mapping_v3_guid)
        if mt.v2_id:
            self._mt_v2_id_to_job_urn[mt.v2_id] = job_urn
        return job

    def _extract_lineage(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        self._load_connections()
        if not self._connections_by_id:
            # ``_load_connections`` already reported the failure; don't
            # waste time polling export jobs we can't resolve to URNs.
            logger.warning("No connections loaded; skipping lineage extraction.")
            return
        if not self._mapping_ids:
            logger.info("No mappings to extract lineage for.")
            return
        batch_size = self.config.export_batch_size
        batches = [
            (self._mapping_ids[i : i + batch_size], i)
            for i in range(0, len(self._mapping_ids), batch_size)
        ]
        # submit+wait runs concurrently; download+parse+emit stays on main thread.
        future_to_batch: Dict[Future[str], Tuple[List[V3Guid], int]] = {}
        with ThreadPoolExecutor(
            max_workers=self.config.max_concurrent_export_jobs
        ) as executor:
            for batch, batch_start in batches:
                future_to_batch[
                    executor.submit(self._submit_and_wait_export, batch, batch_start)
                ] = (batch, batch_start)
            for future in as_completed(future_to_batch):
                batch, batch_start = future_to_batch[future]
                try:
                    job_id = future.result()
                except (
                    InformaticaApiError,
                    InformaticaLoginError,
                    requests.RequestException,
                ) as e:
                    self.report.warning(
                        title="IDMC export batch failed",
                        message="Lineage for this batch will be missing.",
                        context=f"batch_start={batch_start}, size={len(batch)}",
                        exc=e,
                    )
                    self.report.export_jobs_failed.append(f"batch@{batch_start}: {e}")
                    continue
                if job_id:
                    for lineage_info in self.client.download_and_parse_export(
                        job_id, batch
                    ):
                        yield from self._emit_lineage(lineage_info)
        # Final pass: bubble each last-MT's outputs up to its Taskflow's
        # orchestrate so the Taskflow Lineage view has the full pipeline.
        yield from self._emit_orchestrate_aggregated_lineage()

    def _submit_and_wait_export(self, batch: List[V3Guid], batch_start: int) -> str:
        """Submit one export batch and block until complete. Returns job_id on
        success, empty string on non-success (warning already recorded)."""
        job_id = self.client.submit_export_job(batch)
        status = self.client.wait_for_export(job_id)
        if status.state != ExportJobState.SUCCESSFUL:
            if status.state != ExportJobState.TIMEOUT:
                self.report.warning(
                    title="IDMC export job did not succeed",
                    message="Lineage for this batch will be missing.",
                    context=(
                        f"job_id={job_id}, state={status.state.value}, "
                        f"batch_start={batch_start}, "
                        f"message={status.message or '<none>'}"
                    ),
                )
            return ""
        return job_id

    def _emit_orchestrate_aggregated_lineage(self) -> Iterable[MetadataWorkUnit]:
        """Emit each orchestrate's final ``DataJobInputOutput`` with the
        last MT's outputs mirrored.
        """
        for orch_urn, state in self._orchestrate_state.items():
            outputs = state.output_datasets
            if not outputs:
                # No resolved outputs on the last MT — keep the base
                # emission with inputDatajobs=[last_mt] and no edges.
                continue
            yield MetadataChangeProposalWrapper(
                entityUrn=orch_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=[],
                    outputDatasets=sorted(outputs),
                    inputDatajobs=[state.last_mt_urn],
                ),
            ).as_workunit()

    def _load_connections(self) -> None:
        """Load all connections into cache, keyed by federatedId and id (separate dicts)."""
        if self._connections_by_id or self._connections_by_fed_id:
            return
        try:
            for conn in self.client.list_connections():
                if conn.federated_id:
                    self._connections_by_fed_id[conn.federated_id] = conn
                self._connections_by_id[conn.id] = conn
        except (
            InformaticaApiError,
            InformaticaLoginError,
            requests.RequestException,
        ) as e:
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
        # First-hit-wins lookup chain; user map wins over built-in, and
        # conn_type (specific) wins over base_type (generic ``TOOLKIT``).
        lookups = (
            (connection.conn_type, user_map),
            (connection.conn_type, CONNECTION_TYPE_MAP),
            (connection.base_type, user_map),
            (connection.base_type, CONNECTION_TYPE_MAP),
        )
        for key, mapping in lookups:
            if key and key in mapping:
                return mapping[key]
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
        """Fan out ``dataJobInputOutput`` + dataset-level ``upstreamLineage``.

        One Mapping can be referenced by N Mapping Tasks, so a single export
        result is applied to every MT's ``transform`` DataJob that runs it.
        Dataset-level ``upstreamLineage`` on downstream tables is emitted once
        (the upstream set is the same regardless of which MT triggered it).
        """
        if not lineage_info.mapping_id:
            self.report.warning(
                title="IDMC lineage missing mapping id",
                message="Could not align mapping export entry to a submitted v3 GUID; skipping lineage emission.",
                context=f"mapping_name={lineage_info.mapping_name}",
            )
            return
        mt_job_urns = self._mapping_v3_to_mt_job_urns.get(lineage_info.mapping_id)
        if not mt_job_urns:
            self.report.warning(
                title="IDMC lineage references a mapping with no Mapping Tasks",
                message="No Mapping Task was emitted that runs this mapping; lineage will be skipped.",
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
        yield from self._stub_external_datasets(input_urns, output_urns)
        for job_urn in mt_job_urns:
            yield from self._emit_mt_datajob_io(job_urn, input_urns, output_urns)
        yield from self._emit_downstream_upstream_lineage(input_urns, output_urns)

    def _stub_external_datasets(
        self, input_urns: List[str], output_urns: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        """Emit a minimal ``Status`` aspect per unseen external dataset URN.

        Without this stub, DataHub treats URNs that no other connector has
        ingested as non-existent and ``searchAcrossLineage`` filters them
        out of results. ``UpstreamLineage`` on a downstream creates a key
        for *output* URNs implicitly, but inputs are only referenced
        inside aspects and would otherwise remain ``exists: false``.

        Dedup'd per-source-run via ``_stubbed_external_dataset_urns``.
        """
        for ds_urn in (*input_urns, *output_urns):
            if ds_urn in self._stubbed_external_dataset_urns:
                continue
            self._stubbed_external_dataset_urns.add(ds_urn)
            yield MetadataChangeProposalWrapper(
                entityUrn=ds_urn,
                aspect=StatusClass(removed=False),
            ).as_workunit()

    def _emit_mt_datajob_io(
        self,
        job_urn: str,
        input_urns: List[str],
        output_urns: List[str],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit one MT ``transform`` DataJob's ``DataJobInputOutput`` aspect
        and, when this MT is the last step of any Taskflow, bubble its
        outputs up to the orchestrate's accumulator.
        """
        predecessor_mt_urns = self._mt_predecessors.get(job_urn, [])
        yield MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInputOutputClass(
                inputDatasets=input_urns,
                outputDatasets=output_urns,
                inputDatajobs=predecessor_mt_urns,
            ),
        ).as_workunit()
        self.report.lineage_edges_emitted += len(input_urns) + len(output_urns)
        # When this MT is the last step of any Taskflow, push its outputs
        # onto the orchestrate's accumulated ``outputDatasets``.
        for orch_urn in self._orchestrate_by_last_mt.get(job_urn, []):
            state = self._orchestrate_state.get(orch_urn)
            if state is not None:
                state.output_datasets.update(output_urns)

    def _emit_downstream_upstream_lineage(
        self, input_urns: List[str], output_urns: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        """Emit ``UpstreamLineage`` on each target dataset.

        Emitted alongside the MT's ``DataJobInputOutput`` (not instead of)
        because DataHub's UI lineage traversal has asymmetric edge-type
        filters: a Dataset→Dataset Lineage tab doesn't reliably traverse
        ``Consumes``/``Produces`` edges through an intermediate DataJob.
        Keeping the Dataset-to-Dataset ``UpstreamLineage`` aspect covers
        that path explicitly; the DataJob aspect covers the MT's own
        Lineage tab. The two together render correctly in every view we
        tested.
        """
        if not input_urns:
            return
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
        if not table.table_name:
            # Malformed IDMC transformations can yield empty table names; emitting
            # a URN like ``snowflake,DWH.SALES.,PROD`` creates a dangling edge.
            self.report.report_connection_unresolved(
                table.connection_federated_id,
                "unknown",
                "Empty table name in IDMC transformation",
            )
            return None
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
        qualifier = ".".join(parts)
        if self.config.convert_urns_to_lowercase:
            qualifier = qualifier.lower()
        platform_instance = self.config.connection_to_platform_instance.get(conn.id)
        return str(
            DatasetUrn.create_from_ids(
                platform_id=platform,
                table_name=qualifier,
                env=self.config.env,
                platform_instance=platform_instance,
            )
        )

    def _iter_with_tags(self, object_type: IdmcObjectType) -> Iterable[IdmcObject]:
        # Single pass per type; tag matching is client-side. Passing each tag
        # separately would cause K × N API calls since IDMC ignores server-side
        # tag filters for non-Project/Folder types.
        tag_set = (
            set(self.config.tag_filter_names)
            if self.config.tag_filter_names and object_type not in ("Project", "Folder")
            else None
        )
        for obj in self.client.list_objects(object_type):
            if self._is_bundle_object(obj):
                self.report.report_filtered("bundle", object_type, obj.path)
                logger.debug(
                    "Skipping bundle %s: id=%s path=%s",
                    object_type,
                    obj.id,
                    obj.path,
                )
                continue
            if tag_set and not any(t in obj.tags for t in tag_set):
                self.report.report_filtered("tag", object_type, obj.path)
                continue
            yield obj

    @staticmethod
    def _is_project_masquerading_as_folder(obj: IdmcObject) -> bool:
        # IDMC's ``type=Folder`` query sometimes returns project roots
        # (path ``/Explore/<name>``) — emitting those would duplicate the Project.
        return len(InformaticaSource._parse_idmc_path(obj.path)) <= 1

    @staticmethod
    def _is_bundle_object(obj: IdmcObject) -> bool:
        return obj.is_bundle()

    def _owner_list(
        self,
        created_by: Optional[str] = None,
        updated_by: Optional[str] = None,
    ) -> Optional[List[Tuple[CorpUserUrn, str]]]:
        # Creator → DATAOWNER, last updater → TECHNICAL_OWNER; dedup
        # when they're the same principal.
        if not self.config.extract_ownership:
            return None
        created_urn = self._user_urn(created_by)
        updated_urn = self._user_urn(updated_by)
        owners: List[Tuple[CorpUserUrn, str]] = []
        if created_urn:
            owners.append((created_urn, OwnershipTypeClass.DATAOWNER))
        if updated_urn and updated_urn != created_urn:
            owners.append((updated_urn, OwnershipTypeClass.TECHNICAL_OWNER))
        return owners or None

    def _user_urn(self, identifier: Optional[str]) -> Optional[CorpUserUrn]:
        if not identifier:
            return None
        username = identifier
        if self.config.strip_user_email_domain and "@" in username:
            username = username.split("@", 1)[0]
        return CorpUserUrn(username)

    def _tag_list(self, tags: List[str]) -> Optional[List[TagUrn]]:
        if not tags or not self.config.extract_tags:
            return None
        return [TagUrn(name=t) for t in tags]
