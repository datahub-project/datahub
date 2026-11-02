import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

import requests
from defusedxml.common import DefusedXmlException
from pydantic import ValidationError

from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.boomi.client import BoomiApiError, BoomiClient
from datahub.ingestion.source.boomi.component_parser import parse_process_component
from datahub.ingestion.source.boomi.config import BoomiSourceConfig
from datahub.ingestion.source.boomi.constants import (
    API_HOST_TO_UI_HOST,
    DEPLOYMENT_FORMAT,
    DEPLOYMENT_SEPARATOR,
    HTTP_FORBIDDEN,
    PLATFORM,
    PROCESS_JOB_ID,
    PROCESS_URL_TEMPLATE,
    PROP_BRANCH_NAME,
    PROP_COMPONENT_ID,
    PROP_DEPLOYMENTS,
    PROP_FOLDER_PATH,
    PROP_MODIFIED_DATE,
    PROP_VERSION,
)
from datahub.ingestion.source.boomi.models import (
    ComponentMetadata,
    Folder,
    ProcessComponentInfo,
)
from datahub.ingestion.source.boomi.report import BoomiSourceReport
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
    DatasetContainerSubTypes,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import OwnershipTypeClass
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk.container import Container
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity
from datahub.utilities.sentinels import unset

logger = logging.getLogger(__name__)

STAGE_FOLDERS = "Folders"
STAGE_DEPLOYMENTS = "Deployments"
STAGE_PROCESSES = "Processes"

_EPOCH = datetime.min.replace(tzinfo=timezone.utc)
# AllowDenyPattern's default allow entry.
ALLOW_ALL_REGEX = ".*"


class BoomiFolderKey(ContainerKey):
    folder_id: str


@dataclass
class FolderIndex:
    folders: Dict[str, Folder] = field(default_factory=dict)
    allowed_ids: Set[str] = field(default_factory=set)
    # Emitted containers are kept so children inherit their full browse path.
    emitted: Dict[str, Container] = field(default_factory=dict)


def ui_host_for_api_url(api_url: str) -> Optional[str]:
    host = urlparse(api_url).hostname
    return API_HOST_TO_UI_HOST.get(host) if host else None


def _as_utc(value: Optional[datetime]) -> datetime:
    if value is None:
        return _EPOCH
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def dedupe_branch_copies(
    components: Iterable[ComponentMetadata],
) -> Tuple[List[ComponentMetadata], int]:
    # A branch copy shares its componentId with the original, so keep the most
    # recently modified copy of each component.
    latest: Dict[str, ComponentMetadata] = {}
    duplicates = 0
    for component in components:
        existing = latest.get(component.component_id)
        if existing is None:
            latest[component.component_id] = component
            continue
        duplicates += 1
        if (_as_utc(component.modified_date), component.version) > (
            _as_utc(existing.modified_date),
            existing.version,
        ):
            latest[component.component_id] = component
    return list(latest.values()), duplicates


@platform_name("Boomi")
@config_class(BoomiSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Folders as containers")
@capability(SourceCapability.DELETION_DETECTION, "Via stateful ingestion")
@capability(SourceCapability.OWNERSHIP, "From process createdBy and modifiedBy")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class BoomiSource(StatefulIngestionSourceBase, TestableSource):
    config: BoomiSourceConfig
    report: BoomiSourceReport
    platform: str = PLATFORM

    def __init__(self, config: BoomiSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = BoomiSourceReport()
        self.client = BoomiClient(config, self.report)
        self._ui_host = ui_host_for_api_url(config.api_url)

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "BoomiSource":
        config = BoomiSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        try:
            config = BoomiSourceConfig.model_validate(config_dict)
        except (ValidationError, ValueError) as e:
            return TestConnectionReport(
                internal_failure=True,
                internal_failure_reason=f"Failed to parse config: {e}",
            )
        client = BoomiClient(config, BoomiSourceReport())
        try:
            client.check_connectivity()
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(capable=True)
            )
        except (BoomiApiError, requests.RequestException) as e:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False, failure_reason=str(e)
                )
            )
        finally:
            client.close()

    def get_report(self) -> BoomiSourceReport:
        return self.report

    def close(self) -> None:
        self.client.close()
        super().close()

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        with self.report.new_stage(STAGE_FOLDERS):
            index = self._load_folders()
            yield from self._emit_folders(index)
        with self.report.new_stage(STAGE_DEPLOYMENTS):
            deployments = self._load_deployments()
        with self.report.new_stage(STAGE_PROCESSES):
            yield from self._emit_processes(index, deployments)

    def _load_folders(self) -> FolderIndex:
        index = FolderIndex()
        try:
            for folder in self.client.list_folders():
                self.report.folders_scanned += 1
                if folder.deleted:
                    continue
                index.folders[folder.id] = folder
                if self.config.folder_pattern.allowed(folder.full_path):
                    index.allowed_ids.add(folder.id)
                else:
                    self.report.folders_filtered.append(folder.full_path)
        except (BoomiApiError, requests.RequestException, ValidationError) as e:
            # A failure, not a warning, so stale entity removal does not
            # soft-delete the folders that were not emitted this run.
            self.report.failure(
                title="Failed to list Boomi folders",
                message="Processes are emitted without folder containers.",
                exc=e,
            )
        return index

    def _emit_folders(self, index: FolderIndex) -> Iterable[Entity]:
        # Ancestors of an allowed folder are emitted too, so the browse path
        # stays intact when the pattern only allows a subtree.
        for folder_id in sorted(index.allowed_ids):
            yield from self._emit_folder_chain(folder_id, index, set())

    def _emit_folder_chain(
        self, folder_id: str, index: FolderIndex, visiting: Set[str]
    ) -> Iterable[Entity]:
        folder = index.folders.get(folder_id)
        if folder is None or folder_id in index.emitted or folder_id in visiting:
            return
        visiting.add(folder_id)
        if folder.parent_id:
            yield from self._emit_folder_chain(folder.parent_id, index, visiting)
        container = Container(
            self._folder_key(folder_id),
            display_name=folder.name,
            qualified_name=folder.full_path,
            subtype=DatasetContainerSubTypes.BOOMI_FOLDER,
            parent_container=self._parent_container(folder.parent_id, index),
        )
        index.emitted[folder_id] = container
        self.report.folders_emitted += 1
        yield container

    @staticmethod
    def _parent_container(
        folder_id: Optional[str], index: FolderIndex
    ) -> Optional[Container]:
        return index.emitted.get(folder_id) if folder_id else None

    def _folder_key(self, folder_id: str) -> BoomiFolderKey:
        return BoomiFolderKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            folder_id=folder_id,
        )

    def _load_environment_names(self) -> Optional[Dict[str, str]]:
        names: Dict[str, str] = {}
        try:
            for environment in self.client.list_environments():
                self.report.environments_scanned += 1
                names[environment.id] = environment.name
        except (BoomiApiError, requests.RequestException, ValidationError) as e:
            self.report.warning(
                title="Failed to list Boomi environments",
                message="environment_pattern cannot be applied, so deployment properties are skipped and all processes are ingested.",
                exc=e,
            )
            return None
        return names

    def _load_deployments(self) -> Optional[Dict[str, List[str]]]:
        # componentId -> sorted `environment=packageVersion` labels, or just the
        # environment when the package has no version. None means
        # deployments are unknown, so undeployed processes cannot be told apart.
        environment_names = self._load_environment_names()
        if environment_names is None:
            self.report.deployments_unavailable = True
            return None
        deployments: Dict[str, List[str]] = {}
        filtered_environments: Set[str] = set()
        try:
            for package in self.client.list_active_process_deployments():
                self.report.deployments_scanned += 1
                if (
                    self.config.branch_name
                    and package.branch_name
                    and package.branch_name != self.config.branch_name
                ):
                    continue
                environment = environment_names.get(
                    package.environment_id, package.environment_id
                )
                if not self.config.environment_pattern.allowed(environment):
                    filtered_environments.add(environment)
                    continue
                deployments.setdefault(package.component_id, []).append(
                    DEPLOYMENT_FORMAT.format(
                        environment=environment, version=package.package_version
                    )
                    if package.package_version
                    else environment
                )
        except (BoomiApiError, requests.RequestException, ValidationError) as e:
            self.report.deployments_unavailable = True
            if isinstance(e, BoomiApiError) and e.status_code == HTTP_FORBIDDEN:
                self.report.warning(
                    title="Cannot read Boomi deployments",
                    message="The API token is not allowed to query DeployedPackage. Deployment properties are skipped and all processes are ingested.",
                    exc=e,
                )
            else:
                self.report.failure(
                    title="Failed to list Boomi deployments",
                    message="Deployment properties are skipped and all processes are ingested.",
                    exc=e,
                )
            return None
        for environment in sorted(filtered_environments):
            self.report.environments_filtered.append(environment)
        return {key: sorted(values) for key, values in deployments.items()}

    def _folder_path_allowed(self, folder_path: Optional[str]) -> bool:
        # The folder is hidden from this token, listing failed, or the process
        # sits at the account root, so match the path from the component XML.
        # Without a path, deny patterns have nothing to match and let it
        # through; only a restrictive allow list fails closed.
        if folder_path is None:
            return ALLOW_ALL_REGEX in self.config.folder_pattern.allow
        return self.config.folder_pattern.allowed(folder_path)

    def _emit_processes(
        self,
        index: FolderIndex,
        deployments: Optional[Dict[str, List[str]]],
    ) -> Iterable[Entity]:
        try:
            processes, duplicates = dedupe_branch_copies(
                self.client.list_processes(self.config.branch_name)
            )
        except (BoomiApiError, requests.RequestException, ValidationError) as e:
            self.report.failure(
                title="Failed to list Boomi processes",
                message="No processes were ingested. Check the token has Build Read Access.",
                exc=e,
            )
            return
        self.report.processes_duplicate_branch_copies = duplicates
        for process in sorted(processes, key=lambda p: p.component_id):
            self.report.processes_scanned += 1
            if not self.config.process_pattern.allowed(process.name):
                self.report.processes_filtered.append(process.name)
                continue
            folder = index.folders.get(process.folder_id) if process.folder_id else None
            if folder is not None and folder.id not in index.allowed_ids:
                self.report.processes_filtered.append(process.name)
                continue
            process_deployments: List[str] = []
            if deployments is not None:
                process_deployments = deployments.get(process.component_id, [])
                if (
                    not process_deployments
                    and not self.config.include_undeployed_processes
                ):
                    self.report.processes_undeployed_skipped.append(process.name)
                    continue
            try:
                info = self._fetch_process_info(process)
                if folder is None and not self._folder_path_allowed(
                    info.folder_full_path
                ):
                    self.report.processes_filtered.append(process.name)
                    continue
                yield from self._make_process_entities(
                    process, index, info, process_deployments
                )
                self.report.processes_emitted += 1
            except Exception as e:
                self.report.warning(
                    title="Failed to process Boomi process",
                    message="The process was skipped.",
                    context=f"componentId={process.component_id}, name={process.name}",
                    exc=e,
                )

    def _fetch_process_info(self, process: ComponentMetadata) -> ProcessComponentInfo:
        try:
            xml_text = self.client.get_component_xml(process.component_id)
            return parse_process_component(xml_text)
        except (
            BoomiApiError,
            requests.RequestException,
            DefusedXmlException,
            SyntaxError,
        ) as e:
            # defusedxml's ParseError subclasses SyntaxError.
            self.report.process_xml_failures.append(process.component_id)
            self.report.warning(
                title="Failed to read Boomi process component",
                message="The process is emitted without a description.",
                context=f"componentId={process.component_id}, name={process.name}",
                exc=e,
            )
            return ProcessComponentInfo()

    def _make_process_entities(
        self,
        process: ComponentMetadata,
        index: FolderIndex,
        info: ProcessComponentInfo,
        process_deployments: List[str],
    ) -> Iterable[Entity]:
        folder = index.folders.get(process.folder_id) if process.folder_id else None
        folder_path = folder.full_path if folder else info.folder_full_path
        custom_properties = {
            PROP_COMPONENT_ID: process.component_id,
            PROP_VERSION: str(process.version),
        }
        if process.branch_name:
            custom_properties[PROP_BRANCH_NAME] = process.branch_name
        if process.modified_date:
            custom_properties[PROP_MODIFIED_DATE] = process.modified_date.isoformat()
        if folder_path:
            custom_properties[PROP_FOLDER_PATH] = folder_path
        if process_deployments:
            custom_properties[PROP_DEPLOYMENTS] = DEPLOYMENT_SEPARATOR.join(
                process_deployments
            )
        owners = self._owners(process)
        folder_container = self._parent_container(process.folder_id, index)
        flow = DataFlow(
            platform=PLATFORM,
            name=process.component_id,
            display_name=process.name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            description=info.description,
            external_url=self._process_url(process.component_id),
            custom_properties=custom_properties,
            created=process.created_date,
            last_modified=process.modified_date,
            subtype=DataFlowSubTypes.BOOMI_PROCESS,
            owners=owners,
            parent_container=folder_container if folder_container else unset,
        )
        yield flow
        yield DataJob(
            name=PROCESS_JOB_ID,
            flow=flow,
            display_name=process.name,
            description=info.description,
            subtype=DataJobSubTypes.BOOMI_PROCESS_JOB,
            owners=owners,
        )

    def _process_url(self, component_id: str) -> Optional[str]:
        if self._ui_host is None:
            return None
        return PROCESS_URL_TEMPLATE.format(
            ui_host=self._ui_host,
            account_id=self.config.account_id,
            id=component_id,
        )

    def _owners(
        self, process: ComponentMetadata
    ) -> Optional[List[Tuple[CorpUserUrn, str]]]:
        if not self.config.extract_owners:
            return None
        owners: List[Tuple[CorpUserUrn, str]] = []
        if process.created_by:
            owners.append(
                (CorpUserUrn(process.created_by), OwnershipTypeClass.DATAOWNER)
            )
        if process.modified_by and process.modified_by != process.created_by:
            owners.append(
                (CorpUserUrn(process.modified_by), OwnershipTypeClass.TECHNICAL_OWNER)
            )
        return owners or None
