import logging
from typing import Dict, Iterable, Optional, Set

from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.ingestion.source.hightouch.config import (
    HightouchSourceConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.constants import HIGHTOUCH_PLATFORM
from datahub.ingestion.source.hightouch.hightouch_api import HightouchAPIClient
from datahub.ingestion.source.hightouch.urn_builder import HightouchUrnBuilder

logger = logging.getLogger(__name__)


class WorkspaceKey(ContainerKey):
    workspace_id: str


class FolderKey(ContainerKey):
    folder_id: str
    workspace_id: str


class HightouchContainerHandler:
    def __init__(
        self,
        config: HightouchSourceConfig,
        report: HightouchSourceReport,
        urn_builder: HightouchUrnBuilder,
        api_client: HightouchAPIClient,
    ):
        self.config = config
        self.report = report
        self.urn_builder = urn_builder
        self.api_client = api_client
        self._workspaces_cache: Dict[str, str] = {}
        self._folders_cache: Dict[str, str] = {}
        self._emitted_containers: Set[str] = set()

    def preload_workspaces(self) -> None:
        if not self.config.extract_workspaces_to_containers:
            return

        try:
            workspaces = self.api_client.get_workspaces()
            for workspace in workspaces:
                self._workspaces_cache[workspace.id] = workspace.name
            logger.info(f"Preloaded {len(workspaces)} workspace names")
        except Exception:
            logger.warning(
                "Failed to preload workspaces - will use workspace IDs as fallback",
                exc_info=True,
            )

    def get_workspace_name(self, workspace_id: str) -> str:
        if workspace_id not in self._workspaces_cache:
            self._workspaces_cache[workspace_id] = workspace_id
        return self._workspaces_cache[workspace_id]

    def get_folder_name(self, folder_id: str) -> str:
        # Hightouch API doesn't expose folder names, so we use IDs
        if folder_id not in self._folders_cache:
            self._folders_cache[folder_id] = folder_id
        return self._folders_cache[folder_id]

    def generate_workspace_container(
        self, workspace_id: str
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_workspaces_to_containers:
            return

        container_key = WorkspaceKey(
            workspace_id=workspace_id,
            platform=HIGHTOUCH_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        if container_key.guid() in self._emitted_containers:
            return

        workspace_name = self.get_workspace_name(workspace_id)

        container_workunits = gen_containers(
            container_key=container_key,
            name=workspace_name,
            sub_types=[str(BIContainerSubTypes.HIGHTOUCH_WORKSPACE)],
            extra_properties={
                "workspace_id": workspace_id,
                "platform": HIGHTOUCH_PLATFORM,
            },
        )

        for workunit in container_workunits:
            self._emitted_containers.add(container_key.guid())
            yield workunit

    def generate_folder_container(
        self,
        folder_id: str,
        workspace_id: str,
        parent_container_key: Optional[ContainerKey] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_workspaces_to_containers:
            return

        container_key = FolderKey(
            folder_id=folder_id,
            workspace_id=workspace_id,
            platform=HIGHTOUCH_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

        if container_key.guid() in self._emitted_containers:
            return

        folder_name = self.get_folder_name(folder_id)

        container_workunits = gen_containers(
            container_key=container_key,
            name=folder_name,
            sub_types=[str(BIContainerSubTypes.HIGHTOUCH_FOLDER)],
            parent_container_key=parent_container_key,
            extra_properties={
                "folder_id": folder_id,
                "workspace_id": workspace_id,
                "platform": HIGHTOUCH_PLATFORM,
            },
        )

        for workunit in container_workunits:
            self._emitted_containers.add(container_key.guid())
            yield workunit

    def add_entity_to_container(
        self, entity_urn: str, container_key: ContainerKey
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.extract_workspaces_to_containers:
            return

        yield from add_dataset_to_container(container_key, entity_urn)

    def get_workspace_key(self, workspace_id: str) -> WorkspaceKey:
        return WorkspaceKey(
            workspace_id=workspace_id,
            platform=HIGHTOUCH_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

    def get_folder_key(self, folder_id: str, workspace_id: str) -> FolderKey:
        return FolderKey(
            folder_id=folder_id,
            workspace_id=workspace_id,
            platform=HIGHTOUCH_PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
        )
