"""Folder registry builder and container helper for LookerV2Source."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Union

from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import Folder, FolderBase

from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.ingestion.source.looker.looker_common import LookerFolderKey
from datahub.sdk.container import Container
from datahub.sdk.entity import Entity

if TYPE_CHECKING:
    from datahub.ingestion.source.looker_v2.looker_v2_context import LookerV2Context

logger = logging.getLogger(__name__)


class LookerFolderProcessor:
    """Builds the folder registry and exposes folder container helpers.

    Not a pipeline stage — build_registry() is called during _initialize(),
    and get_folder_container() / get_folder_ancestors() are called on-demand
    by DashboardProcessor and LookProcessor.
    """

    def __init__(self, ctx: "LookerV2Context") -> None:
        self._ctx = ctx
        self._processed_folders: List[str] = []

    def build_registry(self) -> None:
        """Bulk-fetch all folders and populate ctx.folder_registry."""
        try:
            all_folders = self._ctx.looker_api.all_folders(
                fields=[
                    "id",
                    "name",
                    "parent_id",
                    "is_personal",
                    "is_personal_descendant",
                ]
            )
            for folder in all_folders:
                if folder.id:
                    self._ctx.folder_registry[folder.id] = folder
            self._ctx.reporter.folder_registry_size = len(self._ctx.folder_registry)
            logger.info(f"Pre-fetched {len(self._ctx.folder_registry)} folders")
        except SDKError as e:
            logger.warning(
                f"Failed to pre-fetch folders (will fall back to on-demand): {e}"
            )
            self._ctx.reporter.report_warning(
                title="Folder Pre-Fetch Failed",
                message="Could not bulk pre-fetch folders. Falling back to per-dashboard API calls.",
                context=str(e),
            )

    def gen_folder_key(self, folder_id: str) -> LookerFolderKey:
        return LookerFolderKey(
            folder_id=folder_id,
            env=self._ctx.config.env,
            platform=self._ctx.config.platform_name,
            instance=self._ctx.config.platform_instance,
        )

    def get_folder_ancestors(self, folder_id: str) -> List[FolderBase]:
        """Get ancestor folders using the pre-fetched registry when available."""
        if self._ctx.folder_registry:
            ancestors: List[FolderBase] = []
            current_folder = self._ctx.folder_registry.get(folder_id)
            if current_folder is None:
                return []
            visited: set = set()
            parent_id = current_folder.parent_id
            while parent_id and parent_id not in visited:
                visited.add(parent_id)
                parent = self._ctx.folder_registry.get(parent_id)
                if parent is None:
                    break
                ancestors.insert(0, parent)
                parent_id = parent.parent_id
            return ancestors
        try:
            return [
                FolderBase(
                    id=f.id,
                    name=f.name,
                    parent_id=f.parent_id,
                    is_personal=f.is_personal,
                    is_personal_descendant=f.is_personal_descendant,
                )
                for f in self._ctx.looker_api.folder_ancestors(folder_id=folder_id)
            ]
        except SDKError as e:
            logger.warning(f"Failed to fetch folder ancestors for {folder_id}: {e}")
            self._ctx.reporter.report_warning(
                title="Folder Ancestors Fetch Failed",
                message="Could not fetch folder ancestors. Folder container hierarchy may be incomplete.",
                context=f"folder_id={folder_id}: {e}",
            )
            return []

    def should_skip_personal_folder(
        self, folder: Union[FolderBase, Folder, None]
    ) -> bool:
        """Return True when the folder should be skipped due to personal-folder policy."""
        if not self._ctx.config.skip_personal_folders:
            return False
        if folder is None:
            return False
        return bool(
            getattr(folder, "is_personal", False)
            or getattr(folder, "is_personal_descendant", False)
        )

    def get_folder_path(self, folder: Union[FolderBase, Folder]) -> str:
        """Return slash-separated ancestor path including this folder."""
        if not folder.id:
            return folder.name or ""
        ancestors = self.get_folder_ancestors(folder.id)
        parts = [a.name or "" for a in ancestors] + [folder.name or ""]
        return "/".join(p for p in parts if p)

    def get_folder_container(self, folder: Union[FolderBase, Folder]) -> List[Entity]:
        """Get or create folder container entity with full ancestor chain."""
        if not folder.id:
            return []

        entities: List[Entity] = []

        ancestors = self.get_folder_ancestors(folder.id)
        for ancestor in ancestors:
            if ancestor.id and ancestor.id not in self._processed_folders:
                self._processed_folders.append(ancestor.id)
                parent_key = None
                if ancestor.parent_id:
                    parent_key = self.gen_folder_key(ancestor.parent_id)
                container = Container(
                    container_key=self.gen_folder_key(ancestor.id),
                    subtype=BIContainerSubTypes.LOOKER_FOLDER,
                    display_name=ancestor.name or f"Folder {ancestor.id}",
                    parent_container=parent_key,
                )
                entities.append(container)

        if folder.id not in self._processed_folders:
            self._processed_folders.append(folder.id)
            parent_key = None
            if hasattr(folder, "parent_id") and folder.parent_id:
                parent_key = self.gen_folder_key(folder.parent_id)
            container = Container(
                container_key=self.gen_folder_key(folder.id),
                subtype=BIContainerSubTypes.LOOKER_FOLDER,
                display_name=folder.name or f"Folder {folder.id}",
                parent_container=parent_key,
            )
            entities.append(container)

        return entities
