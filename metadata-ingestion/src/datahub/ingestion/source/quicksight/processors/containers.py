import logging
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from botocore.exceptions import ClientError

from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.quicksight.processors.enrichment import AssetEnricher
from datahub.ingestion.source.quicksight.quicksight_api import QuickSightAPI
from datahub.ingestion.source.quicksight.quicksight_config import (
    QuickSightSourceConfig,
)
from datahub.ingestion.source.quicksight.quicksight_constants import (
    SUBTYPE_FOLDER,
    SUBTYPE_NAMESPACE,
    SUBTYPE_SHARED_FOLDERS,
)
from datahub.ingestion.source.quicksight.quicksight_report import (
    QuickSightSourceReport,
)
from datahub.metadata.schema_classes import OwnerClass, TagAssociationClass
from datahub.sdk.container import Container

logger = logging.getLogger(__name__)

PLATFORM = "quicksight"

# Resolves an asset id to its parent container as a fully-built ``Container``
# (folder / namespace), or ``None`` for the platform root. Implemented by
# ``ContainersProcessor.parent_for``. A ``Container`` (not a ``ContainerKey``) is
# returned so the SDK derives each asset's BrowsePathsV2 as the parent's full
# path + the folder — giving correct nesting at arbitrary folder depth, which a
# static ``ContainerKey`` inheritance chain cannot express.
ParentResolver = Callable[[str], Optional[Container]]

# QuickSight returns these error codes when a feature (namespaces, folders) is
# only available on Enterprise edition or the caller lacks the relevant
# permission. We degrade gracefully rather than failing the whole run.
_GRACEFUL_DEGRADE_CODES = frozenset(
    {
        "UnsupportedUserEditionException",
        "AccessDeniedException",
        "ResourceNotFoundException",
    }
)


class QuickSightNamespaceKey(ContainerKey):
    # account_id is part of the GUID (not displayed) so namespace/folder
    # container URNs stay unique across accounts even without a platform_instance,
    # matching how the Dataset URNs embed the account id.
    account_id: str
    namespace: str


class QuickSightSharedFoldersKey(ContainerKey):
    # Synthetic account-level root mirroring QuickSight's "Shared folders" UI
    # section. account_id keeps the URN unique across accounts (matching the
    # namespace/folder keys). Inherits ContainerKey directly so it roots at the
    # platform (or the namespace container, set explicitly in _emit_folders).
    account_id: str


class QuickSightFolderKey(ContainerKey):
    # Inherits ContainerKey *directly* (not QuickSightNamespaceKey) so
    # ``parent_key()`` is ``None`` and folders root at the platform. If it
    # inherited the namespace key, the SDK would auto-derive every foldered
    # asset's browse path *through* the namespace container — which we don't
    # emit unless ``add_namespace_container`` is set — leaving a dangling "ghost"
    # container URN above each asset. When namespaces ARE containerized, the
    # folder→namespace parent is set explicitly in ``_emit_folders`` instead.
    # ``namespace`` is retained purely as a GUID component for URN stability.
    account_id: str
    namespace: str
    folder_id: str


def _graceful_code(error: Exception) -> Optional[str]:
    """Return the AWS error code if ``error`` is a gracefully-degradable
    ``ClientError`` (unsupported edition / missing permission), else ``None``."""
    if isinstance(error, ClientError):
        code = error.response.get("Error", {}).get("Code")
        if code in _GRACEFUL_DEGRADE_CODES:
            return code
    return None


class ContainersProcessor:
    """Emits the QuickSight container hierarchy.

    Following the Glue / Redshift / PowerBI / Informatica convention, the owning
    AWS account is **not** modeled as a container — account separation is handled
    via ``platform_instance``. QuickSight namespaces are emitted as containers
    only when ``add_namespace_container`` is enabled (mirroring Tableau's
    ``add_site_container``), since the vast majority of accounts have only the
    single built-in ``default`` namespace. Folders (an Enterprise-edition
    feature) are the primary navigational container: assets nest under their
    folder when they belong to one, otherwise under the namespace container (if
    enabled) or directly under the platform root.

    ``parent_for(asset_id)`` resolves the container an asset should attach to.
    """

    def __init__(
        self,
        config: QuickSightSourceConfig,
        report: QuickSightSourceReport,
        api: QuickSightAPI,
        enricher: AssetEnricher,
    ) -> None:
        self.config = config
        self.report = report
        self.api = api
        self.enricher = enricher
        self.account_id = api.aws_account_id
        # Namespace whose name account-level assets resolve against. Settled in
        # _resolve_namespaces(); 'default' until then.
        self.namespace_name: str = "default"
        # asset_id -> folder_id, populated from list_folder_members so
        # datasets/analyses/dashboards nest under their QuickSight folder. An
        # asset may live in several folders; the first (by sorted folder id) wins
        # since DataHub containers are single-parent.
        self._asset_folder: Dict[str, str] = {}
        # folder_id / namespace name -> the built Container, so assets (and
        # nested folders) can reference the parent's full browse path.
        self._folder_containers: Dict[str, Container] = {}
        self._namespace_containers: Dict[str, Container] = {}
        # Synthetic "Shared folders" root (only when add_shared_folders_container
        # is enabled); top-level folders nest under it.
        self._shared_folders_container: Optional[Container] = None

    @property
    def default_parent(self) -> Optional[Container]:
        """Parent container for assets not in any folder.

        ``None`` (platform root) unless namespaces are containerized, in which
        case it is the resolved namespace container.
        """
        if not self.config.add_namespace_container:
            return None
        return self._namespace_containers.get(self.namespace_name)

    def parent_for(self, asset_id: str) -> Optional[Container]:
        """Resolve the parent container for an asset (dataset/analysis/dashboard).

        Returns the asset's QuickSight folder when it belongs to one, else the
        default parent (namespace container or platform root). Must be called
        after ``get_workunits()`` has built the folder containers.
        """
        folder_id = self._asset_folder.get(asset_id)
        if folder_id is not None and folder_id in self._folder_containers:
            return self._folder_containers[folder_id]
        return self.default_parent

    @property
    def folder_root_parent(self) -> Optional[Container]:
        """Parent for *top-level* folders: the synthetic Shared folders container
        when enabled, else the default parent (namespace container or platform root).
        """
        return self._shared_folders_container or self.default_parent

    def _shared_folders_key(self) -> QuickSightSharedFoldersKey:
        return QuickSightSharedFoldersKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            account_id=self.account_id,
        )

    def _namespace_key(self, namespace: str) -> QuickSightNamespaceKey:
        return QuickSightNamespaceKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            account_id=self.account_id,
            namespace=namespace,
        )

    def folder_key(self, folder_id: str) -> QuickSightFolderKey:
        return QuickSightFolderKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            env=self.config.env,
            account_id=self.account_id,
            namespace=self.namespace_name,
            folder_id=folder_id,
        )

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        namespace_names = self._resolve_namespaces()
        if self.config.add_namespace_container:
            yield from self._emit_namespaces(namespace_names)
        yield from self._emit_folders()

    def _resolve_namespaces(self) -> List[str]:
        """List namespaces (degrading to ``['default']``) and settle the
        namespace that account-level assets resolve against."""
        namespace_names: List[str] = []
        try:
            for ns in self.api.list_namespaces():
                name = ns.get("Name")
                if name:
                    namespace_names.append(name)
        except Exception as e:
            code = _graceful_code(e)
            if code is None:
                raise
            logger.info(
                f"Could not list QuickSight namespaces ({code}); "
                "falling back to the 'default' namespace."
            )

        if not namespace_names:
            namespace_names = ["default"]

        # Account-level assets hang off 'default' when present, else the first.
        self.namespace_name = (
            "default" if "default" in namespace_names else namespace_names[0]
        )
        return namespace_names

    def _emit_namespaces(
        self, namespace_names: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        for name in namespace_names:
            if not self.config.namespace_pattern.allowed(name):
                self.report.containers.dropped(f"namespace:{name}")
                continue
            container, workunits = self._build_container(
                container_key=self._namespace_key(name),
                display_name=name,
                subtype=SUBTYPE_NAMESPACE,
                parent=None,
            )
            self._namespace_containers[name] = container
            yield from workunits

    def _emit_shared_folders_root(self) -> Iterable[MetadataWorkUnit]:
        """Emit the synthetic Shared folders root and cache it as the parent for
        top-level folders. Parents to the namespace container (when enabled) or
        the platform root."""
        container, workunits = self._build_container(
            container_key=self._shared_folders_key(),
            display_name="Shared folders",
            subtype=SUBTYPE_SHARED_FOLDERS,
            parent=self.default_parent,
        )
        self._shared_folders_container = container
        yield from workunits

    def _emit_folders(self) -> Iterable[MetadataWorkUnit]:
        try:
            folder_summaries = list(self.api.list_folders())
        except Exception as e:
            code = _graceful_code(e)
            if code is None:
                raise
            self.report.folders_unsupported = True
            logger.info(f"Skipping QuickSight folders ({code}).")
            return

        # Emit parents before children so each folder's parent Container (and its
        # full browse path) already exists. Sort by folder-path depth, then folder
        # id (the latter makes multi-folder asset membership deterministic —
        # first folder wins).
        ordered = sorted(
            (s for s in folder_summaries if s.get("FolderId")),
            key=lambda s: (
                len(self._parent_folder_ids(s["FolderId"])),
                s["FolderId"],
            ),
        )

        # Emit the synthetic "Shared folders" root before any folder so top-level
        # folders can reference its full browse path. Skipped when there are no
        # folders to avoid emitting an empty grouping.
        if self.config.add_shared_folders_container and ordered:
            yield from self._emit_shared_folders_root()

        for summary in ordered:
            folder_id = summary["FolderId"]
            name = str(summary.get("Name") or folder_id)
            if not self.config.folder_pattern.allowed(name):
                self.report.containers.dropped(f"folder:{folder_id}")
                continue

            parent_folder_id = self._parent_folder_id(folder_id)
            # Nested folders parent to their (already-emitted) parent folder;
            # top-level folders parent to the Shared folders root (when enabled),
            # else the namespace container (when enabled) or the platform root. If
            # the parent folder was filtered out, fall back to the folder root.
            parent: Optional[Container] = (
                self._folder_containers.get(parent_folder_id)
                if parent_folder_id
                else None
            ) or self.folder_root_parent
            container, workunits = self._build_container(
                container_key=self.folder_key(folder_id),
                display_name=name,
                subtype=SUBTYPE_FOLDER,
                parent=parent,
                owners=self.enricher.owners("folder", folder_id),
                tags=self.enricher.tags(summary.get("Arn") or ""),
            )
            self._folder_containers[folder_id] = container
            yield from workunits
            self._record_folder_members(folder_id)

    def _record_folder_members(self, folder_id: str) -> None:
        """Map a folder's member assets to its folder id (first folder wins)."""
        try:
            members = list(self.api.list_folder_members(folder_id))
        except Exception as e:
            if _graceful_code(e) is None:
                raise
            logger.info(f"Could not list members of folder {folder_id}.")
            return
        for member in members:
            member_id = member.get("MemberId")
            if member_id and member_id not in self._asset_folder:
                self._asset_folder[member_id] = folder_id

    def _parent_folder_ids(self, folder_id: str) -> List[str]:
        """Return the ``FolderPath`` ancestor folder ids (root-to-leaf), or []."""
        try:
            folder = self.api.describe_folder(folder_id)
        except Exception as e:
            if _graceful_code(e) is None:
                raise
            return []
        return [arn.split("/")[-1] for arn in (folder.get("FolderPath") or [])]

    def _parent_folder_id(self, folder_id: str) -> Optional[str]:
        """Resolve the immediate parent folder via ``FolderPath`` ancestry.

        ``FolderPath`` is a root-to-leaf list of ancestor folder ARNs; the last
        element is the immediate parent. Returns ``None`` for top-level folders.
        """
        ancestors = self._parent_folder_ids(folder_id)
        return ancestors[-1] if ancestors else None

    def _build_container(
        self,
        container_key: ContainerKey,
        display_name: str,
        subtype: str,
        parent: Optional[Container],
        description: Optional[str] = None,
        owners: Optional[List[OwnerClass]] = None,
        tags: Optional[List[TagAssociationClass]] = None,
    ) -> Tuple[Container, List[MetadataWorkUnit]]:
        # Passing the parent ``Container`` (not its key) makes the SDK build this
        # container's BrowsePathsV2 as the parent's full path + itself, so nested
        # folders and their assets get correct, fully-qualified browse paths.
        container = Container(
            container_key,
            display_name=display_name,
            subtype=subtype,
            description=description,
            parent_container=parent,
            owners=owners,
            tags=tags,
        )
        self.report.containers.processed(container.urn.urn())
        return container, list(container.as_workunits())
