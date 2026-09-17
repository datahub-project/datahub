from typing import Dict, Iterable, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from datahub.ingestion.source.sap_datasphere.constants import (
    SEARCH_FIELD_BUSINESS_NAME,
    SEARCH_FIELD_FOLDER_ID,
    SEARCH_FIELD_FOLDER_NAME,
    SEARCH_FIELD_ID,
    SEARCH_FIELD_KIND,
    SEARCH_FIELD_NAME,
    SEARCH_FIELD_PARENT_HIERARCHIES,
    SEARCH_KEY_HIERARCHY,
    SEARCH_KIND_FOLDER,
)
from datahub.ingestion.source.sap_datasphere.models import JsonDict

# A folder path as a tuple of display names, outermost first. Hashable so it can
# key the emitted-container set.
FolderPath = Tuple[str, ...]


class SpaceFolders(BaseModel):
    # Folder assignments for one space, resolved once and then read-only.
    model_config = ConfigDict(frozen=True)

    # Object technical name -> its folder path. Objects at the space root are
    # absent rather than mapped to an empty path.
    path_by_object: Dict[str, FolderPath] = Field(default_factory=dict)
    # Every folder path seen, including the ancestors of a nested folder that
    # itself holds no objects, so an empty intermediate folder still renders.
    paths: List[FolderPath] = Field(default_factory=list)

    def path_for(self, object_name: str) -> Optional[FolderPath]:
        return self.path_by_object.get(object_name)


def _folder_display_name(entry: JsonDict) -> Optional[str]:
    name = entry.get(SEARCH_FIELD_FOLDER_NAME)
    if isinstance(name, str) and name.strip():
        return name.strip()
    return None


def _hierarchy_entries(record: JsonDict) -> List[JsonDict]:
    # The annotation is a list of scoped hierarchies; only the folder one carries
    # named ancestors, so take the first that yields any.
    hierarchies = record.get(SEARCH_FIELD_PARENT_HIERARCHIES)
    if not isinstance(hierarchies, list):
        return []
    for scoped in hierarchies:
        if not isinstance(scoped, dict):
            continue
        hierarchy = scoped.get(SEARCH_KEY_HIERARCHY)
        if isinstance(hierarchy, list):
            entries = [e for e in hierarchy if isinstance(e, dict)]
            if entries:
                return entries
    return []


def _ancestor_path(record: JsonDict) -> FolderPath:
    """The record's folder path, outermost folder first.

    SAP does not document whether the ParentHierarchies chain runs root-first or
    leaf-first, so it is oriented against the record's own ``folder_id`` (its
    immediate parent): a chain that *starts* at the immediate parent is
    leaf-first and gets reversed.
    """
    entries = _hierarchy_entries(record)
    if entries:
        leaf_id = record.get(SEARCH_FIELD_FOLDER_ID)
        if (
            len(entries) > 1
            and isinstance(leaf_id, str)
            and entries[0].get(SEARCH_FIELD_FOLDER_ID) == leaf_id
        ):
            entries = list(reversed(entries))
        names = [n for n in (_folder_display_name(e) for e in entries) if n]
        if names:
            return tuple(names)
    # No hierarchy annotation: the record still names its immediate folder.
    immediate = _folder_display_name(record)
    return (immediate,) if immediate else ()


def _str_field(record: JsonDict, field: str) -> Optional[str]:
    value = record.get(field)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _own_folder_name(record: JsonDict) -> Optional[str]:
    # A folder's own segment must be its display name ("Sales Reporting"), to
    # match the ancestor chain, not its generated technical name ("Folder_AB12").
    return _str_field(record, SEARCH_FIELD_BUSINESS_NAME) or _str_field(
        record, SEARCH_FIELD_NAME
    )


def parse_folder_assignments(records: Iterable[JsonDict]) -> SpaceFolders:
    """Reduce Repository search records to each object's folder path.

    Folder records (``kind == sap.repo.folder``) contribute their own path so an
    empty folder is still emitted; every other record contributes the path of
    the folder that holds it.
    """
    path_by_object: Dict[str, FolderPath] = {}
    paths: Dict[FolderPath, None] = {}

    def remember(path: FolderPath) -> None:
        # Register every ancestor prefix too: a nested folder can appear in the
        # response without its parent holding any object of its own.
        for depth in range(1, len(path) + 1):
            paths.setdefault(path[:depth], None)

    for record in records:
        if not isinstance(record, dict):
            continue
        ancestors = _ancestor_path(record)
        if record.get(SEARCH_FIELD_KIND) == SEARCH_KIND_FOLDER:
            own = _own_folder_name(record) or _str_field(record, SEARCH_FIELD_ID)
            if own is not None:
                remember(ancestors + (own,))
            continue
        if not ancestors:
            continue
        # Objects are keyed by technical name — that is what the catalog and
        # dwaas-core listings give the rest of the connector.
        object_name = _str_field(record, SEARCH_FIELD_NAME)
        if object_name is None:
            continue
        path_by_object[object_name] = ancestors
        remember(ancestors)

    return SpaceFolders(path_by_object=path_by_object, paths=list(paths))
