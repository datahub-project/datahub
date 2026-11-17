import posixpath
import re
from typing import Callable, Dict, Iterable, List, Optional

from pydantic import BaseModel

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.source.qlik_sense.script_lineage import (
    ParsedScript,
    PrecedingLoad,
    QvdReference,
    ScriptTableLineage,
)

# A $(variable) reference, as left in a name when the app doesn't have it.
VARIABLE_IN_NAME_RE = re.compile(r"\$\([^)]*\)")

# QVDs the Engine writes for BUFFER loads, which no app stores or owns.
_BUFFERS_FOLDER = "/engine/buffers/"


# A canonical QVD path: a UNC share or a drive. App table names never match.
CANONICAL_ABSOLUTE_PATH_RE = re.compile(r"^(?://|[a-z]:/)")


def canonical_path(path: str) -> str:
    """One spelling per file: forward slashes, no `.` or `..`, lower case, as
    Windows paths are case-insensitive. `//server/share` keeps its UNC prefix."""
    return posixpath.normpath(path.strip().replace("\\", "/")).lower()


class QvdPathResolver:
    """Turns a QVD a load script names into its canonical file path, or None
    when the connector can't tell which file it is or it is Qlik's own."""

    def __init__(
        self,
        folders: Dict[str, str],
        ignored_connections: Iterable[str],
        folder_aliases: Dict[str, str],
        variables_known: bool = True,
    ):
        # Whether the app's variables were read, so one it doesn't have is
        # unset rather than unread.
        self.variables_known = variables_known
        # Folder data connection name -> folder path. Qlik matches data
        # connection names without regard to case.
        self.folders = {name.lower(): folder for name, folder in folders.items()}
        self.ignored_connections = {name.lower() for name in ignored_connections}
        # Longest first, so a nested alias wins over its parent.
        self.folder_aliases = sorted(
            (
                (canonical_path(alias), canonical_path(target))
                for alias, target in folder_aliases.items()
            ),
            key=lambda alias: len(alias[0]),
            reverse=True,
        )
        self.unresolved: List[str] = []

    def resolve(self, file: QvdReference) -> Optional[str]:
        if file.connection is None:
            path = canonical_path(file.path)
        elif file.connection.lower() in self.ignored_connections:
            return None
        else:
            folder = self.folders.get(file.connection.lower())
            if folder is None and "$(" in file.connection and self.variables_known:
                # A variable the app doesn't have expanded to nothing when it
                # reloaded, such as an empty connection prefix. Taken only when
                # what is left names a folder connection.
                folder = self.folders.get(
                    VARIABLE_IN_NAME_RE.sub("", file.connection).lower()
                )
            if folder is None:
                self.unresolved.append(f"lib://{file.connection}/{file.path}")
                return None
            path = canonical_path(f"{folder}/{file.path}")
        for alias, target in self.folder_aliases:
            if path == alias or path.startswith(f"{alias}/"):
                path = target + path[len(alias) :]
                break
        if _BUFFERS_FOLDER in f"{path}/":
            return None
        return path


class QvdField(BaseModel):
    name: str
    # The field of the stored table it holds.
    source: Optional[str] = None


class QvdWrite(BaseModel):
    table_urn: str
    # None when the stored table's fields are unknown.
    fields: Optional[List[QvdField]] = None


class AppQvds(BaseModel):
    """The QVDs one app's load script writes and reads, by QVD dataset URN. Kept
    in the app's checkpoint, as a QVD's lineage and schema come from every app
    that touches it."""

    writes: Dict[str, List[QvdWrite]] = {}
    # QVD URN -> fields the app loads from it.
    reads: Dict[str, List[str]] = {}
    # False when a STORE's path could not be resolved, so the app may write
    # QVDs not listed in `writes`.
    writes_complete: bool = True


class AppQvdLineage(BaseModel):
    # App tables loaded from QVDs, by identifier.
    tables: Dict[str, ScriptTableLineage] = {}
    qvds: AppQvds = AppQvds()
    # Canonical path of each QVD the app touches, by URN.
    paths: Dict[str, str] = {}
    # A STORE names a data connection the resolver doesn't know.
    has_unresolved_store: bool = False


def _loaded_fields(load: PrecedingLoad) -> Optional[Dict[str, List[str]]]:
    """Field -> the fields of the file it is loaded from, or None when a load
    such as LOAD * hides them."""
    if load.keeps_all or load.unreadable:
        return None
    return {field.name: field.sources for field in load.fields if field.named}


def app_qvd_lineage(
    app_id: str,
    parsed: ParsedScript,
    resolver: QvdPathResolver,
    make_urn: Callable[[str], str],
    table_fields: Dict[str, List[str]],
) -> AppQvdLineage:
    """`table_fields` holds the fields of the app's other tables by identifier,
    so the fields a STORE writes can be known."""
    lineage = AppQvdLineage()

    def qvd_urn(file: QvdReference) -> Optional[str]:
        path = resolver.resolve(file)
        if path is None:
            return None
        urn = make_urn(path)
        lineage.paths[urn] = path
        return urn

    def table(label: str) -> ScriptTableLineage:
        identifier = f"{app_id}.{label}".lower()
        return lineage.tables.setdefault(
            identifier, ScriptTableLineage(label=label, identifier=identifier)
        )

    unknown_fields = set()
    for load in parsed.qvd_loads:
        urn = qvd_urn(load.file)
        if urn is None:
            continue
        loaded = table(load.label)
        if urn not in loaded.upstream_urns:
            loaded.upstream_urns.append(urn)
        fields = _loaded_fields(load.fields)
        if fields is None:
            unknown_fields.add(loaded.identifier)
            lineage.qvds.reads.setdefault(urn, [])
            continue
        read = lineage.qvds.reads.setdefault(urn, [])
        for field, sources in fields.items():
            upstreams = loaded.column_upstreams.setdefault(field, [])
            for source in sources:
                if source not in read:
                    read.append(source)
                upstream = make_schema_field_urn(urn, source)
                if upstream not in upstreams:
                    upstreams.append(upstream)
    for identifier in unknown_fields:
        # Fields listed by one load don't make the table's schema complete.
        lineage.tables[identifier].column_upstreams = {}

    for store in parsed.qvd_stores:
        unresolved = len(resolver.unresolved)
        urn = qvd_urn(store.file)
        if urn is None:
            lineage.has_unresolved_store |= len(resolver.unresolved) > unresolved
            continue
        stored = table(store.label)
        # The data model's fields are complete even when a LOAD * hid them
        # from the script.
        known = list(
            dict.fromkeys(
                [
                    *table_fields.get(stored.identifier, []),
                    *(
                        []
                        if stored.identifier in unknown_fields
                        else stored.column_upstreams
                    ),
                ]
            )
        )
        if store.fields is not None:
            selected = _loaded_fields(store.fields)
            stored_fields: Optional[List[QvdField]] = (
                None
                if selected is None
                else [
                    QvdField(
                        name=name,
                        source=sources[0] if len(sources) == 1 else None,
                    )
                    for name, sources in selected.items()
                ]
            )
        elif known:
            stored_fields = [QvdField(name=name, source=name) for name in known]
        else:
            stored_fields = None
        lineage.qvds.writes.setdefault(urn, []).append(
            QvdWrite(table_urn=make_urn(stored.identifier), fields=stored_fields)
        )
    return lineage
