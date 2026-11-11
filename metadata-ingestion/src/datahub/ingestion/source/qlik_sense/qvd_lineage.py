import posixpath
from typing import Callable, Dict, Iterable, List, Optional

from pydantic import BaseModel

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.source.qlik_sense.script_lineage import (
    ParsedScript,
    PrecedingLoad,
    QvdReference,
    ScriptTableLineage,
)

# QVDs the Engine writes for BUFFER loads, which no app stores or owns.
_BUFFERS_FOLDER = "/engine/buffers/"


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
    ):
        # Folder data connection name -> folder path.
        self.folders = folders
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
            folder = self.folders.get(file.connection)
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


class AppQvdLineage(BaseModel):
    # App tables loaded from QVDs, by identifier.
    tables: Dict[str, ScriptTableLineage] = {}
    qvds: AppQvds = AppQvds()
    # Canonical path of each QVD the app touches, by URN.
    paths: Dict[str, str] = {}


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
        urn = qvd_urn(store.file)
        if urn is None:
            continue
        stored = table(store.label)
        known = (
            None
            if stored.identifier in unknown_fields
            else list(
                dict.fromkeys(
                    [
                        *table_fields.get(stored.identifier, []),
                        *stored.column_upstreams,
                    ]
                )
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
