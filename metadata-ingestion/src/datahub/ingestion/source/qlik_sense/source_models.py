from typing import Dict, List, NamedTuple, Optional, Set

from pydantic import BaseModel, ConfigDict

from datahub.ingestion.source.qlik_sense.config import FileStorageDetail
from datahub.ingestion.source.qlik_sense.data_classes import (
    App,
    SchemaField as QlikDatasetSchemaField,
)
from datahub.ingestion.source.qlik_sense.file_lineage import FilePlatform, FileStorage
from datahub.ingestion.source.qlik_sense.qvd_lineage import QvdWrite
from datahub.ingestion.source.qlik_sense.script_lineage import (
    ParsedScript,
    ScriptTableLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    UpstreamLineage,
)
from datahub.utilities.str_enum import StrEnum


class ResolvedSource(NamedTuple):
    urn: Optional[str]
    # Why the source could not be told this run, if so.
    unknown_reason: Optional[str]


class GraphNamedPath(NamedTuple):
    path: Optional[str]
    # Whether the apps that may give the path were all read.
    all_read: bool


class FileStorageMatch(NamedTuple):
    storage: Optional[FileStorage]
    detail: Optional[FileStorageDetail]


class UntypedTable(NamedTuple):
    identifier: str
    schema: List[QlikDatasetSchemaField]


class _UnseenSpace(BaseModel):
    # As first written, while the space is keyed by its lowercased name.
    name: str
    needs: Set[str] = set()


class _OnlyAddedTables(BaseModel):
    model_config = ConfigDict(frozen=True)

    app_id: str
    reason: str


class _CataloguedFile(BaseModel):
    model_config = ConfigDict(frozen=True)

    platform: FilePlatform
    # The path part of the file's QRI.
    path_id: str


class _ScriptTableKey(BaseModel):
    """Loads of one dataset URN are one table, but a mapping load is kept
    apart."""

    model_config = ConfigDict(frozen=True)

    identifier: str
    mapping: bool


class _PartialRead(StrEnum):
    KEPT = "kept"
    LIMIT_REACHED = "limit_reached"
    NOT_KEPT = "not_kept"


class _GraphNamedApps(BaseModel):
    apps: Set[str]
    # Whether an app keeps the dataset from its last read, as this run could
    # not tell its sources, such as when its upstream graph could not be read.
    graph_unread: bool


class _BinaryLink(BaseModel):
    """A table an app BINARY loads from another app, whose lineage is written
    once every app has been read."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    app_id: str
    source_app_id: str
    table: str
    dataset_urn: str
    fields: List[str]
    # An included script that is not read could change the table.
    additive: bool = False
    # Named like a script table Qlik renamed, such as `Sales-1` beside a
    # `Sales` load, so it is the file's only if the source app has it.
    maybe_renamed_script_table: bool = False
    # The table's lineage from the app's own script.
    upstream_urns: List[str] = []
    script_fields: List[FineGrainedLineage] = []


class _ParsedApp(BaseModel):
    """The last app whose script was parsed, with the parse."""

    app: App
    parsed: ParsedScript


class _BinaryLoad(BaseModel):
    """The tables an on-prem app's BINARY statement brings in, by identifier,
    the app file, by URN, when it is linked to the file, and the tables linked
    to the app the file was exported from, by URN."""

    tables: Dict[str, ScriptTableLineage] = {}
    paths: Dict[str, str] = {}
    links: Dict[str, _BinaryLink] = {}


class PendingTableLineage(BaseModel):
    """What an app table's lineage is built from, kept until the end of the
    run when a source field's name, or a source app's tables, are only read
    later."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    app_id: str
    table_label: str
    dataset_urn: str
    # Table-level sources in Qlik's lineage graph, resolved when the lineage
    # is written.
    source_qris: List[str]
    script_upstream_urns: List[str]
    # Why the app's script or tables, or the table's field lineage, were not
    # read.
    incomplete_reason: Optional[str]
    script_fields: List[FineGrainedLineage]
    field_source_qris: Dict[str, List[str]]


class PendingStoredFile(BaseModel):
    """An app's edges into a file it stores into on storage, written at the end
    of the run, when every app that may share a stored table has been seen."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    app_id: str
    file_urn: str
    writes: List[QvdWrite]
    # DataHub's lineage of the file; None when this app's edges are only added.
    current: Optional[UpstreamLineage]
    # Tables of the app whose edges into the file it may remove.
    app_table_urns: Set[str]
    # Only Parquet and text files have fields on storage; QVDs don't.
    has_field_schema: bool


class _StorageFile(BaseModel):
    # None when the file is not linked on storage, as a full read leaves it.
    urn: Optional[str]


class _ResolvedSources(BaseModel):
    urns: List[str] = []
    # Why sources could not be resolved this run.
    unknown_reasons: List[str] = []

    @property
    def complete(self) -> bool:
        return not self.unknown_reasons


class _FileSourceResolution(BaseModel):
    urn: Optional[str] = None
    # The script reads the file through a connection whose storage, of the
    # file's platform, is known, so the file has a storage URN even when it
    # could not be told which.
    on_known_storage: bool = False
