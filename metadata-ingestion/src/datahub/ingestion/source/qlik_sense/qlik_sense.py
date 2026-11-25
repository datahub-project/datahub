import base64
import copy
import functools
import hashlib
import json
import logging
import posixpath
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial
from typing import (
    Callable,
    Collection,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from pydantic import BaseModel, ConfigDict

import datahub.emitter.mce_builder as builder
from datahub._version import __version__
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_entity_to_container, gen_containers
from datahub.emitter.mcp_patch_builder import MetadataPatchProposal
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.incremental_lineage_helper import (
    convert_upstream_lineage_to_patch,
    dashboard_info_patch_builder,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.data_lake_common.data_lake_utils import (
    ContainerWUCreator,
)
from datahub.ingestion.source.qlik_sense.app_state import (
    AppCheckpoint,
    QlikAppStateHandler,
    UnmappedFileStore,
)
from datahub.ingestion.source.qlik_sense.config import (
    Constant,
    FileStorageDetail,
    PlatformDetail,
    QlikSourceConfig,
    QlikSourceReport,
)
from datahub.ingestion.source.qlik_sense.data_classes import (
    DATABASE_QRI_PREFIX,
    FIELD_TYPE_MAPPING,
    KNOWN_DATA_PLATFORM_MAPPING,
    QLIK_DATASET_QRI_PREFIX,
    UPSTREAM_RESOURCE_QRI_PREFIXES,
    App,
    AppField,
    AppFieldIndex,
    AppKey,
    BoxType,
    Chart,
    ChartInputs,
    Item,
    LineageTable,
    LineageUpstreams,
    ListedDataset,
    QlikDataset,
    QlikTable,
    ReloadStatement,
    SchemaField as QlikDatasetSchemaField,
    Sheet,
    Space,
    SpaceKey,
    UnchangedApp,
)
from datahub.ingestion.source.qlik_sense.file_lineage import (
    FILE_QRI_RE,
    PLATFORM_BY_QRI_SCHEME,
    FilePlatform,
    FileReference,
    FileSourceResolver,
    FileStorage,
    file_dataset_urn,
    file_storage_of_uri,
    normalized_file_path,
    qri_part_id,
)
from datahub.ingestion.source.qlik_sense.qlik_api import QlikAPI
from datahub.ingestion.source.qlik_sense.qlik_base_api import (
    UNREAD_DATA_CONNECTIONS,
    UNREAD_SPACES,
    QlikBaseAPI,
)
from datahub.ingestion.source.qlik_sense.qlik_onprem_api import QlikOnPremAPI
from datahub.ingestion.source.qlik_sense.qvd_lineage import (
    CANONICAL_ABSOLUTE_PATH_RE,
    AppQvdLineage,
    AppQvds,
    QvdPathResolver,
    QvdWrite,
    app_qvd_lineage,
    canonical_path,
    stored_fields,
)
from datahub.ingestion.source.qlik_sense.script_lineage import (
    BACKTICK_QUOTED_PLATFORMS,
    LIB_PATH_RE,
    SPACE_SEPARATOR,
    VARIABLE_REFERENCE_RE,
    DataConnectionInfo,
    ParsedScript,
    QvdReference,
    QvdStore,
    ResolvedConnection,
    ScriptTable,
    ScriptTableLineage,
    app_own_variables,
    app_table_identifier,
    apply_crosstable,
    apply_preceding_loads,
    brackets_to_backticks,
    drop_condition_variables,
    expand_variables,
    expression_names,
    has_qualified_name,
    infer_data_connection,
    is_connector_sql,
    is_inline_connection,
    merged_variables,
    parse_script,
    reads_unknown_fields,
    redact_connection,
    select_list_size,
    stores_in_competing_branches,
    unquoted_values,
)
from datahub.ingestion.source.state.entity_removal_state import (
    STATEFUL_INGESTION_IGNORED_ENTITY_TYPES,
    GenericCheckpointState,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    auto_stale_entity_removal,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    Status,
    SubTypes,
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    DatasetProperties,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    MySqlDDL,
    NullType,
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ChangeAuditStampsClass,
    ChartInfoClass,
    ContainerPropertiesClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    EdgeClass,
    InputFieldClass,
    InputFieldsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldDataTypeClass,
)
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn
from datahub.specific.chart import ChartPatchBuilder
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnRef,
    SqlParsingResult,
    column_refs_to_schema_field_urns,
    create_lineage_sql_parsed_result,
    create_schema_resolver,
)
from datahub.utilities.str_enum import StrEnum
from datahub.utilities.urns.urn import guess_entity_type

# Logger instance
logger = logging.getLogger(__name__)

MappingT = TypeVar("MappingT")


class _PartialRead(StrEnum):
    KEPT = "kept"
    LIMIT_REACHED = "limit_reached"
    NOT_KEPT = "not_kept"


# Entities per DataHub request when checking that a skipped app's entities exist.
ENTITY_CHECK_BATCH_SIZE = 100

# Lineage graph QRIs of Qlik dataset tables, `<dataset secureQri>#<table>`.
# An analytics app is a sense resource in the lineage graph, a script app a
# dataprep resource.
QLIK_APP_QRI_RE = re.compile(r"^qri:app:(?:sense|dataprep)://([^#]+)")
QVD_EXTENSION = ".qvd"
# Every Qlik Cloud space's connection to its own data files.
DATA_FILES_CONNECTION = "datafiles"


class _LibConnection(BaseModel):
    # "" when the `lib://` connection is not qualified by a space.
    space: str
    name: str


def _lib_connection(connection: str) -> _LibConnection:
    """Splits a `lib://` connection into its space and name. Qlik brackets a
    name that holds the separator, as in `[Team: Sales]:Lake`."""
    connection = connection.strip()
    end = connection.find("]") if connection.startswith("[") else -1
    if end > 0:
        rest = connection[end + 1 :].strip()
        if not rest:
            return _LibConnection(space="", name=connection[1:end])
        if rest.startswith(SPACE_SEPARATOR):
            return _LibConnection(
                space=connection[1:end],
                name=rest[len(SPACE_SEPARATOR) :].strip().strip("[]"),
            )
    space, qualified, name = connection.partition(SPACE_SEPARATOR)
    if not qualified:
        return _LibConnection(space="", name=space.strip("[]"))
    return _LibConnection(
        space=space.strip().strip("[]"), name=name.strip().strip("[]")
    )


def _connection_key(lib_connection: _LibConnection) -> str:
    # How Qlik Cloud's data connection listing keys a connection.
    if not lib_connection.space:
        return lib_connection.name
    return f"{lib_connection.space}{SPACE_SEPARATOR}{lib_connection.name}"


def _lib_name(name: str) -> str:
    # Bracketed only when it must be, so other names keep their URNs.
    return f"[{name}]" if SPACE_SEPARATOR in name else name


def _is_data_files(connection: str) -> bool:
    return _lib_connection(connection).name.lower() == DATA_FILES_CONNECTION


def _get_ignoring_case(
    mapping: Mapping[str, MappingT], name: str
) -> Optional[MappingT]:
    if name in mapping:
        return mapping[name]
    # Qlik resolves connection names regardless of case. A case variant is
    # taken only when a single key matches it.
    matches = [key for key in mapping if key.lower() == name.lower()]
    return mapping[matches[0]] if len(matches) == 1 else None


def _is_qvd(dataset: Union[QlikDataset, ListedDataset]) -> bool:
    return dataset.type.upper() == "QVD" or dataset.name.lower().endswith(QVD_EXTENSION)


def _dataset_subtype(dataset: QlikDataset) -> str:
    return (
        DatasetSubTypes.QLIK_QVD if _is_qvd(dataset) else DatasetSubTypes.QLIK_DATASET
    )


def _dataset_display_name(dataset: QlikDataset) -> str:
    # The URN keeps the file name, so `sales.qvd` and `sales.csv` in one space
    # stay apart and QVDs ingested before keep their URNs.
    if _is_qvd(dataset) and dataset.name.lower().endswith(QVD_EXTENSION):
        return dataset.name[: -len(QVD_EXTENSION)]
    return dataset.name


def _qvd_fields(writes: List[QvdWrite], reads: List[str]) -> Optional[List[str]]:
    """A QVD's fields: what its writers store, or failing that what its readers
    load, which may be only some of them. None when unknown."""
    if writes:
        if any(write.fields is None for write in writes):
            return None
        return list(
            dict.fromkeys(
                field.name for write in writes for field in write.fields or []
            )
        )
    return reads or None


# Why the QVD writes of an app with an unresolved STORE are not all known.
UNRESOLVED_STORE = "a STORE path could not be resolved"


# A reload statement that ran SQL: Qlik keeps the `SQL` prefix when the script
# used one, and a bare `SELECT` or `WITH` is SQL too.
RELOAD_SQL_RE = re.compile(
    r"^\s*(?:SQL\b|(?=(?:SELECT|WITH)\b))(?P<sql>.*)$", re.IGNORECASE | re.DOTALL
)
# Other SQL a reload may have run without the prefix, which is never parsed
# for lineage but may read the tables a label names.
RELOAD_UNPARSED_SQL_RE = re.compile(
    r"^\s*(?:EXEC|EXECUTE|INSERT|MERGE|UPDATE|DELETE)\b", re.IGNORECASE
)
# Reload statements whose tables can be load sources of the app.
READ_QUERY_TYPES = {QueryType.SELECT, QueryType.INSERT}
QUOTE_CHARACTERS = '[]"`'
# A file an app's last reload stored or loaded, as Qlik records it, such as
# `STORE - lib://<space>:DataFiles/orders.qvd;` or `{lib://... (txt)};`.
RELOAD_FILE_RE = re.compile(
    r"^\{?(?P<store>STORE - )?(?P<path>lib://[^{}]+?)\}?;?$", re.IGNORECASE
)


class _ReloadFile(BaseModel):
    path: str
    stored: bool


class _GraphNamedApps(BaseModel):
    apps: Set[str]
    # Whether an app keeps the dataset from its last read, as this run could
    # not tell its sources, such as when its upstream graph could not be read.
    graph_unread: bool


class _ReloadFiles(BaseModel):
    files: List[_ReloadFile] = []
    # Statements that name a `lib://` path in a form RELOAD_FILE_RE doesn't know.
    unrecognised: List[str] = []


def _without_file_format(path: str) -> str:
    """The path without a trailing file format, such as `(qvd)` or
    `(ooxml, filters(Remove(Row, Pos(Top, 1))))`."""
    depth = 0
    for i in range(len(path) - 1, -1, -1) if path.endswith(")") else []:
        depth += {")": 1, "(": -1}.get(path[i], 0)
        if depth == 0:
            return path[:i].rstrip() if path[:i].endswith(" ") else path
    return path


def _reload_files(statements: List[ReloadStatement]) -> _ReloadFiles:
    result = _ReloadFiles()
    for statement in statements:
        discriminator = statement.discriminator.strip()
        match = RELOAD_FILE_RE.match(discriminator)
        if match:
            file = _ReloadFile(
                path=_without_file_format(match.group("path").strip()),
                stored=bool(match.group("store")),
            )
            if file not in result.files:
                result.files.append(file)
        elif "lib://" in discriminator.lower():
            result.unrecognised.append(discriminator)
    return result


def _is_ambiguous_connection(connection: str) -> bool:
    # Qlik load records drop the brackets around a name holding the separator,
    # so `team: sales:lake` may be space `team: sales` or connection `sales:lake`.
    return re.sub(r"\[[^\]]*\]", "", connection).count(SPACE_SEPARATOR) > 1


def _named_space(connection: str) -> Optional[str]:
    """The space a `<space>:<connection>` name, or the connection part of a
    `lib://` path, names."""
    if _is_ambiguous_connection(connection):
        return None
    return _lib_connection(connection).space or None


def _is_ambiguous_lib_path(path: str) -> bool:
    match = LIB_PATH_RE.match(path)
    return match is not None and _is_ambiguous_connection(match.group("connection"))


def _path_space(path: str) -> Optional[str]:
    match = LIB_PATH_RE.match(path)
    return _named_space(match.group("connection")) if match else None


def _path_key(path: str) -> str:
    # Qlik records loads in lowercase, and brackets around a space name with
    # spaces are optional.
    return path.lower().replace("[", "").replace("]", "")


def _lib_path_segments(path: str) -> Optional[List[str]]:
    """The space, connection, folders and file of a `lib://` path. The space
    is empty when the path doesn't name one."""
    match = LIB_PATH_RE.match(path)
    if not match or "$(" in path or _is_ambiguous_connection(match.group("connection")):
        return None
    connection = _lib_connection(match.group("connection"))
    return [
        # Optional brackets are kept as the script spelled them.
        f"[{name}]" if name and f"[{name}]" in match.group("connection") else name
        for name in (connection.space, connection.name)
    ] + _lib_path_parts(match.group("path"))


def _lib_path_parts(path: str) -> List[str]:
    return [part for part in re.split(r"[/\\]", path) if part]


def _lib_path(segments: List[str]) -> str:
    space, connection, *rest = segments
    space, connection = (
        name if name.startswith("[") else _lib_name(name)
        for name in (space, connection)
    )
    qualified = f"{space}{SPACE_SEPARATOR}{connection}" if space else connection
    return "/".join([f"lib://{qualified}", *rest])


def _path_key_parts(path: str) -> List[str]:
    return [part for part in re.split(r"[/\\]", _path_key(path)) if part]


def _path_has_label(path: str, label: str) -> bool:
    """Whether Qlik's label for the file, which holds its folders under the
    connection, is the whole path under the connection."""
    match = LIB_PATH_RE.match(path)
    return match is not None and _path_key_parts(
        match.group("path")
    ) == _path_key_parts(label)


def _label_parts(label: str) -> List[str]:
    return [part.strip().strip(QUOTE_CHARACTERS).lower() for part in label.split(".")]


def _names_identifier(sql: str, name: str) -> bool:
    """Whether lowercased SQL names `name` as a whole identifier, so that
    `orders_archive` doesn't count as naming `orders`."""
    return re.search(rf"(?<![\w$#@]){re.escape(name)}(?![\w$#@])", sql) is not None


# Comments, string literals, quoted identifiers, words and single characters.
SQL_TOKEN_RE = re.compile(
    r"/\*[\s\S]*?(?:\*/|\Z)|--[^\n]*"
    r"|'(?:[^']|'')*'?|\[[^\]]*\]?|\"[^\"]*\"?|`[^`]*`?|[\w$#@]+|\S"
)
# Keywords after which SQL names the tables it reads or writes.
TABLE_CLAUSE_KEYWORDS = {"from", "join", "into", "using", "update"}
# SQL Server allows `INSERT t`, `MERGE t` and `DELETE t` without INTO or FROM.
DML_STATEMENT_KEYWORDS = {"insert", "merge", "delete"}
# CROSS APPLY and OUTER APPLY start a table list again.
APPLY_PREFIX_KEYWORDS = {"cross", "outer"}
# Keywords that end such a list of tables.
TABLE_CLAUSE_END_KEYWORDS = {
    "where",
    "group",
    "order",
    "having",
    "union",
    "intersect",
    "except",
    "minus",
    "on",
    "inner",
    "left",
    "right",
    "full",
    "cross",
    "outer",
    "natural",
    "limit",
    "qualify",
    "window",
    "select",
    "values",
    "set",
    "fetch",
    "offset",
    "when",
    "then",
    "pivot",
    "unpivot",
}


# Keywords that end a table name but not the FROM clause it is in.
JOIN_KEYWORDS = {"on", "inner", "left", "right", "full", "cross", "outer", "natural"}


def _table_clause_names(sql: str) -> Set[str]:
    """The lowercased names in the table lists of SQL, after FROM, JOIN and
    INTO, so that a keyword such as ORDER in ORDER BY doesn't count as naming
    a table called `order`. Aliases and the parts of qualified names are
    included, and so are the names in string literals, which may hold SQL
    run dynamically."""
    names: Set[str] = set()
    in_clause = False
    # Still in a FROM clause after a join's ON condition, where a comma
    # starts another table, as in `FROM a JOIN b ON a.id = b.id, orders c`.
    in_from = False
    # Right after FROM, a comma or a dot, a keyword such as ORDER is a name.
    expects_name = False
    previous = ""
    statement_start = True
    stack: List[Tuple[bool, bool]] = []
    for token in SQL_TOKEN_RE.findall(sql):
        if token.startswith(("/*", "--")):
            continue
        is_word = re.match(r"[\w$#@]", token) is not None
        opens_clause = False
        if token.startswith("'"):
            names |= _table_clause_names(token.strip("'").replace("''", "'"))
        elif token == "(":
            stack.append((in_clause, in_from))
            in_clause = in_from = False
        elif token == ")":
            in_clause, in_from = stack.pop() if stack else (False, False)
        elif token == ";":
            in_clause = in_from = False
        elif token == "," and in_from and not in_clause:
            in_clause = opens_clause = True
        elif token[0] in QUOTE_CHARACTERS:
            if in_clause:
                names.add(token.strip(QUOTE_CHARACTERS).strip().lower())
        elif is_word:
            word = token.lower()
            if (statement_start and word in DML_STATEMENT_KEYWORDS) or (
                previous in DML_STATEMENT_KEYWORDS and word in {"into", "from"}
            ):
                in_clause = opens_clause = True
            elif in_clause and expects_name:
                names.add(word)
            elif word in TABLE_CLAUSE_KEYWORDS or (
                word == "apply" and previous in APPLY_PREFIX_KEYWORDS
            ):
                in_clause = opens_clause = True
            elif word in TABLE_CLAUSE_END_KEYWORDS:
                in_clause = False
                in_from = in_from and word in JOIN_KEYWORDS
            elif in_clause:
                names.add(word)
        if opens_clause:
            in_from = True
        expects_name = opens_clause or (in_clause and token in {",", "."})
        previous = token.lower() if is_word else ""
        statement_start = token == ";"
    return names


def _string_literal_table_names(sql: str) -> Set[str]:
    return {
        name
        for token in SQL_TOKEN_RE.findall(sql)
        if token.startswith("'")
        for name in _table_clause_names(token.strip("'").replace("''", "'"))
    }


def _database_qri_platform(source_qri: str) -> str:
    scheme = source_qri[len(DATABASE_QRI_PREFIX) :].partition("://")[0].lower()
    # A scheme that isn't mapped is compared as is, such as `oracle`, so one
    # that names no DataHub platform matches nothing.
    return KNOWN_DATA_PLATFORM_MAPPING.get(scheme, scheme)


def _urn_names_table(urn: str, source_qri: str, label: str) -> bool:
    """Whether a dataset URN is the database table a lineage graph node, such
    as `qri:db:sqlserver://<hash>#<hash>` labelled `dbo.orders`, names. The
    label leaves out the database, and the URN's case may differ from it."""
    dataset = DatasetUrn.from_string(urn)
    platform = _database_qri_platform(source_qri)
    if dataset.platform != builder.make_data_platform_urn(platform):
        return False
    parts = _label_parts(label)
    return dataset.name.lower().split(".")[-len(parts) :] == parts


class _ReloadSql(BaseModel):
    """A SQL statement of an app's last reload, with the tables it reads."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    upstream_urns: List[str]
    result: SqlParsingResult
    # The tables DataHub holds no schema for, when DataHub was asked.
    urns_without_schema: List[str] = []


class _ParsedReloadSql(BaseModel):
    statements: List[_ReloadSql] = []
    # The names in the table lists of statements that may name a label but
    # were not read, for an unresolved connection, a parse error or dynamic
    # SQL, and the names of tables read without their database or schema.
    skipped_table_names: Set[str] = set()
    # A statement kept a variable Qlik did not expand, so it may name any
    # table.
    unexpanded_variable: bool = False


class _UntracedSources(BaseModel):
    """Database sources of a graph table that the load script doesn't trace
    and whose labels may name the same table, such as `dbo.orders` in two
    databases."""

    name: str
    labels: Dict[str, str]
    # How many of them the script traced to a table any of them may name, so
    # without telling which.
    traced_count: int = 0
    # The tables the script linked, which the reload SQL can't link again.
    script_urns: List[str] = []


def _untraced_source_groups(
    labels: Dict[str, str], script_urns: List[str]
) -> List[_UntracedSources]:
    """The database sources, by graph QRI and label, that the script's tables
    don't trace. A script table traces a source only when it is the one source
    of the graph table that could be it."""
    groups: Dict[Tuple[str, str], Dict[str, str]] = {}
    for qri, label in labels.items():
        key = (_database_qri_platform(qri), _label_parts(label)[-1])
        groups.setdefault(key, {})[qri] = label
    untraced: List[_UntracedSources] = []
    for (_, name), group in groups.items():
        sources = _UntracedSources(
            name=name, labels=dict(group), script_urns=script_urns
        )
        for urn in script_urns:
            named = [
                qri for qri, label in group.items() if _urn_names_table(urn, qri, label)
            ]
            if len(named) == 1:
                sources.labels.pop(named[0], None)
            elif named:
                sources.traced_count += 1
        if len(sources.labels) > sources.traced_count:
            untraced.append(sources)
    return untraced


def _merged_lineage(
    script_table: ScriptTableLineage, reload_table: ScriptTableLineage
) -> ScriptTableLineage:
    upstream_urns = list(script_table.upstream_urns)
    upstream_urns.extend(
        urn for urn in reload_table.upstream_urns if urn not in upstream_urns
    )
    column_upstreams = {
        column: list(upstreams)
        for column, upstreams in script_table.column_upstreams.items()
    }
    # The parser may fold the case of the script's columns, so the reload
    # SQL's field joins the script's column of the same name in any case.
    script_columns = {column.lower(): column for column in column_upstreams}
    for column, upstreams in reload_table.column_upstreams.items():
        merged = column_upstreams.setdefault(
            script_columns.get(column.lower(), column), []
        )
        merged.extend(upstream for upstream in upstreams if upstream not in merged)
    return script_table.model_copy(
        update={"upstream_urns": upstream_urns, "column_upstreams": column_upstreams}
    )


def _own_table_urns(app_id: str, urns: Dict[str, str]) -> Dict[str, str]:
    return {
        qri: urn for qri, urn in urns.items() if _qlik_app_id(qri) in (None, app_id)
    }


def _qlik_app_id(qri: str) -> Optional[str]:
    match = QLIK_APP_QRI_RE.match(qri)
    return match.group(1) if match else None


# A table Qlik renamed because its name was taken, such as `Orders-1`.
DUPLICATE_TABLE_NAME_RE = re.compile(r"^(?P<name>.+)-\d+$")


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


def _schema_field_parent(urn: str) -> Optional[str]:
    # Lineage another tool wrote may hold any kind of URN.
    try:
        return SchemaFieldUrn.from_string(urn).parent
    except Exception:
        return None


def _app_field_names(app: App) -> List[str]:
    names = [field.name for table in app.tables for field in table.datasetSchema]
    names.extend(
        field.name for table in app.data_model_tables for field in table.fields
    )
    names.extend(field for table in app.lineage_tables for field in table.fields)
    return list(dict.fromkeys(names))


def _qlik_field_id(name: str) -> str:
    """The last part of a field node's QRI in Qlik's lineage graph, which is the
    unpadded base64url SHA-256 of the field name. Not documented by Qlik, so it
    is only used to confirm a name, never to make one up."""
    digest = hashlib.sha256(name.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def _refs_of_field_sources(
    refs: List[ColumnRef], field_name: str, field_ids: Set[str]
) -> List[ColumnRef]:
    """The columns whose name Qlik's graph hashes into one of `field_ids`.
    Qlik hashes the name the database returned, whose case the SQL and DataHub
    may not share, so a column also matches the Qlik field it loads when that
    field kept the source's name, unless another column differs from it only
    in case."""
    exact = [ref for ref in refs if _qlik_field_id(ref.column) in field_ids]
    if exact or _qlik_field_id(field_name) not in field_ids:
        return exact
    folded = [ref for ref in refs if ref.column.lower() == field_name.lower()]
    return folded if len({ref.column for ref in folded}) == 1 else []


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


def _patch_workunits(
    patch: MetadataPatchProposal, is_primary_source: bool
) -> Iterable[MetadataWorkUnit]:
    for mcp in patch.build():
        yield MetadataWorkUnit(
            id=MetadataWorkUnit.generate_workunit_id(mcp),
            mcp_raw=mcp,
            is_primary_source=is_primary_source,
        )


def _bounded_list(items: List[str], limit: int = 10) -> str:
    more = f" (and {len(items) - limit} more)" if len(items) > limit else ""
    return ", ".join(items[:limit]) + more


def _error_type(error: Optional[Exception]) -> str:
    # SQL parser errors quote the statement, which can hold credentials when a
    # script statement was split wrongly, so only the type goes in the report.
    return type(error).__name__ if error else "unknown"


@platform_name("Qlik Sense")
@config_class(QlikSourceConfig)
@support_status(SupportStatus.ALPHA)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default.",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, can be disabled via configuration `include_column_lineage`",
)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.OWNERSHIP,
    "Enabled by default, configured using `ingest_owner`",
)
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class QlikSenseSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts the following:
    - Qlik Sense Spaces (Cloud) or Streams (Enterprise on Windows) and Apps as Container.
    - Qlik Datasets (Cloud)
    - Tables of each app's data model
    - Sheets as dashboard and its charts
    """

    config: QlikSourceConfig
    reporter: QlikSourceReport
    platform: str = "qlik-sense"
    qlik_api: QlikBaseAPI

    def __init__(self, config: QlikSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.reporter = QlikSourceReport()
        self.ui_host = config.ui_hostname()
        self._data_connections: Optional[Dict[str, DataConnectionInfo]] = None
        # The files each app's load script reads, to resolve the files in its
        # lineage graph with.
        self._file_sources_by_app: Dict[str, FileSourceResolver] = {}
        self._app_space_ids: Dict[str, str] = {}
        self._apps_variables_unread: Set[str] = set()
        # The tables whose lineage is only added to, by app and reason, reported
        # once per app at the end of the run.
        self._only_added_tables: Dict[Tuple[str, str], List[str]] = {}
        # Apps that load files by a wildcard or a path the connector can't
        # expand, so the files in their lineage graph can't all be named.
        self._apps_file_paths_unknown: Set[str] = set()
        self._apps_with_includes: Set[str] = set()
        self._reported_include_apps: Set[str] = set()
        self._resolved_file_qris: Set[str] = set()
        # Files on connections of unknown storage, by URN, as `lib://` paths,
        # and those that lineage links to, which get a dataset.
        self._unmapped_files: Dict[str, str] = {}
        # How load scripts spell each `lib://` path prefix, by its lowercase
        # key, and the number of paths it was built from.
        self._script_spellings: Dict[str, Counter[str]] = {}
        self._script_spellings_built_from = -1
        self._linked_unmapped_files: Set[str] = set()
        # Tables kept apps store into files on storage that was unknown when
        # their last read recorded them.
        self._kept_storage_file_stores: Dict[str, List[str]] = {}
        # The tables each app stores into such a file, linked at the end of
        # the run only if some app's lineage graph reads the file, as a STORE
        # in the script may never run.
        self._unmapped_file_stores: DefaultDict[str, Dict[str, List[str]]] = (
            defaultdict(dict)
        )
        # Datasets emitted at the end of the run for the apps whose lineage
        # names them, added to those apps' checkpoints so a later run that
        # keeps an app keeps them too.
        self._app_end_of_run_urns: DefaultDict[str, Set[str]] = defaultdict(set)
        # Unmapped files each app's lineage read this run.
        self._app_read_unmapped_files: DefaultDict[str, Set[str]] = defaultdict(set)
        # Upstream graphs of the apps read this run, fetched when a source only
        # they can name is met, None when the read failed, and the resources
        # only they name, by URN, with their labels and the app tables storing
        # into them.
        self._app_qris: Dict[str, str] = {}
        self._lineage_upstreams: Dict[str, Optional[LineageUpstreams]] = {}
        self._upstream_graph_read_failures: List[str] = []
        # Apps whose last graph-named datasets are kept, as this run could not
        # tell whether they are still sources.
        self._apps_graph_named_kept: Set[str] = set()
        self._graph_named_resources: Dict[str, str] = {}
        self._graph_resource_writers: Dict[str, Set[str]] = {}
        # The files apps stored or loaded in their last reload, by app id.
        self._reload_files: Dict[str, List[_ReloadFile]] = {}
        # Apps whose last reload's statements could not be read.
        self._reload_files_unread: Set[str] = set()
        self._reload_path_failures: List[str] = []
        # Spaces the API key can't see that lineage goes through, by lowercased
        # name, with the name as written and what lineage needs from them.
        self._unseen_spaces: Dict[str, Tuple[str, Set[str]]] = {}
        self._listed_space_names: Optional[Set[str]] = None
        # The apps whose upstream graph names each graph-named dataset.
        self._graph_resource_readers: Dict[str, Set[str]] = {}
        self._allowed_space_ids: Set[str] = set()
        self._reported_no_graph = False
        # Datasets with a column edge whose source field could not be named.
        self._unnamed_field_lineage: Set[str] = set()
        # A listed QVD of an allowed space had no secure QRI to match.
        self._datasets_without_qri = False
        # Every listed dataset, so one in a space the recipe leaves out is not
        # named from the lineage graph instead.
        self._listed_dataset_qris: Set[str] = set()
        # A listed QVD of a left-out space had no secure QRI to match.
        self._excluded_datasets_without_qri = False
        # App tables whose lineage waits for field names read later in the run.
        self._pending_table_lineage: List[PendingTableLineage] = []
        self._pending_stored_files: List[PendingStoredFile] = []
        # Table QRI -> dataset URN for the tables of apps read or kept this run.
        # Lineage only links to tables in here, as other URNs may never exist.
        self._app_table_urns_by_qri: Dict[str, str] = {}
        # The table URNs of each app read or kept this run.
        self._table_urns_by_app: Dict[str, Set[str]] = {}
        # Tables the last run recorded for apps whose tables are known this
        # run, so edges from a table since renamed or dropped are still owned.
        self._previous_app_table_urns: Set[str] = set()
        # Apps read or kept this run, by whether all their tables are known.
        self._apps_tables_known: Set[str] = set()
        self._apps_tables_unknown: Set[str] = set()
        # Apps whose tables are known, but whose lineage graph names tables
        # with no URN, so only references to those tables are unknown.
        self._apps_graph_tables_unresolved: Set[str] = set()
        # Fingerprints of apps with table lineage still pending, written after
        # it so a failed run can't skip the app as unchanged without it.
        self._deferred_fingerprints: Dict[str, MetadataWorkUnit] = {}
        # On-prem, the QVDs each app read or kept this run writes and reads, and
        # the apps whose QVDs are unknown.
        self._app_qvds: Dict[str, AppQvds] = {}
        # Apps whose QVD writes are not known this run, with why.
        self._apps_qvds_unknown: Dict[str, str] = {}
        # On Qlik Cloud, the files on storage each app read this run stores into.
        self._app_stored_files: Dict[str, List[str]] = {}
        self._unresolved_qvd_loads = 0
        self._unlabelled_qvd_loads = 0
        # On-prem, the URNs each app BINARY loads come from produced, read or
        # kept this run, the data model fields of those apps, the tables linked
        # to them once every app has been read, and the apps whose read
        # ingested tables a BINARY statement brought in.
        self._binary_source_apps: Set[str] = (
            set(self.config.on_prem.binary_file_to_app.values())
            if self.config.on_prem is not None
            else set()
        )
        self._app_urns: Dict[str, Set[str]] = {}
        # Apps whose last read is kept, whose checkpoints may hold unmapped
        # files they linked then.
        self._kept_app_ids: Set[str] = set()
        self._app_model_fields: Dict[str, Dict[str, List[str]]] = {}
        self._binary_links: List[_BinaryLink] = []
        self._apps_with_binary_tables: Set[str] = set()
        self._last_parsed: Optional[_ParsedApp] = None
        # The data model fields of the app being emitted, by table identifier.
        self._data_model_fields: Dict[str, List[QlikDatasetSchemaField]] = {}
        # Datasets and the app table QRIs stored into them, linked once every
        # app has been read.
        self._dataset_store_sources: Dict[str, List[str]] = {}
        self._dataset_store_field_sources: Dict[str, Dict[str, List[str]]] = {}
        self._dataset_store_fields_unknown: Set[str] = set()
        # Graph tables whose fields have no type, such as a table the script
        # drops after storing it, by URN.
        self._untyped_tables: Dict[str, Tuple[str, List[QlikDatasetSchemaField]]] = {}
        self._dataset_field_types: Dict[str, Dict[str, Optional[str]]] = {}
        # Field names seen this run, by their field id in Qlik's lineage graph.
        self._field_names_by_id: Dict[str, str] = {}
        # Owned here rather than by the automatic processor so that entities of
        # apps skipped as unchanged or only partly read can be kept in the state.
        self.stale_entity_removal_handler: Optional[StaleEntityRemovalHandler] = (
            StaleEntityRemovalHandler(
                state_provider=self.state_provider,
                report=self.reporter,
                config=self.config,
                state_type_class=GenericCheckpointState,
                pipeline_name=ctx.pipeline_name,
                run_id=ctx.run_id,
                platform=self.platform,
            )
        )
        self.app_state = QlikAppStateHandler(
            state_provider=self.state_provider,
            config=self.config,
            pipeline_name=ctx.pipeline_name,
            run_id=ctx.run_id,
            stale_job_id=self.stale_entity_removal_handler.job_id
            if self.stale_entity_removal_handler
            and self.stale_entity_removal_handler.is_checkpointing_enabled()
            else None,
        )
        self._fingerprint_salt: Optional[str] = None
        # Filled from worker threads, so only copied to the report at the end.
        self._change_check_failures: List[str] = []
        if config.skips_unchanged_apps() and not self._skips_unchanged_apps():
            self.reporter.warning(
                title="skip_unchanged_apps is ignored",
                message="Skipping unchanged apps needs stateful ingestion, so every "
                "app is read.",
            )
        try:
            self.qlik_api = self._create_api(self.config)
        except Exception as e:
            logger.warning(e)
            exit(
                1
            )  # Exit pipeline as we are not able to connect to Qlik Client Service.

    @staticmethod
    def _create_api(config: QlikSourceConfig) -> QlikBaseAPI:
        return QlikOnPremAPI(config) if config.on_prem else QlikAPI(config)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = QlikSourceConfig.parse_obj_allow_extras(config_dict)
            api = QlikSenseSource._create_api(config)
            if isinstance(api, QlikOnPremAPI):
                api.check_engine_port()
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    @classmethod
    def create(cls, config_dict, ctx):
        config = QlikSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_excluded_workunit_processors(self):
        return (
            [AutoStaleEntityRemovalProcessor]
            if self.stale_entity_removal_handler
            else []
        )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        processors = super().get_workunit_processors()
        if self.stale_entity_removal_handler:
            processors.append(
                partial(auto_stale_entity_removal, self.stale_entity_removal_handler)
            )
        return processors

    def _skips_unchanged_apps(self) -> bool:
        return (
            self.config.skips_unchanged_apps()
            and self.app_state.is_checkpointing_enabled()
        )

    def _init_fingerprint_salt(self) -> None:
        # The recipe and data connections shape every app's output, and a new
        # connector version may emit differently, so any change re-reads all apps.
        self._fingerprint_salt = json.dumps(
            {
                "version": __version__,
                "config": self.config.model_dump(
                    mode="json",
                    exclude={
                        "stateful_ingestion": True,
                        "max_workers": True,
                        "max_partial_reads_kept": True,
                        "on_prem": {"skip_unchanged_apps"},
                    },
                ),
                "connections": {
                    name: info.model_dump(mode="json")
                    for name, info in sorted(self._get_data_connections().items())
                },
            },
            sort_keys=True,
        )

    def _salted_fingerprint(self, fingerprint: str) -> str:
        assert self._fingerprint_salt is not None
        return hashlib.sha256(
            f"{self._fingerprint_salt}|{fingerprint}".encode("utf-8")
        ).hexdigest()

    def _usable_checkpoint(
        self, app_id: str, failures: List[str]
    ) -> Optional[AppCheckpoint]:
        """The app's last read, when it can stand in for this run's read: it
        is recorded by the run the stale entity removal state comes from, and
        it holds the entities to keep. A failed lookup goes to `failures`."""
        try:
            previous = self.app_state.previous(app_id)
        except Exception as e:
            logger.debug(f"Unable to load the last checkpoint of app {app_id}: {e}")
            failures.append(f"{app_id}: unable to load the last checkpoint: {e}")
            return None
        return previous if previous is not None and previous.urns else None

    def _is_app_unchanged(self, app_id: str, fingerprint: str) -> bool:
        previous = self._usable_checkpoint(app_id, self._change_check_failures)
        if previous is None or previous.fingerprint != self._salted_fingerprint(
            fingerprint
        ):
            return False
        if self.config.on_prem is not None and (
            previous.qvds is None or previous.binary_tables is None
        ):
            # Recorded before QVD or BINARY lineage, which are only known by
            # reading it again.
            return False
        # Checkpoints are committed even when the sink failed, so only trust one
        # whose metadata actually reached DataHub and was not deleted since.
        graph = self.ctx.graph
        if graph is None:
            return False
        container_urn = self._gen_app_key(app_id).as_urn()
        try:
            properties = graph.get_aspect(container_urn, ContainerPropertiesClass)
            if (
                properties is None
                or (properties.customProperties or {}).get(Constant.FINGERPRINT)
                != previous.fingerprint
            ):
                return False
            return self._entities_present(graph, previous.urns)
        except Exception as e:
            logger.debug(f"Unable to read app container {container_urn}: {e}")
            self._change_check_failures.append(
                f"{app_id}: unable to read the app back from DataHub: {e}"
            )
            return False

    @staticmethod
    def _entities_present(graph: DataHubGraph, urns: List[str]) -> bool:
        """Whether every entity the app's last read produced is in DataHub and
        not soft-deleted, which also catches entities the sink failed to write
        and hard-deleted ones. Entities stale removal deleted after they left
        Qlik are not in the checkpoint."""
        by_type: Dict[str, List[str]] = {}
        for urn in urns:
            by_type.setdefault(guess_entity_type(urn), []).append(urn)
        for entity_type, typed_urns in by_type.items():
            for start in range(0, len(typed_urns), ENTITY_CHECK_BATCH_SIZE):
                batch = typed_urns[start : start + ENTITY_CHECK_BATCH_SIZE]
                entities = graph.get_entities(
                    entity_type, batch, aspects=[Status.ASPECT_NAME]
                )
                for urn in batch:
                    status = entities.get(urn, {}).get(Status.ASPECT_NAME)
                    if status is None or getattr(status[0], "removed", True):
                        return False
        return True

    def _keep_previous_read(self, app_id: str) -> Optional[AppCheckpoint]:
        try:
            previous = self.app_state.previous(app_id)
        except Exception as e:
            # Reporting a failure stops stale entity removal from soft-deleting
            # what the app's last read produced.
            self.reporter.failure(
                title="Unable to load the last checkpoint of a Qlik app",
                message="Entities from the app's last read could not be kept, so "
                "stale metadata removal is skipped for this run.",
                context=app_id,
                exc=e,
            )
            self._apps_tables_unknown.add(app_id)
            self._apps_qvds_unknown[app_id] = "its last read could not be loaded"
            return None
        if previous is None:
            self._apps_tables_unknown.add(app_id)
            self._apps_qvds_unknown[app_id] = "it has no last read to keep"
            return None
        self._kept_app_ids.add(app_id)
        if app_id in self._binary_source_apps:
            self._app_urns.setdefault(app_id, set()).update(previous.urns)
        if previous.binary_tables:
            self._apps_with_binary_tables.add(app_id)
        if previous.data_model_fields is not None:
            self._app_model_fields[app_id] = previous.data_model_fields
        if previous.qvds is None:
            self._apps_qvds_unknown[app_id] = (
                "its last read was recorded before QVD lineage"
            )
        else:
            self._app_qvds[app_id] = previous.qvds
            if not previous.qvds.writes_complete:
                self._apps_qvds_unknown[app_id] = UNRESOLVED_STORE
        if previous.field_names:
            self._learn_field_names(previous.field_names)
        unmapped_stores: Dict[str, UnmappedFileStore] = {}
        stored_file_urns = list(previous.stored_file_urns)
        for file_urn, store in previous.unmapped_file_stores.items():
            storage_file = self._storage_of_unmapped_store(store)
            if storage_file is None:
                unmapped_stores[file_urn] = store
                self._add_unmapped_file(file_urn, store.path)
                urns = self._unmapped_file_stores[file_urn].setdefault(app_id, [])
                urns.extend(urn for urn in store.table_urns if urn not in urns)
                continue
            # Its storage is known now, so the file is the storage file a full
            # read links, not the Qlik dataset named by its connection.
            if storage_file.urn is not None:
                tables = self._kept_storage_file_stores.setdefault(storage_file.urn, [])
                tables.extend(urn for urn in store.table_urns if urn not in tables)
                if storage_file.urn not in stored_file_urns:
                    stored_file_urns.append(storage_file.urn)
        dropped = (
            set(previous.unmapped_file_stores)
            - set(unmapped_stores)
            - set(previous.read_unmapped_file_urns)
        )
        previous = previous.model_copy(
            update={
                "urns": [urn for urn in previous.urns if urn not in dropped],
                "unmapped_file_stores": unmapped_stores,
                "stored_file_urns": sorted(stored_file_urns),
            }
        )
        if previous.table_urns_by_qri is not None:
            kept = _own_table_urns(app_id, previous.table_urns_by_qri)
            self._app_table_urns_by_qri.update(kept)
            self._table_urns_by_app[app_id] = set(kept.values())
        # None was recorded before table URNs were kept in the state.
        if previous.table_urns_by_qri is None or previous.tables_unknown:
            self._apps_tables_unknown.add(app_id)
        else:
            self._apps_tables_known.add(app_id)
            if previous.graph_tables_unresolved:
                self._apps_graph_tables_unresolved.add(app_id)
        assert self.stale_entity_removal_handler is not None
        for urn in previous.urns:
            self.stale_entity_removal_handler.add_entity_to_state(
                guess_entity_type(urn), urn
            )
        return previous

    def _storage_of_unmapped_store(
        self, store: UnmappedFileStore
    ) -> Optional[_StorageFile]:
        """The storage file a kept app's store into a file on a connection of
        unknown storage now resolves to, or None while that storage is still
        unknown, as when the data connections or Qlik datasets could not be
        listed."""
        lib_path = LIB_PATH_RE.match(store.path)
        if lib_path is None:
            return None
        # The recorded connection is space-qualified, so no app space applies.
        storage, detail = self._file_storage(lib_path.group("connection"), "")
        if storage is None:
            return None
        path = lib_path.group("path")
        catalogued = self._catalogued_as_qlik_dataset(storage, path)
        # A full read keeps last run's edges when the datasets can't be listed.
        if catalogued is None:
            return None
        # A file Qlik catalogues is not linked on storage.
        if catalogued:
            return _StorageFile(urn=None)
        return _StorageFile(urn=self._file_urn(storage, detail, path))

    def _keep_unchanged_app(self, app: UnchangedApp) -> None:
        previous = self._keep_previous_read(app.id)
        # Without the kept entities, which _keep_previous_read reports as a
        # failure, nothing is recorded, so the next run reads the app again.
        if previous is not None and previous.urns:
            self.app_state.record(
                app.id,
                AppCheckpoint(
                    fingerprint=self._salted_fingerprint(app.fingerprint),
                    urns=previous.urns,
                    table_urns_by_qri=previous.table_urns_by_qri,
                    tables_unknown=previous.tables_unknown,
                    graph_tables_unresolved=previous.graph_tables_unresolved,
                    qvds=self._app_qvds.get(app.id),
                    data_model_fields=self._app_model_fields.get(app.id),
                    binary_tables=previous.binary_tables,
                    stored_file_urns=previous.stored_file_urns,
                    unmapped_file_stores=previous.unmapped_file_stores,
                    read_unmapped_file_urns=previous.read_unmapped_file_urns,
                    field_names=previous.field_names,
                ),
            )
        self.reporter.apps_skipped_unchanged += 1

    def _keep_partly_read_app(self, app: App) -> _PartialRead:
        """Keeps what the app's last read produced instead of emitting a partial
        read, which would overwrite its relationships, for up to
        max_partial_reads_kept consecutive runs."""
        failures: List[str] = []
        previous = self._usable_checkpoint(app.id, failures)
        for failure in failures:
            self.reporter.warning(
                title="Unable to load the last checkpoint of a Qlik app",
                message="The partly read app could not keep its last read.",
                context=failure,
            )
        if previous is None:
            return _PartialRead.NOT_KEPT
        if previous.partial_reads >= self.config.max_partial_reads_kept:
            return _PartialRead.LIMIT_REACHED
        kept = self._keep_previous_read(app.id)
        if kept is None or not kept.urns:
            return _PartialRead.NOT_KEPT
        # Its QVD writes are from its last full read and may have changed, so
        # they protect its QVD edges without replacing anyone else's.
        self._apps_qvds_unknown.setdefault(
            app.id, "it was partly read, so its QVDs are from its last full read"
        )
        # No fingerprint, so the next run reads the app again.
        self.app_state.record(
            app.id,
            AppCheckpoint(
                fingerprint=None,
                urns=kept.urns,
                partial_reads=previous.partial_reads + 1,
                table_urns_by_qri=previous.table_urns_by_qri,
                tables_unknown=previous.tables_unknown,
                graph_tables_unresolved=previous.graph_tables_unresolved,
                qvds=previous.qvds,
                data_model_fields=previous.data_model_fields,
                binary_tables=previous.binary_tables,
                stored_file_urns=kept.stored_file_urns,
                unmapped_file_stores=kept.unmapped_file_stores,
                read_unmapped_file_urns=previous.read_unmapped_file_urns,
                field_names=previous.field_names,
            ),
        )
        return _PartialRead.KEPT

    def _gen_recorded_app_workunit(self, app: App) -> Iterable[MetadataWorkUnit]:
        fingerprint = (
            self._salted_fingerprint(app.fingerprint)
            if app.fingerprint
            and app.read_complete
            and not app.data_load_failed
            # Its connection paths were unresolved, so it is read again next run.
            and not self._data_connections_unread()
            else None
        )
        urns: Dict[str, None] = {}
        for wu in self._gen_app_workunit(app, fingerprint):
            urn = wu.get_urn()
            if guess_entity_type(urn) not in STATEFUL_INGESTION_IGNORED_ENTITY_TYPES:
                urns[urn] = None
            yield wu
        if not app.read_complete:
            # Only reached without a usable last read, and reported as a failure.
            # Recording the partial entity list as the app's last read would let
            # a later partial read keep only those entities.
            return
        # A complete read records only what it produced, so entities removed in
        # Qlik are soft-deleted even when the change check keeps failing. No
        # fingerprint never matches, so such an app is read again next run.
        self.app_state.record(
            app.id,
            AppCheckpoint(
                fingerprint=fingerprint,
                urns=list(urns),
                table_urns_by_qri=self._app_table_urns(app),
                # So a later run that keeps the app can't take its tables as
                # complete, nor a 404 as the app having no tables.
                tables_unknown=self._tables_unknown(app),
                graph_tables_unresolved=self._graph_tables_unresolved(app),
                qvds=self._app_qvds.get(app.id),
                data_model_fields=self._app_model_fields.get(app.id),
                binary_tables=app.id in self._apps_with_binary_tables,
                stored_file_urns=self._app_stored_files.get(app.id, []),
                unmapped_file_stores={
                    file_urn: UnmappedFileStore(
                        path=self._unmapped_files[file_urn], table_urns=stores[app.id]
                    )
                    for file_urn, stores in self._unmapped_file_stores.items()
                    if app.id in stores
                },
                field_names=_app_field_names(app),
            ),
        )

    def _gen_space_key(self, space_id: str) -> SpaceKey:
        return SpaceKey(
            space=space_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _gen_app_key(self, app_id: str) -> AppKey:
        return AppKey(
            app=app_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _get_audit_stamp(
        self, date: datetime, username: Optional[str]
    ) -> AuditStampClass:
        return AuditStampClass(
            time=int(date.timestamp() * 1000),
            actor=builder.make_user_urn(username) if username else builder.UNKNOWN_USER,
        )

    def _get_owner_name(self, owner_id: Optional[str]) -> Optional[str]:
        return self.qlik_api.get_user_name(owner_id) if owner_id else None

    def _get_allowed_spaces(self) -> List[Space]:
        all_spaces = self.qlik_api.get_spaces()
        allowed_spaces = [
            space
            for space in all_spaces
            if self.config.space_pattern.allowed(space.name)
        ]
        logger.info(f"Number of spaces = {len(all_spaces)}")
        self.reporter.report_number_of_spaces(len(all_spaces))
        logger.info(f"Number of allowed spaces = {len(allowed_spaces)}")
        return allowed_spaces

    def _gen_space_workunit(self, space: Space) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik space to Datahub container
        """
        owner_username = self._get_owner_name(space.ownerId)
        yield from gen_containers(
            container_key=self._gen_space_key(space.id),
            name=space.name,
            description=space.description,
            sub_types=[BIContainerSubTypes.QLIK_SPACE],
            extra_properties={Constant.TYPE: str(space.type)},
            owner_urn=(
                builder.make_user_urn(owner_username)
                if self.config.ingest_owner and owner_username
                else None
            ),
            external_url=f"https://{self.ui_host}/hub/stream/{space.id}"
            if self.config.on_prem
            else f"https://{self.ui_host}/catalog?space_filter={space.id}",
            created=int(space.createdAt.timestamp() * 1000),
            last_modified=int(space.updatedAt.timestamp() * 1000),
        )

    def _gen_entity_status_aspect(self, entity_urn: str) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn, aspect=Status(removed=False)
        ).as_workunit()

    def _gen_entity_owner_aspect(
        self, entity_urn: str, user_name: str
    ) -> MetadataWorkUnit:
        aspect = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=builder.make_user_urn(user_name),
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        )
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=aspect,
        ).as_workunit()

    def _gen_dashboard_urn(self, dashboard_identifier: str) -> str:
        return builder.make_dashboard_urn(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            name=dashboard_identifier,
        )

    def _gen_dashboard_info_workunit(
        self, sheet: Sheet, app_id: str
    ) -> MetadataWorkUnit:
        dashboard_urn = self._gen_dashboard_urn(sheet.id)
        custom_properties: Dict[str, str] = {"chartCount": str(len(sheet.charts))}
        dashboard_info_cls = DashboardInfoClass(
            title=sheet.title,
            description=sheet.description,
            charts=[
                builder.make_chart_urn(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    name=chart.qId,
                )
                for chart in sheet.charts
            ],
            lastModified=ChangeAuditStampsClass(
                created=self._get_audit_stamp(sheet.createdAt, sheet.ownerId),
                lastModified=self._get_audit_stamp(sheet.updatedAt, sheet.ownerId),
            ),
            customProperties=custom_properties,
            dashboardUrl=f"https://{self.ui_host}/sense/app/{app_id}/sheet/{sheet.id}/state/analysis",
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=dashboard_info_cls
        ).as_workunit()

    def _chart_inputs(
        self,
        charts: Iterable[Chart],
        variables: Dict[str, str],
        app_fields: AppFieldIndex,
        input_tables_urns: List[str],
        has_unknown_schema: bool,
    ) -> Dict[str, ChartInputs]:
        inputs = {}
        for chart in charts:
            # Read with the app's variables expanded; the chart keeps its
            # expressions as Qlik has them.
            expanded = chart.model_copy(
                update={
                    "field_expressions": [
                        expand_variables(expression, variables)
                        for expression in chart.field_expressions
                    ]
                }
            )
            fields = self._get_chart_input_fields(expanded, app_fields)
            inputs[chart.qId] = ChartInputs(
                fields=fields,
                tables=self._get_chart_inputs(
                    expanded,
                    fields,
                    app_fields,
                    input_tables_urns,
                    has_unknown_schema,
                ),
            )
        return inputs

    def _gen_charts_workunit(
        self,
        charts: List[Chart],
        chart_inputs: Optional[Dict[str, ChartInputs]],
        app_id: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Map Qlik charts to DataHub charts. `chart_inputs` is None when the
        app's tables are not all known."""
        for chart in charts:
            chart_urn = builder.make_chart_urn(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                name=chart.qId,
            )

            yield self._gen_entity_status_aspect(chart_urn)

            custom_properties = {
                "Dimension": str(chart.qDimension),
                "Measure": str(chart.qMeasure),
            }
            inputs = chart_inputs.get(chart.qId) if chart_inputs is not None else None

            if inputs is None:
                # Unknown inputs would overwrite the ones the last full read
                # emitted, so only the other chart fields are patched.
                yield from self._gen_chart_info_patch(
                    chart_urn, chart, custom_properties
                )
            else:
                yield MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=ChartInfoClass(
                        title=chart.title,
                        description=chart.visualization,
                        lastModified=ChangeAuditStampsClass(),
                        customProperties=custom_properties,
                        inputs=inputs.tables,
                    ),
                ).as_workunit()

            # Emitted even when empty so fields a chart stopped using are cleared,
            # but not from a partial list of a chart whose measures were unread.
            if inputs is not None and not chart.fields_unread:
                yield MetadataChangeProposalWrapper(
                    entityUrn=chart_urn,
                    aspect=InputFieldsClass(
                        fields=[
                            InputFieldClass(
                                schemaFieldUrn=builder.make_schema_field_urn(
                                    field.dataset_urn, field.field.name
                                ),
                                schemaField=self._gen_schema_field(field.field),
                            )
                            for field in inputs.fields
                        ]
                    ),
                ).as_workunit()

            yield from add_entity_to_container(
                container_key=self._gen_app_key(app_id),
                entity_type="chart",
                entity_urn=chart_urn,
            )

    @staticmethod
    def _gen_chart_info_patch(
        chart_urn: str, chart: Chart, custom_properties: Dict[str, str]
    ) -> Iterable[MetadataWorkUnit]:
        # GMS creates missing chart info from its patch template, which also
        # stamps lastModified. An empty stamp here would erase the last one.
        patch = ChartPatchBuilder(chart_urn)
        if chart.title:
            patch.set_title(chart.title)
        if chart.visualization:
            patch.set_description(chart.visualization)
        for key, value in custom_properties.items():
            patch.add_custom_property(key, value)
        for mcp in patch.build():
            yield MetadataWorkUnit(
                id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
            )

    @staticmethod
    def _chart_field_names(expression: str, app_fields: AppFieldIndex) -> List[str]:
        # A dimension is usually a bare field name, which may contain spaces.
        return (
            [expression] if app_fields.get(expression) else expression_names(expression)
        )

    @staticmethod
    def _app_field_key(name: str, app_fields: AppFieldIndex) -> Optional[str]:
        """The app field a chart name reads. A derived field such as
        Date.autoCalendar.Year reads its base field, the longest leading part
        of the name that is a field (field names can contain dots), unless a
        field of that exact name exists."""
        key = name
        while not app_fields.get(key) and "." in key:
            key = key.rsplit(".", 1)[0]
        return key if app_fields.get(key) else None

    @staticmethod
    def _get_chart_input_fields(
        chart: Chart, app_fields: AppFieldIndex
    ) -> List[AppField]:
        matched: Dict[str, AppField] = {}
        for expression in chart.field_expressions:
            for name in QlikSenseSource._chart_field_names(expression, app_fields):
                key = QlikSenseSource._app_field_key(name, app_fields)
                for app_field in app_fields.get(key) if key else []:
                    matched.setdefault(
                        f"{app_field.dataset_urn}.{app_field.field.name}", app_field
                    )
        return list(matched.values())

    def _get_chart_inputs(
        self,
        chart: Chart,
        chart_fields: List[AppField],
        app_fields: AppFieldIndex,
        input_tables_urns: List[str],
        has_unknown_schema: bool,
    ) -> List[str]:
        # The fields behind a $(variable) expansion that could not be expanded
        # are unknown, so the chart may read any of the app's tables, as may a
        # measure that could not be read, a derived field such as
        # Date.autoCalendar.Year whose base field is not known, or a name that
        # is not a known field while some table's columns are unknown (e.g.
        # SELECT *). Set modifier values only filter.
        unmatched = [
            name
            for expression in chart.field_expressions
            for name in self._chart_field_names(expression, app_fields)
            if self._app_field_key(name, app_fields) is None
        ]
        if (
            not chart.fields_complete
            or any(map(reads_unknown_fields, chart.field_expressions))
            or any("." in name for name in unmatched)
            or (has_unknown_schema and unmatched)
        ):
            return input_tables_urns
        # Otherwise every name is known: a chart that reads no field of the
        # app, such as a text object, has no input table.
        return list(dict.fromkeys(field.dataset_urn for field in chart_fields))

    @staticmethod
    def _index_app_fields(
        tables: Dict[str, List[QlikDatasetSchemaField]],
        script_table_urns: Collection[str] = (),
    ) -> AppFieldIndex:
        index = AppFieldIndex()
        for dataset_urn, fields in tables.items():
            for field in fields:
                app_field = AppField(dataset_urn=dataset_urn, field=field)
                index.exact.setdefault(field.name, []).append(app_field)
                if dataset_urn in script_table_urns:
                    index.folded.setdefault(field.name.lower(), []).append(app_field)
        return index

    @staticmethod
    def _tables_without_chart_path(
        app: App, chart_inputs: Dict[str, ChartInputs], table_urns: List[str]
    ) -> List[str]:
        """The app's tables no chart links to the app. A table reaches the app
        through a chart only if the chart's sheet is written with its charts,
        which needs all of them to have been read."""
        charts_on_complete_sheets = {
            chart.qId
            for sheet in app.sheets
            if sheet.charts_complete
            for chart in sheet.charts
        }
        charted = {
            urn
            for chart_id, inputs in chart_inputs.items()
            if chart_id in charts_on_complete_sheets
            for urn in inputs.tables
        }
        return [urn for urn in table_urns if urn not in charted]

    def _gen_app_dashboard_workunit(
        self,
        app: App,
        owner_username: Optional[str],
        table_urns: Optional[List[str]],
    ) -> Iterable[MetadataWorkUnit]:
        """The app as a dashboard of its sheets, as well as their container.
        `table_urns` are the app's tables no chart reads, such as those of an
        app without sheets, so lineage still reaches the app from them; tables
        a chart reads already do through the chart and its sheet. None when
        the app's tables are not all known."""
        dashboard_urn = self._gen_dashboard_urn(app.id)
        yield self._gen_entity_status_aspect(dashboard_urn)
        sheet_urns = [self._gen_dashboard_urn(sheet.id) for sheet in app.sheets]
        last_modified = ChangeAuditStampsClass(
            created=self._get_audit_stamp(app.createdAt, app.ownerId),
            lastModified=self._get_audit_stamp(app.updatedAt, app.ownerId),
        )
        dashboard_url = f"https://{self.ui_host}/sense/app/{app.id}/overview"
        if table_urns is None and not self.config.include_sheets_and_charts:
            # The app's tables reached it through sheets that are no longer
            # ingested, so the tables known for it are linked directly.
            dataset_urns: List[str] = self._last_known_table_urns(app)
        else:
            dataset_urns = table_urns or []
        info = DashboardInfoClass(
            title=app.qTitle,
            description=app.description or "",
            dashboards=[EdgeClass(destinationUrn=urn) for urn in sheet_urns],
            datasetEdges=[EdgeClass(destinationUrn=urn) for urn in dataset_urns],
            lastModified=last_modified,
            customProperties={Constant.QRI: app.qri},
            dashboardUrl=dashboard_url,
        )
        if app.sheets_complete and table_urns is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn, aspect=info
            ).as_workunit()
        else:
            # Sheets or tables that could not be read this run may still exist,
            # so the edges read are added to the last full read's, while the
            # app's own fields are still set.
            patch = dashboard_info_patch_builder(dashboard_urn, info, None)
            if app.sheets_complete:
                # The sheets are all known, so sheet edges this run doesn't
                # have, such as those of sheets no longer ingested, go, in the
                # same patch so the two can't be applied to different bases.
                for urn in self._stale_sheet_edges(dashboard_urn, sheet_urns):
                    patch.remove_dashboard(urn)
            for mcp in patch.build():
                yield MetadataWorkUnit(
                    id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
                )
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=SubTypes(typeNames=[BIAssetSubTypes.QLIK_APP_DASHBOARD]),
        ).as_workunit()
        # Inside the app's own container, with its sheets and tables, so the
        # app is one node when browsing its space.
        yield from add_entity_to_container(
            container_key=self._gen_app_key(app.id),
            entity_type="dashboard",
            entity_urn=dashboard_urn,
        )
        dpi_aspect = self._gen_dataplatform_instance_aspect(dashboard_urn)
        if dpi_aspect:
            yield dpi_aspect
        if self.config.ingest_owner and owner_username:
            yield self._gen_entity_owner_aspect(dashboard_urn, owner_username)

    def _last_known_table_urns(self, app: App) -> List[str]:
        """The app's tables this run registered, such as those its lineage
        graph names, and those its last read recorded, including on-prem
        tables, which have no QRI and are named after the app."""
        urns = dict.fromkeys(self._app_table_urns(app).values())
        previous = self._usable_checkpoint(app.id, [])
        if previous is not None:
            urns.update(dict.fromkeys((previous.table_urns_by_qri or {}).values()))
            urns.update(
                dict.fromkeys(
                    urn for urn in previous.urns if self._is_app_table_urn(urn, app.id)
                )
            )
        return list(urns)

    def _is_app_table_urn(self, urn: str, app_id: str) -> bool:
        if guess_entity_type(urn) != "dataset":
            return False
        try:
            name = DatasetUrn.from_string(urn).name
        except Exception:
            return False
        instance = self.config.platform_instance
        if instance and name.startswith(f"{instance}."):
            name = name[len(instance) + 1 :]
        return name.startswith(f"{app_id.lower()}.")

    def _stale_sheet_edges(
        self, dashboard_urn: str, sheet_urns: List[str]
    ) -> List[str]:
        graph = self.ctx.graph
        if graph is None:
            return []
        try:
            current = graph.get_aspect(dashboard_urn, DashboardInfoClass)
        except Exception as e:
            self.reporter.warning(
                title="Unable to read Qlik app dashboard",
                message="Sheet edges of the app's dashboard that this run did not "
                "find are kept.",
                context=dashboard_urn,
                exc=e,
            )
            return []
        return [
            edge.destinationUrn
            for edge in (current.dashboards or [] if current else [])
            if edge.destinationUrn not in sheet_urns
        ]

    def _gen_sheets_workunit(
        self,
        app: App,
        chart_inputs: Optional[Dict[str, ChartInputs]],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik Sheet to Datahub dashboard
        """
        for sheet in app.sheets:
            dashboard_urn = self._gen_dashboard_urn(sheet.id)

            yield self._gen_entity_status_aspect(dashboard_urn)

            # A partial chart list would overwrite the one the last full read
            # emitted.
            if sheet.charts_complete:
                yield self._gen_dashboard_info_workunit(sheet, app.id)

            yield MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=SubTypes(typeNames=[BIAssetSubTypes.QLIK_SHEET]),
            ).as_workunit()

            yield from add_entity_to_container(
                container_key=self._gen_app_key(app.id),
                entity_type="dashboard",
                entity_urn=dashboard_urn,
            )

            dpi_aspect = self._gen_dataplatform_instance_aspect(dashboard_urn)
            if dpi_aspect:
                yield dpi_aspect

            owner_username = self._get_owner_name(sheet.ownerId)
            if self.config.ingest_owner and owner_username:
                yield self._gen_entity_owner_aspect(dashboard_urn, owner_username)

            yield from self._gen_charts_workunit(sheet.charts, chart_inputs, app.id)

    def _gen_app_table_upstream_lineage(
        self, dataset_urn: str, table: QlikTable
    ) -> Optional[MetadataWorkUnit]:
        upstream_dataset_urn: Optional[str] = None
        if table.type == BoxType.BLACKBOX:
            upstream_dataset_platform_detail = (
                self.config.data_connection_to_platform_instance.get(
                    table.dataconnectorName,
                    PlatformDetail(
                        env=self.config.env,
                        platform_instance=self.config.platform_instance,
                    ),
                )
            )
            if not table.selectStatement:
                return None
            platform = upstream_dataset_platform_detail.platform or (
                KNOWN_DATA_PLATFORM_MAPPING.get(
                    table.dataconnectorPlatform, table.dataconnectorPlatform
                )
            )
            # The database and schema Qlik reports for the table win over the
            # connection's configured defaults.
            result = create_lineage_sql_parsed_result(
                query=table.selectStatement.strip(),
                default_db=table.databaseName
                or upstream_dataset_platform_detail.database,
                default_schema=table.schemaName
                or upstream_dataset_platform_detail.default_schema,
                platform=platform,
                # Like script tables, an unset env follows the recipe's env.
                env=upstream_dataset_platform_detail.env
                if "env" in upstream_dataset_platform_detail.model_fields_set
                else self.config.env,
                platform_instance=upstream_dataset_platform_detail.platform_instance,
            )
            if not result.in_tables:
                self.reporter.warning(
                    title="Unable to parse Qlik table SQL",
                    message="Upstream lineage for this Qlik table is skipped.",
                    context=f"table={table.tableName}, connection={table.dataconnectorName}, "
                    f"error={_error_type(result.debug_info.table_error)}",
                )
                return None
            qualified = [
                urn
                for urn in result.in_tables
                if has_qualified_name(
                    urn, platform, upstream_dataset_platform_detail.platform_instance
                )
            ]
            if not qualified:
                self.reporter.warning(
                    title="Qlik table is missing its database or schema",
                    message="Upstream lineage for this Qlik table is skipped. Set "
                    "`database` and `default_schema` for its connection in "
                    "`data_connection_to_platform_instance`.",
                    context=f"table={table.tableName}, upstreams={result.in_tables}",
                )
                return None
            upstream_dataset_urn = qualified[0]
        elif table.type == BoxType.LOADFILE:
            upstream_dataset_urn = self._gen_qlik_dataset_urn(
                f"{table.spaceId}.{table.databaseName}".lower()
            )

        if upstream_dataset_urn:
            # Generate finegrained lineage
            fine_grained_lineages = [
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=[
                        builder.make_schema_field_urn(upstream_dataset_urn, field.name)
                    ],
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=[
                        builder.make_schema_field_urn(dataset_urn, field.name)
                    ],
                )
                for field in table.datasetSchema
            ]
            return MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(
                            dataset=upstream_dataset_urn, type=DatasetLineageType.COPY
                        )
                    ],
                    fineGrainedLineages=self._column_lineage(fine_grained_lineages),
                ),
            ).as_workunit()
        else:
            return None

    def _gen_app_table_properties(
        self, dataset_urn: str, table: QlikTable
    ) -> MetadataWorkUnit:
        dataset_properties = DatasetProperties(
            name=table.tableName,
            qualifiedName=table.tableAlias,
        )
        dataset_properties.customProperties.update(
            {
                Constant.TYPE: DatasetSubTypes.QLIK_TABLE,
                Constant.DATACONNECTORID: table.dataconnectorid,
                Constant.DATACONNECTORNAME: table.dataconnectorName,
            }
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

    def _get_app_table_identifier(self, table: QlikTable) -> Optional[str]:
        if table.tableQri:
            return table.tableQri.lower()
        return None

    def _gen_app_table_entity(
        self,
        table_identifier: str,
        schema: List[QlikDatasetSchemaField],
        app_id: str,
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self._gen_qlik_dataset_urn(table_identifier)
        if app_id in self._binary_source_apps:
            self._app_urns.setdefault(app_id, set()).add(dataset_urn)

        yield self._gen_entity_status_aspect(dataset_urn)

        yield self._gen_schema_metadata(table_identifier, schema)

        yield from add_entity_to_container(
            container_key=self._gen_app_key(app_id),
            entity_type="dataset",
            entity_urn=dataset_urn,
        )

        dpi_aspect = self._gen_dataplatform_instance_aspect(dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypes(typeNames=[DatasetSubTypes.QLIK_TABLE]),
        ).as_workunit()

    def _get_data_connections(self) -> Dict[str, DataConnectionInfo]:
        if self._data_connections is None:
            self._data_connections = self.qlik_api.get_data_connections()
        return self._data_connections

    def _find_data_connection(
        self, connection: str, space_id: str
    ) -> Optional[DataConnectionInfo]:
        connections = self._get_data_connections()
        lib_connection = _lib_connection(connection)
        space_name = self.qlik_api.spaces.get(space_id)
        if self.config.on_prem is None and space_name and not lib_connection.space:
            # Qlik Cloud looks a bare name up in the app's own space only. A
            # same-named connection elsewhere may point at another database
            # when the app's own one is not visible to the API key.
            return _get_ignoring_case(
                connections, f"{space_name}{SPACE_SEPARATOR}{lib_connection.name}"
            )
        return _get_ignoring_case(connections, _connection_key(lib_connection))

    def _mapped_connection(
        self, connection: str, space_id: str, mapping: Dict[str, MappingT]
    ) -> Optional[MappingT]:
        """A recipe mapping's entry for a connection a load script names. On
        Qlik Cloud a bare name is the app's own space's connection, so an
        entry keyed `<space>:<connection>` matches it first; a bare key
        matches the connection in any space."""
        lib_connection = _lib_connection(connection)
        # As the script spells it, then as Qlik lists it, without brackets.
        names = list(
            dict.fromkeys(
                [connection, _connection_key(lib_connection), lib_connection.name]
            )
        )
        space_name = self.qlik_api.spaces.get(space_id)
        if self.config.on_prem is None and space_name and not lib_connection.space:
            names.insert(0, f"{space_name}{SPACE_SEPARATOR}{lib_connection.name}")
        return next(
            (
                entry
                for name in names
                if (entry := _get_ignoring_case(mapping, name)) is not None
            ),
            None,
        )

    def _resolve_script_connection(
        self, connection: Optional[str], space_id: str
    ) -> Optional[ResolvedConnection]:
        if not connection:
            return None
        configured = self._mapped_connection(
            connection, space_id, self.config.data_connection_to_platform_instance
        )
        inferred = self._find_data_connection(connection, space_id)
        # Inline ODBC/OLEDB connection strings carry the driver and database
        # themselves. A bare name says nothing about the platform behind it.
        if inferred is None and is_inline_connection(connection):
            inferred = infer_data_connection(connection, connection)
        platform = (configured.platform if configured else None) or (
            inferred.platform if inferred else None
        )
        if not platform:
            return None
        return ResolvedConnection(
            platform=platform,
            database=(configured.database if configured else None)
            or (inferred.database if inferred else None),
            default_schema=configured.default_schema if configured else None,
            env=configured.env
            if configured and "env" in configured.model_fields_set
            else self.config.env,
            platform_instance=configured.platform_instance if configured else None,
        )

    def _parse_script_sql(
        self, sql: str, connection: ResolvedConnection
    ) -> SqlParsingResult:
        return create_lineage_sql_parsed_result(
            query=sql,
            default_db=connection.database,
            default_schema=connection.default_schema,
            platform=connection.platform,
            platform_instance=connection.platform_instance,
            env=connection.env,
            graph=self.ctx.graph,
        )

    def _parse_script_table(
        self, app_id: str, script_table: ScriptTable, connection: ResolvedConnection
    ) -> Optional[SqlParsingResult]:
        sql = drop_condition_variables(script_table.sql)
        if "$(" in sql:
            # orders_$(vYear) would parse as a table named orders_$.
            references = VARIABLE_REFERENCE_RE.findall(sql)
            variables = {name.strip() for name in references}
            # Loop and SUB variables stay unknown whatever the recipe says.
            settable = sorted(variables - set(script_table.runtime_variables))
            context = f"app={app_id}, table={script_table.label}"
            if settable:
                context += f", settable={', '.join(settable)}"
            if script_table.runtime_variables:
                context += (
                    f", set at run time={', '.join(script_table.runtime_variables)}"
                )
            if sql.count("$(") > len(references):
                # Nested references such as $(v$(x)) and expressions such as
                # $(=Date(...)) have no single name, so show the SQL itself.
                context += f", sql={sql[:200]}"
            self.reporter.warning(
                title="Qlik load script SQL uses a variable that is not set",
                message="Upstream lineage for this script table is skipped."
                + (
                    " Set the variables listed as settable in "
                    "`script_variables` to include it; no script sets them."
                    if settable
                    else ""
                ),
                context=context,
            )
            return None
        result = self._parse_script_sql(sql, connection)
        if (
            result.debug_info.table_error
            and connection.platform in BACKTICK_QUOTED_PLATFORMS
            and "[" in sql
        ):
            result = self._parse_script_sql(brackets_to_backticks(sql), connection)
        if result.in_tables:
            return result
        if result.debug_info.table_error:
            self.reporter.warning(
                title="Unable to parse Qlik load script SQL",
                message="Upstream lineage for this script table is skipped.",
                context=f"app={app_id}, table={script_table.label}, "
                f"error={_error_type(result.debug_info.table_error)}",
            )
        else:
            logger.debug(
                f"App {app_id} script table {script_table.label!r} reads no tables"
            )
        return None

    @staticmethod
    def _merge_script_table(
        tables: Dict[Tuple[str, bool], ScriptTableLineage],
        app_id: str,
        script_table: ScriptTable,
        connection_name: Optional[str],
        upstream_urns: List[str],
        result: SqlParsingResult,
    ) -> None:
        # Loads of one dataset URN are one table, so labels that differ only
        # in case are merged. Mapping tables are a separate namespace, so a
        # mapping load is kept apart.
        identifier = app_table_identifier(app_id, script_table.label)
        lineage = tables.setdefault(
            (identifier, script_table.mapping),
            ScriptTableLineage(
                label=script_table.label,
                identifier=identifier,
                label_unknown=script_table.label_unknown,
                connection=connection_name,
                mapping=script_table.mapping,
            ),
        )
        lineage.upstream_urns.extend(
            urn for urn in upstream_urns if urn not in lineage.upstream_urns
        )
        sql_columns: Dict[str, List[str]] = {}
        for column in result.column_lineage or []:
            sql_columns.setdefault(column.downstream.column, []).extend(
                column_refs_to_schema_field_urns(
                    ref for ref in column.upstreams if ref.table in upstream_urns
                )
            )
        fields: Optional[Dict[str, List[str]]] = apply_preceding_loads(
            script_table.preceding_loads, sql_columns
        )
        if script_table.reshaped:
            # Only a CrossTable whose fields are known keeps column lineage;
            # otherwise the fields are not the columns and only table lineage
            # is kept.
            selected = [
                column.downstream.column for column in result.column_lineage or []
            ]
            fields = apply_crosstable(
                script_table.crosstable,
                script_table.preceding_loads,
                sql_columns,
                # The parser lists columns in SELECT order, but leaves out a *
                # and repeats a name selected twice, which would shift slots.
                sql_columns_in_order=len(set(selected))
                == len(selected)
                == select_list_size(script_table.sql),
            )
        for field, upstreams in (fields or {}).items():
            lineage.column_upstreams.setdefault(field, []).extend(upstreams)

    def _parsed_script(self, app: App) -> ParsedScript:
        # Parsed before an on-prem app's read is checked and again when it is
        # emitted, so the last parse is kept.
        if self._last_parsed is not None and self._last_parsed.app is app:
            return self._last_parsed.parsed
        parsed = (
            parse_script(
                app.script,
                self._app_variables(app),
                self._app_own_variables(app).keys(),
            )
            if app.script
            else ParsedScript()
        )
        self._last_parsed = _ParsedApp(app=app, parsed=parsed)
        return parsed

    def _build_script_tables(
        self, app: App, parsed: Optional[ParsedScript] = None
    ) -> List[ScriptTableLineage]:
        if not app.script:
            return []
        tables: Dict[Tuple[str, bool], ScriptTableLineage] = {}
        # Tables the data model already traces are ingested from it instead.
        traced = {
            name.lower()
            for table in app.tables
            if table.tableQri
            for name in (table.tableName, table.tableAlias)
        }
        unqualified: List[str] = []
        for script_table in (parsed or self._parsed_script(app)).tables:
            if script_table.label.lower() in traced:
                continue
            connection_name = (
                redact_connection(script_table.connection)
                if script_table.connection
                else None
            )
            self.reporter.script_tables_found += 1
            if is_connector_sql(script_table.sql):
                logger.debug(
                    f"App {app.id} script table {script_table.label!r} reads from "
                    "a Qlik connector, not a database table"
                )
                continue
            connection = self._resolve_script_connection(
                script_table.connection, app.spaceId
            )
            logger.debug(
                f"App {app.id} script table {script_table.label!r} via connection "
                f"{connection_name!r} resolved to {connection}"
            )
            if connection is None:
                self.reporter.unresolved_data_connections.append(
                    f"{app.id}: {connection_name}"
                )
                self._note_unseen_space(
                    _named_space(script_table.connection or ""), "data connections"
                )
                continue
            result = self._parse_script_table(app.id, script_table, connection)
            if result is None:
                continue
            upstream_urns = [
                urn
                for urn in result.in_tables
                if has_qualified_name(
                    urn, connection.platform, connection.platform_instance
                )
            ]
            unqualified.extend(
                urn for urn in result.in_tables if urn not in upstream_urns
            )
            if upstream_urns:
                self._merge_script_table(
                    tables, app.id, script_table, connection_name, upstream_urns, result
                )
        if unqualified:
            self.reporter.warning(
                title="Qlik load script table is missing its database or schema",
                message="Lineage to these tables is skipped. Set `database` and "
                "`default_schema` for their connection in "
                "`data_connection_to_platform_instance`.",
                context=f"app={app.id}, tables={sorted(set(unqualified))}",
            )
        # A data table and a mapping table with the same identifier share one
        # dataset URN, so only the data table, which charts read, is kept.
        script_tables = [
            lineage
            for (identifier, mapping), lineage in tables.items()
            if not (mapping and (identifier, False) in tables)
        ]
        self.reporter.script_tables_with_lineage += len(script_tables)
        return script_tables

    @staticmethod
    def _script_table_fields(
        table: ScriptTableLineage,
    ) -> List[QlikDatasetSchemaField]:
        return [
            QlikDatasetSchemaField(name=column) for column in table.column_upstreams
        ]

    def _gen_script_tables_workunit(
        self,
        tables: List[ScriptTableLineage],
        app_id: str,
        replace_lineage: bool = True,
        binary_links: Optional[Dict[str, _BinaryLink]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        for table in tables:
            dataset_urn = self._gen_qlik_dataset_urn(table.identifier)
            yield from self._gen_app_table_entity(
                table.identifier,
                self._table_schema(table.identifier, self._script_table_fields(table)),
                app_id,
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetProperties(
                    name=table.label,
                    qualifiedName=table.label,
                    customProperties={
                        Constant.TYPE: "Qlik Script Table",
                        Constant.DATACONNECTORNAME: table.connection or "",
                    },
                ),
            ).as_workunit()
            link = (binary_links or {}).get(dataset_urn)
            if link is not None:
                # Written once the source app's tables are known, so a source
                # not linked this run doesn't wipe the edge.
                self._binary_links.append(
                    link.model_copy(
                        update={
                            "upstream_urns": table.upstream_urns,
                            "script_fields": self._script_column_lineage(
                                dataset_urn, table
                            )
                            or [],
                        }
                    )
                )
                continue
            if not replace_lineage:
                yield from self._patch_upstreams(
                    dataset_urn,
                    table.upstream_urns,
                    DatasetLineageType.TRANSFORMED,
                    self._column_lineage(
                        self._script_column_lineage(dataset_urn, table)
                    ),
                )
                continue
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(dataset=urn, type=DatasetLineageType.TRANSFORMED)
                        for urn in table.upstream_urns
                    ],
                    fineGrainedLineages=self._column_lineage(
                        self._script_column_lineage(dataset_urn, table)
                    ),
                ),
            ).as_workunit()

    @staticmethod
    def _script_column_lineage(
        dataset_urn: str, table: ScriptTableLineage
    ) -> Optional[List[FineGrainedLineage]]:
        return [
            FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=upstreams,
                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                downstreams=[builder.make_schema_field_urn(dataset_urn, column)],
            )
            for column, upstreams in table.column_upstreams.items()
            if upstreams
        ] or None

    @functools.cached_property
    def _dataset_urns_by_secure_qri(self) -> Optional[Dict[str, str]]:
        """Dataset URNs by secure QRI, or None when the datasets could not be
        listed. First read after the allowed spaces are known."""
        datasets = self.qlik_api.list_datasets()
        if datasets is None:
            self.reporter.warning(
                title="Unable to list Qlik datasets",
                message="Lineage to and from Qlik datasets, such as QVDs, is only "
                "added to this run.",
                context=str(self.qlik_api.dataset_listing_error),
            )
            return None
        self._listed_dataset_qris = {d.secure_qri for d in datasets if d.secure_qri}
        # Only datasets of allowed spaces are ingested.
        allowed = [d for d in datasets if d.space_id in self._allowed_space_ids]
        # Qlik traces QVDs, so one listed without a QRI may be a source the
        # graph names; files it doesn't trace, such as text files, never are.
        self._datasets_without_qri = any(
            d.secure_qri is None and _is_qvd(d) for d in allowed
        )
        self._excluded_datasets_without_qri = any(
            d.secure_qri is None and _is_qvd(d)
            for d in datasets
            if d.space_id not in self._allowed_space_ids
        )
        return {
            dataset.secure_qri: self._gen_qlik_dataset_urn(
                self._qlik_dataset_identifier(dataset.space_id, dataset.name)
            )
            for dataset in allowed
            if dataset.secure_qri is not None
        }

    @staticmethod
    def _lineage_table_identifier(app_id: str, table: LineageTable) -> str:
        # The identifier load script tables use, so a table keeps its URN
        # whether or not its load script SQL resolves to a database table.
        label = table.label or table.qri.rpartition("#")[2]
        return app_table_identifier(app_id, label)

    def _app_table_urns(self, app: App) -> Dict[str, str]:
        urns: Dict[str, str] = {}
        for table in app.tables:
            if table.tableQri:
                urn = self._gen_qlik_dataset_urn(table.tableQri.lower())
                urns[table.tableQri] = urn
                if table.graphQri:
                    urns[table.graphQri] = urn
        # A tableQri is the table's source, which can be another app's table,
        # such as for a BINARY load, whose own URN must not be replaced.
        urns = _own_table_urns(app.id, urns)
        urns.update(
            (
                table.qri,
                self._gen_qlik_dataset_urn(
                    self._lineage_table_identifier(app.id, table)
                ),
            )
            for table in app.lineage_tables
        )
        return urns

    def _register_app_tables(self, app: App) -> None:
        table_urns = self._app_table_urns(app)
        self._app_table_urns_by_qri.update(table_urns)
        self._table_urns_by_app[app.id] = set(table_urns.values())
        self._app_qris[app.id] = app.qri
        if self._tables_unknown(app):
            self._apps_tables_unknown.add(app.id)
            return
        self._apps_tables_known.add(app.id)
        if self._graph_tables_unresolved(app):
            self._apps_graph_tables_unresolved.add(app.id)
        if app.unmatched_graph_tables:
            self.reporter.warning(
                title="Qlik app tables not matched to their lineage graph",
                message="These tables of the app's Qlik Cloud lineage graph match "
                "no data model table by name, so lineage that other apps' tables "
                "or datasets have from them is only added to.",
                context=f"{app.id}: {_bounded_list(app.unmatched_graph_tables)}",
            )
        previous = self._usable_checkpoint(app.id, [])
        if previous is not None and previous.table_urns_by_qri:
            self._previous_app_table_urns.update(previous.table_urns_by_qri.values())

    @staticmethod
    def _tables_unknown(app: App) -> bool:
        """Whether some of the app's tables may have no URN this run, so a
        table that loads from the app can't be told to load nothing else."""
        return app.tables_read_failed

    @staticmethod
    def _graph_tables_unresolved(app: App) -> bool:
        """Whether a table node of the app's lineage graph may have no URN
        this run, though the app's tables were read."""
        return bool(app.unmatched_graph_tables) or bool(
            app.tables and app.lineage_graph_missing
        )

    def _app_tables_known(self, app_id: str) -> bool:
        if app_id in self._apps_tables_unknown:
            return False
        if app_id in self._apps_tables_known:
            return True
        # Not read or kept this run, so deleted or outside the recipe, unless
        # part of the run could not be read.
        return not self.qlik_api.unread_items

    def _resolve_lineage_sources(
        self, source_qris: List[str], dataset_urn: str, own_app_id: Optional[str]
    ) -> "_ResolvedSources":
        """URNs of the sources this run ingests. A source that can't be resolved
        this run, because the datasets or its app could not be read, makes the
        result incomplete. `own_app_id` is the app whose table it is."""
        resolved = _ResolvedSources()
        for source_qri in source_qris:
            urn, unknown = self._resolve_source(source_qri, dataset_urn, own_app_id)
            if unknown and unknown not in resolved.unknown_reasons:
                resolved.unknown_reasons.append(unknown)
            if urn and urn not in resolved.urns:
                resolved.urns.append(urn)
        return resolved

    def _resolve_source(
        self,
        table_qri: str,
        dataset_urn: str,
        own_app_id: Optional[str],
        name_from_graph: bool = True,
    ) -> Tuple[Optional[str], Optional[str]]:
        """The URN of a source table node, if this run ingests it, and why the
        source could not be told this run, if so. An unknown source may still
        have a URN, which is only added."""
        app_id = _qlik_app_id(table_qri)
        if app_id is not None:
            # Such as a BINARY load of another app; a table of an app the run
            # does not ingest has no URN and is left out.
            urn = self._app_table_urns_by_qri.get(table_qri)
            if not self._app_tables_known(app_id):
                # A table the graph names this run is still added, but the
                # app's other tables are unknown.
                return (
                    urn if urn != dataset_urn else None
                ), f"the tables of source app {app_id} are not known this run"
            if urn is None and app_id in self._apps_graph_tables_unresolved:
                return None, (
                    f"source app {app_id} has lineage graph tables matched to "
                    "none of its tables"
                )
        else:
            # Data files, and files Qlik catalogued on a data connection such
            # as ADLS, are datasets. Other sources, such as database tables,
            # have opaque QRIs; the load script parser traces SQL loads instead.
            datasets = self._dataset_urns_by_secure_qri
            urn = (
                datasets.get(table_qri.rpartition("#")[0]) or datasets.get(table_qri)
                if datasets
                else None
            )
            graph_app_id = own_app_id if name_from_graph else None
            on_known_storage = False
            if urn is None and own_app_id is not None and FILE_QRI_RE.match(table_qri):
                # Files on a storage connection resolve from the load script,
                # without the list of datasets.
                file_source = self._file_source_resolution(own_app_id, table_qri)
                urn, on_known_storage = file_source.urn, file_source.on_known_storage
                inputs_unknown = (
                    self._file_inputs_unknown(own_app_id) if urn is None else None
                )
                if inputs_unknown:
                    # The file may be one the script reads through a path,
                    # connection or included script this run could not read,
                    # so the last run's edge to it must stay. Without the
                    # listing, it may be a listed file the graph can't name,
                    # and without the connections, one on known storage.
                    if on_known_storage:
                        return None, inputs_unknown
                    if datasets is None or self._data_connections_unread():
                        self._keep_graph_named_sources(own_app_id, table_qri)
                        return None, inputs_unknown
                    return (
                        self._graph_named_source(graph_app_id, table_qri),
                        inputs_unknown,
                    )
            if urn is None and datasets is None:
                self._keep_graph_named_sources(own_app_id, table_qri)
                return None, "the Qlik datasets could not be listed"
            if (
                urn is None
                and table_qri.startswith(QLIK_DATASET_QRI_PREFIX)
                and (
                    self._datasets_without_qri
                    or (
                        self._excluded_datasets_without_qri
                        and not self._is_listed_dataset(table_qri)
                    )
                )
            ):
                # It may be a listed dataset the listing gave no QRI for, so
                # it is not named from the graph either.
                self._keep_graph_named_sources(own_app_id, table_qri)
                return None, "a listed Qlik dataset has no QRI to match"
            # A file on known storage that could not be told which is left out
            # rather than named apart from its storage dataset.
            if urn is None and graph_app_id is not None and not on_known_storage:
                urn = self._graph_named_source(graph_app_id, table_qri)
                if (
                    urn is None
                    and self._graph_could_name(table_qri)
                    and self._upstream_graph_unread(graph_app_id)
                ):
                    # The upstream graph that may name it could not be read.
                    return None, "the app's upstream lineage graph could not be read"
        return (urn if urn != dataset_urn else None), None

    def _graph_could_name(self, table_qri: str) -> bool:
        # A dataset listed in a space the recipe leaves out stays out. When one
        # has no QRI, any dataset QRI the graph names may be it.
        if (
            table_qri.startswith(QLIK_DATASET_QRI_PREFIX)
            and self._excluded_datasets_without_qri
        ):
            return False
        return table_qri.startswith(
            UPSTREAM_RESOURCE_QRI_PREFIXES
        ) and not self._is_listed_dataset(table_qri)

    def _is_listed_dataset(self, table_qri: str) -> bool:
        return any(
            qri in self._listed_dataset_qris
            for qri in (table_qri.rpartition("#")[0], table_qri)
        )

    def _graph_named_source(
        self, app_id: Optional[str], table_qri: str
    ) -> Optional[str]:
        """A dataset or file only the app's upstream graph names, such as a QVD
        in a space the API key can't see, under a URN from its QRI. Its writer
        apps' tables become its upstreams at the end of the run."""
        if app_id is None or not self._graph_could_name(table_qri):
            return None
        upstreams = self._app_lineage_upstreams(app_id)
        if upstreams is None:
            return None
        # The graph may name the resource itself rather than one of its tables.
        candidates = (table_qri.rpartition("#")[0], table_qri)
        resource_qri = next(
            (qri for qri in candidates if qri in upstreams.resource_labels), None
        )
        if resource_qri is None:
            return None
        label = upstreams.resource_labels[resource_qri]
        # A file another app's script resolves keeps that URN there, so the two
        # are separate datasets until its storage is mapped.
        urn = self._gen_qlik_dataset_urn(resource_qri.lower())
        self._graph_named_resources[urn] = label
        self._graph_resource_readers.setdefault(urn, set()).add(app_id)
        self._app_end_of_run_urns[app_id].add(urn)
        # The dataset is the whole resource, so a store into any of its tables
        # is an upstream, as for listed datasets.
        writers = self._graph_resource_writers.setdefault(urn, set())
        for stored_qri, writer_qris in upstreams.stored_from.items():
            if resource_qri in (stored_qri, stored_qri.rpartition("#")[0]):
                writers.update(writer_qris)
        return urn

    def _app_lineage_upstreams(self, app_id: str) -> Optional[LineageUpstreams]:
        """The app's upstream graph, read when a source only it can name is
        first met, or None when it could not be read."""
        if app_id not in self._lineage_upstreams:
            try:
                upstreams: Optional[LineageUpstreams] = (
                    self.qlik_api.get_lineage_upstreams(self._app_qris[app_id])
                )
            except Exception as e:
                logger.debug(f"Unable to read upstream lineage of app {app_id}: {e}")
                upstreams = None
                self._upstream_graph_read_failures.append(f"{app_id}: {e}")
                self._keep_graph_named_urns(app_id)
            self._lineage_upstreams[app_id] = upstreams
        return self._lineage_upstreams[app_id]

    def _keep_graph_named_sources(self, app_id: Optional[str], table_qri: str) -> None:
        """Keeps the datasets the app's last read named from its upstream graph
        when this run can't tell whether a source it may name is one of them."""
        if app_id is not None and table_qri.startswith(UPSTREAM_RESOURCE_QRI_PREFIXES):
            self._keep_graph_named_urns(app_id)

    def _keep_graph_named_urns(self, app_id: str) -> None:
        """Keeps the datasets the app's last read named from its upstream
        graph, as this run could not name them again."""
        if (
            self.stale_entity_removal_handler is None
            or app_id in self._apps_graph_named_kept
        ):
            return
        self._apps_graph_named_kept.add(app_id)
        failures: List[str] = []
        previous = self._usable_checkpoint(app_id, failures)
        for failure in failures:
            # A failure stops stale entity removal from soft-deleting them.
            self.reporter.failure(
                title="Unable to load the last checkpoint of a Qlik app",
                message="Datasets the app's last read named from Qlik's lineage "
                "graph could not be kept, so stale metadata removal is skipped "
                "for this run.",
                context=failure,
            )
        if previous is None:
            return
        # The URN up to its env, as `_graph_named_source` builds it.
        prefixes = tuple(
            self._gen_qlik_dataset_urn(prefix).rpartition(",")[0]
            for prefix in UPSTREAM_RESOURCE_QRI_PREFIXES
        )
        for urn in previous.urns:
            if urn.startswith(prefixes):
                self._app_end_of_run_urns[app_id].add(urn)
                self.stale_entity_removal_handler.add_entity_to_state(
                    guess_entity_type(urn), urn
                )

    def _upstream_graph_unread(self, app_id: str) -> bool:
        return (
            app_id in self._lineage_upstreams
            and self._lineage_upstreams[app_id] is None
        )

    def _file_inputs_unknown(self, app_id: str) -> Optional[str]:
        """Why the files the app's load script reads are not all known, if so."""
        if app_id in self._apps_with_includes:
            if app_id not in self._reported_include_apps:
                self._reported_include_apps.add(app_id)
                self.reporter.warning(
                    title="Qlik app loads files from an included script",
                    message="The app includes a script with $(Include) or "
                    "$(Must_Include) that was not read, or calls a SUB its script "
                    "does not define. An include is not read on-prem, nor when its "
                    "path varies as the script runs or does not expand to a "
                    "lib:// path, it is nested more than 10 levels deep or in a "
                    "cycle, or the file could not be read. A file in the app's "
                    "lineage graph that its own script does not load only adds to "
                    "the table's lineage.",
                    context=app_id,
                )
            return "a file in its lineage graph may be loaded by an included script"
        if app_id in self._apps_variables_unread:
            return "the app's variables could not be read"
        if app_id in self._apps_file_paths_unknown:
            return "the app loads files through paths the connector can't name"
        if self._data_connections_unread():
            return "the data connections could not be listed"
        return None

    def _data_connections_unread(self) -> bool:
        return any(
            item.name == UNREAD_DATA_CONNECTIONS for item in self.qlik_api.unread_items
        )

    def _app_variables(self, app: App) -> Dict[str, str]:
        return merged_variables(self.config.script_variables, app.variables)

    def _app_own_variables(self, app: App) -> Dict[str, str]:
        return app_own_variables(self.config.script_variables, app.variables)

    def _file_source_resolver(
        self, app: App, parsed: ParsedScript
    ) -> FileSourceResolver:
        references = []
        unnamed = []
        settable = []
        for load in parsed.file_loads:
            if load.uses_unknown_variable:
                unnamed.append(load.as_written)
                continue
            # Paths are often built from variables the script sets with LET or
            # in an included script, which the app's variables hold. The graph
            # follows the last reload, so either spelling may be the file's.
            candidates = [
                load.expanded,
                expand_variables(
                    load.as_written, unquoted_values(self._app_variables(app))
                ).strip(),
            ]
            named = [
                FileReference(
                    connection=lib_path.group("connection"),
                    path=lib_path.group("path"),
                )
                for path in dict.fromkeys(candidates)
                if (lib_path := LIB_PATH_RE.match(path))
                and not any(part in path for part in ("$(", "*", "?"))
            ]
            if named:
                references.extend(named)
            if not named or any("*" in path or "?" in path for path in candidates):
                # A wildcard reads files the graph names one by one, which no
                # path can be matched against, and either spelling may be the
                # one the last reload read.
                unnamed.append(load.expanded)
                # Loop and branch variables stay unknown whatever the recipe
                # says, but a variable nothing sets can come from it.
                if "$(" in load.expanded:
                    settable.append(load.expanded)
        if unnamed and self.config.on_prem is None:
            # On-prem apps have no lineage graph files to match.
            self._apps_file_paths_unknown.add(app.id)
            self.reporter.warning(
                title="Qlik app loads files the connector can't name",
                message="Files loaded through a wildcard, an unexpanded variable "
                "or a path outside a data connection can't be matched to the "
                "files in the app's lineage graph, so lineage to them is only "
                "added to."
                + (
                    " Set the variables of the paths listed as settable in "
                    "`script_variables` to name their files; no script sets "
                    "them."
                    if settable
                    else ""
                ),
                context=f"{app.id}: {', '.join(unnamed[:5])}"
                + (f"; settable: {', '.join(settable[:5])}" if settable else ""),
            )
        return FileSourceResolver(references)

    def _file_storage(
        self, connection: str, space_id: str
    ) -> Tuple[Optional[FileStorage], Optional[FileStorageDetail]]:
        detail = self._mapped_connection(
            connection, space_id, self.config.file_connection_to_storage
        )
        info = self._find_data_connection(connection, space_id)
        visible = info.file_storage if info else None
        if detail is not None and detail.storage_uri:
            storage = file_storage_of_uri(detail.storage_uri)
            if storage is not None and visible is not None:
                # The connection's own properties still check the QRI.
                storage.qri_connection_id = visible.qri_connection_id
            return storage, detail
        return visible, detail

    def _file_urn(
        self, storage: FileStorage, detail: Optional[FileStorageDetail], path: str
    ) -> str:
        return file_dataset_urn(
            storage,
            # As Qlik Cloud names it, so lineage joins whichever way it was read.
            normalized_file_path(path),
            platform_instance=detail.platform_instance if detail else None,
            # An env the mapping leaves unset is the recipe's.
            env=detail.env
            if detail and "env" in detail.model_fields_set
            else self.config.env,
            lowercase=detail.convert_urns_to_lowercase if detail else False,
        )

    def _unmapped_file_urn(
        self, connection: str, path: str, space_id: str
    ) -> Optional[str]:
        """A Qlik dataset for a file on a connection whose storage is unknown,
        named by the connection and path, so the apps that write and read it
        still join. DataFiles hold Qlik's own datasets."""
        lib_connection = _lib_connection(connection)
        space, name = lib_connection.space, lib_connection.name
        if not space:
            # Qlik Cloud resolves a bare name in the app's own space only. A
            # personal space has no name another app could write the path with.
            if self.config.on_prem is None and space_id != Constant.PERSONAL_SPACE_ID:
                space = self.qlik_api.spaces.get(space_id, "").strip().strip("[]")
        path = normalized_file_path(path)
        qualified = f"{_lib_name(space)}{SPACE_SEPARATOR}{_lib_name(name)}/{path}"
        if (
            not (space and name and path)
            or _is_data_files(connection)
            # Characters a dataset URN can't hold.
            or any(char in qualified for char in ",()")
        ):
            return None
        urn = self._gen_qlik_dataset_urn(qualified.lower())
        self._add_unmapped_file(urn, f"lib://{qualified}")
        return urn

    def _add_unmapped_file(self, urn: str, spelling: str) -> None:
        # Apps may spell the file in different case and are read in no fixed
        # order, so one spelling is picked the same way every run.
        known = self._unmapped_files.get(urn)
        self._unmapped_files[urn] = spelling if known is None else min(known, spelling)

    def _unmapped_storage_file_urn(
        self, connection: str, path: str, space_id: str, report: bool = True
    ) -> Optional[str]:
        """Reports a connection whose storage is unknown and names the file on
        it by the connection and path, when it can be."""
        if self._data_connections_unread():
            # Its storage may be known once listed; the unread listing is reported.
            return None
        if report and connection not in self.reporter.unresolved_file_connections:
            self.reporter.unresolved_file_connections.append(connection)
        return self._unmapped_file_urn(connection, path, space_id)

    def _queue_file_store_lineage(
        self,
        app: App,
        parsed: ParsedScript,
        table_fields: Dict[str, List[QlikDatasetSchemaField]],
    ) -> None:
        """Lineage into the QVD, text and Parquet files an app's load script
        stores on external storage, such as Azure Storage. Other apps may store
        into the same file, so only this app's tables' edges are replaced, and
        only when every file it stores into is known."""
        stores = [*parsed.qvd_stores, *parsed.file_stores]
        # Whether the files the app stores into are all known this run.
        complete = (
            app.read_complete
            and not parsed.has_unresolved_store
            and not parsed.has_unresolved_file_store
            and app.id not in self._apps_with_includes
            and not app.variables_read_failed
            # An unread script hides its STOREs.
            and app.script is not None
            and not app.script_read_failed
            and not self._data_connections_unread()
            and self._dataset_urns_by_secure_qri is not None
        )
        file_urns: Dict[int, str] = {}
        unmapped_indexes: Set[int] = set()
        for index, store in enumerate(stores):
            if store.file.connection is None:
                continue
            storage, detail = self._file_storage(store.file.connection, app.spaceId)
            if storage is None:
                if _is_data_files(store.file.connection):
                    continue
                info = self._find_data_connection(store.file.connection, app.spaceId)
                # A visible connection of another connector, such as SFTP, has
                # no storage to map; its file is named by connection and path,
                # as its readers' lineage graphs name it.
                listed_without_storage = info is not None and info.file_platform is None
                if not listed_without_storage:
                    # Its file may be a storage file the app stored into before.
                    complete = False
                unmapped_urn = self._unmapped_storage_file_urn(
                    store.file.connection,
                    store.file.path,
                    app.spaceId,
                    report=not listed_without_storage,
                )
                if unmapped_urn is not None:
                    file_urns[index] = unmapped_urn
                    unmapped_indexes.add(index)
                continue
            catalogued = self._catalogued_as_qlik_dataset(storage, store.file.path)
            if catalogued is None:
                complete = False
            # A file Qlik catalogues is the Qlik dataset that loads of it and
            # its own lineage graph name.
            if catalogued is False:
                file_urns[index] = self._file_urn(storage, detail, store.file.path)
        table_urns = set(table_fields)
        urns_by_store = {id(store): file_urns.get(i) for i, store in enumerate(stores)}

        def same_file(store: QvdStore, other: QvdStore) -> bool:
            # Two names of one connection, such as with and without its space,
            # store into the same file.
            urn = urns_by_store[id(store)]
            return store.file.key == other.file.key or (
                urn is not None and urn == urns_by_store[id(other)]
            )

        competing = stores_in_competing_branches(
            stores,
            lambda store: (
                self._stored_table_urn(app, store.label, table_urns)
                or store.label.lower()
            ),
            same_file,
        )
        if competing:
            complete = False
            self._report_competing_stores(app, competing)
        writes: Dict[str, List[QvdWrite]] = {}
        # A STORE without a format writes a QVD, whatever the file is named.
        qvd_file_urns = {
            file_urn
            for index, file_urn in file_urns.items()
            if index < len(parsed.qvd_stores)
        }
        for index, store in enumerate(stores):
            file_urn = file_urns.get(index)
            if file_urn is None or store in competing:
                continue
            table_urn = self._stored_table_urn(app, store.label, table_urns)
            # A table the app doesn't ingest, such as one a script variable
            # names, has no dataset to link, and may be one the file's
            # existing edges come from.
            if table_urn is None:
                complete = False
                continue
            if index in unmapped_indexes:
                # Linked at the end of the run, once an app that reads the
                # file is known.
                urns = self._unmapped_file_stores[file_urn].setdefault(app.id, [])
                if table_urn not in urns:
                    urns.append(table_urn)
                continue
            write = QvdWrite(
                table_urn=table_urn,
                fields=stored_fields(
                    store, [field.name for field in table_fields.get(table_urn, [])]
                ),
            )
            if write not in writes.setdefault(file_urn, []):
                writes[file_urn].append(write)
                self.reporter.file_stores_linked += 1
        previous = self._usable_checkpoint(app.id, [])
        previous_files = previous.stored_file_urns if previous else []
        currents = {
            file_urn: self._current_upstreams(file_urn) if complete else None
            for file_urn in sorted({*writes, *previous_files})
        }
        # Files the app may still have edges into, so a later run that knows
        # all its stores and can read their lineage removes them.
        self._app_stored_files[app.id] = sorted(
            file_urn
            for file_urn, current in currents.items()
            if file_urn in writes or current is None
        )
        app_table_urns = {
            *table_urns,
            *self._app_table_urns(app).values(),
            *((previous.table_urns_by_qri or {}).values() if previous else []),
        }
        for file_urn, current in currents.items():
            if file_urn not in writes and current is None:
                continue
            self._pending_stored_files.append(
                PendingStoredFile(
                    app_id=app.id,
                    file_urn=file_urn,
                    writes=writes.get(file_urn, []),
                    current=current,
                    app_table_urns=app_table_urns,
                    has_field_schema=file_urn not in qvd_file_urns,
                )
            )

    def _gen_pending_stored_files(self) -> Iterable[MetadataWorkUnit]:
        shared_by_app: Dict[str, Optional[Set[str]]] = {}
        for pending in self._pending_stored_files:
            if pending.app_id not in shared_by_app:
                shared_by_app[pending.app_id] = self._table_urns_of_other_apps(
                    pending.app_id
                )
            shared = shared_by_app[pending.app_id]
            # A table another app also has, such as a QVD both load, may be
            # what that app stores into the file, so its edge is never this
            # app's to remove.
            owned = set() if shared is None else pending.app_table_urns - shared
            if shared is None:
                # Nothing is removed this run, so a later run must still know
                # the app may have edges into the file.
                self._keep_stored_file(pending.app_id, pending.file_urn)
            yield from self._gen_qvd_upstreams(
                pending.file_urn,
                pending.writes,
                pending.current,
                owned.__contains__,
                has_field_schema=pending.has_field_schema,
            )
        self._pending_stored_files = []

    def _keep_stored_file(self, app_id: str, file_urn: str) -> None:
        files = self._app_stored_files.setdefault(app_id, [])
        if file_urn not in files:
            files.append(file_urn)
            files.sort()
        self.app_state.add_stored_files(app_id, [file_urn])

    def _table_urns_of_other_apps(self, app_id: str) -> Optional[Set[str]]:
        """The tables of the other apps read or kept this run, or recorded by
        the last run; None when another app's tables may be missing from them,
        as when part of the run or the last run's checkpoints were not read."""
        if self.qlik_api.unread_items or any(
            other != app_id for other in self._apps_tables_unknown
        ):
            return None
        urns = {
            urn
            for other, table_urns in self._table_urns_by_app.items()
            if other != app_id
            for urn in table_urns
        }
        try:
            last_apps = self.app_state.previous_apps()
        except Exception as e:
            self.reporter.warning(
                title="Unable to load the last checkpoints of Qlik apps",
                message="Which tables other apps share with the app is unknown, "
                "so its edges into the files it stores into are only added to.",
                context=app_id,
                exc=e,
            )
            return None
        if last_apps is None:
            return None
        for other, checkpoint in last_apps.items():
            if other != app_id:
                urns.update((checkpoint.table_urns_by_qri or {}).values())
        return urns

    def _catalogued_as_qlik_dataset(
        self, storage: FileStorage, path: str
    ) -> Optional[bool]:
        """Whether Qlik catalogues the file as a dataset this run ingests, or
        None when the datasets could not be listed. Without the connection's
        own properties, a dataset of any connection holding the path counts."""
        datasets = self._dataset_urns_by_secure_qri
        if datasets is None:
            return None
        connections = self._catalogued_files.get(
            (storage.platform, qri_part_id(normalized_file_path(path))), set()
        )
        return (
            storage.qri_connection_id in connections
            if storage.qri_connection_id is not None
            else bool(connections)
        )

    @functools.cached_property
    def _catalogued_files(self) -> Dict[Tuple[FilePlatform, str], Set[str]]:
        """The connection parts of catalogued files' QRIs, by platform and the
        path part."""
        files: Dict[Tuple[FilePlatform, str], Set[str]] = {}
        for qri in self._dataset_urns_by_secure_qri or {}:
            match = FILE_QRI_RE.match(qri)
            platform = (
                PLATFORM_BY_QRI_SCHEME.get(match.group("scheme")) if match else None
            )
            if match and platform:
                files.setdefault((platform, match.group("path")), set()).add(
                    match.group("connection")
                )
        return files

    def _gen_stored_file_lineage(
        self, file_urn: str, table_urns: Iterable[str]
    ) -> Iterable[MetadataWorkUnit]:
        patch = DatasetPatchBuilder(file_urn)
        for urn in table_urns:
            patch.add_upstream_lineage(
                Upstream(dataset=urn, type=DatasetLineageType.COPY)
            )
        # Only added to: other apps, read or not, may store into the same file.
        yield from _patch_workunits(patch, is_primary_source=False)

    def _report_competing_stores(self, app: App, stores: List[QvdStore]) -> None:
        stored = [
            f"{store.label} INTO lib://{store.file.connection}/{store.file.path}"
            if store.file.connection
            else f"{store.label} INTO {store.file.path}"
            for store in stores
        ]
        self.reporter.qvd_stores_in_competing_branches.extend(
            f"{app.id}: {store}" for store in stored
        )
        self.reporter.warning(
            title="Qlik STOREs in competing branches are not linked",
            message="The app stores a table into different files from different "
            "branches of an IF or SWITCH, so which file it wrote can't be told "
            "and none of them is linked.",
            context=f"app={app.id}, stores={stored}",
        )

    def _stored_table_urn(
        self, app: App, label: str, table_urns: Set[str]
    ) -> Optional[str]:
        urn = self._gen_qlik_dataset_urn(app_table_identifier(app.id, label))
        if urn in table_urns:
            return urn
        # A load-model table is ingested under its QRI, not the app and label,
        # and the script may name it by its alias.
        urns = {
            self._gen_qlik_dataset_urn(table.tableQri.lower())
            for table in app.tables
            if table.tableQri
            and label.lower() in (table.tableName.lower(), table.tableAlias.lower())
        } & table_urns
        return next(iter(urns)) if len(urns) == 1 else None

    @functools.cached_property
    def _qvd_storage_urns(self) -> Dict[str, Callable[[str], str]]:
        return {
            name: partial(self._file_urn, storage, detail)
            for name, detail in self.config.file_connection_to_storage.items()
            if detail.storage_uri
            and (storage := file_storage_of_uri(detail.storage_uri))
        }

    def _file_source_resolution(
        self, app_id: str, file_qri: str
    ) -> _FileSourceResolution:
        """The dataset of a file in an app's lineage graph, such as a CSV on
        Azure Storage, when the app's load script reads it through a data
        connection whose storage is known."""
        resolver = self._file_sources_by_app.get(app_id)
        space_id = self._app_space_ids.get(app_id)
        if resolver is None or space_id is None:
            return _FileSourceResolution()
        candidates = resolver.candidates(file_qri)
        # A connection whose properties match the QRI's names the file for
        # certain; one whose storage is only mapped is trusted only when no
        # other connection the script reads the same path through could hold it.
        confirmed: Set[str] = set()
        unconfirmed: Set[str] = set()
        # Unconfirmed URNs whose storage location hashes to the QRI's part.
        likely: Set[str] = set()
        # Files on connections of unknown storage, named by connection and path.
        unmapped: Set[str] = set()
        unknown = False
        for reference in candidates.references:
            storage, detail = self._file_storage(reference.connection, space_id)
            if storage is None:
                info = self._find_data_connection(reference.connection, space_id)
                if _is_data_files(reference.connection) or (
                    info is not None and info.file_platform != candidates.platform
                ):
                    # DataFiles, listed or not, and a visible connection of
                    # another kind can't hold a file on this storage.
                    continue
                unmapped_urn = self._unmapped_storage_file_urn(
                    reference.connection,
                    reference.path,
                    space_id,
                    # A file the QRI places on another connector, such as
                    # SFTP, has no storage `file_connection_to_storage` maps.
                    report=candidates.platform is not None,
                )
                if unmapped_urn is None:
                    unknown = True
                else:
                    unmapped.add(unmapped_urn)
                continue
            if storage.platform != candidates.platform or (
                storage.qri_connection_id is not None
                and storage.qri_connection_id != candidates.connection_id
            ):
                continue
            urn = self._file_urn(storage, detail, reference.path)
            (confirmed if storage.qri_connection_id else unconfirmed).add(urn)
            if storage.likely_qri_connection_id == candidates.connection_id:
                likely.add(urn)
        unresolved = _FileSourceResolution(
            on_known_storage=bool(confirmed or unconfirmed)
        )
        if confirmed:
            urns = confirmed
        elif unknown:
            return unresolved
        elif unmapped:
            urns = unconfirmed | unmapped
        elif len(unconfirmed) > 1 and len(likely & unconfirmed) == 1:
            # Such as the prod and dev branches of an IF, of which only one ran.
            urns = likely & unconfirmed
        else:
            urns = unconfirmed
        if len(urns) != 1:
            return unresolved
        if file_qri not in self._resolved_file_qris:
            self._resolved_file_qris.add(file_qri)
            self.reporter.file_sources_resolved += 1
        urn = next(iter(urns))
        if urn in unmapped:
            self._link_unmapped_file(app_id, urn)
        return _FileSourceResolution(
            urn=urn, on_known_storage=unresolved.on_known_storage
        )

    def _link_unmapped_file(self, app_id: str, urn: str) -> None:
        self._linked_unmapped_files.add(urn)
        self._app_end_of_run_urns[app_id].add(urn)
        self._app_read_unmapped_files[app_id].add(urn)

    def _record_end_of_run_urns(self) -> None:
        for app_id, urns in self._app_end_of_run_urns.items():
            self.app_state.add_urns(app_id, urns)
        for app_id, urns in self._app_read_unmapped_files.items():
            self.app_state.add_read_unmapped_files(app_id, urns)

    def _learn_field_names(self, names: Iterable[str]) -> None:
        for name in names:
            self._field_names_by_id.setdefault(_qlik_field_id(name), name)

    def _graph_column_lineage(
        self,
        dataset_urn: str,
        field_source_qris: Dict[str, List[str]],
        own_app_id: Optional[str],
        table_source_qris: Collection[str] = (),
    ) -> List[FineGrainedLineage]:
        """Column lineage from the field level of Qlik's lineage graph. A source
        field is named only when its id is the hash of a field name the run
        has seen, such as the field it is loaded into."""
        if not self.config.include_column_lineage:
            # An unnamed source field would otherwise keep the table's lineage
            # from being written whole, for column lineage that isn't emitted.
            return []
        # A resource only named from the graph at field level would have no
        # edge to the table, so it is only named when a table source.
        table_sources = {
            *table_source_qris,
            *(qri.rpartition("#")[0] for qri in table_source_qris),
        }
        lineages = []
        for field, source_qris in field_source_qris.items():
            upstreams: List[str] = []
            for field_qri in source_qris:
                table_qri, _, field_id = field_qri.rpartition("#")
                urn, _ = self._resolve_source(
                    table_qri,
                    dataset_urn,
                    own_app_id,
                    name_from_graph=bool(
                        {table_qri, table_qri.rpartition("#")[0]} & table_sources
                    ),
                )
                name = (
                    field
                    if _qlik_field_id(field) == field_id
                    else self._field_names_by_id.get(field_id)
                )
                if urn and not name and urn not in self._graph_named_resources:
                    # The source field is real but no field of that name has
                    # been read yet, such as a renamed load from a QVD read
                    # later in the run, so the table's column lineage is not
                    # all known. A source only the upstream graph names is
                    # never read, so its field names never become known.
                    self._unnamed_field_lineage.add(dataset_urn)
                if urn and name:
                    upstream = builder.make_schema_field_urn(urn, name)
                    if upstream not in upstreams:
                        upstreams.append(upstream)
            if upstreams:
                lineages.append(
                    FineGrainedLineage(
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=upstreams,
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=[builder.make_schema_field_urn(dataset_urn, field)],
                    )
                )
        return lineages

    def _column_lineage(
        self, lineages: Optional[List[FineGrainedLineage]]
    ) -> Optional[List[FineGrainedLineage]]:
        return (lineages or None) if self.config.include_column_lineage else None

    @staticmethod
    def _patch_upstreams(
        dataset_urn: str,
        upstream_urns: List[str],
        lineage_type: str,
        fine_grained: Optional[List[FineGrainedLineage]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Adds lineage without replacing the edges an earlier run wrote."""
        if not upstream_urns and not fine_grained:
            return
        yield convert_upstream_lineage_to_patch(
            dataset_urn,
            UpstreamLineage(
                upstreams=[
                    Upstream(dataset=urn, type=lineage_type) for urn in upstream_urns
                ],
                fineGrainedLineages=fine_grained or None,
            ),
            None,
        )

    def _parse_reload_sql(self, app: App, labels: List[str]) -> _ParsedReloadSql:
        """The SQL statements of the app's last reload that may read a table
        one of `labels` names. Qlik ran them with variables set and included
        scripts inlined, so they hold SQL the load script only builds at run
        time, such as from a list of tables in a file."""
        names = {_label_parts(label)[-1] for label in labels}
        parsed = _ParsedReloadSql()
        unqualified: List[str] = []
        unresolved: Set[str] = set()
        for statement in app.reload_statements or []:
            match = RELOAD_SQL_RE.match(statement.statement)
            unparsed = match is None and bool(
                RELOAD_UNPARSED_SQL_RE.match(statement.statement)
            )
            if not match and not unparsed:
                continue
            sql = drop_condition_variables(
                match.group("sql").strip() if match else statement.statement.strip()
            )
            if is_connector_sql(sql):
                continue
            if "$(" in sql:
                parsed.unexpanded_variable = True
                continue
            if not any(_names_identifier(sql.lower(), name) for name in names):
                continue
            if unparsed:
                parsed.skipped_table_names |= _table_clause_names(sql)
                continue
            connection_name = statement.discriminator.strip().rstrip(";").strip()
            connection = self._resolve_script_connection(connection_name, app.spaceId)
            if connection is None:
                unresolved.add(redact_connection(connection_name))
                self._note_unseen_space(
                    _named_space(connection_name), "data connections"
                )
                parsed.skipped_table_names |= _table_clause_names(sql)
                continue
            result = self._parse_script_sql(sql, connection)
            if (
                result.debug_info.table_error
                and connection.platform in BACKTICK_QUOTED_PLATFORMS
                and "[" in sql
            ):
                result = self._parse_script_sql(brackets_to_backticks(sql), connection)
            if result.debug_info.table_error:
                self.reporter.warning(
                    title="Unable to parse Qlik reload SQL",
                    message="Lineage to the tables the statement reads is skipped.",
                    context=f"app={app.id}, connection="
                    f"{redact_connection(connection_name)}, "
                    f"error={_error_type(result.debug_info.table_error)}",
                )
                parsed.skipped_table_names |= _table_clause_names(sql)
                continue
            # An UPDATE, DELETE or MERGE loads no rows, and the tables an
            # INSERT writes are not read.
            read_urns = (
                [urn for urn in result.in_tables if urn not in result.out_tables]
                if result.query_type in READ_QUERY_TYPES
                else []
            )
            if not read_urns:
                # Such as EXEC('SELECT ...'), whose tables the parser can't see.
                parsed.skipped_table_names |= _table_clause_names(sql)
                continue
            parsed.skipped_table_names.update(
                part
                for urn in result.out_tables
                for part in DatasetUrn.from_string(urn).name.lower().split(".")
            )
            # SQL in a string literal runs dynamically and is never parsed.
            parsed.skipped_table_names |= _string_literal_table_names(sql)
            upstream_urns = [
                urn
                for urn in read_urns
                if has_qualified_name(
                    urn, connection.platform, connection.platform_instance
                )
            ]
            statement_unqualified = [
                urn for urn in read_urns if urn not in upstream_urns
            ]
            unqualified.extend(statement_unqualified)
            # A table without its database or schema may be the label's table
            # in another database, so it counts as SQL that could not be read.
            parsed.skipped_table_names.update(
                part
                for urn in statement_unqualified
                for part in DatasetUrn.from_string(urn).name.lower().split(".")
            )
            if upstream_urns:
                parsed.statements.append(
                    _ReloadSql(
                        upstream_urns=upstream_urns,
                        result=result,
                        urns_without_schema=self._urns_without_schema(
                            upstream_urns, result, connection
                        ),
                    )
                )
        for connection_name in sorted(unresolved):
            self.reporter.unresolved_data_connections.append(
                f"{app.id}: {connection_name}"
            )
        if unqualified:
            self.reporter.warning(
                title="Qlik reload SQL table is missing its database or schema",
                message="Lineage to these tables is skipped. Set `database` and "
                "`default_schema` for their connection in "
                "`data_connection_to_platform_instance`.",
                context=f"app={app.id}, tables={sorted(set(unqualified))}",
            )
        return parsed

    def _urns_without_schema(
        self,
        urns: List[str],
        result: SqlParsingResult,
        connection: ResolvedConnection,
    ) -> List[str]:
        """Which of the tables a statement reads DataHub holds no schema for.
        The parser only counts them for the whole statement."""
        debug_info = result.debug_info
        if (
            self.ctx.graph is None
            or debug_info.table_schemas_resolved >= debug_info.tables_discovered
        ):
            return []
        resolver = create_schema_resolver(
            platform=connection.platform,
            env=connection.env,
            graph=self.ctx.graph,
            platform_instance=connection.platform_instance,
        )
        return [urn for urn in urns if resolver.resolve_urn(urn)[1] is None]

    def _reload_sql_tables(
        self, app: App, script_tables: Dict[str, ScriptTableLineage]
    ) -> Dict[str, ScriptTableLineage]:
        """Lineage of the graph tables' database sources that the load script
        doesn't trace, from the SQL of the app's last reload, by table QRI.
        The graph names each source table by a label such as `dbo.orders`,
        which is linked only when exactly one table the SQL reads matches it."""
        untraced: Dict[str, List[_UntracedSources]] = {}
        for table in app.lineage_tables:
            script_table = script_tables.get(table.qri)
            groups = _untraced_source_groups(
                table.database_source_labels,
                script_table.upstream_urns if script_table else [],
            )
            if groups:
                untraced[table.qri] = groups
        if not untraced or app.reload_statements is None:
            return {}
        parsed = self._parse_reload_sql(
            app,
            [
                label
                for groups in untraced.values()
                for group in groups
                for label in group.labels.values()
            ],
        )
        lineages: Dict[str, ScriptTableLineage] = {}
        for table in app.lineage_tables:
            if table.qri not in untraced:
                continue
            lineage = ScriptTableLineage(
                label=table.label,
                identifier=self._lineage_table_identifier(app.id, table),
            )
            for group in untraced[table.qri]:
                urn = self._reload_sql_source(app, parsed, group)
                if urn is None:
                    continue
                if urn not in lineage.upstream_urns:
                    lineage.upstream_urns.append(urn)
                if len(group.labels) == 1 and not group.traced_count:
                    # With several such sources, which one the table is isn't
                    # known, so neither are the graph fields it loads.
                    (source_qri,) = group.labels
                    self._add_reload_column_lineage(
                        lineage, table, parsed, source_qri, urn
                    )
            if lineage.upstream_urns:
                self.reporter.reload_sql_tables_with_lineage += 1
                lineages[table.qri] = lineage
        return lineages

    def _reload_sql_source(
        self, app: App, parsed: _ParsedReloadSql, group: _UntracedSources
    ) -> Optional[str]:
        """The one table the reload SQL reads, and the load script doesn't,
        that the graph labels of a group of database sources name."""
        # The SQL may spell a table the script linked in another case.
        script_urns = {urn.lower() for urn in group.script_urns}
        matches = {
            urn
            for statement in parsed.statements
            for urn in statement.upstream_urns
            if urn.lower() not in script_urns
            and any(
                _urn_names_table(urn, qri, label) for qri, label in group.labels.items()
            )
        }
        labels = ", ".join(sorted(set(group.labels.values())))
        unread = parsed.unexpanded_variable or group.name in parsed.skipped_table_names
        untraced_count = len(group.labels) - group.traced_count
        if len(matches) > 1 or (matches and untraced_count > 1):
            self.reporter.ambiguous_reload_sql_sources.append(
                f"{app.id}: {labels} matches {sorted(matches)}"
            )
            return None
        if not matches:
            if not unread:
                self.reporter.unmatched_reload_sql_sources.extend(
                    dict.fromkeys(
                        f"{app.id}: {label} ({_database_qri_platform(qri)})"
                        for qri, label in group.labels.items()
                    )
                )
            return None
        if unread:
            # A statement that could not be read may name another table with
            # the label, so the match is not certain.
            self.reporter.ambiguous_reload_sql_sources.append(
                f"{app.id}: {labels} may also be named by reload SQL that could "
                "not be read"
            )
            return None
        urn = matches.pop()
        if any(urn in statement.urns_without_schema for statement in parsed.statements):
            # Linked as the SQL names it, which may differ in case from the
            # dataset DataHub holds.
            self.reporter.reload_sql_tables_not_in_datahub.append(f"{app.id}: {urn}")
        return urn

    @staticmethod
    def _add_reload_column_lineage(
        lineage: ScriptTableLineage,
        table: LineageTable,
        parsed: _ParsedReloadSql,
        source_qri: str,
        urn: str,
    ) -> None:
        """Maps the graph table's fields to the columns of `urn` that the one
        reload statement reading it selects under the same name."""
        statements = [s for s in parsed.statements if urn in s.upstream_urns]
        if len(statements) != 1:
            # Another statement reading the table may load another table.
            return
        # The ids of the source's columns each field loads, when the graph's
        # field level names them.
        source_field_ids: Dict[str, Set[str]] = {}
        for field, field_qris in table.field_source_qris.items():
            for field_qri in field_qris:
                table_qri, _, field_id = field_qri.rpartition("#")
                if table_qri == source_qri:
                    source_field_ids.setdefault(field, set()).add(field_id)
        # The graph table's fields are Qlik's names, which may differ in case
        # from the columns.
        fields = {field.lower(): field for field in table.fields}
        for column in statements[0].result.column_lineage or []:
            field_name = fields.get(column.downstream.column.lower())
            if field_name is None:
                continue
            refs = [ref for ref in column.upstreams if ref.table == urn]
            if source_field_ids:
                # A LOAD over the SQL may rename fields, which the reload SQL
                # doesn't show, so the graph decides which column each loads.
                refs = _refs_of_field_sources(
                    refs, field_name, source_field_ids.get(field_name, set())
                )
            if not refs:
                continue
            upstreams = lineage.column_upstreams.setdefault(field_name, [])
            upstreams.extend(
                upstream
                for upstream in column_refs_to_schema_field_urns(refs)
                if upstream not in upstreams
            )

    def _read_database_source_labels(self, app: App) -> None:
        """Labels the graph tables' database sources by, such as `dbo.orders`,
        which only the upstream graph gives: from the app's upstream graph when
        a source only it names already had it read, or else from the one-level
        graph alone."""
        unlabelled = [
            (table, qri)
            for table in app.lineage_tables
            for qri in table.source_qris
            if qri.startswith(DATABASE_QRI_PREFIX)
            and qri not in table.database_source_labels
        ]
        # Without the reload's SQL, a label names nothing to link.
        if not unlabelled or app.reload_statements is None:
            return
        upstreams = self._lineage_upstreams.get(app.id)
        if upstreams is not None and upstreams.database_labels is not None:
            labels = upstreams.database_labels
        else:
            try:
                labels = self.qlik_api.get_database_labels(app.qri)
            except Exception as e:
                # The tables stay unlabelled, so their lineage is only added to.
                logger.debug(f"Unable to read database labels of app {app.id}: {e}")
                self.reporter.database_label_read_failures.append(f"{app.id}: {e}")
                return
        for table, qri in unlabelled:
            if qri in labels:
                table.database_source_labels[qri] = labels[qri]

    def _gen_lineage_tables_workunit(
        self, app: App, script_tables: Dict[str, ScriptTableLineage]
    ) -> Iterable[MetadataWorkUnit]:
        if app.reload_statements is not None:
            self._record_reload_files(app.id, app.reload_statements)
        self._read_database_source_labels(app)
        reload_tables = self._reload_sql_tables(app, script_tables)
        for table in app.lineage_tables:
            script_table = script_tables.get(table.qri)
            reload_table = reload_tables.get(table.qri)
            if reload_table is not None:
                script_table = (
                    reload_table
                    if script_table is None
                    else _merged_lineage(script_table, reload_table)
                )
            identifier = self._lineage_table_identifier(app.id, table)
            dataset_urn = self._gen_qlik_dataset_urn(identifier)
            # The reload SQL only adds lineage: the fields stay the script's,
            # or the graph's, plus those the SQL gives lineage to.
            fields = self._lineage_table_fields(table, script_tables.get(table.qri))
            if reload_table is not None:
                names = {field.name.lower() for field in fields}
                fields.extend(
                    QlikDatasetSchemaField(name=column)
                    for column in reload_table.column_upstreams
                    if column.lower() not in names
                )
            schema = self._table_schema(identifier, fields)
            if schema and all(field.dataType is None for field in schema):
                self._untyped_tables[dataset_urn] = (identifier, schema)
            yield from self._gen_app_table_entity(identifier, schema, app.id)
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetProperties(
                    name=table.label,
                    qualifiedName=table.label,
                    customProperties={
                        Constant.TYPE: DatasetSubTypes.QLIK_TABLE,
                        Constant.QRI: table.qri,
                        **(
                            {Constant.DATACONNECTORNAME: script_table.connection}
                            if script_table and script_table.connection
                            else {}
                        ),
                    },
                ),
            ).as_workunit()
            pending = PendingTableLineage(
                app_id=app.id,
                table_label=table.label,
                dataset_urn=dataset_urn,
                source_qris=table.source_qris,
                script_upstream_urns=script_table.upstream_urns if script_table else [],
                incomplete_reason=(
                    "the app's load script could not be read"
                    if app.script_read_failed
                    else "the app's tables could not be read"
                    if app.tables_read_failed
                    else "the table's field lineage could not be read"
                    if table.field_lineage_failed
                    else "the SQL of the app's last reload could not be read"
                    if app.reload_statements_read_failed
                    and any(
                        qri.startswith(DATABASE_QRI_PREFIX) for qri in table.source_qris
                    )
                    else "the data connections could not be read"
                    if self._data_connections_unread()
                    and any(
                        qri.startswith(DATABASE_QRI_PREFIX) for qri in table.source_qris
                    )
                    else "a database table in the lineage graph has no label"
                    if any(
                        qri.startswith(DATABASE_QRI_PREFIX)
                        and qri not in table.database_source_labels
                        for qri in table.source_qris
                    )
                    else None
                ),
                script_fields=(
                    self._script_column_lineage(dataset_urn, script_table) or []
                    if script_table
                    else []
                ),
                field_source_qris=table.field_source_qris,
            )
            if self._reads_an_app_not_yet_read(app.id, table):
                # Its tables are only known once that app is read.
                self._pending_table_lineage.append(pending)
                continue
            graph_fields = self._graph_column_lineage(
                dataset_urn,
                table.field_source_qris,
                own_app_id=app.id,
                table_source_qris=table.source_qris,
            )
            if dataset_urn in self._unnamed_field_lineage:
                # A source field is named by an item read later in the run, such
                # as a renamed load from a QVD listed after the app, so the
                # table's lineage is written, whole or as a patch, once every
                # name is known.
                self._unnamed_field_lineage.discard(dataset_urn)
                self._pending_table_lineage.append(pending)
                continue
            yield from self._gen_table_lineage(pending, graph_fields)

    def _reads_an_app_not_yet_read(self, app_id: str, table: LineageTable) -> bool:
        source_apps = {
            _qlik_app_id(qri)
            for qri in [
                *table.source_qris,
                *(
                    qri.rpartition("#")[0]
                    for qris in table.field_source_qris.values()
                    for qri in qris
                ),
            ]
        } - {None, app_id}
        return any(
            source_app not in self._apps_tables_known
            and source_app not in self._apps_tables_unknown
            for source_app in source_apps
        )

    def _gen_pending_table_lineage(self) -> Iterable[MetadataWorkUnit]:
        for pending in self._pending_table_lineage:
            graph_fields = self._graph_column_lineage(
                pending.dataset_urn,
                pending.field_source_qris,
                own_app_id=pending.app_id,
                table_source_qris=pending.source_qris,
            )
            yield from self._gen_table_lineage(pending, graph_fields)
        self._pending_table_lineage = []
        for (app_id, reason), labels in self._only_added_tables.items():
            self.reporter.warning(
                title="Lineage of Qlik app tables only added to",
                message="A source of these tables could not be read this run, so "
                "the lineage read this run is added to their existing lineage "
                "instead of replacing it.",
                context=f"app={app_id}, reason={reason}, "
                f"tables={_bounded_list(sorted(labels))}",
            )
        self._only_added_tables = {}
        yield from self._gen_pending_stored_files()
        yield from self._deferred_fingerprints.values()
        self._deferred_fingerprints = {}

    def _gen_table_lineage(
        self, pending: "PendingTableLineage", graph_fields: List[FineGrainedLineage]
    ) -> Iterable[MetadataWorkUnit]:
        fine_grained = self._column_lineage([*pending.script_fields, *graph_fields])
        resolved = self._resolve_lineage_sources(
            pending.source_qris, pending.dataset_urn, own_app_id=pending.app_id
        )
        upstream_urns = list(resolved.urns)
        upstream_urns.extend(
            urn for urn in pending.script_upstream_urns if urn not in upstream_urns
        )
        reason = (
            pending.incomplete_reason
            or "; ".join(resolved.unknown_reasons)
            or (
                "a source field's name was not read"
                if pending.dataset_urn in self._unnamed_field_lineage
                else None
            )
        )
        if reason is None:
            # The app owns its tables, so the lineage is written whole, which
            # also drops upstreams the app no longer reads.
            yield MetadataChangeProposalWrapper(
                entityUrn=pending.dataset_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(dataset=urn, type=DatasetLineageType.TRANSFORMED)
                        for urn in upstream_urns
                    ],
                    fineGrainedLineages=fine_grained,
                ),
            ).as_workunit()
            return
        # A dataset, the load script and the SQL lineage in it, or the name of
        # a source field could not be read this run: replacing the lineage
        # would drop the edges an earlier run wrote, so what was read is added
        # to them instead.
        self._only_added_tables.setdefault((pending.app_id, reason), []).append(
            pending.table_label
        )
        yield from self._patch_upstreams(
            pending.dataset_urn,
            upstream_urns,
            DatasetLineageType.TRANSFORMED,
            fine_grained,
        )

    @staticmethod
    def _match_script_tables(
        lineage_tables: List[LineageTable], script_tables: List[ScriptTableLineage]
    ) -> Dict[str, ScriptTableLineage]:
        """Script tables that are also lineage graph tables, by graph table QRI,
        so each table is ingested once."""
        by_label = {
            table.label.lower(): table for table in script_tables if not table.mapping
        }
        return {
            table.qri: by_label[table.label.lower()]
            for table in lineage_tables
            if table.label.lower() in by_label
        }

    def _gen_app_tables_workunit(
        self, tables: List[QlikTable], app_id: str
    ) -> Iterable[MetadataWorkUnit]:
        for table in tables:
            table_identifier = self._get_app_table_identifier(table)
            if not table_identifier:
                continue
            dataset_urn = self._gen_qlik_dataset_urn(table_identifier)

            yield from self._gen_app_table_entity(
                table_identifier, table.datasetSchema, app_id
            )

            yield self._gen_app_table_properties(dataset_urn, table)

            upstream_lineage_workunit = self._gen_app_table_upstream_lineage(
                dataset_urn, table
            )
            if upstream_lineage_workunit:
                yield upstream_lineage_workunit

    @staticmethod
    def _name_unknown_tables(app: App, script_tables: List[ScriptTableLineage]) -> None:
        """Gives a script table the parser could not name its name in the data
        model, when exactly one data model table no other table claims has
        exactly its columns. Otherwise it keeps its made-up name."""
        # A lineage graph table stays claimable: an unlabelled load the graph
        # also traces must take its name, so both are ingested as one table.
        claimed = {
            table.label.lower() for table in script_tables if not table.label_unknown
        }
        unclaimed = [
            table
            for table in app.data_model_tables
            if table.name.lower() not in claimed
        ]
        by_fields: Dict[frozenset, List[str]] = {}
        for model_table in unclaimed:
            fields = frozenset(field.name.lower() for field in model_table.fields)
            by_fields.setdefault(fields, []).append(model_table.name)
        unknown = [
            table
            for table in script_tables
            if table.label_unknown and not table.mapping and table.column_upstreams
        ]
        columns = [frozenset(c.lower() for c in t.column_upstreams) for t in unknown]
        for table, table_columns in zip(unknown, columns, strict=True):
            names = by_fields.get(table_columns, [])
            # Two unnamed tables with the same columns can't be told apart.
            if len(names) == 1 and columns.count(table_columns) == 1:
                table.label = names[0]
                table.identifier = app_table_identifier(app.id, names[0])
                table.label_unknown = False

    def _use_data_model_field_names(
        self, script_tables: List[ScriptTableLineage]
    ) -> None:
        """Spells each column as the data model does, so column lineage lands on
        the schema's fields. The SQL parser may change a column's case."""
        for table in script_tables:
            fields = self._data_model_fields.get(table.identifier)
            if not fields:
                continue
            names = {field.name for field in fields}
            by_lower: Dict[str, List[str]] = {}
            for name in names:
                by_lower.setdefault(name.lower(), []).append(name)
            column_upstreams: Dict[str, List[str]] = {}
            for column, upstreams in table.column_upstreams.items():
                matches = by_lower.get(column.lower(), [])
                # Qlik field names are case-sensitive, so a column matching two
                # fields keeps its own spelling.
                name = column if column in names or len(matches) != 1 else matches[0]
                merged = column_upstreams.setdefault(name, [])
                merged.extend(u for u in upstreams if u not in merged)
            table.column_upstreams = column_upstreams

    def _lineage_table_fields(
        self, table: LineageTable, script_table: Optional[ScriptTableLineage]
    ) -> List[QlikDatasetSchemaField]:
        """The fields the load script or, failing that, the field level of the
        lineage graph gives a table, for a table the data model no longer
        holds, such as one the script drops after storing it."""
        parsed = self._script_table_fields(script_table) if script_table else []
        return parsed or [QlikDatasetSchemaField(name=name) for name in table.fields]

    def _table_schema(
        self, identifier: str, parsed: List[QlikDatasetSchemaField]
    ) -> List[QlikDatasetSchemaField]:
        """A table's fields as the app's data model holds them, which the load
        script only shows when its SELECT lists them."""
        return self._data_model_fields.get(identifier) or parsed

    def _app_table_fields(
        self, tables: List[QlikTable], script_tables: List[ScriptTableLineage]
    ) -> Dict[str, List[QlikDatasetSchemaField]]:
        table_fields: Dict[str, List[QlikDatasetSchemaField]] = {}
        for table in tables:
            identifier = self._get_app_table_identifier(table)
            if identifier:
                table_fields[self._gen_qlik_dataset_urn(identifier)] = (
                    table.datasetSchema
                )
        for script_table in script_tables:
            # A mapping table is only used by ApplyMap and is not in the data
            # model, so charts cannot read it.
            if script_table.mapping:
                continue
            table_fields[self._gen_qlik_dataset_urn(script_table.identifier)] = (
                self._table_schema(
                    script_table.identifier, self._script_table_fields(script_table)
                )
            )
        return table_fields

    def _gen_app_workunit(
        self, app: App, fingerprint: Optional[str] = None
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Qlik App to Datahub container
        """
        owner_username = self._get_owner_name(app.ownerId)
        self._data_model_fields = {
            app_table_identifier(app.id, table.name): table.fields
            for table in app.data_model_tables
        }
        self._learn_field_names(_app_field_names(app))
        if self.config.on_prem is None and not app.tables:
            self.reporter.apps_without_load_model.append(app.id)
        parsed = self._parsed_script(app)
        self._file_sources_by_app[app.id] = self._file_source_resolver(app, parsed)
        if parsed.has_includes:
            self._apps_with_includes.add(app.id)
        if app.variables_read_failed:
            self._apps_variables_unread.add(app.id)
            self.reporter.warning(
                title="Unable to read Qlik app variables",
                message="Load script paths and chart expressions that use these "
                "apps' variables are only expanded from `script_variables`. "
                "Lineage to the files they load is only added to this run, and "
                "chart fields read through other variables are left out.",
                context=app.id,
            )
        self._app_space_ids[app.id] = app.spaceId
        script_tables = self._build_script_tables(app, parsed=parsed)
        self._name_unknown_tables(app, script_tables)
        qvd_lineage = (
            self._app_qvd_lineage(app, parsed, script_tables)
            if self.config.on_prem is not None
            else None
        )
        binary_load = None
        if qvd_lineage is not None:
            script_tables = self._merge_tables(script_tables, qvd_lineage.tables)
            # Read, even with no tables, so a BINARY load from it isn't
            # reported as from an app the run didn't read.
            if app.id in self._binary_source_apps:
                self._app_urns.setdefault(app.id, set())
            self._record_model_fields(app)
            binary_load = self._binary_load(app, parsed)
            if binary_load.tables:
                self._apps_with_binary_tables.add(app.id)
            script_tables = self._merge_tables(script_tables, binary_load.tables)
        self._use_data_model_field_names(script_tables)
        matched_script_tables = self._match_script_tables(
            app.lineage_tables, script_tables
        )
        matched_ids = {id(table) for table in matched_script_tables.values()}
        script_tables = [
            table for table in script_tables if id(table) not in matched_ids
        ]
        table_fields = self._app_table_fields(app.tables, script_tables)
        script_table_urns = {
            self._gen_qlik_dataset_urn(table.identifier) for table in script_tables
        }
        self._register_app_tables(app)
        for table in app.lineage_tables:
            matched = matched_script_tables.get(table.qri)
            urn = self._gen_qlik_dataset_urn(
                self._lineage_table_identifier(app.id, table)
            )
            table_fields[urn] = self._table_schema(
                self._lineage_table_identifier(app.id, table),
                self._lineage_table_fields(table, matched),
            )
            if matched:
                script_table_urns.add(urn)
        input_tables_urns = list(table_fields)
        container_workunits = list(
            gen_containers(
                container_key=self._gen_app_key(app.id),
                name=app.qTitle,
                description=app.description,
                # "Qlik App" first, as the UI labels a container by its first
                # subtype; "Workbook" makes the UI treat the app as its charts'
                # workbook, as it does for Tableau.
                sub_types=[
                    BIContainerSubTypes.QLIK_APP,
                    BIContainerSubTypes.WORKBOOK,
                ],
                parent_container_key=self._gen_space_key(app.spaceId),
                extra_properties={
                    Constant.QRI: app.qri,
                    **({Constant.USAGE: app.qUsage} if app.qUsage else {}),
                },
                owner_urn=(
                    builder.make_user_urn(owner_username)
                    if self.config.ingest_owner and owner_username
                    else None
                ),
                external_url=f"https://{self.ui_host}/sense/app/{app.id}/overview",
                created=int(app.createdAt.timestamp() * 1000),
                last_modified=int(app.updatedAt.timestamp() * 1000),
            )
        )
        yield from container_workunits
        app_fields = self._index_app_fields(table_fields, script_table_urns)
        has_unknown_schema = any(not fields for fields in table_fields.values())
        chart_inputs = (
            None
            if app.tables_read_failed
            else self._chart_inputs(
                (chart for sheet in app.sheets for chart in sheet.charts),
                self._app_variables(app),
                app_fields,
                input_tables_urns,
                has_unknown_schema,
            )
        )
        yield from self._gen_app_dashboard_workunit(
            app,
            owner_username,
            None
            if chart_inputs is None
            else self._tables_without_chart_path(app, chart_inputs, input_tables_urns),
        )
        yield from self._gen_sheets_workunit(app, chart_inputs)
        yield from self._gen_app_tables_workunit(app.tables, app.id)
        yield from self._gen_lineage_tables_workunit(app, matched_script_tables)
        yield from self._gen_script_tables_workunit(
            script_tables,
            app.id,
            # A table the lineage graph could not be read for may be one an
            # earlier run merged with a graph table, whose other edges stay.
            replace_lineage=not app.tables_read_failed,
            binary_links=binary_load.links if binary_load is not None else {},
        )
        if qvd_lineage is not None:
            yield from self._gen_file_entities(
                qvd_lineage.paths, DatasetSubTypes.QLIK_QVD
            )
        if binary_load is not None:
            yield from self._gen_file_entities(
                binary_load.paths, DatasetSubTypes.QLIK_APP_FILE
            )
        if qvd_lineage is None:
            self._queue_file_store_lineage(app, parsed, table_fields)
        if fingerprint:
            # Skipping an unchanged app trusts this fingerprint, so it is written
            # only after everything else the app produced.
            fingerprint_wu = self._gen_fingerprint_workunit(
                container_workunits, fingerprint
            )
            if any(p.app_id == app.id for p in self._pending_table_lineage) or any(
                p.app_id == app.id for p in self._pending_stored_files
            ):
                self._deferred_fingerprints[app.id] = fingerprint_wu
            else:
                yield fingerprint_wu

    def _gen_fingerprint_workunit(
        self, container_workunits: List[MetadataWorkUnit], fingerprint: str
    ) -> MetadataWorkUnit:
        properties = next(
            aspect
            for aspect in (
                wu.get_aspect_of_type(ContainerPropertiesClass)
                for wu in container_workunits
            )
            if aspect
        )
        stamped = copy.deepcopy(properties)
        stamped.customProperties = {
            **(stamped.customProperties or {}),
            Constant.FINGERPRINT: fingerprint,
        }
        urn = container_workunits[0].get_urn()
        return MetadataWorkUnit(
            id=f"{urn}-fingerprint",
            mcp=MetadataChangeProposalWrapper(entityUrn=urn, aspect=stamped),
        )

    def _gen_qlik_dataset_urn(self, dataset_identifier: str) -> str:
        return builder.make_dataset_urn_with_platform_instance(
            name=dataset_identifier,
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
        )

    def _gen_dataplatform_instance_aspect(
        self, entity_urn: str
    ) -> Optional[MetadataWorkUnit]:
        if self.config.platform_instance:
            aspect = DataPlatformInstanceClass(
                platform=builder.make_data_platform_urn(self.platform),
                instance=builder.make_dataplatform_instance_urn(
                    self.platform, self.config.platform_instance
                ),
            )
            return MetadataChangeProposalWrapper(
                entityUrn=entity_urn, aspect=aspect
            ).as_workunit()
        else:
            return None

    @staticmethod
    def _gen_schema_field(field: QlikDatasetSchemaField) -> SchemaField:
        return SchemaField(
            fieldPath=field.name,
            type=SchemaFieldDataTypeClass(
                type=(
                    FIELD_TYPE_MAPPING.get(field.dataType, NullType)()
                    if field.dataType
                    else NullType()
                )
            ),
            nativeDataType=field.dataType if field.dataType else "",
            nullable=field.nullable,
            isPartOfKey=field.primaryKey,
        )

    def _gen_schema_fields(
        self, schema: List[QlikDatasetSchemaField]
    ) -> List[SchemaField]:
        return [self._gen_schema_field(field) for field in schema]

    def _gen_schema_metadata(
        self, dataset_identifier: str, dataset_schema: List[QlikDatasetSchemaField]
    ) -> MetadataWorkUnit:
        dataset_urn = self._gen_qlik_dataset_urn(dataset_identifier)

        schema_metadata = SchemaMetadata(
            schemaName=dataset_identifier,
            platform=builder.make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            fields=self._gen_schema_fields(dataset_schema),
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=schema_metadata
        ).as_workunit()

    def _gen_dataset_properties(
        self, dataset_urn: str, dataset: QlikDataset
    ) -> MetadataWorkUnit:
        dataset_properties = DatasetProperties(
            name=_dataset_display_name(dataset),
            description=dataset.description,
            qualifiedName=dataset.name,
            externalUrl=f"https://{self.ui_host}/dataset/{dataset.itemId}",
            created=TimeStamp(time=int(dataset.createdAt.timestamp() * 1000)),
            lastModified=TimeStamp(time=int(dataset.updatedAt.timestamp() * 1000)),
        )
        dataset_properties.customProperties.update(
            {
                Constant.QRI: dataset.secureQri,
                Constant.SPACEID: dataset.spaceId,
                Constant.TYPE: _dataset_subtype(dataset),
                Constant.DATASETTYPE: dataset.type,
                Constant.SIZE: str(dataset.size),
                Constant.ROWCOUNT: str(dataset.rowCount),
            }
        )

        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

    @staticmethod
    def _qlik_dataset_identifier(space_id: str, name: str) -> str:
        return f"{space_id}.{name}".lower()

    def _gen_dataset_workunit(self, dataset: QlikDataset) -> Iterable[MetadataWorkUnit]:
        dataset_identifier = self._qlik_dataset_identifier(
            dataset.spaceId, dataset.name
        )
        dataset_urn = self._gen_qlik_dataset_urn(dataset_identifier)
        if dataset.stored_from_qris is not None:
            self._dataset_store_sources[dataset_urn] = dataset.stored_from_qris
            self._dataset_store_field_sources[dataset_urn] = (
                dataset.stored_field_source_qris
            )
            if dataset.stored_field_lineage_failed:
                self._dataset_store_fields_unknown.add(dataset_urn)
        self._learn_field_names(field.name for field in dataset.datasetSchema)
        self._dataset_field_types[dataset_urn] = {
            field.name: field.dataType for field in dataset.datasetSchema
        }

        yield self._gen_entity_status_aspect(dataset_urn)

        yield self._gen_schema_metadata(dataset_identifier, dataset.datasetSchema)

        yield self._gen_dataset_properties(dataset_urn, dataset)

        yield from add_entity_to_container(
            container_key=self._gen_space_key(dataset.spaceId),
            entity_type="dataset",
            entity_urn=dataset_urn,
        )

        dpi_aspect = self._gen_dataplatform_instance_aspect(dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        owner_username = self._get_owner_name(dataset.ownerId)
        if self.config.ingest_owner and owner_username:
            yield self._gen_entity_owner_aspect(dataset_urn, owner_username)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypes(typeNames=[_dataset_subtype(dataset)]),
        ).as_workunit()

    def _app_qvd_lineage(
        self, app: App, parsed: ParsedScript, script_tables: List[ScriptTableLineage]
    ) -> AppQvdLineage:
        resolver = self._qvd_path_resolver(app)
        lineage = app_qvd_lineage(
            app.id,
            parsed,
            resolver,
            self._gen_qlik_dataset_urn,
            table_fields={
                **{
                    table.identifier: list(table.column_upstreams)
                    for table in script_tables
                    if not table.mapping
                },
                **{
                    identifier: [field.name for field in fields]
                    for identifier, fields in self._data_model_fields.items()
                },
            },
        )
        for path in [*parsed.unresolved_qvd_paths, *resolver.unresolved]:
            self.reporter.unresolved_qvd_paths.append(f"{app.id}: {path}")
            self._unresolved_qvd_loads += 1
        for path in parsed.unlabelled_qvd_loads:
            self.reporter.unlabelled_qvd_loads.append(f"{app.id}: {path}")
            self._unlabelled_qvd_loads += 1
        self.reporter.qvds_found += len(lineage.paths)
        self.reporter.storage_qvds_resolved += len(lineage.storage_urns)
        if app.script is None or app.script_read_failed:
            # QVDs the script writes can't be known, so their lineage is kept.
            self._apps_qvds_unknown[app.id] = "its load script could not be read"
            return lineage
        if self._data_connections_unread():
            # Without the folder connections, the lib:// QVDs it writes are
            # unknown.
            unknown_reason: Optional[str] = "data connections could not be listed"
        elif parsed.has_unresolved_store or lineage.has_unresolved_store:
            # Such as a loop STORE, or a path built from a variable that failed
            # to read.
            unknown_reason = UNRESOLVED_STORE
        else:
            unknown_reason = None
        if unknown_reason:
            # The writes that resolved, such as to a mapped storage connection,
            # are still added, and the app's edges into QVDs are kept.
            lineage.qvds.writes_complete = False
            self._apps_qvds_unknown[app.id] = unknown_reason
        self._app_qvds[app.id] = lineage.qvds
        return lineage

    def _qvd_path_resolver(self, app: App) -> QvdPathResolver:
        on_prem = self.config.on_prem
        assert on_prem is not None
        return QvdPathResolver(
            folders={
                name: info.folder_path
                for name, info in self._get_data_connections().items()
                if info.folder_path
            },
            ignored_connections=on_prem.qvd_ignored_connections,
            folder_aliases=on_prem.qvd_folder_aliases,
            variables_known=not app.variables_read_failed,
            storage_urns=self._qvd_storage_urns,
        )

    @staticmethod
    def _merge_tables(
        script_tables: List[ScriptTableLineage],
        tables: Dict[str, ScriptTableLineage],
    ) -> List[ScriptTableLineage]:
        """App tables found another way, such as loaded from or stored into
        QVDs, merged into the script tables of the same name, so each table is
        ingested once."""
        by_identifier = {
            table.identifier: table for table in script_tables if not table.mapping
        }
        merged = list(script_tables)
        for identifier, qvd_table in tables.items():
            table = by_identifier.get(identifier)
            if table is None:
                merged.append(qvd_table)
                continue
            table.upstream_urns.extend(
                urn for urn in qvd_table.upstream_urns if urn not in table.upstream_urns
            )
            for column, upstreams in qvd_table.column_upstreams.items():
                known = table.column_upstreams.setdefault(column, [])
                known.extend(urn for urn in upstreams if urn not in known)
        return merged

    def _gen_file_entities(
        self, paths: Dict[str, str], subtype: DatasetSubTypes
    ) -> Iterable[MetadataWorkUnit]:
        # Emitted by every app that touches the file, so it stays while any of
        # them does; a QVD's schema and lineage come from all of them at the end.
        # The folders too: each app's checkpoint only keeps what it emitted,
        # so they are deduplicated per app, not per run.
        containers = ContainerWUCreator(
            self.platform, self.config.platform_instance, self.config.env
        )
        for urn, path in paths.items():
            yield from self._gen_file_entity(
                urn, posixpath.basename(path), subtype, path
            )
            # In Folder containers from the server and share (or drive) down,
            # as the data lake sources place files; without a container the
            # browse path would be cut at the dots of the server name. A
            # relative path has no known root to start the folders from.
            if CANONICAL_ABSOLUTE_PATH_RE.match(path):
                yield from containers.create_container_hierarchy(path.lstrip("/"), urn)
            else:
                yield from self._gen_lib_path_location(urn, path)

    def _note_unseen_space(self, space: Optional[str], need: str) -> None:
        """Records a space a script or Qlik names, when the API key can't see
        it, as one the run needs access to for complete lineage."""
        if not space or self.config.on_prem is not None:
            return
        if self._listed_space_names is None:
            # Without the listing, every space would look unseen.
            if any(item.name == UNREAD_SPACES for item in self.qlik_api.unread_items):
                return
            self._listed_space_names = {
                name.lower() for name in self.qlik_api.spaces.values()
            }
        if space.lower() in self._listed_space_names:
            return
        _, needs = self._unseen_spaces.setdefault(space.lower(), (space, set()))
        needs.add(need)

    def _ingested_space_id(self, name: str) -> Optional[str]:
        # Qlik resolves `lib://` space names regardless of case, so a space is
        # matched that way, and only when no other space shares the name.
        ids = [
            space_id
            for space_id, space_name in self.qlik_api.spaces.items()
            if space_name.lower() == name.lower()
        ]
        if len(ids) != 1 or ids[0] not in self._allowed_space_ids:
            return None
        return ids[0]

    def _script_spelling(self, path: str) -> str:
        """A `lib://` path spelled as the load scripts and stores the run read
        spell it. Qlik ignores case in these paths and records loads in
        lowercase, so the same space, connection or folder would otherwise be
        named in several spellings."""
        spellings = self._lib_path_spellings()
        segments = _lib_path_segments(path) or self._voted_segments(path, spellings)
        if segments is None:
            return path
        spelled = []
        for i, segment in enumerate(segments):
            votes = spellings.get(_path_key("/".join(segments[: i + 1])))
            # The most used spelling, and the same one every run on a tie.
            spelled.append(
                min(votes, key=lambda name: (-votes[name], name)) if votes else segment
            )
        return _lib_path(spelled)

    @staticmethod
    def _voted_segments(
        path: str, spellings: Dict[str, Counter[str]]
    ) -> Optional[List[str]]:
        """The segments of a path whose space and connection can't be told
        apart, when exactly one way of splitting them is spelled elsewhere."""
        match = LIB_PATH_RE.match(path)
        if not match or "$(" in path:
            return None
        connection = match.group("connection")
        splits = [
            [connection[:i].strip(), connection[i + 1 :].strip()]
            for i, char in enumerate(connection)
            if char == SPACE_SEPARATOR
        ]
        known = [split for split in splits if _path_key("/".join(split)) in spellings]
        if len(known) != 1:
            return None
        return [*known[0], *_lib_path_parts(match.group("path"))]

    def _lib_path_spellings(self) -> Dict[str, Counter[str]]:
        paths = [
            *self._unmapped_files.values(),
            *(
                f"lib://{reference.connection}/{reference.path}"
                for resolver in self._file_sources_by_app.values()
                for reference in resolver.references()
            ),
            *(
                file.path
                for files in self._reload_files.values()
                for file in files
                if file.stored
            ),
        ]
        # Reload records are read through the end of the run, so the spellings
        # are built again when there are more paths.
        if len(paths) != self._script_spellings_built_from:
            spellings: Dict[str, Counter[str]] = defaultdict(Counter)
            for path in paths:
                segments = _lib_path_segments(path) or []
                for i, segment in enumerate(segments):
                    spellings[_path_key("/".join(segments[: i + 1]))][segment] += 1
            self._script_spellings = spellings
            self._script_spellings_built_from = len(paths)
        return self._script_spellings

    def _gen_lib_path_location(self, urn: str, path: str) -> Iterable[MetadataWorkUnit]:
        """Places a file named by its `lib://<space>:<connection>/<path>` in its
        space, under its connection and folders, as Qlik shows it. A space the
        run doesn't ingest is named, not linked, so no container is made up for
        it. Without a browse path, DataHub splits the URN at its dots."""
        match = LIB_PATH_RE.match(self._script_spelling(path))
        if not match:
            return
        if _is_ambiguous_connection(match.group("connection")):
            # Placed under no guessed space; the URN and path stay as recorded.
            return
        lib_connection = _lib_connection(match.group("connection"))
        space, connection = lib_connection.space, lib_connection.name
        if not space:
            return
        folders = [
            part for part in re.split(r"[/\\]", match.group("path"))[:-1] if part
        ]
        space_id = self._ingested_space_id(space)
        if space_id is not None:
            space_key = self._gen_space_key(space_id)
            yield from add_entity_to_container(
                container_key=space_key, entity_type="dataset", entity_urn=urn
            )
            space_urn = space_key.as_urn()
            entries = [BrowsePathEntryClass(id=space_urn, urn=space_urn)]
        else:
            entries = [BrowsePathEntryClass(id=space)]
        entries.extend(BrowsePathEntryClass(id=part) for part in [connection, *folders])
        yield MetadataChangeProposalWrapper(
            entityUrn=urn, aspect=BrowsePathsV2Class(path=entries)
        ).as_workunit()

    def _gen_file_entity(
        self,
        urn: str,
        name: str,
        subtype: DatasetSubTypes,
        path: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        yield self._gen_entity_status_aspect(urn)
        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DatasetProperties(
                name=name[: -len(QVD_EXTENSION)]
                if name.lower().endswith(QVD_EXTENSION)
                else name,
                qualifiedName=path,
                customProperties={
                    Constant.TYPE: subtype,
                    **({"path": path} if path else {}),
                },
            ),
        ).as_workunit()
        dpi_aspect = self._gen_dataplatform_instance_aspect(urn)
        if dpi_aspect:
            yield dpi_aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=SubTypes(typeNames=[subtype]),
        ).as_workunit()

    def _gen_unmapped_file_entities(self) -> Iterable[MetadataWorkUnit]:
        for file_urn, kept_urns in sorted(self._kept_storage_file_stores.items()):
            yield from self._gen_stored_file_lineage(file_urn, kept_urns)
        # A reader whose last read is kept linked the file then. Its checkpoint
        # is looked up again rather than every app's reads held in memory. A
        # writer's entities hold the file too, so only recorded reads count.
        unread = set(self._unmapped_file_stores) - self._linked_unmapped_files
        for app_id in sorted(self._kept_app_ids) if unread else []:
            previous = self.app_state.previous(app_id)
            if previous is not None:
                self._linked_unmapped_files.update(
                    unread.intersection(previous.read_unmapped_file_urns)
                )
        for file_urn, stores in sorted(self._unmapped_file_stores.items()):
            if file_urn not in self._linked_unmapped_files:
                self.reporter.unread_stored_files.append(self._unmapped_files[file_urn])
                continue
            table_urns: List[str] = []
            for app_id, urns in stores.items():
                self._app_end_of_run_urns[app_id].add(file_urn)
                table_urns.extend(urn for urn in urns if urn not in table_urns)
            yield from self._gen_stored_file_lineage(file_urn, table_urns)
        paths = {urn: self._unmapped_files[urn] for urn in self._linked_unmapped_files}
        qvds = {
            urn: path
            for urn, path in paths.items()
            if path.lower().endswith(QVD_EXTENSION)
        }
        yield from self._gen_file_entities(qvds, DatasetSubTypes.QLIK_QVD)
        yield from self._gen_file_entities(
            {urn: path for urn, path in paths.items() if urn not in qvds},
            DatasetSubTypes.QLIK_DATASET,
        )

    def _record_reload_files(
        self, app_id: str, statements: List[ReloadStatement]
    ) -> None:
        reload_files = _reload_files(statements)
        for discriminator in reload_files.unrecognised:
            self.reporter.unrecognised_reload_files.append(f"{app_id}: {discriminator}")
        self._reload_files[app_id] = reload_files.files

    def _read_reload_files(self, app_ids: Set[str]) -> None:
        """Reads the files of the apps' last reload that were not read with the
        apps, in parallel, as the run would otherwise wait on each in turn."""
        unread = sorted(app_ids - self._reload_files.keys())
        if not unread:
            return
        with ThreadPoolExecutor(
            max_workers=self.config.resolved_max_workers()
        ) as executor:
            futures = {
                app_id: executor.submit(self.qlik_api.get_reload_statements, app_id)
                for app_id in unread
            }
        for app_id, future in futures.items():
            try:
                statements = future.result()
            except Exception as e:
                logger.debug(f"Unable to read reload statements of app {app_id}: {e}")
                self.reporter.apps_without_reload_paths.append(f"{app_id}: {e}")
                self._reload_path_failures.append(f"{app_id}: {e}")
                self._reload_files_unread.add(app_id)
                statements = []
            self._record_reload_files(app_id, statements)

    def _graph_named_apps(self, urn: str) -> _GraphNamedApps:
        """The apps whose last reload may give the path of a dataset only the
        upstream graph names: those that load it, the writer apps this run
        ingests, and those that keep it from their last read as this run
        could not tell their sources."""
        keeping = {
            app_id
            for app_id in self._apps_graph_named_kept
            if urn in self._app_end_of_run_urns.get(app_id, ())
        }
        return _GraphNamedApps(
            apps=set(self._graph_resource_readers.get(urn, set()))
            | keeping
            | {
                app_id
                for qri in self._graph_resource_writers.get(urn, set())
                if qri in self._app_table_urns_by_qri
                and (app_id := _qlik_app_id(qri)) is not None
            },
            graph_unread=bool(keeping),
        )

    def _graph_named_path(
        self, label: str, apps: _GraphNamedApps
    ) -> Tuple[Optional[str], bool]:
        """The `lib://` path of a dataset only the upstream graph names, such as
        `lib://<space>:DataFiles/orders.qvd` for a QVD in a space the API key
        can't see, and whether the apps that may give it were all read. Qlik's
        label is only a name, so a path is given only when every one of those
        reloads was read and exactly one file they read or stored is at it."""
        # By key, as the same file is recorded in several spellings.
        paths: Dict[str, str] = {}
        for app_id in sorted(apps.apps):
            for file in self._reload_files.get(app_id, []):
                if _path_has_label(file.path, label):
                    paths.setdefault(_path_key(file.path), file.path)
        reloads_unread = bool(apps.apps & self._reload_files_unread)
        if len(paths) == 1 and not reloads_unread:
            return next(iter(paths.values())), True
        if len(paths) != 1:
            for path in paths.values():
                self._note_unseen_space(_path_space(path), "datasets")
        # An unread reload may hold another file with the label.
        return None, not apps.graph_unread and not reloads_unread

    def _leave_out_unplaced(self, urn: str, label: str, all_read: bool) -> None:
        """A dataset the graph names but whose path is not certain is not
        emitted, so the tables reading it link to its URN alone. Datasets an
        earlier run emitted are then soft-deleted, unless a reload that may give
        the path, or the graph of an app that keeps it, could not be read."""
        if not all_read:
            if self.stale_entity_removal_handler is not None:
                self.stale_entity_removal_handler.add_entity_to_state(
                    guess_entity_type(urn), urn
                )
            return
        self.reporter.graph_named_datasets_without_path.append(f"{label}: {urn}")
        for urns in self._app_end_of_run_urns.values():
            urns.discard(urn)

    def _read_graph_named_reloads(self) -> Dict[str, _GraphNamedApps]:
        """The apps that may give the path of each dataset only the upstream
        graphs name, with their last reload read."""
        apps_by_urn = {
            urn: self._graph_named_apps(urn) for urn in self._graph_named_resources
        }
        self._read_reload_files(
            {app_id for apps in apps_by_urn.values() for app_id in apps.apps}
        )
        return apps_by_urn

    def _gen_graph_named_resources(
        self, apps_by_urn: Dict[str, _GraphNamedApps]
    ) -> Iterable[MetadataWorkUnit]:
        """The datasets and files only apps' upstream graphs name whose path is
        certain, and the tables of the writer apps this run ingests as their
        upstreams."""
        emitted: Set[str] = set()
        for urn, label in self._graph_named_resources.items():
            path, all_read = self._graph_named_path(label, apps_by_urn[urn])
            if path is None:
                self._leave_out_unplaced(urn, label, all_read)
                continue
            path = self._script_spelling(path)
            if _is_ambiguous_lib_path(path):
                # Its space can't be told from its connection.
                self._leave_out_unplaced(urn, label, all_read)
                continue
            emitted.add(urn)
            self._note_unseen_space(_path_space(path), "datasets")
            yield from self._gen_file_entity(
                urn,
                # The label holds the folders under the connection, which the
                # browse path already gives, so the name is the file's alone.
                re.split(r"[/\\]", label)[-1],
                DatasetSubTypes.QLIK_QVD
                if label.lower().endswith(QVD_EXTENSION)
                else DatasetSubTypes.QLIK_DATASET,
                path,
            )
            yield from self._gen_lib_path_location(urn, path)
        for urn, writer_qris in self._graph_resource_writers.items():
            if urn not in emitted:
                # Its writers' edges would make the dataset exist.
                continue
            # Only writer apps this run ingests: another app's table has no URN.
            writer_urns = {
                writer
                for qri in writer_qris
                if (writer := self._app_table_urns_by_qri.get(qri)) is not None
                and writer != urn
            }
            yield from self._patch_upstreams(
                urn, sorted(writer_urns), DatasetLineageType.COPY
            )
        if self.reporter.graph_named_datasets_without_path:
            self.reporter.info(
                title="Qlik datasets without a certain path are left out",
                message="Qlik's lineage graph names these datasets, but the "
                "reloads of the apps that read or store them give no single path "
                "for them, such as a file name read through two connections, so "
                "they are not ingested, and the tables reading them link to their "
                "URN alone. Give the API key's user access to the spaces listed "
                'under "Qlik spaces the API key can\'t see", or, for files on '
                "cloud storage, map their connection in `file_connection_to_storage`.",
                context=_bounded_list(
                    sorted(self.reporter.graph_named_datasets_without_path)
                ),
            )
        if self._reload_path_failures:
            self.reporter.warning(
                title="Unable to read the file paths of Qlik apps' last reload",
                message="The files these apps read or stored in their last reload "
                "could not be read, so datasets only Qlik's lineage graph names "
                "for them get no path this run and are kept as the last run left "
                "them. They are read again on the next run.",
                context=_bounded_list(self._reload_path_failures),
            )
        if self.reporter.unrecognised_reload_files:
            self.reporter.info(
                title="Unrecognised file statements in Qlik apps' last reload",
                message="These statements of the apps' last reload name a "
                "`lib://` path in a form the connector doesn't read, so the file "
                "gives no path to datasets only Qlik's lineage graph names.",
                context=_bounded_list(list(self.reporter.unrecognised_reload_files)),
            )

    def _record_model_fields(self, app: App) -> None:
        # Only for the apps BINARY loads come from, as the checkpoint keeps them.
        if app.id not in self._binary_source_apps:
            return
        if app.data_model_tables:
            self._app_model_fields[app.id] = {
                table.name.lower(): [field.name for field in table.fields]
                for table in app.data_model_tables
            }
            return
        if not app.data_load_failed:
            return
        # Its fields from its last read, so BINARY loads from it keep their
        # column lineage while its data model can't be read.
        failures: List[str] = []
        previous = self._usable_checkpoint(app.id, failures)
        for failure in failures:
            self.reporter.warning(
                title="Unable to load the last checkpoint of a Qlik app",
                message="The data model fields of an app BINARY loads come from "
                "are not known, so lineage to its tables is only added to.",
                context=failure,
            )
        if previous is not None and previous.data_model_fields is not None:
            self._app_model_fields[app.id] = previous.data_model_fields

    def _binary_source_app(
        self, file: QvdReference, resolved: Optional[str]
    ) -> Optional[str]:
        """The app `binary_file_to_app` says the file was exported from, matched
        by the path as the script names it or the file it resolves to."""
        on_prem = self.config.on_prem
        assert on_prem is not None
        apps = on_prem.binary_file_to_app
        return apps.get(canonical_path(file.as_written)) or (
            apps.get(resolved) if resolved else None
        )

    def _check_binary_read(self, app: App) -> None:
        """Counts an app as partly read when the tables its BINARY statement
        loads can't be found for a reason the next run may not have, and its
        last read ingested such tables, so stale removal doesn't delete them.
        After max_partial_reads_kept runs they are let go, as the cause, such
        as section access denying the app's data, may never go away."""
        on_prem = self.config.on_prem
        assert on_prem is not None
        if not on_prem.read_data_model:
            return
        binary = self._parsed_script(app).binary
        if binary is None:
            return
        reason = (
            "its data model could not be loaded"
            if app.data_load_failed
            else "its variables, which the BINARY path uses, could not be read"
            if binary.uses_variables and app.variables_read_failed
            else None
        )
        if reason is None:
            return
        failures: List[str] = []
        previous = self._usable_checkpoint(app.id, failures)
        if failures:
            # Its last read may hold BINARY tables.
            app.read_errors.append(
                f"The tables the app's BINARY load brings in are not known: {reason}"
            )
            return
        if previous is None or not previous.binary_tables:
            return
        if previous.partial_reads >= self.config.max_partial_reads_kept:
            self.reporter.warning(
                title="Qlik BINARY tables are no longer kept",
                message="The tables these apps load with BINARY could not be "
                "found for more than max_partial_reads_kept runs, so they are "
                "no longer kept from the app's last read.",
                context=f"{app.id}: {reason}",
            )
            return
        app.read_errors.append(
            f"The tables the app's BINARY load brings in are not known: {reason}"
        )

    def _binary_load(self, app: App, parsed: ParsedScript) -> _BinaryLoad:
        """The app's tables a BINARY statement loads: the data model's tables
        the script doesn't create itself."""
        binary = _BinaryLoad()
        statement = parsed.binary
        if statement is None:
            return binary
        self.reporter.binary_loads_found += 1

        def without_lineage(reason: str) -> _BinaryLoad:
            self.reporter.binary_loads_without_lineage.append(
                f"{app.id}: {statement.written}: {reason}"
            )
            return binary

        file = statement.file
        if file is None:
            return without_lineage("the path names no single file")
        if not app.data_model_tables:
            return without_lineage(
                "the app's data model was not read, see on_prem.read_data_model"
            )
        resolver = self._qvd_path_resolver(app)
        resolved = resolver.resolve(file)
        source_app_id = self._binary_source_app(file, resolved)
        if source_app_id == app.id:
            return without_lineage("binary_file_to_app maps it to the app itself")
        if source_app_id is not None and parsed.drops_unnamed_tables:
            # Any data model table could be one the script dropped and loaded again.
            return without_lineage(
                "the script drops, replaces or renames to tables it doesn't "
                "name, so the tables the file brings in are not known"
            )
        labels = {label.lower() for label in parsed.table_labels}
        created = set(labels)
        if source_app_id is not None:
            # A load under a BINARY table's name adds to it, or Qlik renames
            # the new table, so only a table the script drops and creates again,
            # or renames a table to, is not the file's. Tables the source app
            # lacks are left out later.
            created &= {
                label.lower()
                for label in [*parsed.dropped_tables, *parsed.renamed_tables]
            }
        upstream_urns: List[str] = []
        if source_app_id is not None and parsed.has_includes:
            self.reporter.warning(
                title="Qlik BINARY tables may be changed by an included script",
                message="A script included with $(Include) or $(Must_Include) "
                "that was not read, or a SUB the script calls but does not "
                "define, could drop or reload the tables a mapped BINARY load "
                "brings in, so their lineage is only added to.",
                context=app.id,
            )
        if source_app_id is None:
            if parsed.has_unnamed_tables or parsed.has_includes:
                # Any data model table could be one of those.
                return without_lineage(
                    "the script creates tables it doesn't name, so the tables "
                    "the file brings in are not known; map the file in "
                    "binary_file_to_app"
                )
            urn = resolver.storage_urn(file)
            if urn is None:
                if resolved is None:
                    return without_lineage(
                        "its data connection is not a known folder connection"
                    )
                urn = self._gen_qlik_dataset_urn(resolved)
                binary.paths[urn] = resolved
            upstream_urns = [urn]
        for table in app.data_model_tables:
            if table.name.lower() in created:
                continue
            suffixed = DUPLICATE_TABLE_NAME_RE.match(table.name)
            # Qlik names a new table `Orders-1` when Orders already exists, but
            # a real name such as `Orders-2024` looks the same.
            maybe_renamed = bool(suffixed and suffixed.group("name").lower() in labels)
            if maybe_renamed and source_app_id is None:
                without_lineage(
                    f"table {table.name} may be a script table Qlik renamed; "
                    "map the file in binary_file_to_app"
                )
                continue
            identifier = app_table_identifier(app.id, table.name)
            fields = [field.name for field in table.fields]
            binary.tables[identifier] = ScriptTableLineage(
                label=table.name,
                identifier=identifier,
                upstream_urns=upstream_urns,
                column_upstreams={field: [] for field in fields},
            )
            if source_app_id is not None:
                dataset_urn = self._gen_qlik_dataset_urn(identifier)
                binary.links[dataset_urn] = _BinaryLink(
                    app_id=app.id,
                    source_app_id=source_app_id,
                    table=table.name,
                    dataset_urn=dataset_urn,
                    fields=fields,
                    additive=parsed.has_includes,
                    maybe_renamed_script_table=maybe_renamed,
                )
        return binary

    def _binary_link_source(
        self, link: _BinaryLink, unread: Set[Tuple[str, str]]
    ) -> Optional[str]:
        """The source app's table a BINARY table is linked to, if that app was
        read or kept this run and ingests it, so the edge never points at a
        dataset that doesn't exist."""
        source_urns = self._app_urns.get(link.source_app_id)
        if source_urns is None:
            unread.add((link.app_id, link.source_app_id))
            return None
        source_urn = self._gen_qlik_dataset_urn(
            app_table_identifier(link.source_app_id, link.table)
        )
        if source_urn not in source_urns:
            source_model = self._app_model_fields.get(link.source_app_id)
            if source_model is None:
                self.reporter.binary_loads_without_lineage.append(
                    f"{link.app_id}: app {link.source_app_id} ingests no table "
                    f"{link.table}"
                )
            elif link.table.lower() in source_model:
                self.reporter.binary_loads_without_lineage.append(
                    f"{link.app_id}: app {link.source_app_id} doesn't ingest table "
                    f"{link.table}, as it isn't loaded with SQL, from a QVD or "
                    "with BINARY there, such as a RESIDENT or INLINE table"
                )
            # Otherwise the source app's data model lacks it: the script's own.
            return None
        return source_urn

    def _binary_upstream_lineage(
        self,
        link: _BinaryLink,
        source_urn: Optional[str],
        fine_grained: List[FineGrainedLineage],
    ) -> UpstreamLineage:
        upstreams = [
            Upstream(dataset=urn, type=DatasetLineageType.TRANSFORMED)
            for urn in link.upstream_urns
        ]
        if source_urn is not None:
            upstreams.append(Upstream(dataset=source_urn, type=DatasetLineageType.COPY))
        return UpstreamLineage(
            upstreams=upstreams, fineGrainedLineages=self._column_lineage(fine_grained)
        )

    def _patch_binary_upstreams(
        self,
        link: _BinaryLink,
        source_urn: Optional[str],
        fine_grained: List[FineGrainedLineage],
    ) -> Iterable[MetadataWorkUnit]:
        lineage = self._binary_upstream_lineage(link, source_urn, fine_grained)
        if not lineage.upstreams and not lineage.fineGrainedLineages:
            return
        yield convert_upstream_lineage_to_patch(link.dataset_urn, lineage, None)

    def _gen_binary_lineage(self) -> Iterable[MetadataWorkUnit]:
        """Lineage of the tables an app BINARY loads from another app, written
        once every app has been read. Without the source app's table or its
        fields, the table's own lineage is only added to, so the edges an
        earlier run wrote stay."""
        unread: Set[Tuple[str, str]] = set()
        for link in self._binary_links:
            source_model = self._app_model_fields.get(link.source_app_id)
            in_source_model = (
                source_model is not None and link.table.lower() in source_model
            )
            if link.maybe_renamed_script_table and not in_source_model:
                if link.source_app_id not in self._app_urns:
                    unread.add((link.app_id, link.source_app_id))
                    yield from self._patch_binary_upstreams(
                        link, None, link.script_fields
                    )
                    continue
                self.reporter.binary_loads_without_lineage.append(
                    f"{link.app_id}: table {link.table} is not in app "
                    f"{link.source_app_id}'s data model, so it is taken as a "
                    "script table Qlik renamed"
                    if source_model is not None
                    else f"{link.app_id}: table {link.table} may be a script "
                    f"table Qlik renamed, and app {link.source_app_id}'s data "
                    "model is not known"
                )
                # The script's own edges stay, only added to if it may be the file's.
                if source_model is None:
                    yield from self._patch_binary_upstreams(
                        link, None, link.script_fields
                    )
                    continue
                # It is the script's own table, so its script lineage is
                # written whole, which also drops a COPY edge an earlier run
                # wrote from the source app.
                if link.additive:
                    yield from self._patch_binary_upstreams(
                        link, None, link.script_fields
                    )
                    continue
                yield MetadataChangeProposalWrapper(
                    entityUrn=link.dataset_urn,
                    aspect=self._binary_upstream_lineage(
                        link, None, link.script_fields
                    ),
                ).as_workunit()
                continue
            source_urn = self._binary_link_source(link, unread)
            if source_urn is None and source_model is not None and not in_source_model:
                if link.additive:
                    yield from self._patch_binary_upstreams(
                        link, None, link.script_fields
                    )
                    continue
                # Not from the file, so its script lineage is written whole.
                yield MetadataChangeProposalWrapper(
                    entityUrn=link.dataset_urn,
                    aspect=self._binary_upstream_lineage(
                        link, None, link.script_fields
                    ),
                ).as_workunit()
                continue
            if source_urn is None or source_model is None:
                # Without the source app's fields, the column edges an earlier
                # run wrote stay.
                yield from self._patch_binary_upstreams(
                    link, source_urn, link.script_fields
                )
                continue
            source_fields = set(source_model.get(link.table.lower(), []))
            fine_grained = [
                *link.script_fields,
                *(
                    FineGrainedLineage(
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        upstreams=[builder.make_schema_field_urn(source_urn, field)],
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        downstreams=[
                            builder.make_schema_field_urn(link.dataset_urn, field)
                        ],
                    )
                    for field in link.fields
                    if field in source_fields
                ),
            ]
            if link.additive:
                yield from self._patch_binary_upstreams(link, source_urn, fine_grained)
                continue
            yield MetadataChangeProposalWrapper(
                entityUrn=link.dataset_urn,
                aspect=self._binary_upstream_lineage(link, source_urn, fine_grained),
            ).as_workunit()
        for app_id, source_app_id in sorted(unread):
            self.reporter.binary_loads_without_lineage.append(
                f"{app_id}: app {source_app_id} was not read or kept this run"
            )
        if self.reporter.binary_loads_without_lineage:
            self.reporter.warning(
                title="Some Qlik BINARY loads have no lineage",
                message="The tables these apps load with BINARY are not linked "
                "to their source. See binary_loads_without_lineage.",
                context=_bounded_list(list(self.reporter.binary_loads_without_lineage)),
            )

    def _gen_qvd_lineage(self) -> Iterable[MetadataWorkUnit]:
        """Schema and lineage of on-prem QVDs, from every app that writes or
        reads them this run or was kept from an earlier one."""
        self._report_unresolved_qvds()
        writes: Dict[str, List[QvdWrite]] = {}
        reads: Dict[str, List[str]] = {}
        for qvds in self._app_qvds.values():
            for urn, app_writes in qvds.writes.items():
                writes.setdefault(urn, []).extend(app_writes)
            for urn, read_fields in qvds.reads.items():
                known = reads.setdefault(urn, [])
                known.extend(field for field in read_fields if field not in known)
        # Storage files are never removed as stale, so the edges of apps that
        # stopped storing into them are only removed here.
        stopped = self._storage_qvds_written_before()
        if not (writes or reads or stopped):
            return
        unknown = dict(self._apps_qvds_unknown)
        if self.qlik_api.unread_items:
            unknown["apps that could not be listed"] = "they were not read"
        if unknown:
            # An app whose QVD writes are unknown may write any QVD, so its
            # edges are kept and a QVD only it writes keeps its last schema.
            self.reporter.warning(
                title="Lineage of Qlik QVDs only added to",
                message="The QVDs some apps write are not known this run, so "
                "their edges into QVDs are kept, and only QVDs written by an app "
                "read this run get a new schema.",
                context=_bounded_list(
                    [f"{app}: {reason}" for app, reason in sorted(unknown.items())]
                ),
            )
        qvd_urns = sorted({*writes, *reads, *stopped})
        # Read in batches up front rather than one request per QVD. Removal is
        # limited to edges from apps whose writes are known, so it runs even
        # while other apps are unknown.
        current_lineage = self._current_upstreams_batch(qvd_urns)
        for urn in qvd_urns:
            # The fields apps read are a subset, so a QVD whose writer is
            # unknown or not read this run keeps its schema.
            fields = (
                _qvd_fields(writes.get(urn, []), reads.get(urn, []))
                if urn in writes
                or not (
                    unknown or self._has_unread_qvd_writer(current_lineage.get(urn))
                )
                else None
            )
            if fields is not None and self._is_own_dataset(urn):
                yield MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=SchemaMetadata(
                        schemaName=urn,
                        platform=builder.make_data_platform_urn(self.platform),
                        version=0,
                        hash="",
                        platformSchema=MySqlDDL(tableSchema=""),
                        fields=self._gen_schema_fields(
                            [QlikDatasetSchemaField(name=field) for field in fields]
                        ),
                    ),
                ).as_workunit()
            # A QVD no app writes any more still loses its old writers' edges.
            yield from self._gen_qvd_upstreams(
                urn,
                writes.get(urn, []),
                current_lineage.get(urn),
                self._owned_qvd_writer,
                # The storage sources don't read QVDs, so a QVD on storage has
                # no fields in DataHub.
                has_field_schema=self._is_own_dataset(urn),
            )

    def _storage_qvds_written_before(self) -> Set[str]:
        """Storage QVDs that apps whose writes are known this run stored into
        on their last read."""
        failures: List[str] = []
        urns: Set[str] = set()
        for app_id in self._app_qvds:
            if app_id in self._apps_qvds_unknown:
                continue
            previous = self._usable_checkpoint(app_id, failures)
            if previous is not None and previous.qvds is not None:
                urns.update(
                    urn for urn in previous.qvds.writes if not self._is_own_dataset(urn)
                )
        if failures:
            self.reporter.warning(
                title="Unable to load the last checkpoint of a Qlik app",
                message="Edges from these apps into storage QVDs they no longer "
                "write are kept.",
                context=_bounded_list(failures),
            )
        return urns

    def _report_unresolved_qvds(self) -> None:
        if not (self._unresolved_qvd_loads or self._unlabelled_qvd_loads):
            return
        self.reporter.warning(
            title="Some Qlik QVD loads have no lineage",
            message="QVD paths that name no single file the connector can "
            "resolve, such as a wildcard, an unexpanded variable, a relative path "
            "or an unknown data connection, and QVDs loaded without a table label, "
            "are not linked. See unresolved_qvd_paths and unlabelled_qvd_loads.",
            context=f"{self._unresolved_qvd_loads} unresolved QVD paths, "
            f"{self._unlabelled_qvd_loads} unlabelled QVD loads",
        )

    def _qvd_writer_app_id(self, urn: str) -> Optional[str]:
        """The app id of an on-prem app table. QVDs are named by their file
        path, and app tables by their app id."""
        try:
            dataset = DatasetUrn.from_string(urn)
        except Exception:
            return None
        if dataset.platform != builder.make_data_platform_urn(self.platform):
            return None
        name = dataset.name
        instance = self.config.platform_instance
        if instance and name.startswith(f"{instance}."):
            name = name[len(instance) + 1 :]
        if CANONICAL_ABSOLUTE_PATH_RE.match(name):
            return None
        return name.split(".", 1)[0]

    def _owned_qvd_writer(self, urn: str) -> bool:
        """An app table of an app whose QVD writes are known this run, so its
        edges into QVDs are this connector's to remove."""
        app_id = self._qvd_writer_app_id(urn)
        return (
            app_id is not None
            and app_id in self._app_qvds
            and app_id not in self._apps_qvds_unknown
        )

    def _has_unread_qvd_writer(self, current: Optional[UpstreamLineage]) -> bool:
        """Whether an app this run did not read, such as one outside the
        recipe's streams, wrote the QVD, so its schema is that app's."""
        return current is not None and any(
            self._qvd_writer_app_id(upstream.dataset) is not None
            and not self._owned_qvd_writer(upstream.dataset)
            for upstream in current.upstreams
        )

    def _gen_qvd_upstreams(
        self,
        qvd_urn: str,
        writes: List[QvdWrite],
        current: Optional[UpstreamLineage],
        is_owned: Callable[[str], bool],
        has_field_schema: bool,
    ) -> Iterable[MetadataWorkUnit]:
        """`is_owned` tells the tables whose edges into the file this run knows
        all of, so that those no longer written are removed."""
        table_urns = list(dict.fromkeys(write.table_urn for write in writes))
        fine_grained = [
            FineGrainedLineage(
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=[
                    builder.make_schema_field_urn(write.table_urn, field.source)
                ],
                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                downstreams=[builder.make_schema_field_urn(qvd_urn, field.name)],
            )
            for write in writes
            for field in write.fields or []
            if field.source and has_field_schema
        ]
        # Added as a patch, so lineage from elsewhere stays.
        patch = DatasetPatchBuilder(qvd_urn)
        for table_urn in table_urns:
            patch.add_upstream_lineage(
                Upstream(dataset=table_urn, type=DatasetLineageType.COPY)
            )
        if self.config.include_column_lineage:
            for lineage in fine_grained:
                patch.add_fine_grained_lineage(lineage)
        # Edges from tables of apps whose writes are known this run, that no
        # longer store into the QVD, are removed one by one.
        if current is not None:
            seen = {
                *(u.dataset for u in current.upstreams),
                *(
                    parent
                    for lineage in current.fineGrainedLineages or []
                    for u in lineage.upstreams or []
                    if (parent := _schema_field_parent(u))
                ),
            }
            owned = {urn for urn in seen if is_owned(urn)}
            for upstream in current.upstreams:
                if upstream.dataset in owned and upstream.dataset not in table_urns:
                    patch.remove_upstream_lineage(upstream.dataset)
            if self.config.include_column_lineage:
                # A write whose fields are unknown, such as a STORE of a LOAD *
                # table, may still write the columns an earlier run saw.
                fields_unknown = {
                    write.table_urn for write in writes if write.fields is None
                }
                for lineage in self._stale_field_lineage(
                    current.fineGrainedLineages or [],
                    fine_grained,
                    owned - fields_unknown,
                ):
                    patch.remove_fine_grained_lineage(lineage)
        # A file on storage is not this source's to remove when stale.
        yield from _patch_workunits(
            patch, is_primary_source=self._is_own_dataset(qvd_urn)
        )

    def _is_own_dataset(self, urn: str) -> bool:
        return DatasetUrn.from_string(urn).platform == builder.make_data_platform_urn(
            self.platform
        )

    def _check_missing_lineage_graph(self, app: App) -> None:
        # Qlik also answers 404 for lineage it has not computed yet, so a 404
        # only means the app has no tables when its last read had none.
        failures: List[str] = []
        previous = self._usable_checkpoint(app.id, failures)
        if failures:
            # Without the last read, the app's tables may still exist.
            app.read_errors.append(
                "Qlik Cloud has no lineage graph for the app, and its last read "
                "could not be loaded"
            )
            app.tables_read_failed = True
        elif previous is not None and (
            previous.table_urns_by_qri or previous.graph_tables_unresolved
        ):
            # Unresolved graph tables are tables the last read saw too.
            app.read_errors.append(
                "Qlik Cloud has no lineage graph for the app, whose last read "
                "had tables"
            )
            app.tables_read_failed = True
        else:
            self.reporter.apps_without_lineage_graph.append(app.id)
            # A last read whose tables are unknown can't tell either.
            unknown = (
                previous is None
                or previous.table_urns_by_qri is None
                or previous.tables_unknown
            )
            if unknown and self.app_state.is_checkpointing_enabled():
                # With no last read to compare with, the graph may only be
                # missing because Qlik has not computed it yet, such as right
                # after a reload, so the app's lineage is only added to. Without
                # stateful ingestion there is never a last read, so the app is
                # taken as having no tables, as before.
                app.tables_read_failed = True
                self.reporter.warning(
                    title="Qlik Cloud has no lineage graph for app",
                    message="Qlik may not have computed it yet, so the lineage "
                    "of the app's tables is only added to this run.",
                    context=app.id,
                )

    def _gen_item_workunits(
        self,
        item: Union[Item, UnchangedApp],
        space_name: str,
        record_state: bool,
    ) -> Iterable[MetadataWorkUnit]:
        if isinstance(item, UnchangedApp):
            self._keep_unchanged_app(item)
        elif isinstance(item, App):
            for fallback in item.read_fallbacks:
                self.reporter.info(
                    title="Qlik Engine batch read unavailable",
                    message="Fell back to one Engine request per chart or master "
                    "measure, which is slower on large apps.",
                    context=f"{item.id}: {fallback}",
                )
            self.reporter.apps_read += 1
            if item.lineage_graph_missing:
                self._check_missing_lineage_graph(item)
            if self.config.on_prem is not None and record_state:
                self._check_binary_read(item)
            logger.info(
                f"Processed app {item.qTitle} in space {space_name} "
                f"({self.reporter.apps_read} apps read from the Engine)"
            )
            partial_read = (
                self._keep_partly_read_app(item)
                if not item.read_complete and record_state
                else _PartialRead.NOT_KEPT
            )
            if partial_read == _PartialRead.KEPT:
                self.reporter.warning(
                    title="Unable to fully read Qlik app",
                    message="Nothing from this read of the app is emitted. Its "
                    "metadata stays as of its last complete read until a read of "
                    "it completes.",
                    context=self._read_errors_context(item),
                )
                return
            if partial_read == _PartialRead.LIMIT_REACHED:
                self.reporter.failure(
                    title="Qlik app has been only partly read for too many runs",
                    message="Its last complete read is no longer kept, so this "
                    "partial read is ingested. Stale metadata removal is skipped "
                    "for this run.",
                    context=self._read_errors_context(item),
                )
            elif not item.read_complete:
                # Reporting a failure also stops stale entity removal from
                # soft-deleting what this app's last full read produced.
                self.reporter.failure(
                    title="Unable to fully read Qlik app",
                    message="Some sheets, charts, tables or load script lineage "
                    "of this app could not be read from Qlik.",
                    context=self._read_errors_context(item),
                )
            if item.script_read_failed:
                self.reporter.warning(
                    title="Unable to read Qlik app load script",
                    message="Load script lineage of this app is skipped. Reading "
                    "load scripts needs the Owner, Can manage or Can operate role "
                    "in managed spaces.",
                    context=item.id,
                )
            yield from (
                self._gen_recorded_app_workunit(item)
                if record_state
                else self._gen_app_workunit(item)
            )
        elif isinstance(item, QlikDataset):
            yield from self._gen_dataset_workunit(item)

    def _current_upstreams_batch(
        self, dataset_urns: List[str]
    ) -> Dict[str, Optional[UpstreamLineage]]:
        """DataHub's current lineage of each dataset; None for those that could
        not be read, whose lineage is then only added to."""
        graph = self.ctx.graph
        if graph is None:
            return {urn: self._current_upstreams(urn) for urn in dataset_urns}
        current: Dict[str, Optional[UpstreamLineage]] = {}
        for start in range(0, len(dataset_urns), ENTITY_CHECK_BATCH_SIZE):
            batch = dataset_urns[start : start + ENTITY_CHECK_BATCH_SIZE]
            try:
                entities = graph.get_entities(
                    "dataset", batch, aspects=[UpstreamLineage.ASPECT_NAME]
                )
            except Exception as e:
                self.reporter.warning(
                    title="Unable to read Qlik dataset lineage from DataHub",
                    message="The lineage of these datasets from apps that store "
                    "into them is only added to.",
                    context=_bounded_list(batch),
                    exc=e,
                )
                current.update(dict.fromkeys(batch))
                continue
            for urn in batch:
                found = entities.get(urn, {}).get(UpstreamLineage.ASPECT_NAME)
                current[urn] = (
                    found[0]
                    if found and isinstance(found[0], UpstreamLineage)
                    else UpstreamLineage(upstreams=[])
                )
        return current

    def _current_upstreams(self, dataset_urn: str) -> Optional[UpstreamLineage]:
        graph = self.ctx.graph
        if graph is None:
            if not self._reported_no_graph:
                self._reported_no_graph = True
                self.reporter.info(
                    title="Qlik dataset lineage only added to",
                    message="Without a DataHub connection, such as with a file "
                    "sink, edges from app tables that no longer store into a "
                    "dataset can't be found, so none are removed.",
                )
            return None
        try:
            return graph.get_aspect(dataset_urn, UpstreamLineage) or UpstreamLineage(
                upstreams=[]
            )
        except Exception as e:
            self.reporter.warning(
                title="Unable to read Qlik dataset lineage from DataHub",
                message="The dataset's lineage from apps that store into it is "
                "only added to.",
                context=dataset_urn,
                exc=e,
            )
            return None

    def _gen_stored_table_types(self) -> Iterable[MetadataWorkUnit]:
        """Types for the fields of untyped graph tables, from the datasets they
        are stored into: a STORE writes a field's values unchanged, so a field
        stored under its own name has the stored field's type. A field stored
        into datasets that disagree on its type keeps none."""
        types: Dict[str, Dict[str, Set[Optional[str]]]] = {}
        for dataset_urn, field_sources in self._dataset_store_field_sources.items():
            dataset_types = self._dataset_field_types.get(dataset_urn, {})
            for field, source_qris in field_sources.items():
                for field_qri in source_qris:
                    table_qri, _, field_id = field_qri.rpartition("#")
                    table_urn = self._app_table_urns_by_qri.get(table_qri)
                    if (
                        table_urn in self._untyped_tables
                        and field_id == _qlik_field_id(field)
                        and dataset_types.get(field)
                    ):
                        types.setdefault(table_urn, {}).setdefault(field, set()).add(
                            dataset_types[field]
                        )
        for table_urn, field_types in types.items():
            identifier, schema = self._untyped_tables[table_urn]
            # Columns traced from a load script may have their case changed by
            # the SQL parser, so a field matches a stored one of another case
            # when no other stored field folds to the same name.
            folded: Dict[str, List[str]] = {}
            for name in field_types:
                folded.setdefault(name.lower(), []).append(name)
            typed = []
            for schema_field in schema:
                matches = folded.get(schema_field.name.lower(), [])
                stored_name: Optional[str] = (
                    schema_field.name
                    if schema_field.name in field_types
                    else (matches[0] if len(matches) == 1 else None)
                )
                stored_types = (
                    field_types.get(stored_name, set()) if stored_name else set()
                )
                typed.append(
                    schema_field.model_copy(
                        update={"dataType": next(iter(stored_types))}
                    )
                    if len(stored_types) == 1
                    else schema_field
                )
            yield self._gen_schema_metadata(identifier, typed)

    def _gen_dataset_store_lineage(self) -> Iterable[MetadataWorkUnit]:
        datasets = self._dataset_urns_by_secure_qri
        qlik_datasets = set(datasets.values()) if datasets is not None else None
        owned = self._known_app_table_urns()
        for dataset_urn, source_qris in self._dataset_store_sources.items():
            resolved = self._resolve_lineage_sources(
                source_qris, dataset_urn, own_app_id=None
            )
            stored_fields = self._graph_column_lineage(
                dataset_urn,
                self._dataset_store_field_sources.get(dataset_urn, {}),
                own_app_id=None,
            )
            if not resolved.complete:
                self.reporter.warning(
                    title="Lineage of Qlik dataset only added to",
                    message="An app table that stores into the dataset could not "
                    "be resolved this run, such as one of an app that could not be "
                    "read, so its storing tables are added to the dataset's "
                    "lineage and none are removed.",
                    context=dataset_urn,
                )
            # Added as a patch, so lineage from elsewhere, such as added in
            # DataHub, stays.
            patch = DatasetPatchBuilder(dataset_urn)
            for urn in resolved.urns:
                patch.add_upstream_lineage(
                    Upstream(dataset=urn, type=DatasetLineageType.COPY)
                )
            for lineage in stored_fields:
                patch.add_fine_grained_lineage(lineage)
            # Edges this connector owns are those from the tables of apps read
            # in full or kept this run. Those that no longer store into the
            # dataset are removed one by one; while a storing app is unread
            # this run, nothing is.
            current = (
                self._current_upstreams(dataset_urn)
                if resolved.complete and qlik_datasets is not None
                else None
            )
            if current is not None:
                for upstream in current.upstreams:
                    if upstream.dataset in owned and upstream.dataset not in (
                        resolved.urns
                    ):
                        patch.remove_upstream_lineage(upstream.dataset)
                if (
                    self.config.include_column_lineage
                    and dataset_urn not in self._dataset_store_fields_unknown
                    and dataset_urn not in self._unnamed_field_lineage
                ):
                    for lineage in self._stale_field_lineage(
                        current.fineGrainedLineages or [], stored_fields, owned
                    ):
                        patch.remove_fine_grained_lineage(lineage)
            for mcp in patch.build():
                yield MetadataWorkUnit(
                    id=MetadataWorkUnit.generate_workunit_id(mcp), mcp_raw=mcp
                )

    def _known_app_table_urns(self) -> Set[str]:
        return self._previous_app_table_urns | {
            urn
            for qri, urn in self._app_table_urns_by_qri.items()
            if _qlik_app_id(qri) in self._apps_tables_known
        }

    @staticmethod
    def _stale_field_lineage(
        current: List[FineGrainedLineage],
        stored: List[FineGrainedLineage],
        owned: Set[str],
    ) -> List[FineGrainedLineage]:
        """The column edges from owned tables that this run no longer stores,
        each as a lineage holding only those upstream fields."""
        stored_edges = {
            (downstream, upstream)
            for lineage in stored
            for downstream in lineage.downstreams or []
            for upstream in lineage.upstreams or []
        }
        stale = []
        for lineage in current:
            # A column edge can only be removed by patch through its single
            # downstream field; one another tool wrote with none or several
            # is not this connector's.
            if len(lineage.downstreams or []) != 1:
                continue
            upstreams = [
                upstream
                for upstream in lineage.upstreams or []
                if _schema_field_parent(upstream) in owned
                and not any(
                    (downstream, upstream) in stored_edges
                    for downstream in lineage.downstreams or []
                )
            ]
            if upstreams:
                stale_lineage = FineGrainedLineage.from_obj(lineage.to_obj())
                stale_lineage.upstreams = upstreams
                stale.append(stale_lineage)
        return stale

    @staticmethod
    def _read_errors_context(app: App) -> str:
        first, *rest = app.read_errors
        more = f" (and {len(rest)} more)" if rest else ""
        return f"{app.id}: {first}{more}"

    def _report_reload_sql(self) -> None:
        if self.qlik_api.reload_statement_read_failures:
            self.reporter.warning(
                title="Unable to read the SQL of Qlik apps' last reload",
                message="Database tables that only the SQL of these apps' last "
                "reload names are left out, and the lineage of the apps' tables "
                "that read databases is only added to this run.",
                context=_bounded_list(self.qlik_api.reload_statement_read_failures),
            )
        if self.reporter.database_label_read_failures:
            self.reporter.warning(
                title="Unable to read the labels of Qlik apps' database tables",
                message="The upstream lineage graph that labels the database "
                "tables these apps load from could not be read, so lineage of the "
                "apps' tables that read databases is only added to this run.",
                context=_bounded_list(list(self.reporter.database_label_read_failures)),
            )
        if self.reporter.ambiguous_reload_sql_sources:
            self.reporter.warning(
                title="Qlik lineage graph database table matches several tables",
                message="The SQL of the app's last reload reads more than one "
                "table the graph's label could name, or a statement that may "
                "name it could not be read, so lineage to it is skipped.",
                context=_bounded_list(
                    sorted(self.reporter.ambiguous_reload_sql_sources)
                ),
            )
        if self.reporter.unmatched_reload_sql_sources:
            self.reporter.warning(
                title="Qlik lineage graph database table matches no reload SQL table",
                message="No table the SQL of the app's last reload reads matches "
                "the graph's label and platform, so lineage to it is skipped.",
                context=_bounded_list(
                    sorted(self.reporter.unmatched_reload_sql_sources)
                ),
            )
        if self.reporter.reload_sql_tables_not_in_datahub:
            self.reporter.warning(
                title="Qlik reload SQL table schema not found in DataHub",
                message="These tables are linked as the reload SQL names them. If "
                "the warehouse is ingested with another case, the edge points at "
                "a dataset that doesn't exist; set the warehouse source's URN "
                "case to match.",
                context=_bounded_list(
                    sorted(self.reporter.reload_sql_tables_not_in_datahub)
                ),
            )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Qlik Sense plugin execution is started")
        allowed_spaces = self._get_allowed_spaces()
        skip_unchanged = self._skips_unchanged_apps()
        record_state = self.app_state.is_checkpointing_enabled()
        if skip_unchanged:
            self._init_fingerprint_salt()
        space_names = {space.id: space.name for space in allowed_spaces}
        self._allowed_space_ids = set(space_names)
        for space in allowed_spaces:
            yield from self._gen_space_workunit(space)
        logger.info(f"Reading the items of {len(allowed_spaces)} spaces")
        # Items of all spaces share one pool of workers, so max_workers bounds
        # the whole run.
        items = self.qlik_api.get_items(
            [
                "personal" if space.id == Constant.PERSONAL_SPACE_ID else space.id
                for space in allowed_spaces
            ],
            is_unchanged=self._is_app_unchanged if skip_unchanged else None,
        )
        for item in items:
            space_id = getattr(item, "spaceId", "")
            yield from self._gen_item_workunits(
                item, space_names.get(space_id, space_id), record_state
            )
        yield from self._gen_pending_table_lineage()
        yield from self._gen_dataset_store_lineage()
        yield from self._gen_stored_table_types()
        yield from self._gen_qvd_lineage()
        yield from self._gen_binary_lineage()
        # Before any file is placed, so the spellings hold every STORE.
        graph_named_apps = self._read_graph_named_reloads()
        yield from self._gen_unmapped_file_entities()
        yield from self._gen_graph_named_resources(graph_named_apps)
        self._record_end_of_run_urns()
        if self.qlik_api.opened_without_data:
            self.reporter.warning(
                title="Qlik apps read without their data",
                message="Opening these apps with their data, or reading their "
                "data model, was denied, as with section access, or timed out, so "
                "they were read without it: their tables have no field types, and "
                "the next run reads them again.",
                context=_bounded_list(self.qlik_api.opened_without_data),
            )
        refusals = self.qlik_api.engine_refusals
        if refusals is not None and refusals.longest_streak_seconds > 0:
            self.reporter.info(
                title="Qlik Engine refused connections",
                message="App reads waited for the Engine to accept connections, "
                "as it does while its service restarts, which slows the run.",
                context=f"Longest run of refusals: "
                f"{refusals.longest_streak_seconds:.0f}s",
            )
        if self.qlik_api.field_lineage_failures:
            self.reporter.warning(
                title="Unable to read Qlik column lineage",
                message="Column lineage of these tables is only added to this run, "
                "and their table lineage is still read.",
                context=_bounded_list(self.qlik_api.field_lineage_failures),
            )
        for failure in self.qlik_api.include_read_failures:
            # Entries are `<path> (app <id> in space <name>): <reason>`, and a
            # bracketed space name in the path may hold `: ` itself.
            path = failure.partition(" (app ")[0]
            self._note_unseen_space(_path_space(path), "included scripts")
        if self._unseen_spaces:
            self.reporter.warning(
                title="Qlik spaces the API key can't see",
                message="Lineage goes through these spaces, which the API key's "
                "user can't see, so it is incomplete: included scripts in them "
                "are not read, their data connections don't resolve, and their "
                "datasets are named only from Qlik's lineage graph or left out. "
                "Give the user Can view in each space for its scripts and "
                "datasets, and Can consume data for its data connections, or map "
                "the connections in `data_connection_to_platform_instance` and "
                "`file_connection_to_storage`.",
                context=_bounded_list(
                    sorted(
                        f"{name}: {', '.join(sorted(needs))}"
                        for name, needs in self._unseen_spaces.values()
                    )
                ),
            )
        if self.qlik_api.include_read_failures:
            self.reporter.warning(
                title="Unable to read included Qlik scripts",
                message="Apps include these scripts with $(Include) or "
                "$(Must_Include), but they could not be read, so what they load "
                "is missing from lineage, and the apps' lineage is only added to. "
                "Each entry gives the path, the app and space including it, and "
                "the reason. A path without a space resolves in that app's space. "
                "An access error needs Can view for the API key's user in the "
                "space holding the file.",
                context=_bounded_list(sorted(set(self.qlik_api.include_read_failures))),
            )
        if self.reporter.unresolved_file_connections:
            self.reporter.warning(
                title="Qlik file connections have no known storage",
                message="Files on these data connections are named by their "
                "space, connection and path, so they only join the Qlik apps "
                "that write and read them. A personal-space app's files on a "
                "connection named without its space are named from the loading "
                "app's lineage graph, when its tables come from the graph, and "
                "stores into them are not linked. Add each Azure Storage, Amazon "
                "S3 or Google Cloud Storage connection to "
                "`file_connection_to_storage` with the URI of its storage; other "
                "connectors, such as SFTP, can't be mapped.",
                context=_bounded_list(
                    sorted(self.reporter.unresolved_file_connections)
                ),
            )
        if self.qlik_api.lineage_read_failures:
            self.reporter.warning(
                title="Unable to read Qlik dataset lineage",
                message="Lineage from the apps that store these datasets is skipped.",
                context=_bounded_list(self.qlik_api.lineage_read_failures),
            )
        self._report_reload_sql()
        if self._upstream_graph_read_failures:
            self.reporter.warning(
                title="Unable to read Qlik upstream lineage graph",
                message="Sources of these apps that only Qlik's lineage graph "
                "names, such as datasets in spaces the API key can't see, are "
                "left out, and the apps' lineage is only added to this run. "
                "Datasets the last run named from the graph are kept.",
                context=_bounded_list(self._upstream_graph_read_failures),
            )
        change_check_failures = [
            *self.qlik_api.change_check_failures,
            *self._change_check_failures,
        ]
        for failure in change_check_failures:
            self.reporter.apps_change_check_failed.append(failure)
        if change_check_failures:
            # Reported here rather than from the worker threads that check apps.
            self.reporter.warning(
                title="Unable to check whether Qlik apps changed",
                message="These apps were read in full instead of skipped. "
                "See apps_change_check_failed.",
                context=f"{len(change_check_failures)} apps, for example "
                f"{change_check_failures[0]}",
            )
        failed_users = self.qlik_api.failed_user_lookups
        if failed_users:
            self.reporter.warning(
                title="Unable to look up Qlik owners",
                message="Ownership is left out for entities owned by these users.",
                context=", ".join(
                    f"{user_id}: {cause}"
                    for user_id, cause in sorted(failed_users.items())
                ),
            )
        for unread in self.qlik_api.unread_items:
            # Also stops stale entity removal from soft-deleting what was not read.
            self.reporter.failure(
                title="Unable to read from Qlik",
                message="Left out of this run after a request failed.",
                context=f"{unread.name}: {unread.cause}",
            )
        if self.reporter.failures:
            # Like the stale entity removal state, a failed run keeps the last
            # read of the apps it did not reach.
            self.app_state.keep_unrecorded_apps()
        if self.app_state.is_too_large():
            self.reporter.warning(
                title="Qlik app checkpoint is too large to save",
                message="Unchanged apps are not skipped and partly read apps are "
                "reported as failures until it fits. Raise "
                "stateful_ingestion.max_checkpoint_state_size.",
            )

    def get_report(self) -> SourceReport:
        return self.reporter
