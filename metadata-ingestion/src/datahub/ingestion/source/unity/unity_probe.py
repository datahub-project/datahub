import re
from contextlib import contextmanager
from typing import Callable, Dict, FrozenSet, Iterator, List, Optional

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    BadRequest,
    DatabricksError,
    NotFound,
    PermissionDenied,
)
from databricks.sdk.errors.platform import STATUS_CODE_MAPPING
from databricks.sql import connect
from databricks.sql.client import Connection
from databricks.sql.exc import Error as SqlConnectorError, ServerOperationError

from datahub.ingestion.agent.error_policy import (
    NETWORK_REASON_HINTS,
    name_foreign,
    network_reason,
)
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import (
    PersonalWithholding,
    soft_listing,
    take,
)
from datahub.ingestion.agent.sql_passthrough import (
    CatalogRows,
    QueryBudget,
    SqlCatalogPassthrough,
)
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeSoftError,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.connection import (
    create_workspace_client,
    get_sql_connection_params,
)
from datahub.ingestion.source.unity.hive_metastore_proxy import HIVE_METASTORE
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    Notebook,
    Table,
    make_schema,
    metric_view_supported,
    qualified_table_name,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport

# Databricks error codes are SCREAMING_SNAKE identifiers ("PERMISSION_DENIED"),
# within the shared reader's 32-character name. Anything else in that slot is
# not one, and the HTTP status names the failure instead.
_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,31}")

# Spark prefixes an error message with its error class: "[CAST_INVALID_INPUT]
# ...", at most one dotted subclass deep ("UNRESOLVED_COLUMN.WITH_SUGGESTION").
_SPARK_ERROR_CLASS = re.compile(r"\[([A-Z][A-Z0-9_]{0,31}(?:\.[A-Z0-9_]{1,31})?)\]")

# The Spark error classes that mean the caller's query is wrong, so rewriting
# it helps (exit 2). Any other class, or none -- the probe's own
# STATEMENT_TIMEOUT, INTERNAL_ERROR, a warehouse that is stopping -- is the
# warehouse's (exit 3). Matched on the class before any dotted subclass.
# INSUFFICIENT_PERMISSIONS is deliberately not here: rewriting the query won't
# grant the privilege, so it stays exit 3, as permission failures do in every
# probe, with a hint naming the grants instead.
_CALLER_SPARK_CLASSES: FrozenSet[str] = frozenset(
    {
        "AMBIGUOUS_COLUMN_OR_FIELD",
        "AMBIGUOUS_REFERENCE",
        "CAST_INVALID_INPUT",
        "DATATYPE_MISMATCH",
        "MISSING_AGGREGATION",
        "MISSING_GROUP_BY",
        "PARSE_EMPTY_STATEMENT",
        "PARSE_SYNTAX_ERROR",
        "SCHEMA_NOT_FOUND",
        "TABLE_OR_VIEW_NOT_FOUND",
        "UNRESOLVED_COLUMN",
        "UNRESOLVED_FIELD",
        "UNRESOLVED_ROUTINE",
    }
)

# The 403 code Unity Catalog gives for one object the principal lacks a
# privilege on. Any other 403 refuses the whole session: an invalid or
# revoked token, a principal without workspace access, or an IP access list
# block, whose HTML page leaves the SDK only the HTTP reason ("FORBIDDEN").
_OBJECT_PERMISSION_DENIED = "PERMISSION_DENIED"
_SPARK_PERMISSION_CLASS = "INSUFFICIENT_PERMISSIONS"
_SQL_GRANT_HINT = (
    "the principal lacks a privilege the query needs; grant it USE CATALOG and "
    "USE SCHEMA on the parents and SELECT on the queried object, and CAN USE on "
    "the warehouse"
)
_SESSION_REFUSED_HINT = (
    "the workspace refused this session, not one object; check that the "
    "token or service principal is valid and not revoked, that the principal "
    "has access to this workspace, and that its IP access list admits this "
    "machine"
)

# The 404 codes naming a catalog, schema or table: each is an argument the
# caller passed. Any other 404 is the source's -- METASTORE_DOES_NOT_EXIST, or
# an HTML page from a workspace_url pointing at the wrong host.
_OBJECT_NOT_FOUND: FrozenSet[str] = frozenset(
    {"CATALOG_DOES_NOT_EXIST", "SCHEMA_DOES_NOT_EXIST", "TABLE_DOES_NOT_EXIST"}
)
_NOT_FOUND_HINT = (
    "check that workspace_url names this workspace and that a metastore is "
    "assigned to it"
)


# Notebook paths are listed from an allowlist, not filtered by a denylist.
# Databricks names home folders and personal repos after the user's login
# (usually an email address) and reaches them by several spellings --
# /Users/, /users/, //Users/, /Workspace/Users/, /Repos/<user>/ -- so any
# denylist of "personal" roots misses one. Only the shared root is safe to
# list unconditionally; everything else is listed only when ingestion itself
# would read it.
_SHARED_ROOT = "/shared/"
_WORKSPACE_PREFIX = "/workspace/"
_REPEATED_SLASHES = re.compile(r"/{2,}")


def _is_shared_path(path: str) -> bool:
    """Whether a notebook path lies under /Shared/ however it is spelled:
    repeated slashes collapsed, a leading /Workspace stripped, compared
    case-insensitively. A path with a `.` or `..` segment is never shared,
    so `/Shared/../Users/...` cannot pass."""
    normal = _REPEATED_SLASHES.sub("/", path).casefold()
    if normal.startswith(_WORKSPACE_PREFIX):
        normal = normal[len(_WORKSPACE_PREFIX) - 1 :]
    if any(segment in (".", "..") for segment in normal.split("/")):
        return False
    return normal.startswith(_SHARED_ROOT)


_HIVE_NOT_PROBED = (
    "hive_metastore is read by ingestion through a SQL warehouse "
    "(HiveMetastoreProxy), which the probe does not start; its contents are "
    "not listed here"
)

_METRIC_VIEW_UNSUPPORTED = (
    "this databricks-sdk does not know the METRIC_VIEW table type, so metric "
    "views cannot be told apart and none are listed; ingestion warns the same "
    "way when include_metric_views is on. Upgrade databricks-sdk"
)


def _sdk_error_code(exc: DatabricksError) -> Optional[str]:
    code = getattr(exc, "error_code", None)
    if isinstance(code, str) and _ERROR_CODE.fullmatch(code):
        return code
    return None


def _spark_error_class(exc: ServerOperationError) -> Optional[str]:
    # Error.__str__ returns `message`, which may be None.
    message = getattr(exc, "message", None)
    if not isinstance(message, str):
        return None
    match = _SPARK_ERROR_CLASS.match(message)
    return match.group(1) if match else None


class _Missing(ProbeArgumentError):
    """The caller named something that is not there (exit 2). A subclass so
    the pinned-catalog loop can tell it from a bad request."""


class UnityCatalogMetadataProbe(SqlCatalogPassthrough):
    """Metadata-only probe for Databricks Unity Catalog.

    Enumerates through UnityCatalogApiProxy -- the REST fetchers ingestion
    uses -- rather than a SQLAlchemy Inspector: ingestion reads catalogs,
    schemas and tables from the UC API, and the SQLAlchemy URL this config
    builds carries neither a token nor an http_path, so the inherited SQL
    provider could not authenticate at all.
    """

    sql_dialect = "databricks"
    query_budget = QueryBudget(timeout_seconds=30)

    @staticmethod
    def probe_error_code(exc: BaseException) -> Optional[str]:
        # The generic readers find no status on a DatabricksError: the SDK
        # maps it onto a subclass and keeps the vendor code in error_code.
        if isinstance(exc, ServerOperationError):
            return _spark_error_class(exc)
        if not isinstance(exc, DatabricksError):
            return None
        code = _sdk_error_code(exc)
        if code is not None:
            return code
        for status, error_cls in STATUS_CODE_MAPPING.items():
            if isinstance(exc, error_cls):
                return f"HTTP {status}"
        return None

    def __init__(
        self, workspace_client: WorkspaceClient, config: UnityCatalogSourceConfig
    ) -> None:
        self._client = workspace_client
        self._config = config
        self._report = UnityCatalogReport()
        # hive_metastore_proxy=None on purpose: building one opens a SQL
        # engine against the warehouse, which a listing must not do.
        self._proxy = UnityCatalogApiProxy(
            workspace_client,
            report=self._report,
            hive_metastore_proxy=None,
            databricks_api_page_size=config.databricks_api_page_size,
        )

    @classmethod
    def for_config(
        cls, config: UnityCatalogSourceConfig
    ) -> "UnityCatalogMetadataProbe":
        """The ingestion's own client builder, so auth, user-agent and
        warehouse_id resolve exactly as UnityCatalogSource.__init__ does."""
        try:
            client = create_workspace_client(config)
        except (ValueError, DatabricksError, requests.RequestException, OSError) as exc:
            # OAuth and Azure resolve endpoints while the client is built, and
            # their failures carry raw response bodies, so only the label is
            # shown. A defect in the builder is not caught here.
            reason = network_reason(exc)
            hint = (
                f"{reason} - {NETWORK_REASON_HINTS[reason]}"
                if reason is not None
                else "check workspace_url and the credentials in the recipe"
            )
            raise ProbeConnectionError(
                f"could not build the Databricks workspace client "
                f"{name_foreign(exc, cls)}; {hint}"
            ) from None
        return cls(client, config)

    @property
    def probe_report(self) -> UnityCatalogReport:
        return self._report

    @contextmanager
    def _calling(
        self, operation: str, missing: Optional[str] = None, degrade: bool = True
    ) -> Iterator[None]:
        """Map an SDK failure onto the probe's exit codes, labelled only.

        `missing` names what the caller asked for; when given, a 404 whose
        code names a catalog, schema or table is their bad argument (exit 2).
        With `degrade`, a missing UC privilege is a ProbeSoftError, for the
        command's soft_listing to report as empty with a warning. Every other
        failure is the source's (exit 3).
        """
        try:
            yield
        except (ProbeArgumentError, ProbeSoftError):
            raise
        except NotFound as exc:
            label = name_foreign(exc, type(self))
            if missing is not None and _sdk_error_code(exc) in _OBJECT_NOT_FOUND:
                raise _Missing(
                    f"no {missing} visible to this credential {label}"
                ) from None
            raise ProbeConnectionError(
                f"{operation} failed {label}; {_NOT_FOUND_HINT}"
            ) from None
        except PermissionDenied as exc:
            label = name_foreign(exc, type(self))
            if degrade and _sdk_error_code(exc) == _OBJECT_PERMISSION_DENIED:
                raise ProbeSoftError(
                    f"{operation}: this credential may not read it {label}; "
                    f"reported as empty"
                ) from None
            raise ProbeConnectionError(
                f"{operation} failed {label}; {_SESSION_REFUSED_HINT}"
            ) from None
        except BadRequest as exc:
            raise ProbeArgumentError(
                f"{operation} was refused as a bad request "
                f"{name_foreign(exc, type(self))}"
            ) from None
        except (DatabricksError, requests.RequestException, ValueError) as exc:
            # SDK auth raises ValueError(resp.content); that is the source's
            # failure (exit 3).
            raise ProbeConnectionError(
                f"{operation} failed {name_foreign(exc, type(self))}"
            ) from None

    @probe_method(kind=DatasetContainerSubTypes.CATALOG, row_limit_param="limit")
    def catalogs(self, limit: int = 200) -> List[str]:
        """Catalogs this recipe reads. When `catalogs` pins a list, only those
        names are looked up, as ingestion does -- one that does not exist is
        reported in warnings, not listed. Otherwise every catalog the
        credential can browse, plus hive_metastore when include_hive_metastore
        is on. Includes catalogs catalog_pattern would exclude, so
        `probe filter --kind Catalog` can explain them. Raw names; the filter
        escapes them the way ingestion builds the id it matches."""
        self._note_metastore()
        pinned = self._config.pinned_catalogs()
        if pinned is not None:
            return self._pinned_catalogs(pinned, limit)
        names: List[str] = []
        if self._config.include_hive_metastore:
            # UnityCatalogApiProxy.catalogs yields it first when ingestion has
            # a hive_metastore proxy, which it builds for this flag.
            names.append(HIVE_METASTORE)
        seen = set(names)

        def unseen(catalog: Catalog) -> bool:
            # On UC-enabled workspaces the listing can return hive_metastore
            # itself; ingestion would read it twice under one name, with one
            # verdict, so it is listed once.
            if catalog.name in seen:
                return False
            seen.add(catalog.name)
            return True

        # Not degraded: the listing leaves out catalogs the principal cannot
        # see, so a 403 here refuses the session, never one catalog.
        with self._calling("listing catalogs", degrade=False):
            listed = take(
                self._proxy.catalogs(metastore=None),
                limit - len(names),
                keep=unseen,
            )
        names.extend(catalog.name for catalog in listed)
        return names[:limit]

    def _note_metastore(self) -> None:
        """With include_metastore, catalog_pattern and schema_pattern match ids
        prefixed with the assigned metastore (source.py reads it through
        assigned_metastore), and a run record carries no metastore to pass on.
        Name it, so `probe filter` can be given it as the outermost --parent
        instead of degrading to the bare name. Advisory: an unassigned
        metastore or a missing privilege only warns; other failures propagate."""
        if not self._config.include_metastore:
            return
        with soft_listing(self._warn):
            with self._calling("reading the assigned metastore"):
                try:
                    metastore = self._proxy.assigned_metastore()
                except NotFound as exc:
                    # process_metastores calls assigned_metastore without a
                    # try, so on this 404 ingestion ends the run.
                    self._warn(
                        f"include_metastore is on but no metastore is assigned "
                        f"to this workspace {name_foreign(exc, type(self))}; "
                        f"ingestion would abort reading it (process_metastores). "
                        f"Set include_metastore: false, or assign a metastore"
                    )
                    return
            if metastore is None:
                self._warn(
                    "include_metastore is on but the workspace reports no "
                    "assigned metastore; ingestion would report 'Metastore not "
                    "found' and ingest no catalogs (process_metastores)"
                )
                return
            self._warn(
                f"include_metastore is on, so catalog_pattern and schema_pattern "
                f"match ids prefixed with the metastore '{metastore.name}'; pass "
                f"it as the first --parent to `probe filter`, ahead of the "
                f"catalog and schema"
            )

    def _pinned_catalogs(self, pinned: List[str], limit: int) -> List[str]:
        # source._get_catalogs: catalogs.get per pinned name, never a listing,
        # and no hive_metastore proxy catalog on this path.
        names: List[str] = []
        for name in pinned:
            try:
                with soft_listing(self._warn):
                    with self._calling(
                        f"reading catalog '{name}'", missing=f"catalog '{name}'"
                    ):
                        catalog = self._proxy.catalog(name, metastore=None)
                    if catalog is not None and catalog.name not in names:
                        names.append(catalog.name)
            except _Missing:
                self._warn(
                    f"catalog '{name}' named in `catalogs` was not found; "
                    f"ingestion would fail on it, since _get_catalogs reads "
                    f"each pinned catalog without catching the 404"
                )
            if len(names) >= limit:
                break
        return names

    def _hive_via_warehouse(self, catalog: str) -> bool:
        """Whether ingestion reads this catalog over SQL, which the probe does
        not do. Only with include_hive_metastore: then UnityCatalogApiProxy
        routes hive_metastore to HiveMetastoreProxy. Without it, a UC-listed
        hive_metastore goes through the same REST listings as any catalog."""
        if catalog != HIVE_METASTORE or not self._config.include_hive_metastore:
            return False
        self._warn(_HIVE_NOT_PROBED)
        return True

    def _catalog(self, name: str) -> Catalog:
        with self._calling(f"reading catalog '{name}'", missing=f"catalog '{name}'"):
            catalog = self._proxy.catalog(name, metastore=None)
        if catalog is None:
            raise _Missing(f"no catalog '{name}' visible to this credential")
        return catalog

    @probe_method(
        kind=DatasetContainerSubTypes.SCHEMA,
        row_limit_param="limit",
        parent_params=("catalog",),
    )
    def schemas(self, catalog: str, limit: int = 200) -> List[str]:
        """Schemas in one catalog (raw names), including ones schema_pattern
        would exclude -- information_schema among them, which ingestion always
        denies. The catalog travels with the result, so `probe filter` needs
        no --parent. hive_metastore is read by ingestion over a SQL warehouse
        and is not probed here; it comes back empty with a warning."""
        self._note_metastore()
        if self._hive_via_warehouse(catalog):
            return []
        with soft_listing(self._warn):
            catalog_obj = self._catalog(catalog)
            with self._calling(f"listing schemas of catalog '{catalog}'"):
                return [s.name for s in take(self._proxy.schemas(catalog_obj), limit)]
        return []

    def _table_like(
        self, catalog: str, schema: str, limit: int, keep: Callable[[Table], bool]
    ) -> List[str]:
        # A run record's parent is [catalog, schema]; name the metastore so
        # it can be passed as the outermost --parent.
        self._note_metastore()
        if self._hive_via_warehouse(catalog):
            return []
        with soft_listing(self._warn):
            # tables() reads only the schema's catalog and name, so the schema
            # is built without a schemas.get round trip.
            schema_obj = make_schema(self._catalog(catalog), schema)
            with self._calling(
                f"listing tables of '{catalog}.{schema}'",
                missing=f"schema '{catalog}.{schema}'",
            ):
                return [
                    t.name
                    for t in take(self._proxy.tables(schema_obj), limit, keep=keep)
                ]
        return []

    @probe_method(
        kind=DatasetSubTypes.TABLE,
        row_limit_param="limit",
        parent_params=("catalog", "schema"),
    )
    def tables(self, catalog: str, schema: str, limit: int = 200) -> List[str]:
        """Tables in one schema -- not views or metric views, which ingestion
        also judges by view_pattern and metric_view_pattern. Includes ones
        table_pattern would exclude. The catalog and schema travel with the
        result, so `probe filter` needs no --parent."""
        return self._table_like(
            catalog,
            schema,
            limit,
            lambda table: not table.is_view and not table.is_metric_view,
        )

    @probe_method(
        kind=DatasetSubTypes.VIEW,
        row_limit_param="limit",
        parent_params=("catalog", "schema"),
    )
    def views(self, catalog: str, schema: str, limit: int = 200) -> List[str]:
        """Views and materialized views in one schema. Ingestion judges each
        by table_pattern first and then by view_pattern, and `probe filter
        --kind View` does the same."""
        return self._table_like(catalog, schema, limit, lambda table: table.is_view)

    @probe_method(
        kind=DatasetSubTypes.METRIC_VIEW,
        row_limit_param="limit",
        parent_params=("catalog", "schema"),
    )
    def metric_views(self, catalog: str, schema: str, limit: int = 200) -> List[str]:
        """Metric views in one schema. table_pattern always applies to them;
        metric_view_pattern only while include_metric_views is on. Empty, with
        a warning, on a databricks-sdk too old to know the METRIC_VIEW table
        type."""
        if not metric_view_supported():
            self._warn(_METRIC_VIEW_UNSUPPORTED)
            return []
        return self._table_like(
            catalog, schema, limit, lambda table: table.is_metric_view
        )

    @probe_method()
    def columns(self, catalog: str, schema: str, table: str) -> List[Dict[str, object]]:
        """Columns of one table or view: name, type, nullability, comment and
        partition index. Structural metadata only -- no cell values are read."""
        if self._hive_via_warehouse(catalog):
            return []
        full_name = qualified_table_name(catalog, schema, table)
        with soft_listing(self._warn):
            with self._calling(
                f"reading table '{full_name}'", missing=f"table '{full_name}'"
            ):
                # include_browse as every proxy listing passes it: a table
                # the principal may only browse is still listed by `tables`.
                info = self._client.tables.get(full_name=full_name, include_browse=True)
            if info.columns is None:
                self._warn(
                    f"the API returned no column list for '{full_name}'; "
                    f"reported as empty, which does not mean it has no columns"
                )
                return []
            return [
                {
                    "name": column.name,
                    "type": column.type_text,
                    "nullable": column.nullable,
                    "comment": column.comment,
                    "partition_index": column.partition_index,
                }
                for column in info.columns
            ]
        return []

    @probe_method(kind=DatasetSubTypes.NOTEBOOK, row_limit_param="limit")
    def notebooks(self, limit: int = 200) -> List[str]:
        """Notebook paths in the workspace, exactly as ingestion sees them --
        the string notebook_pattern is matched against. A path is listed only
        when (a) this recipe would ingest it (include_notebooks on and
        notebook_pattern allowing it), or (b) it lies under /Shared/
        (also /Workspace/Shared/, any case, repeated slashes ignored); those
        are listed whatever include_notebooks says, and `probe filter --kind
        Notebook` reports them excluded while it is off. Every other path --
        user folders, personal repos -- is withheld and only counted in
        warnings, because those paths name people. Paths only, never notebook
        source. Walks the workspace tree, so a large workspace is slow: the
        walk stops once `limit` paths are listed, and withheld paths do not
        count towards it."""
        withholding = PersonalWithholding[Notebook](
            # Fail-closed: anything not provably under /Shared/ counts as
            # personal.
            is_personal=lambda n: not _is_shared_path(n.path),
            would_ingest=lambda n: self._config.notebook_ingested(n.path),
        )
        paths: List[str] = []
        with soft_listing(self._warn):
            with self._calling("listing workspace notebooks"):
                paths = [
                    n.path
                    for n in take(
                        self._proxy.workspace_notebooks(),
                        limit,
                        keep=withholding.keep,
                    )
                ]
        if withholding.withheld:
            count = withholding.count_text(stopped_early=len(paths) >= limit)
            self._warn(
                f"{count} notebook{'' if withholding.withheld == 1 else 's'} "
                f"outside /Shared/ withheld: ingestion would not read them with "
                f"this recipe, and paths outside the shared folder (user "
                f"folders, personal repos) name people"
            )
        return paths

    @probe_method(row_limit_param="limit")
    def service_principals(self, limit: int = 200) -> List[str]:
        """Display names of the workspace's service principals this credential
        can list. Ingestion lists them to show an owner that is a service
        principal by its display name rather than its application id; when it
        cannot, it warns and those owners stay ids. Only principals ingestion
        keeps (a display name and an application id) are listed, and only
        their display names: these are machine identities, but application
        ids add nothing a recipe check needs. A credential that may not list
        them gets an empty result and a warning."""
        with soft_listing(self._warn):
            with self._calling("listing service principals"):
                return [
                    principal.display_name
                    for principal in take(self._proxy.service_principals(), limit)
                ]
        return []

    @probe_method(
        name="sql",
        scoped_sql_param="query",
        row_limit_param="limit",
        shapes_own_result=True,
    )
    def sql(self, query: str, limit: int = 50) -> Dict[str, object]:
        """Run a read-only catalog query on the SQL warehouse named by
        warehouse_id, which this command requires (no other command touches
        the warehouse). Running it may start a stopped warehouse -- auto-start
        and serverless warehouses wake for any statement -- which costs money
        and can take minutes. Only a single SELECT over information_schema
        (`system.information_schema.*` or `<catalog>.information_schema.*`)
        is permitted: the framework scope-checks `query` before the warehouse
        sees it, so query history, audit logs and user tables are refused.
        The server stops a statement after 30 seconds. Returns `columns` plus
        positional `rows`, with `truncated` telling you whether more exist
        beyond `limit`."""
        return super().sql(query=query, limit=limit)

    def _open_warehouse(self) -> Connection:
        # The params ingestion's own SQL reads use (proxy._execute_sql_query),
        # so auth and user-agent match. STATEMENT_TIMEOUT is Databricks SQL's
        # server-side ceiling in seconds: abandoning the cursor client-side
        # would leave the warehouse running -- and billing -- the statement.
        options: Dict[str, object] = {}
        timeout = self.query_budget.timeout_seconds
        if timeout is not None:
            options["session_configuration"] = {"STATEMENT_TIMEOUT": str(timeout)}
        return connect(**get_sql_connection_params(self._client), **options)

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        if not self._config.warehouse_id:
            raise ProbeArgumentError(
                "`sql` runs on a Databricks SQL warehouse; set warehouse_id in "
                "the recipe to use it (the other probe commands do not need one)"
            )
        try:
            connection = self._open_once(
                "sql-warehouse", self._open_warehouse, close=lambda c: c.close()
            )
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchmany(limit)
                columns = [d[0] for d in cursor.description or []]
        except ServerOperationError as exc:
            label = name_foreign(exc, type(self))
            spark_class = _spark_error_class(exc)
            if spark_class and spark_class.split(".")[0] in _CALLER_SPARK_CLASSES:
                # The label, never the message: a Spark error can quote a
                # cell value ("The value '...' cannot be cast").
                raise ProbeArgumentError(
                    f"the warehouse rejected the query {label}; correct the query"
                ) from None
            if spark_class and spark_class.split(".")[0] == _SPARK_PERMISSION_CLASS:
                raise ProbeConnectionError(
                    f"the query failed on warehouse "
                    f"'{self._config.warehouse_id}' {label}: {_SQL_GRANT_HINT}"
                ) from None
            raise ProbeConnectionError(
                f"the query failed on warehouse '{self._config.warehouse_id}' {label}"
            ) from None
        except (
            SqlConnectorError,
            DatabricksError,
            requests.RequestException,
            # The connector authenticates through the workspace client, whose
            # OAuth exchange raises ValueError(resp.content).
            ValueError,
        ) as exc:
            raise ProbeConnectionError(
                f"could not run the query on warehouse "
                f"'{self._config.warehouse_id}' {name_foreign(exc, type(self))}"
            ) from None
        return CatalogRows(columns=columns, rows=[list(row) for row in rows])
