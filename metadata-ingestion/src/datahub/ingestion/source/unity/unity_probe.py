import itertools
import re
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, TypeVar

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
from databricks.sql.exc import Error as SqlConnectorError, ServerOperationError

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.sql_passthrough import (
    CatalogRows,
    QueryBudget,
    SqlCatalogPassthrough,
)
from datahub.ingestion.agent.verdicts import ProbeConnectionError
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
    Schema,
    Table,
    escape_unity_name,
    qualified_table_name,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport

# Databricks error codes are SCREAMING_SNAKE identifiers ("PERMISSION_DENIED").
# Anything else in that slot is not one and is dropped with the rest of the text.
_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}")

# Spark prefixes an error message with its error class: "[CAST_INVALID_INPUT] ...".
_SPARK_ERROR_CLASS = re.compile(r"\[([A-Z][A-Z0-9_.]{0,127})\]")

_WITHHELD = (
    "the SDK's error text is withheld because it can carry the request log, "
    "its URL and, with debug headers on, its credentials"
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


_T = TypeVar("_T")


def _take(
    source: Iterable[_T], limit: int, keep: Callable[[_T], bool] = lambda _: True
) -> List[_T]:
    """The first `limit` items `keep` admits, stopping the SDK's paging there.

    Closes the source explicitly rather than leaving an abandoned generator to
    the garbage collector: proxy.tables patches the SDK's TableInfo class for
    as long as its loop is suspended.
    """
    iterator = iter(source)
    try:
        return list(itertools.islice(filter(keep, iterator), limit))
    finally:
        close = getattr(iterator, "close", None)
        if callable(close):
            close()


def _describe_failure(exc: BaseException) -> str:
    """What failed, without the SDK's message.

    The message is not safe to repeat: an unparseable response becomes "unable
    to parse response ... Request log: ```...```" (errors/parser.py), and the
    OAuth token exchange raises ValueError(resp.content). The class, the HTTP
    status it maps to and the error code are enough to act on.
    """
    parts = [type(exc).__name__]
    for status, error_cls in STATUS_CODE_MAPPING.items():
        if isinstance(exc, error_cls):
            parts.append(f"HTTP {status}")
            break
    code = getattr(exc, "error_code", None)
    if isinstance(code, str) and _ERROR_CODE.fullmatch(code):
        parts.append(code)
    return ", ".join(parts)


def _describe_warehouse_failure(exc: BaseException) -> str:
    """What the warehouse refused, without its message: a Spark error can
    quote a cell value ("The value '...' cannot be cast"), and a request
    error can quote HTTP headers."""
    parts = [type(exc).__name__]
    match = _SPARK_ERROR_CLASS.match(str(exc))
    if match:
        parts.append(match.group(1))
    return ", ".join(parts)


class _Missing(ValueError):
    """The caller named something that is not there (exit 2). A subclass so
    the pinned-catalog loop can tell it from a bad request."""


class _Degraded(Exception):
    """A container this credential may not read. Caught by the command that
    raised it, after the reason was recorded as a warning; never propagated."""


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
    warnings: List[str]

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
        self._sql_connection: Optional[Any] = None
        self.warnings = []

    @classmethod
    def for_config(
        cls, config: UnityCatalogSourceConfig
    ) -> "UnityCatalogMetadataProbe":
        """The ingestion's own client builder, so auth, user-agent and
        warehouse_id resolve exactly as UnityCatalogSource.__init__ does."""
        try:
            client = create_workspace_client(config)
        except Exception as exc:
            # OAuth and Azure resolve endpoints while the client is built, and
            # their failures carry raw response bodies.
            raise ProbeConnectionError(
                f"could not build the Databricks workspace client "
                f"({_describe_failure(exc)}); check workspace_url and the "
                f"credentials in the recipe. {_WITHHELD}"
            ) from None
        return cls(client, config)

    @property
    def probe_report(self) -> UnityCatalogReport:
        return self._report

    def __exit__(self, *exc: object) -> None:
        # WorkspaceClient holds nothing closable; only `sql` opens a connection.
        if self._sql_connection is not None:
            connection, self._sql_connection = self._sql_connection, None
            connection.close()

    def _warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

    @contextmanager
    def _calling(self, operation: str, missing: Optional[str] = None) -> Iterator[None]:
        """Map an SDK failure onto the probe's exit codes, scrubbed.

        `missing` names what the caller asked for; when given, a 404 is their
        bad argument (exit 2). A 403 becomes a warning plus an empty result
        (_Degraded). Everything else is the source's failure (exit 3). No SDK
        message survives any branch -- see _describe_failure.
        """
        try:
            yield
        except NotFound as exc:
            if missing is None:
                raise ProbeConnectionError(
                    f"{operation} failed ({_describe_failure(exc)}); {_WITHHELD}"
                ) from None
            raise _Missing(
                f"no {missing} visible to this credential ({_describe_failure(exc)})"
            ) from None
        except PermissionDenied as exc:
            self._warn(
                f"{operation}: this credential may not read it "
                f"({_describe_failure(exc)}); reported as empty"
            )
            raise _Degraded() from None
        except BadRequest as exc:
            raise ValueError(
                f"{operation} was refused as a bad request "
                f"({_describe_failure(exc)}); {_WITHHELD}"
            ) from None
        except (DatabricksError, requests.RequestException, ValueError) as exc:
            # ValueError cannot be narrowed: the SDK's auth layer raises a bare
            # ValueError while authenticating the first request, and its text
            # can be the raw token-endpoint body (oauth.py's
            # ValueError(resp.content)) -- no subclass marks those, so the
            # scrub has to take the whole class. Nothing this probe raises
            # itself runs inside the block, and _Missing / _Degraded above are
            # already handled. It is the source refusing the session, not the
            # caller's argument, hence exit 3.
            raise ProbeConnectionError(
                f"{operation} failed ({_describe_failure(exc)}); {_WITHHELD}"
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
        if self._config.catalogs:
            return self._pinned_catalogs(self._config.catalogs, limit)
        names: List[str] = []
        if self._config.include_hive_metastore:
            # UnityCatalogApiProxy.catalogs yields it first when ingestion has
            # a hive_metastore proxy, which it builds for this flag.
            names.append(HIVE_METASTORE)
        seen = set(names)

        def unseen(catalog: Catalog) -> bool:
            # On UC-enabled workspaces the listing can return hive_metastore
            # itself, which ingestion would then see twice.
            if catalog.name in seen:
                return False
            seen.add(catalog.name)
            return True

        try:
            with self._calling("listing catalogs"):
                listed = _take(
                    self._proxy.catalogs(metastore=None), limit - len(names), unseen
                )
                names.extend(catalog.name for catalog in listed)
        except _Degraded:
            pass
        return names[:limit]

    def _note_metastore(self) -> None:
        """With include_metastore, catalog_pattern and schema_pattern match ids
        prefixed with the assigned metastore (source.py reads it through
        assigned_metastore), and a run record carries no metastore to pass on.
        Name it, so `probe filter` can be given it as the outermost --parent
        instead of degrading to the bare name."""
        if not self._config.include_metastore:
            return
        try:
            with self._calling("reading the assigned metastore"):
                metastore = self._proxy.assigned_metastore()
        except _Degraded:
            return
        if metastore is None:
            self._warn(
                "include_metastore is on but the workspace reports no assigned "
                "metastore; ingestion would report 'Metastore not found' and "
                "ingest no catalogs (process_metastores)"
            )
            return
        self._warn(
            f"include_metastore is on, so catalog_pattern and schema_pattern "
            f"match ids prefixed with the metastore '{metastore.name}'; pass it "
            f"as the first --parent to `probe filter` for Catalog and Schema "
            f"verdicts"
        )

    def _pinned_catalogs(self, pinned: List[str], limit: int) -> List[str]:
        # source._get_catalogs: catalogs.get per pinned name, never a listing,
        # and no hive_metastore proxy catalog on this path.
        names: List[str] = []
        for name in pinned:
            try:
                with self._calling(
                    f"reading catalog '{name}'", missing=f"catalog '{name}'"
                ):
                    catalog = self._proxy.catalog(name, metastore=None)
            except _Missing:
                self._warn(
                    f"catalog '{name}' named in `catalogs` was not found; "
                    f"ingestion would fail on it, since _get_catalogs reads "
                    f"each pinned catalog without catching the 404"
                )
                continue
            except _Degraded:
                continue
            if catalog is not None and catalog.name not in names:
                names.append(catalog.name)
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
        try:
            catalog_obj = self._catalog(catalog)
            with self._calling(f"listing schemas of catalog '{catalog}'"):
                return [s.name for s in _take(self._proxy.schemas(catalog_obj), limit)]
        except _Degraded:
            return []

    def _table_like(
        self, catalog: str, schema: str, limit: int, keep: Callable[[Table], bool]
    ) -> List[str]:
        if self._hive_via_warehouse(catalog):
            return []
        try:
            catalog_obj = self._catalog(catalog)
            # The shape proxy._create_schema builds, without a schemas.get
            # round trip: tables() reads only catalog.name and name.
            schema_obj = Schema(
                id=f"{catalog_obj.id}.{escape_unity_name(schema)}",
                name=schema,
                catalog=catalog_obj,
                comment=None,
                owner=None,
            )
            with self._calling(
                f"listing tables of '{catalog}.{schema}'",
                missing=f"schema '{catalog}.{schema}'",
            ):
                return [
                    t.name for t in _take(self._proxy.tables(schema_obj), limit, keep)
                ]
        except _Degraded:
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
        metric_view_pattern only while include_metric_views is on. Empty on a
        databricks-sdk too old to know the METRIC_VIEW table type."""
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
        try:
            with self._calling(
                f"reading table '{full_name}'", missing=f"table '{full_name}'"
            ):
                info = self._client.tables.get(full_name=full_name)
        except _Degraded:
            return []
        return [
            {
                "name": column.name,
                "type": column.type_text,
                "nullable": column.nullable,
                "comment": column.comment,
                "partition_index": column.partition_index,
            }
            for column in info.columns or []
        ]

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
        source. Walks the workspace tree, so a large workspace is slow; the
        walk stops at `limit`."""
        withheld = 0

        def shown(notebook: Notebook) -> bool:
            nonlocal withheld
            if _is_shared_path(notebook.path) or self._ingests_notebook(notebook.path):
                return True
            withheld += 1
            return False

        try:
            with self._calling("listing workspace notebooks"):
                paths = [
                    n.path
                    for n in _take(self._proxy.workspace_notebooks(), limit, shown)
                ]
        except _Degraded:
            return []
        if withheld:
            # A walk that filled the limit stopped early, so more may follow.
            count = f"at least {withheld}" if len(paths) >= limit else str(withheld)
            self._warn(
                f"{count} notebook{'' if withheld == 1 else 's'} outside /Shared/ "
                f"withheld: ingestion would not read them with this recipe, and "
                f"paths outside the shared folder (user folders, personal repos) "
                f"name people"
            )
        return paths

    def _ingests_notebook(self, path: str) -> bool:
        # The predicate get_workunits_internal + process_notebooks apply.
        return self._config.include_notebooks and self._config.notebook_pattern.allowed(
            path
        )

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

    def execute_catalog_query(self, query: str, limit: int) -> CatalogRows:
        if not self._config.warehouse_id:
            raise ValueError(
                "`sql` runs on a Databricks SQL warehouse; set warehouse_id in "
                "the recipe to use it (the other probe commands do not need one)"
            )
        try:
            if self._sql_connection is None:
                # The params ingestion's own SQL reads use
                # (proxy._execute_sql_query), so auth and user-agent match.
                # STATEMENT_TIMEOUT is Databricks SQL's server-side ceiling in
                # seconds: abandoning the cursor client-side would leave the
                # warehouse running -- and billing -- the statement.
                self._sql_connection = connect(
                    **get_sql_connection_params(self._client),
                    session_configuration={
                        "STATEMENT_TIMEOUT": str(self.query_budget.timeout_seconds)
                    },
                )
            with self._sql_connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchmany(limit)
                columns = [d[0] for d in cursor.description or []]
        except ServerOperationError as exc:
            raise ValueError(
                f"the warehouse rejected the query "
                f"({_describe_warehouse_failure(exc)}); its message is withheld "
                f"because it can quote cell values"
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
                f"'{self._config.warehouse_id}' "
                f"({_describe_warehouse_failure(exc)}); {_WITHHELD}"
            ) from None
        return CatalogRows(columns=columns, rows=[list(row) for row in rows])
