import itertools
import re
from contextlib import closing, contextmanager
from typing import Any, Callable, Dict, Iterator, List, Optional

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    BadRequest,
    DatabricksError,
    NotFound,
    PermissionDenied,
)
from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.sql_passthrough import QueryBudget, SqlCatalogPassthrough
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.connection import create_workspace_client
from datahub.ingestion.source.unity.hive_metastore_proxy import HIVE_METASTORE
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.proxy_types import (
    Catalog,
    Schema,
    Table,
    escape_unity_name,
    qualified_table_name,
)
from datahub.ingestion.source.unity.report import UnityCatalogReport

# Databricks error codes are SCREAMING_SNAKE identifiers ("PERMISSION_DENIED").
# Anything else in that slot is not one and is dropped with the rest of the text.
_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}")

_WITHHELD = (
    "the SDK's error text is withheld because it can carry the request log, "
    "its URL and, with debug headers on, its credentials"
)


_HIVE_NOT_PROBED = (
    "hive_metastore is read by ingestion through a SQL warehouse "
    "(HiveMetastoreProxy), which the probe does not start; its contents are "
    "not listed here"
)


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
    def for_config(cls, config: UnityCatalogSourceConfig) -> "UnityCatalogMetadataProbe":
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
            # ValueError too: the SDK raises it for an auth failure during the
            # first request (oauth.py's ValueError(resp.content)), which is the
            # source refusing the session, not the caller's argument.
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
        if self._config.catalogs:
            return self._pinned_catalogs(self._config.catalogs, limit)
        names: List[str] = []
        if self._config.include_hive_metastore:
            # UnityCatalogApiProxy.catalogs yields it first when ingestion has
            # a hive_metastore proxy, which it builds for this flag.
            names.append(HIVE_METASTORE)
        try:
            with self._calling("listing catalogs"):
                for catalog in self._proxy.catalogs(metastore=None):
                    if catalog.name not in names:
                        names.append(catalog.name)
                    if len(names) >= limit:
                        break
        except _Degraded:
            pass
        return names[:limit]

    def _pinned_catalogs(self, pinned: List[str], limit: int) -> List[str]:
        # source._get_catalogs: catalogs.get per pinned name, never a listing,
        # and no hive_metastore proxy catalog on this path.
        names: List[str] = []
        for name in pinned:
            try:
                with self._calling(f"reading catalog '{name}'", missing=f"catalog '{name}'"):
                    catalog = self._proxy.catalog(name, metastore=None)
            except _Missing:
                self._warn(
                    f"catalog '{name}' named in `catalogs` was not found; "
                    f"ingestion reads nothing for it"
                )
                continue
            except _Degraded:
                continue
            if catalog is not None and catalog.name not in names:
                names.append(catalog.name)
            if len(names) >= limit:
                break
        return names

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
        if catalog == HIVE_METASTORE:
            self._warn(_HIVE_NOT_PROBED)
            return []
        try:
            catalog_obj = self._catalog(catalog)
            with self._calling(f"listing schemas of catalog '{catalog}'"):
                return [
                    schema.name
                    for schema in itertools.islice(
                        self._proxy.schemas(catalog_obj), limit
                    )
                ]
        except _Degraded:
            return []

    def _table_like(
        self, catalog: str, schema: str, limit: int, keep: Callable[[Table], bool]
    ) -> List[str]:
        if catalog == HIVE_METASTORE:
            self._warn(_HIVE_NOT_PROBED)
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
            with (
                self._calling(f"listing tables of '{catalog}.{schema}'"),
                # Closed explicitly: proxy.tables patches the SDK's TableInfo
                # around its loop, and a generator abandoned at the limit
                # would hold that patch until garbage collection.
                closing(iter(self._proxy.tables(schema_obj))) as listed,
            ):
                matching = (table.name for table in listed if keep(table))
                return list(itertools.islice(matching, limit))
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
        if catalog == HIVE_METASTORE:
            self._warn(_HIVE_NOT_PROBED)
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
        """Notebook paths in the workspace -- the string notebook_pattern is
        matched against. Listed whatever include_notebooks says; `probe filter
        --kind Notebook` reports them excluded while it is off. Paths only,
        never notebook source. Walks the workspace tree, so a large workspace
        is slow; the walk stops at `limit`."""
        try:
            with (
                self._calling("listing workspace notebooks"),
                closing(iter(self._proxy.workspace_notebooks())) as listed,
            ):
                return [
                    notebook.path for notebook in itertools.islice(listed, limit)
                ]
        except _Degraded:
            return []
