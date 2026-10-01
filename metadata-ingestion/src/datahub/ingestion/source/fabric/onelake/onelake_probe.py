"""Metadata-only probe for Fabric OneLake.

Reads through the connector's own OneLakeClient, so a probe call carries the
same auth, retry policy and timeout as ingestion. Commands take display names
(what every *_pattern matches) and accept GUIDs too, because the URNs an agent
reads in DataHub carry GUIDs.
"""

from contextlib import contextmanager
from typing import Callable, Dict, Iterator, List, Optional, Set, Tuple

import requests
from azure.core.exceptions import ClientAuthenticationError

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
    GenericContainerSubTypes,
)
from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.models import FabricWorkspace
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.config import FabricOneLakeSourceConfig
from datahub.ingestion.source.fabric.onelake.filter_names import (
    effective_schema_name,
)
from datahub.ingestion.source.fabric.onelake.models import (
    FabricItem,
    FabricTable,
    FabricView,
)
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeClientReport
from datahub.ingestion.source.fabric.onelake.schema_client import (
    SchemaExtractionClient,
    SqlAnalyticsEndpointClient,
    create_schema_extraction_client,
)

SchemaClientFactory = Callable[[FabricWorkspace, FabricItem], SchemaExtractionClient]
_ITEM_TYPES = ("Lakehouse", "Warehouse")


class FabricReadError(Exception):
    """A Fabric read failed; the message is already scrubbed.

    Not a ValueError: the caller's arguments were fine, so this must not map to
    "fix your input". run_probe_method sees the matching entry in `failures`
    and reports a read failure.
    """


def _scrubbed(operation: str, exc: BaseException) -> str:
    # Status and operation only. requests' HTTPError text carries the request
    # URL, the client logs the response body next to it, and Azure SDK
    # credential errors can quote tenant/client details -- none of which a
    # caller needs to act on "HTTP 403 while listing workspaces".
    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code if exc.response is not None else None
        return f"{operation} failed: HTTP {status}"
    if isinstance(exc, ClientAuthenticationError):
        return (
            f"{operation} failed: could not get a token with the recipe's "
            f"credential ({type(exc).__name__})"
        )
    if isinstance(exc, ImportError):
        # pyodbc is imported when the first SQL connection opens, and fails
        # here when the unixODBC / Microsoft ODBC driver is not installed.
        return (
            f"{operation} failed: pyodbc or its ODBC driver could not be "
            f"loaded where the probe runs (ImportError)"
        )
    return f"{operation} failed ({type(exc).__name__})"


class FabricOneLakeMetadataProbe:
    def __init__(
        self,
        client: OneLakeClient,
        config: FabricOneLakeSourceConfig,
        schema_client_factory: Optional[SchemaClientFactory] = None,
    ) -> None:
        self._client = client
        self._config = config
        self._schema_client_factory = schema_client_factory
        self._schema_clients: Dict[Tuple[str, str], SchemaExtractionClient] = {}
        self.warnings: List[str] = []
        self.failures: List[str] = []

    @classmethod
    def for_config(
        cls, config: FabricOneLakeSourceConfig
    ) -> "FabricOneLakeMetadataProbe":
        # Same construction as FabricOneLakeSource.__init__. It opens nothing:
        # BaseFabricClient only builds a requests.Session, and FabricAuthHelper
        # acquires the credential on first use.
        return cls(
            OneLakeClient(
                FabricAuthHelper(config.credential),
                timeout=config.api_timeout,
                report=FabricOneLakeClientReport(),
            ),
            config,
        )

    def __enter__(self) -> "FabricOneLakeMetadataProbe":
        return self

    def __exit__(self, *exc: object) -> None:
        for schema_client in self._schema_clients.values():
            # close() is on SqlAnalyticsEndpointClient, not on the
            # SchemaExtractionClient protocol it is handed out as.
            close = getattr(schema_client, "close", None)
            if callable(close):
                close()
        self._client.close()

    def _warn(self, message: str) -> None:
        if message not in self.warnings:
            self.warnings.append(message)

    @contextmanager
    def _reading(self, operation: str) -> Iterator[None]:
        """Record a failed Fabric read and re-raise it scrubbed.

        Wraps only calls into the client, never this module's own argument
        checks, so a ValueError raised here is the source's (a malformed JSON
        body is a ValueError too), not the caller's.
        """
        try:
            yield
        except Exception as exc:
            message = _scrubbed(operation, exc)
            self.failures.append(message)
            # `from None`: the original's text is exactly what is being kept
            # out of the output, and a printed chain would put it back.
            raise FabricReadError(message) from None

    @probe_method(
        kind=GenericContainerSubTypes.FABRIC_WORKSPACE, row_limit_param="limit"
    )
    def workspaces(self, limit: int = 200) -> List[Dict[str, str]]:
        """Workspaces this credential can see, including ones workspace_pattern
        would exclude -- a denied workspace is reported, not hidden. `name` is
        what workspace_pattern matches; `id` is the GUID in emitted URNs."""
        out: List[Dict[str, str]] = []
        with self._reading("listing workspaces"):
            for ws in self._client.list_workspaces():
                out.append({"name": ws.name, "id": ws.id})
                if len(out) >= limit:
                    break
        return out

    @probe_method(
        kind=DatasetContainerSubTypes.FABRIC_LAKEHOUSE,
        row_limit_param="limit",
        parent_params=("workspace",),
    )
    def lakehouses(self, workspace: str, limit: int = 200) -> List[Dict[str, str]]:
        """Lakehouses in one workspace (display name or GUID), judged by
        lakehouse_pattern on `name`; `id` is the GUID in emitted URNs. Warns
        when extract_lakehouses is off, since ingestion then emits none."""
        if not self._config.extract_lakehouses:
            self._warn(
                "extract_lakehouses is false: ingestion emits no lakehouses or "
                "their tables"
            )
        ws = self._workspace(workspace)
        with self._reading(f"listing lakehouses in workspace '{ws.name}'"):
            items = list(self._client.list_lakehouses(ws.id))
        return [{"name": i.name, "id": i.id} for i in items][:limit]

    @probe_method(
        kind=DatasetContainerSubTypes.FABRIC_WAREHOUSE,
        row_limit_param="limit",
        parent_params=("workspace",),
    )
    def warehouses(self, workspace: str, limit: int = 200) -> List[Dict[str, str]]:
        """Warehouses in one workspace (display name or GUID), judged by
        warehouse_pattern on `name`; `id` is the GUID in emitted URNs. Warns
        when extract_warehouses is off, since ingestion then emits none."""
        if not self._config.extract_warehouses:
            self._warn(
                "extract_warehouses is false: ingestion emits no warehouses or "
                "their tables"
            )
        ws = self._workspace(workspace)
        with self._reading(f"listing warehouses in workspace '{ws.name}'"):
            items = list(self._client.list_warehouses(ws.id))
        return [{"name": i.name, "id": i.id} for i in items][:limit]

    @probe_method(
        kind=DatasetContainerSubTypes.FABRIC_SCHEMA,
        row_limit_param="limit",
        parent_params=("workspace", "item"),
    )
    def schemas(
        self,
        workspace: str,
        item: str,
        item_type: Optional[str] = None,
        limit: int = 200,
    ) -> List[str]:
        """Schemas in one lakehouse or warehouse (display name or GUID), as
        ingestion forms them: from the schemas of its tables, and of its views
        when extract_views is on, with schema-less lakehouse tables under dbo.
        Judged by schema_pattern. item_type (Lakehouse or Warehouse) is needed
        only when a lakehouse and a warehouse share the name."""
        if not self._config.extract_schemas:
            self._warn(
                "extract_schemas is false: ingestion emits no schema containers "
                "(tables sit directly under their item), though schema_pattern "
                "still filters the tables"
            )
        ws = self._workspace(workspace)
        fabric_item = self._item(ws, item, item_type)
        names = {
            effective_schema_name(t.schema_name)
            for t in self._item_tables(ws, fabric_item)
        }
        if self._config.extract_views:
            names |= self._view_schemas(ws, fabric_item)
        return sorted(names)[:limit]

    @probe_method(
        kind=DatasetSubTypes.TABLE,
        row_limit_param="limit",
        parent_params=("workspace", "item", "schema"),
    )
    def tables(
        self,
        workspace: str,
        item: str,
        schema: str,
        item_type: Optional[str] = None,
        limit: int = 200,
    ) -> List[str]:
        """Tables in one schema of a lakehouse or warehouse, including ones
        table_pattern would exclude. Tables of a schemas-disabled lakehouse are
        under schema 'dbo'. table_pattern matches '<schema>.<table>', and the
        result's parent_path lets `probe filter` build that. Empty with a
        warning means the listing could not be read, not that there are no
        tables."""
        ws = self._workspace(workspace)
        fabric_item = self._item(ws, item, item_type)
        return [
            t.name
            for t in self._item_tables(ws, fabric_item)
            if effective_schema_name(t.schema_name) == schema
        ][:limit]

    def _item_tables(self, ws: FabricWorkspace, item: FabricItem) -> List[FabricTable]:
        with self._reading(f"listing tables of {item.type} '{item.name}'"):
            if item.type == "Lakehouse":
                return list(
                    self._client.list_lakehouse_tables(
                        ws.id, item.id, on_degraded=self._warn
                    )
                )
            return list(
                self._client.list_warehouse_tables(
                    ws.id, item.id, on_degraded=self._warn
                )
            )

    def _view_schemas(self, ws: FabricWorkspace, item: FabricItem) -> Set[str]:
        # Degrades rather than fails: `schemas` has already answered from the
        # tables, and a views-only gap is partial, not absent. So no entry in
        # `failures`, which would turn the whole answer into exit 3.
        operation = f"reading the views of {item.type} '{item.name}'"
        try:
            views = self._open_schema_client(ws, item).get_all_views(
                workspace_id=ws.id, item_id=item.id
            )
        except Exception as exc:
            self._warn(
                f"{_scrubbed(operation, exc)}; schemas that hold only views are "
                f"missing from this list"
            )
            return set()
        return {effective_schema_name(v.schema_name) for v in views}

    def _require_sql_endpoint(self) -> None:
        sql_endpoint = self._config.sql_endpoint
        if sql_endpoint is None or not sql_endpoint.enabled:
            # The recipe's choice, so the caller's to fix (exit 2).
            raise ValueError(
                "this recipe has no enabled sql_endpoint, so ingestion reads no "
                "views or columns; set sql_endpoint.enabled: true"
            )

    def _open_schema_client(
        self, ws: FabricWorkspace, item: FabricItem
    ) -> SchemaExtractionClient:
        key = (ws.id, item.id)
        cached = self._schema_clients.get(key)
        if cached is not None:
            return cached
        factory = self._schema_client_factory or self._default_schema_client
        client = factory(ws, item)
        self._schema_clients[key] = client
        return client

    def _schema_client(
        self, ws: FabricWorkspace, item: FabricItem
    ) -> SchemaExtractionClient:
        self._require_sql_endpoint()
        try:
            return self._open_schema_client(ws, item)
        except ValueError:
            # create_schema_extraction_client raises ValueError for exactly one
            # thing: get_sql_analytics_endpoint_url found no endpoint (it turns
            # every error, a 403 included, into None). Alone that would read as
            # a bad argument at exit 2; it is a read failure.
            message = (
                f"SQL Analytics Endpoint for {item.type} '{item.name}' could not "
                f"be opened: it is not provisioned, or this credential cannot "
                f"read the item; ingestion skips its columns, views and usage"
            )
        except Exception as exc:
            message = _scrubbed(
                f"opening the SQL Analytics Endpoint for {item.type} '{item.name}'",
                exc,
            )
        self.failures.append(message)
        raise FabricReadError(message) from None

    def _default_schema_client(
        self, ws: FabricWorkspace, item: FabricItem
    ) -> SchemaExtractionClient:
        sql_endpoint = self._config.sql_endpoint
        if sql_endpoint is None:
            raise ValueError("sql_endpoint is not configured")
        # The factory ingestion's _create_schema_client calls, so the probe
        # connects to the same endpoint, database and driver settings.
        return create_schema_extraction_client(
            method=self._config.extract_schema.method,
            auth_helper=self._client.auth_helper,
            config=sql_endpoint,
            report=None,
            workspace_id=ws.id,
            item_id=item.id,
            item_type=item.type,
            base_client=self._client,
            item_display_name=item.name,
        )

    @probe_method(
        kind=DatasetSubTypes.VIEW,
        row_limit_param="limit",
        parent_params=("workspace", "item", "schema"),
    )
    def views(
        self,
        workspace: str,
        item: str,
        schema: str,
        item_type: Optional[str] = None,
        limit: int = 200,
    ) -> List[str]:
        """Views in one schema of a lakehouse or warehouse, from
        INFORMATION_SCHEMA.VIEWS on the item's SQL Analytics Endpoint -- the
        same query ingestion runs. Includes views view_pattern would exclude;
        view_pattern matches '<schema>.<view>'. Needs sql_endpoint.enabled and
        the ODBC driver. Warns when extract_views is off."""
        if not self._config.extract_views:
            self._warn("extract_views is false: ingestion emits no views")
        ws = self._workspace(workspace)
        fabric_item = self._item(ws, item, item_type)
        views = self._item_views(ws, fabric_item)
        return [
            v.name for v in views if effective_schema_name(v.schema_name) == schema
        ][:limit]

    def _item_views(self, ws: FabricWorkspace, item: FabricItem) -> List[FabricView]:
        schema_client = self._schema_client(ws, item)
        with self._reading(
            f"reading INFORMATION_SCHEMA.VIEWS of {item.type} '{item.name}'"
        ):
            return schema_client.get_all_views(workspace_id=ws.id, item_id=item.id)

    @probe_method()
    def columns(
        self,
        workspace: str,
        item: str,
        schema: str,
        table: str,
        item_type: Optional[str] = None,
    ) -> List[Dict[str, object]]:
        """Columns of one table or view: name, data type and nullability, from
        INFORMATION_SCHEMA.COLUMNS on the item's SQL Analytics Endpoint.
        Structural metadata only; no values are read. Schema-less lakehouse
        tables are under 'dbo'."""
        if not self._config.extract_schema.enabled:
            self._warn(
                "extract_schema.enabled is false: ingestion emits no column metadata"
            )
        ws = self._workspace(workspace)
        fabric_item = self._item(ws, item, item_type)
        schema_client = self._schema_client(ws, fabric_item)
        with self._reading(
            f"reading INFORMATION_SCHEMA.COLUMNS of {fabric_item.type} "
            f"'{fabric_item.name}'"
        ):
            by_table = schema_client.get_all_table_columns(
                workspace_id=ws.id, item_id=fabric_item.id
            )
        found = by_table.get((schema, table))
        if found is None:
            # Every table and view has columns, so no entry means no object.
            raise ValueError(
                f"no table or view '{schema}.{table}' in the SQL Analytics "
                f"Endpoint of {fabric_item.type} '{fabric_item.name}' (a newly "
                f"created lakehouse table can take a while to appear there)"
            )
        return [
            {"name": c.name, "type": c.data_type, "nullable": c.is_nullable}
            for c in found
        ]

    @probe_method()
    def view_definition(
        self,
        workspace: str,
        item: str,
        schema: str,
        view: str,
        item_type: Optional[str] = None,
    ) -> Optional[str]:
        """The stored CREATE VIEW text (DDL, not query results) that ingestion
        parses for view lineage. Null, with a warning, when the credential
        lacks VIEW DEFINITION permission on it."""
        ws = self._workspace(workspace)
        fabric_item = self._item(ws, item, item_type)
        for v in self._item_views(ws, fabric_item):
            if effective_schema_name(v.schema_name) == schema and v.name == view:
                if v.view_definition is None:
                    self._warn(
                        f"the definition of '{schema}.{view}' is not readable "
                        f"(VIEW DEFINITION permission); ingestion emits the view "
                        f"without lineage"
                    )
                return v.view_definition
        raise ValueError(
            f"no view '{schema}.{view}' in {fabric_item.type} '{fabric_item.name}'"
        )

    @probe_method()
    def sql_endpoint(
        self, workspace: str, item: str, item_type: Optional[str] = None
    ) -> Dict[str, object]:
        """The SQL Analytics Endpoint host ingestion would connect to for one
        lakehouse or warehouse, or null. Null means ingestion skips columns,
        views and usage for it: the endpoint is not provisioned, or the
        credential cannot read the item (the connector cannot tell which).
        Makes REST calls only; opens no SQL connection."""
        sql_endpoint = self._config.sql_endpoint
        if sql_endpoint is None or not sql_endpoint.enabled:
            self._warn(
                "sql_endpoint is not enabled in this recipe, so ingestion "
                "connects to no SQL Analytics Endpoint"
            )
        ws = self._workspace(workspace)
        fabric_item = self._item(ws, item, item_type)
        # Swallows every error into None itself, so there is nothing for
        # _reading to scrub; the warning below names both possible causes.
        host = SqlAnalyticsEndpointClient.get_sql_analytics_endpoint_url(
            self._client, ws.id, fabric_item.id, fabric_item.type
        )
        if host is None:
            self._warn(
                f"no SQL Analytics Endpoint resolvable for {fabric_item.type} "
                f"'{fabric_item.name}': it is not provisioned, or this credential "
                f"cannot read the item"
            )
        return {"item": fabric_item.name, "item_type": fabric_item.type, "host": host}

    def _workspace(self, workspace: str) -> FabricWorkspace:
        with self._reading("listing workspaces"):
            matches = [
                ws
                for ws in self._client.list_workspaces()
                if workspace in (ws.name, ws.id)
            ]
        if not matches:
            raise ValueError(
                f"no workspace named or with id '{workspace}' is visible to this "
                f"credential"
            )
        if len(matches) > 1:
            raise ValueError(
                f"'{workspace}' matches {len(matches)} workspaces; pass the "
                f"workspace GUID instead"
            )
        return matches[0]

    def _item(
        self, ws: FabricWorkspace, item: str, item_type: Optional[str]
    ) -> FabricItem:
        if item_type is not None and item_type not in _ITEM_TYPES:
            raise ValueError(
                f"item_type must be one of {', '.join(_ITEM_TYPES)}, got '{item_type}'"
            )
        matches: List[FabricItem] = []
        if item_type in (None, "Lakehouse"):
            with self._reading(f"listing lakehouses in workspace '{ws.name}'"):
                matches += [
                    i
                    for i in self._client.list_lakehouses(ws.id)
                    if item in (i.name, i.id)
                ]
        if item_type in (None, "Warehouse"):
            with self._reading(f"listing warehouses in workspace '{ws.name}'"):
                matches += [
                    i
                    for i in self._client.list_warehouses(ws.id)
                    if item in (i.name, i.id)
                ]
        if not matches:
            raise ValueError(
                f"no lakehouse or warehouse '{item}' in workspace '{ws.name}'"
            )
        if len(matches) > 1:
            raise ValueError(
                f"'{item}' names more than one item "
                f"({', '.join(m.type for m in matches)}) in workspace "
                f"'{ws.name}'; pass item_type=Lakehouse or "
                f"item_type=Warehouse, or the item's GUID"
            )
        return matches[0]
