"""Metadata-only probe for Fabric OneLake.

Reads through the connector's own OneLakeClient, so a probe call carries the
same auth, retry policy and timeout as ingestion. Commands take display names
(what every *_pattern matches) and accept GUIDs too, because the URNs an agent
reads in DataHub carry GUIDs.
"""

from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

import requests
from azure.core.exceptions import ClientAuthenticationError

from datahub.ingestion.agent.error_policy import DEFECT_TYPES
from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import (
    ProbeProviderBase,
    resolve_name,
    take,
)
from datahub.ingestion.agent.verdicts import ProbeArgumentError, ProbeReadFailed
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
    FabricItemType,
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
_ITEM_TYPES: Tuple[FabricItemType, ...] = ("Lakehouse", "Warehouse")


class FabricReadError(ProbeReadFailed):
    """A Fabric read failed (exit 3), in a message the probe wrote.

    A trusted type, so its text is shown: that text is built by `_scrubbed`
    from the operation, the HTTP status and the exception's class only, and
    must stay that way. Not a ValueError: the caller's arguments were fine.
    Raised, never also recorded in `failures`, which would print it twice.
    Plain listings do not use it: their failures reach the framework, which
    names them by class and HTTP status.
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


def _close_if_closable(client: SchemaExtractionClient) -> None:
    # close() is on SqlAnalyticsEndpointClient, not on the
    # SchemaExtractionClient protocol it is handed out as.
    close = getattr(client, "close", None)
    if callable(close):
        close()


class FabricOneLakeMetadataProbe(ProbeProviderBase):
    # BaseFabricClient logs an HTTP error's response body at ERROR; an error
    # body can echo request details, so it stays off the probe's output.
    silenced_loggers = ("datahub.ingestion.source.fabric.common.base_client",)

    def __init__(
        self,
        client: OneLakeClient,
        config: FabricOneLakeSourceConfig,
        schema_client_factory: Optional[SchemaClientFactory] = None,
    ) -> None:
        self._client = client
        self._config = config
        self._schema_client_factory = schema_client_factory
        self.failures: List[str] = []
        # Registered first, so it closes last and still closes when a SQL
        # client's close raises.
        self._on_exit(client.close)

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

    @probe_method(
        kind=GenericContainerSubTypes.FABRIC_WORKSPACE, row_limit_param="limit"
    )
    def workspaces(self, limit: int = 200) -> List[Dict[str, str]]:
        """Workspaces this credential can see, including ones workspace_pattern
        would exclude -- a denied workspace is reported, not hidden. `name` is
        what workspace_pattern matches; `id` is the GUID in emitted URNs."""
        return [
            {"name": ws.name, "id": ws.id}
            for ws in take(self._client.list_workspaces(), limit)
        ]

    @probe_method(
        kind=DatasetContainerSubTypes.FABRIC_LAKEHOUSE,
        row_limit_param="limit",
        parent_params=("workspace",),
    )
    def lakehouses(self, workspace: str, limit: int = 200) -> List[Dict[str, str]]:
        """Lakehouses in one workspace (display name or GUID), judged by
        lakehouse_pattern on `name`; `id` is the GUID in emitted URNs. Warns
        when extract_lakehouses is off, since ingestion then emits none."""
        return self._items(
            workspace, "Lakehouse", self._config.extract_lakehouses, limit
        )

    @probe_method(
        kind=DatasetContainerSubTypes.FABRIC_WAREHOUSE,
        row_limit_param="limit",
        parent_params=("workspace",),
    )
    def warehouses(self, workspace: str, limit: int = 200) -> List[Dict[str, str]]:
        """Warehouses in one workspace (display name or GUID), judged by
        warehouse_pattern on `name`; `id` is the GUID in emitted URNs. Warns
        when extract_warehouses is off, since ingestion then emits none."""
        return self._items(
            workspace, "Warehouse", self._config.extract_warehouses, limit
        )

    def _items(
        self, workspace: str, item_type: FabricItemType, extracted: bool, limit: int
    ) -> List[Dict[str, str]]:
        if not extracted:
            self._warn(
                f"extract_{item_type.lower()}s is false: ingestion emits no "
                f"{item_type.lower()}s or their tables"
            )
        ws = self._workspace(workspace)
        return [
            {"name": i.name, "id": i.id}
            for i in take(self._listers()[item_type](ws.id), limit)
        ]

    def _listers(self) -> Dict[FabricItemType, Callable[[str], Iterable[FabricItem]]]:
        return {
            "Lakehouse": self._client.list_lakehouses,
            "Warehouse": self._client.list_warehouses,
        }

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
        result's parent_path lets `probe filter` build that. A lakehouse table
        listing that is refused fails the command (exit 3), with whatever was
        listed before the refusal."""
        ws = self._workspace(workspace)
        fabric_item = self._item(ws, item, item_type)
        return take(
            (
                t.name
                for t in self._item_tables(ws, fabric_item)
                if effective_schema_name(t.schema_name) == schema
            ),
            limit,
        )

    def _item_tables(self, ws: FabricWorkspace, item: FabricItem) -> List[FabricTable]:
        if item.type == "Lakehouse":
            # A refused lakehouse table listing is this command's answer gone
            # missing, not an empty lakehouse: a failure, so the command exits
            # 3 with what was listed before it.
            return list(
                self._client.list_lakehouse_tables(
                    ws.id, item.id, on_degraded=self.failures.append
                )
            )
        # A warehouse 404 does mean ingestion sees no tables there: a warning.
        return list(
            self._client.list_warehouse_tables(ws.id, item.id, on_degraded=self._warn)
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
        except DEFECT_TYPES:
            raise
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
            raise ProbeArgumentError(
                "this recipe has no enabled sql_endpoint, so ingestion reads no "
                "views or columns; set sql_endpoint.enabled: true"
            )

    def _open_schema_client(
        self, ws: FabricWorkspace, item: FabricItem
    ) -> SchemaExtractionClient:
        factory = self._schema_client_factory or self._default_schema_client
        return self._open_once(
            ("schema-client", ws.id, item.id),
            lambda: factory(ws, item),
            close=_close_if_closable,
        )

    def _schema_client(
        self, ws: FabricWorkspace, item: FabricItem
    ) -> SchemaExtractionClient:
        self._require_sql_endpoint()
        # The lookup that raises: a failed read of the item reaches the
        # framework as what it is, instead of the factory's "no endpoint",
        # which turns every error, a 403 included, into None.
        if self._endpoint_host(ws, item) is None:
            raise FabricReadError(
                f"{item.type} '{item.name}' has no SQL Analytics Endpoint (not "
                f"provisioned); ingestion skips its columns, views and usage"
            )
        try:
            return self._open_schema_client(ws, item)
        except DEFECT_TYPES:
            raise
        except Exception as exc:
            raise FabricReadError(
                _scrubbed(
                    f"opening the SQL Analytics Endpoint for {item.type} '{item.name}'",
                    exc,
                )
            ) from None

    def _endpoint_host(self, ws: FabricWorkspace, item: FabricItem) -> Optional[str]:
        return self._open_once(
            ("endpoint-host", ws.id, item.id),
            lambda: SqlAnalyticsEndpointClient.fetch_sql_analytics_endpoint_url(
                self._client, ws.id, item.id, item.type
            ),
        )

    def _default_schema_client(
        self, ws: FabricWorkspace, item: FabricItem
    ) -> SchemaExtractionClient:
        sql_endpoint = self._config.sql_endpoint
        if sql_endpoint is None:
            raise ProbeArgumentError("sql_endpoint is not configured")
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
        return self._schema_client(ws, item).get_all_views(
            workspace_id=ws.id, item_id=item.id
        )

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
        ws = self._workspace(workspace, in_parent_path=False)
        fabric_item = self._item(ws, item, item_type, in_parent_path=False)
        by_table = self._schema_client(ws, fabric_item).get_all_table_columns(
            workspace_id=ws.id, item_id=fabric_item.id
        )
        found = by_table.get((schema, table))
        if found is None:
            # Every table and view has columns, so no entry means no object.
            raise ProbeArgumentError(
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
        ws = self._workspace(workspace, in_parent_path=False)
        fabric_item = self._item(ws, item, item_type, in_parent_path=False)
        for v in self._item_views(ws, fabric_item):
            if effective_schema_name(v.schema_name) == schema and v.name == view:
                if v.view_definition is None:
                    self._warn(
                        f"the definition of '{schema}.{view}' is not readable "
                        f"(VIEW DEFINITION permission); ingestion emits the view "
                        f"without lineage"
                    )
                return v.view_definition
        raise ProbeArgumentError(
            f"no view '{schema}.{view}' in {fabric_item.type} '{fabric_item.name}'"
        )

    @probe_method()
    def sql_endpoint(
        self, workspace: str, item: str, item_type: Optional[str] = None
    ) -> Dict[str, object]:
        """The SQL Analytics Endpoint host ingestion would connect to for one
        lakehouse or warehouse, or null when the item has none (not
        provisioned), in which case ingestion skips its columns, views and
        usage. A failed read of the item is a read failure, not null. Makes
        REST calls only; opens no SQL connection."""
        sql_endpoint = self._config.sql_endpoint
        if sql_endpoint is None or not sql_endpoint.enabled:
            self._warn(
                "sql_endpoint is not enabled in this recipe, so ingestion "
                "connects to no SQL Analytics Endpoint"
            )
        ws = self._workspace(workspace, in_parent_path=False)
        fabric_item = self._item(ws, item, item_type, in_parent_path=False)
        # The lookup that raises: ingestion turns a failed read into None too,
        # but "no endpoint" for "could not look" is what a probe must not say.
        host = self._endpoint_host(ws, fabric_item)
        if host is None:
            self._warn(
                f"{fabric_item.type} '{fabric_item.name}' has no SQL Analytics "
                f"Endpoint (not provisioned, or not yet provisioned); ingestion "
                f"skips its columns, views and usage"
            )
        return {"item": fabric_item.name, "item_type": fabric_item.type, "host": host}

    def _warn_if_resolved_by_id(
        self, arg: str, param: str, label: str, name: str, obj_id: str
    ) -> None:
        # run_probe_method builds the listing's parent_path from the raw
        # arguments, so a GUID given here travels into `probe filter
        # --from-run`, which matches the patterns against display names. The
        # provider cannot rewrite that path, so it says so instead.
        if arg == obj_id and arg != name:
            self._warn(
                f"'{arg}' was resolved by id to {label} '{name}'; this "
                f"listing's parent_path carries the id, and probe filter "
                f"matches display names -- re-run with --{param} '{name}' "
                f"before using it with probe filter --from-run"
            )

    def _workspace(
        self, workspace: str, *, in_parent_path: bool = True
    ) -> FabricWorkspace:
        listed = list(self._client.list_workspaces())
        resolved = resolve_name(
            workspace,
            listed,
            key=lambda ws: ws.name,
            id_key=lambda ws: ws.id,
            kind="workspace",
            where="visible to this credential",
            list_command="probe run workspaces",
            on_ambiguous="pass the workspace GUID instead",
        )
        if in_parent_path and resolved.by_id:
            self._warn_if_resolved_by_id(
                workspace, "workspace", "workspace", resolved.name, resolved.record.id
            )
        return resolved.record

    def _item(
        self,
        ws: FabricWorkspace,
        item: str,
        item_type: Optional[str],
        *,
        in_parent_path: bool = True,
    ) -> FabricItem:
        if item_type is not None and item_type not in _ITEM_TYPES:
            raise ProbeArgumentError(
                f"--item-type must be one of {', '.join(_ITEM_TYPES)}, got "
                f"'{item_type}'"
            )
        listers = self._listers()
        listed: List[FabricItem] = []
        # Each type is listed on its own: a 403 on lakehouses must not hide a
        # warehouse the caller can read. A failure only fails the call when
        # nothing was found, since then the item may be in the unread listing.
        failed: List[str] = []
        types: Tuple[FabricItemType, ...] = (
            tuple(t for t in _ITEM_TYPES if t == item_type)
            if item_type
            else _ITEM_TYPES
        )
        for type_name in types:
            operation = f"listing {type_name.lower()}s in workspace '{ws.name}'"
            try:
                listed += list(listers[type_name](ws.id))
            except DEFECT_TYPES:
                raise
            except Exception as exc:
                failed.append(_scrubbed(operation, exc))

        def unread() -> None:
            # The item may be in the listing that could not be read.
            if failed:
                raise FabricReadError("; ".join(failed))

        resolved = resolve_name(
            item,
            listed,
            key=lambda i: i.name,
            id_key=lambda i: i.id,
            distinguish=lambda i: i.type,
            kind="lakehouse or warehouse",
            where=f"in workspace '{ws.name}'",
            list_command="probe run lakehouses / probe run warehouses",
            on_ambiguous=(
                "pass --item-type Lakehouse or --item-type Warehouse, or the "
                "item's GUID"
            ),
            on_miss=unread,
        )
        for message in failed:
            self._warn(
                f"{message}; '{item}' was resolved to {resolved.record.type} "
                f"'{resolved.name}', and an item of the other type with the "
                f"same name could not be ruled out"
            )
        if in_parent_path and resolved.by_id:
            self._warn_if_resolved_by_id(
                item, "item", resolved.record.type, resolved.name, resolved.record.id
            )
        return resolved.record
