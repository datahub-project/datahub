"""Metadata-only probe for Fabric OneLake.

Reads through the connector's own OneLakeClient, so a probe call carries the
same auth, retry policy and timeout as ingestion. Commands take display names
(what every *_pattern matches) and accept GUIDs too, because the URNs an agent
reads in DataHub carry GUIDs.
"""

from contextlib import contextmanager
from typing import Callable, Dict, Iterator, List, Optional, Set

import requests

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
from datahub.ingestion.source.fabric.onelake.models import FabricItem, FabricTable
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeClientReport
from datahub.ingestion.source.fabric.onelake.schema_client import (
    SchemaExtractionClient,
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
        self._schema_clients: List[SchemaExtractionClient] = []
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
        for schema_client in self._schema_clients:
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
        # Replaced once the SQL Analytics Endpoint commands land.
        return set()

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
                f"'{item}' names more than one item ({', '.join(m.type for m in matches)}) "
                f"in workspace '{ws.name}'; pass item_type=Lakehouse or "
                f"item_type=Warehouse, or the item's GUID"
            )
        return matches[0]
