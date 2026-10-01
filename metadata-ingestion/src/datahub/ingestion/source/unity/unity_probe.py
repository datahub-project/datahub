import re
from contextlib import contextmanager
from typing import Any, Iterator, List, Optional

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
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.connection import create_workspace_client
from datahub.ingestion.source.unity.hive_metastore_proxy import HIVE_METASTORE
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.report import UnityCatalogReport

# Databricks error codes are SCREAMING_SNAKE identifiers ("PERMISSION_DENIED").
# Anything else in that slot is not one and is dropped with the rest of the text.
_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}")

_WITHHELD = (
    "the SDK's error text is withheld because it can carry the request log, "
    "its URL and, with debug headers on, its credentials"
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
