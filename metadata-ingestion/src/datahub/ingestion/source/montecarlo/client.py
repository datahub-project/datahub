import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from pydantic import BaseModel, Field

from datahub.ingestion.source.montecarlo.config import MonteCarloSourceConfig

logger = logging.getLogger(__name__)

# GraphQL documents issued against the Monte Carlo Data Collector (MCD) gateway.
# Field selections follow the pycarlo / MCD schema. Connections are Relay-style
# (edges/node/pageInfo) and are walked by ``_paginate``.
_MONITORS_QUERY = """
query getMonitors($monitorTypes: [String], $domainIds: [UUID], $limit: Int, $offset: Int) {
  getMonitors(monitorTypes: $monitorTypes, domainIds: $domainIds, limit: $limit, offset: $offset) {
    uuid
    name
    description
    monitorType
    entityMcons
    resourceId
    severity
    priority
    isPaused
    monitorFields
    dataQualityDimension
    prevExecutionTime
    nextExecutionTime
  }
}
"""

_CUSTOM_RULES_QUERY = """
query getCustomRules($first: Int, $after: String) {
  getCustomRules(first: $first, after: $after) {
    edges {
      node {
        uuid
        ruleType
        description
        customSql
        entityMcons
        severity
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

_ALERTS_QUERY = """
query getAlerts($first: Int, $after: String, $createdTime: TimeFilter) {
  getAlerts(first: $first, after: $after, createdTime: $createdTime) {
    edges {
      node {
        id
        type
        subTypes
        severity
        priority
        status
        createdTime
        monitorUuid
        assetMcons
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

_GET_TABLE_QUERY = """
query getTable($mcon: String) {
  getTable(mcon: $mcon) {
    mcon
    fullTableId
    warehouse {
      connectionType
    }
  }
}
"""


class MonteCarloAssertionDef(BaseModel):
    """A monitor or custom rule, normalized into a single assertion definition."""

    uuid: str
    name: Optional[str] = None
    description: Optional[str] = None
    monitor_type: Optional[str] = None
    rule_type: Optional[str] = None
    custom_sql: Optional[str] = None
    entity_mcons: List[str] = Field(default_factory=list)
    resource_id: Optional[str] = None
    severity: Optional[str] = None
    data_quality_dimension: Optional[str] = None
    is_custom_rule: bool = False

    @property
    def native_type(self) -> str:
        """Monte Carlo's native monitor/rule type, used as the CUSTOM assertion type."""
        return self.monitor_type or self.rule_type or "MONITOR"


class MonteCarloAlert(BaseModel):
    """An alert/incident raised by Monte Carlo, mapped to an assertion failure."""

    uuid: str
    alert_type: Optional[str] = None
    sub_types: List[str] = Field(default_factory=list)
    severity: Optional[str] = None
    priority: Optional[str] = None
    status: Optional[str] = None
    created_time: Optional[datetime] = None
    monitor_uuid: Optional[str] = None
    asset_mcons: List[str] = Field(default_factory=list)


class ResolvedTable(BaseModel):
    """Resolution of an MCON to a concrete warehouse table + connection type."""

    mcon: str
    full_table_id: str
    connection_type: Optional[str] = None


class MonteCarloClient:
    """Thin wrapper over ``pycarlo.core.Client`` handling auth, pagination and parsing.

    Secrets are injected programmatically via ``pycarlo.core.Session`` rather than the
    environment, per the credential-handling rule in AGENTS.md.
    """

    def __init__(self, config: MonteCarloSourceConfig, page_size: int = 100) -> None:
        # Imported lazily so the dependency is only required when the source runs.
        from pycarlo.core import Client, Session

        session_kwargs: Dict[str, Any] = {
            "mcd_id": config.api_id,
            "mcd_token": config.api_token.get_secret_value(),
        }
        if config.api_endpoint:
            session_kwargs["endpoint"] = config.api_endpoint
        self._client = Client(session=Session(**session_kwargs))
        self.config = config
        self.page_size = page_size

    def _call(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = self._client(query, variables=variables)
        except Exception as e:
            raise RuntimeError(
                f"Monte Carlo API call failed (query={query[:60]!r})"
            ) from e
        if response is None:
            raise RuntimeError(f"Monte Carlo API returned None (query={query[:60]!r})")
        # pycarlo returns a Box (dict-like); normalize to a plain dict so the rest of
        # the code (and mocked unit tests) can use ordinary item access.
        if hasattr(response, "to_dict"):
            return response.to_dict()
        return dict(response)

    def _paginate(
        self, query: str, root_field: str, variables: Dict[str, Any]
    ) -> Iterable[Dict[str, Any]]:
        after: Optional[str] = None
        while True:
            page_vars = {**variables, "first": self.page_size, "after": after}
            connection = self._call(query, page_vars).get(root_field) or {}
            for edge in connection.get("edges") or []:
                node = edge.get("node")
                if node:
                    yield node
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            after = page_info.get("endCursor")
            if not after:
                break

    def _paginate_offset(
        self, query: str, root_field: str, variables: Dict[str, Any]
    ) -> Iterable[Dict[str, Any]]:
        # getMonitors returns a plain list rather than a Relay connection, so it is
        # walked with limit/offset (instead of _paginate's cursor) and stops once a
        # short page signals the last batch.
        offset = 0
        while True:
            page_vars = {**variables, "limit": self.page_size, "offset": offset}
            page = self._call(query, page_vars).get(root_field) or []
            yield from page
            if len(page) < self.page_size:
                break
            offset += self.page_size

    def get_monitors(self) -> Iterable[MonteCarloAssertionDef]:
        variables: Dict[str, Any] = {}
        if self.config.monitor_types_allow:
            variables["monitorTypes"] = self.config.monitor_types_allow
        if self.config.domain_ids:
            variables["domainIds"] = self.config.domain_ids
        for raw in self._paginate_offset(_MONITORS_QUERY, "getMonitors", variables):
            uuid = raw.get("uuid")
            if not uuid:
                logger.warning("Skipping monitor with missing uuid: %r", raw)
                continue
            yield MonteCarloAssertionDef(
                uuid=uuid,
                name=raw.get("name"),
                description=raw.get("description"),
                monitor_type=raw.get("monitorType"),
                entity_mcons=raw.get("entityMcons") or [],
                resource_id=raw.get("resourceId"),
                severity=raw.get("severity"),
                data_quality_dimension=raw.get("dataQualityDimension"),
                is_custom_rule=False,
            )

    def get_custom_rules(self) -> Iterable[MonteCarloAssertionDef]:
        for raw in self._paginate(_CUSTOM_RULES_QUERY, "getCustomRules", {}):
            uuid = raw.get("uuid")
            if not uuid:
                logger.warning("Skipping custom rule with missing uuid: %r", raw)
                continue
            yield MonteCarloAssertionDef(
                uuid=uuid,
                description=raw.get("description"),
                rule_type=raw.get("ruleType"),
                custom_sql=raw.get("customSql"),
                entity_mcons=raw.get("entityMcons") or [],
                severity=raw.get("severity"),
                is_custom_rule=True,
            )

    def get_alerts(self) -> Iterable[MonteCarloAlert]:
        start_time = datetime.now(tz=timezone.utc) - timedelta(
            days=self.config.alerts_lookback_days
        )
        variables = {"createdTime": {"gte": start_time.isoformat()}}
        for raw in self._paginate(_ALERTS_QUERY, "getAlerts", variables):
            alert_id = raw.get("id")
            if not alert_id:
                logger.warning("Skipping alert with missing id: %r", raw)
                continue
            yield MonteCarloAlert(
                uuid=alert_id,
                alert_type=raw.get("type"),
                sub_types=raw.get("subTypes") or [],
                severity=raw.get("severity"),
                priority=raw.get("priority"),
                status=raw.get("status"),
                created_time=raw.get("createdTime"),
                monitor_uuid=raw.get("monitorUuid"),
                asset_mcons=raw.get("assetMcons") or [],
            )

    def get_table(self, mcon: str) -> Optional[ResolvedTable]:
        table = self._call(_GET_TABLE_QUERY, {"mcon": mcon}).get("getTable")
        if not table:
            return None
        full_table_id = table.get("fullTableId")
        if not full_table_id:
            logger.warning(
                "getTable response missing fullTableId for mcon=%s; skipping.", mcon
            )
            return None
        warehouse = table.get("warehouse") or {}
        return ResolvedTable(
            mcon=table.get("mcon", mcon),
            full_table_id=full_table_id,
            connection_type=warehouse.get("connectionType"),
        )
