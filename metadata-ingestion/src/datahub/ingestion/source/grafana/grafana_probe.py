from typing import Dict, Iterator, List, Mapping, Optional, Union

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import (
    ProbeProviderBase,
    soft_listing,
    take,
)
from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.ingestion.source.grafana.grafana_api import GrafanaAPIClient
from datahub.ingestion.source.grafana.grafana_config import GrafanaSourceConfig
from datahub.ingestion.source.grafana.report import GrafanaSourceReport

# Ingestion's session sets no timeout. A probe answers a person waiting on
# it, so an unresponsive server fails the command instead of hanging it.
_TIMEOUT_SECONDS = 30

_Params = Mapping[str, Union[str, int]]


class GrafanaMetadataProbe(ProbeProviderBase):
    """Metadata-only probe over Grafana's HTTP API.

    Requests go through GrafanaAPIClient's session, so the bearer token and
    verify_ssl are applied exactly as ingestion applies them. The listings
    are paged here rather than through get_folders/get_dashboards: those
    record a failure and return what they have, where a probe must tell a 403
    (a warning, empty result) from a 401 or 5xx (the command fails), and
    get_dashboards fetches every dashboard's full JSON, panels included, to
    read a title the search result already carries.

    No `api` passthrough: dashboard JSON carries panel queries (row values
    in WHERE literals), and the user, team and annotation endpoints are
    personal data.
    """

    def __init__(self, config: GrafanaSourceConfig) -> None:
        self._config = config
        self._report = GrafanaSourceReport()
        self.failures: List[str] = []

    @classmethod
    def for_config(cls, config: GrafanaSourceConfig) -> "GrafanaMetadataProbe":
        return cls(config)

    @property
    def probe_report(self) -> object:
        """The report GrafanaAPIClient writes into: its "SSL Configuration
        Warning" (verify_ssl off) reaches the caller as it reaches a run."""
        return self._report

    def _client(self) -> GrafanaAPIClient:
        return self._open_once(
            "client",
            lambda: GrafanaAPIClient(
                base_url=self._config.url,
                token=self._config.service_account_token,
                verify_ssl=self._config.verify_ssl,
                page_size=self._config.page_size,
                report=self._report,
            ),
            close=lambda client: client.session.close(),
        )

    def _get_list(self, path: str, params: Optional[_Params]) -> Optional[List[object]]:
        """One GET returning a JSON array, or None after recording a failure
        when the server sent something else. An HTTP error raises, for the
        caller's soft_listing to judge."""
        response = self._client().session.get(
            f"{self._config.url}{path}", params=params, timeout=_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, list):
            self.failures.append(
                f"{path} returned a {type(body).__name__}, not a list; the "
                f"listing stops there"
            )
            return None
        return body

    def _paged(self, path: str, params: _Params) -> Iterator[Dict[str, object]]:
        """Every record of a page/limit endpoint, paged as GrafanaAPIClient
        pages it: until a page comes back empty."""
        page = 1
        while True:
            batch = self._get_list(
                path, {**params, "page": page, "limit": self._config.page_size}
            )
            if not batch:
                return
            for item in batch:
                if isinstance(item, dict):
                    yield item
            page += 1

    @probe_method(kind=BIContainerSubTypes.GRAFANA_FOLDER, row_limit_param="limit")
    def folders(self, limit: int = 200) -> List[Dict[str, object]]:
        """Top-level folders, by title (what folder_pattern is matched on),
        with each one's uid. Includes folders the recipe would exclude: judge
        them with `probe filter --kind Folder`. Nested folders are not listed,
        because ingestion reads /api/folders without a parent and never sees
        them either. In basic_mode ingestion emits no folders at all, and
        `probe filter` says so. A 403 or 404 degrades to [] with a warning."""
        if self._config.basic_mode:
            self._warn(
                "basic_mode is set, so ingestion emits no folders; they are "
                "listed for reference and judged excluded by basic_mode"
            )
        with soft_listing(self._warn, 403, 404, context="folders listing"):
            return take(
                (
                    {"name": str(item.get("title") or ""), "uid": item.get("uid")}
                    for item in self._paged("/api/folders", {})
                ),
                limit,
            )
        return []

    @probe_method(kind=BIContainerSubTypes.GRAFANA_DASHBOARD, row_limit_param="limit")
    def dashboards(self, limit: int = 200) -> List[Dict[str, object]]:
        """Dashboards, by title (what dashboard_pattern is matched on), with
        each one's uid (the id in its DataHub URN) and the title of its
        folder. Includes dashboards the recipe would exclude: judge them with
        `probe filter --kind Dashboard`. A dashboard's folder does not decide
        whether it is ingested; only dashboard_pattern does.

        In basic_mode this makes the request ingestion makes there: one
        unpaged /api/search with no type filter, whose results include
        folders (`type` dash-folder). Ingestion emits those as dashboards too,
        so they are listed. A 403 or 404 degrades to [] with a warning."""
        with soft_listing(self._warn, 403, 404, context="dashboards listing"):
            if self._config.basic_mode:
                items: Iterator[Dict[str, object]] = iter(
                    [
                        item
                        for item in self._get_list("/api/search", None) or []
                        if isinstance(item, dict)
                    ]
                )
            else:
                items = self._paged("/api/search", {"type": "dash-db"})
            return take((_dashboard_record(item) for item in items), limit)
        return []


def _dashboard_record(item: Mapping[str, object]) -> Dict[str, object]:
    return {
        # As ingestion: basic mode matches item.get("title", ""), enhanced
        # mode the full dashboard's title, which is the same string.
        "name": str(item.get("title") or ""),
        "uid": item.get("uid"),
        "type": item.get("type"),
        "folder": item.get("folderTitle"),
    }
