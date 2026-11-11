import logging
from typing import Any, Dict, List, Optional, Type, TypeVar

import requests
from pydantic import BaseModel, ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.hightouch.config import (
    HightouchAPIConfig,
    HightouchSourceReport,
)
from datahub.ingestion.source.hightouch.models import (
    HightouchContract,
    HightouchDestination,
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
    HightouchSyncRun,
)

T = TypeVar("T", bound=BaseModel)

logger = logging.getLogger(__name__)

PAGE_SIZE = 100
# Guards against an API that never signals the last page.
MAX_PAGES = 10_000
# The workspace or key has no access to Events: there are no contracts to keep.
CONTRACTS_UNAVAILABLE_STATUSES = {401, 403, 404}


class HightouchAPIClient:
    def __init__(self, config: HightouchAPIConfig, report: HightouchSourceReport):
        self.config = config
        self.report = report
        self.session = requests.Session()
        self.session.mount(
            "https://",
            HTTPAdapter(
                max_retries=Retry(
                    total=3,
                    backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET"],
                )
            ),
        )
        self.session.headers.update(
            {
                "Authorization": f"Bearer {config.api_key.get_secret_value()}",
                "Content-Type": "application/json",
            }
        )

    def _get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        self.report.api_calls += 1
        response = self.session.get(
            f"{self.config.base_url}/{endpoint}",
            params=params,
            timeout=self.config.request_timeout_sec,
        )
        response.raise_for_status()
        return response.json()

    def _get_all(self, endpoint: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for _ in range(MAX_PAGES):
            response = self._get(
                endpoint, params={"limit": PAGE_SIZE, "offset": len(items)}
            )
            page_items = response.get("data", [])
            items.extend(page_items)
            # Only some list endpoints return hasMore; for the others a short page
            # is the last one.
            has_more = response.get("hasMore")
            if not page_items or has_more is False:
                return items
            if has_more is None and len(page_items) < PAGE_SIZE:
                return items
        # Entities past the cap are missing from this run, so this is a failure:
        # it keeps stale-entity removal from soft-deleting them.
        self.report.failure(
            title="Paginated response truncated",
            message=f"Stopped after {MAX_PAGES} pages, so some entities are missing.",
            context=f"endpoint: {endpoint}, items: {len(items)}",
        )
        return items

    def _parse_all(
        self, items: List[Dict[str, Any]], model_class: Type[T], entity_name: str
    ) -> List[T]:
        parsed = []
        for item in items:
            try:
                parsed.append(model_class.model_validate(item))
            except ValidationError as e:
                self.report.entity_parse_failures[entity_name] = (
                    self.report.entity_parse_failures.get(entity_name, 0) + 1
                )
                # A skipped entity is missing from this run, so this is a failure:
                # it keeps stale-entity removal from soft-deleting it.
                self.report.failure(
                    title="Failed to parse Hightouch entity",
                    message="An entity returned by the Hightouch API could not be "
                    "parsed and was skipped.",
                    context=f"{entity_name} {item.get('id')}",
                    exc=e,
                )
        return parsed

    def check_access(self) -> None:
        """One cheap documented call that fails the way ingestion would: bad key,
        missing permission or no network."""
        self._get("sources", params={"limit": 1})

    def get_sources(self) -> List[HightouchSourceConnection]:
        return self._parse_all(
            self._get_all("sources"), HightouchSourceConnection, "source"
        )

    def get_models(self) -> List[HightouchModel]:
        return self._parse_all(self._get_all("models"), HightouchModel, "model")

    def get_destinations(self) -> List[HightouchDestination]:
        return self._parse_all(
            self._get_all("destinations"), HightouchDestination, "destination"
        )

    def get_syncs(self) -> List[HightouchSync]:
        return self._parse_all(self._get_all("syncs"), HightouchSync, "sync")

    def get_sync_runs(self, sync_id: str, limit: int) -> List[HightouchSyncRun]:
        runs = []
        response = self._get(f"syncs/{sync_id}/runs", params={"limit": limit})
        for item in response.get("data", []):
            try:
                runs.append(HightouchSyncRun.model_validate(item))
            except ValidationError as e:
                # Runs are not tracked for stale removal, so a skipped run is only
                # a warning.
                self.report.warning(
                    title="Failed to parse Hightouch sync run",
                    message="A sync run could not be parsed and was skipped.",
                    context=f"sync_id: {sync_id}, run: {item.get('id')}",
                    exc=e,
                )
        return runs

    def get_contracts(self) -> Optional[List[HightouchContract]]:
        """Event contracts, or None when they could not be read. Contracts are
        plan-gated, so a request error never aborts the run."""
        try:
            items = self._get_all("events/contracts")
        except requests.exceptions.RequestException as e:
            response = getattr(e, "response", None)
            status = response.status_code if response is not None else None
            if status in CONTRACTS_UNAVAILABLE_STATUSES:
                self.report.warning(
                    title="Event Contracts are not available",
                    message=f"The API key cannot read Event Contracts (HTTP {status}); "
                    "they may not be enabled for the workspace. Set "
                    "include_contracts: false to stop requesting them.",
                    exc=e,
                )
            else:
                # Contracts may exist but could not be read, so this is a failure:
                # it keeps stale-entity removal from soft-deleting them.
                self.report.failure(
                    title="Could not fetch Event Contracts",
                    message="Contract ingestion was skipped; the rest of the run is "
                    "unaffected.",
                    exc=e,
                )
            return None
        return self._parse_all(items, HightouchContract, "contract")
