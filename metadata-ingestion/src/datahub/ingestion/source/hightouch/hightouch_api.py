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
from datahub.ingestion.source.hightouch.constants import (
    API_ENDPOINT_CONTRACTS,
    API_ENDPOINT_DESTINATIONS,
    API_ENDPOINT_MODELS,
    API_ENDPOINT_SOURCES,
    API_ENDPOINT_SYNCS,
    API_ENDPOINT_USERS,
    API_ENDPOINT_WORKSPACES,
    API_PAGINATION_DEFAULT_LIMIT,
    API_PAGINATION_INITIAL_OFFSET,
    API_PAGINATION_MAX_PAGES,
    API_RESPONSE_FIELD_DATA,
    API_RESPONSE_FIELD_HAS_MORE,
    ENTITY_NAME_CONTRACT,
    ENTITY_NAME_DESTINATION,
    ENTITY_NAME_MODEL,
    ENTITY_NAME_SOURCE,
    ENTITY_NAME_SYNC,
    ENTITY_NAME_SYNC_RUN,
    ENTITY_NAME_USER,
    ENTITY_NAME_WORKSPACE,
    HTTP_CONTENT_TYPE_JSON,
    HTTP_HEADER_AUTHORIZATION,
    HTTP_HEADER_CONTENT_TYPE,
    HTTP_METHOD_GET,
    HTTP_PROTOCOL_HTTP,
    HTTP_PROTOCOL_HTTPS,
    HTTP_RETRY_ALLOWED_METHODS,
    HTTP_RETRY_BACKOFF_FACTOR,
    HTTP_RETRY_MAX_ATTEMPTS,
    HTTP_RETRY_STATUS_CODES,
)
from datahub.ingestion.source.hightouch.models import (
    HightouchContract,
    HightouchDestination,
    HightouchFieldMapping,
    HightouchModel,
    HightouchSourceConnection,
    HightouchSync,
    HightouchSyncRun,
    HightouchUser,
    HightouchWorkspace,
)

T = TypeVar("T", bound=BaseModel)

logger = logging.getLogger(__name__)

HTTP_STATUS_NOT_FOUND = 404


class HightouchAPIClient:
    def __init__(
        self,
        config: HightouchAPIConfig,
        report: Optional[HightouchSourceReport] = None,
    ):
        self.config = config
        # The report is optional so the client can be constructed standalone (e.g. in
        # unit tests); when present, skips and parse failures are surfaced to operators.
        self.report = report
        self.session = self._create_session()

    def _report_parse_failure(self, entity_name: str, detail: str) -> None:
        if self.report is not None:
            self.report.report_entity_parse_failure(entity_name)
            self.report.warning(
                title="Failed to parse Hightouch entity",
                message="An entity returned by the Hightouch API could not be parsed "
                "and was skipped.",
                context=f"{entity_name}: {detail}",
            )
        else:
            logger.warning(f"Failed to parse {entity_name}: {detail}")

    def _create_session(self) -> requests.Session:
        session = requests.Session()

        retry_strategy = Retry(
            total=HTTP_RETRY_MAX_ATTEMPTS,
            backoff_factor=HTTP_RETRY_BACKOFF_FACTOR,
            status_forcelist=HTTP_RETRY_STATUS_CODES,
            allowed_methods=HTTP_RETRY_ALLOWED_METHODS,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount(HTTP_PROTOCOL_HTTP, adapter)
        session.mount(HTTP_PROTOCOL_HTTPS, adapter)

        session.headers.update(
            {
                HTTP_HEADER_AUTHORIZATION: f"Bearer {self.config.api_key.get_secret_value()}",
                HTTP_HEADER_CONTENT_TYPE: HTTP_CONTENT_TYPE_JSON,
            }
        )

        return session

    def _make_request(
        self, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        url = f"{self.config.base_url}/{endpoint}"

        try:
            response = self.session.request(
                method, url, timeout=self.config.request_timeout_sec, **kwargs
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to {url}: {e}")
            raise

    def _make_paginated_request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        all_items: List[Dict[str, Any]] = []
        offset = API_PAGINATION_INITIAL_OFFSET
        limit = (
            params.get("limit", API_PAGINATION_DEFAULT_LIMIT)
            if params
            else API_PAGINATION_DEFAULT_LIMIT
        )

        request_params = params.copy() if params else {}
        request_params["limit"] = limit

        page = 0
        while True:
            request_params["offset"] = offset

            response = self._make_request(
                HTTP_METHOD_GET, endpoint, params=request_params
            )

            items = response.get(API_RESPONSE_FIELD_DATA, [])
            all_items.extend(items)
            page += 1
            logger.debug(
                f"{endpoint}: fetched page {page} (offset={offset}, "
                f"items={len(items)}, total_so_far={len(all_items)})"
            )

            # Distinguish an explicit hasMore=false from an omitted field: if the
            # field is missing but the page came back full, the API may have more
            # data that we would otherwise silently drop.
            has_more = response.get(API_RESPONSE_FIELD_HAS_MORE)
            if has_more is None and len(items) == limit:
                self._warn_pagination(
                    f"{endpoint}: response omitted '{API_RESPONSE_FIELD_HAS_MORE}' "
                    f"on a full page (limit={limit}); results may be truncated."
                )

            if not has_more or len(items) == 0:
                break

            offset += len(items)

            if page >= API_PAGINATION_MAX_PAGES:
                self._warn_pagination(
                    f"{endpoint}: hit the pagination cap of "
                    f"{API_PAGINATION_MAX_PAGES} pages; results are truncated at "
                    f"{len(all_items)} items."
                )
                break

        return all_items

    def _warn_pagination(self, message: str) -> None:
        if self.report is not None:
            self.report.warning(
                title="Paginated response may be truncated",
                message=message,
            )
        else:
            logger.warning(message)

    def _report_field_mappings_dropped(self, message: str) -> None:
        if self.report is not None:
            self.report.report_field_mappings_dropped()
            self.report.warning(
                title="Dropped sync field mappings",
                message="Some field mappings could not be interpreted and were "
                "skipped, so column-level lineage may be incomplete.",
                context=message,
            )
        else:
            logger.warning(message)

    def _fetch_entities(
        self, endpoint: str, model_class: Type[T], entity_name: str
    ) -> List[T]:
        all_data = self._make_paginated_request(endpoint)
        entities = []

        for item_data in all_data:
            try:
                entity = model_class.model_validate(item_data)
                entities.append(entity)
            except ValidationError as e:
                self._report_parse_failure(entity_name, str(e))
                continue

        return entities

    def _fetch_entity_by_id(
        self, endpoint: str, entity_id: str, model_class: Type[T], entity_name: str
    ) -> Optional[T]:
        try:
            response = self._make_request(HTTP_METHOD_GET, f"{endpoint}/{entity_id}")
            return model_class.model_validate(response)
        except requests.exceptions.HTTPError as e:
            # A referenced entity may have been deleted after the sync/model that
            # points at it was read; a 404 must not abort the whole run.
            if (
                e.response is not None
                and e.response.status_code == HTTP_STATUS_NOT_FOUND
            ):
                reference = f"{entity_name} {entity_id}"
                if self.report is not None:
                    self.report.report_referenced_entity_inaccessible(reference)
                    self.report.warning(
                        title="Referenced Hightouch entity not found",
                        message="A referenced entity could not be fetched (404) and "
                        "was skipped. Lineage or metadata that depends on it may be "
                        "incomplete.",
                        context=reference,
                    )
                else:
                    logger.warning(f"{reference} not found (404); skipping.")
                return None
            raise
        except ValidationError as e:
            self._report_parse_failure(f"{entity_name} {entity_id}", str(e))
            return None

    def get_workspaces(self) -> List[HightouchWorkspace]:
        return self._fetch_entities(
            API_ENDPOINT_WORKSPACES, HightouchWorkspace, ENTITY_NAME_WORKSPACE
        )

    def get_workspace_by_id(self, workspace_id: str) -> Optional[HightouchWorkspace]:
        return self._fetch_entity_by_id(
            API_ENDPOINT_WORKSPACES,
            workspace_id,
            HightouchWorkspace,
            ENTITY_NAME_WORKSPACE,
        )

    def get_sources(self) -> List[HightouchSourceConnection]:
        return self._fetch_entities(
            API_ENDPOINT_SOURCES, HightouchSourceConnection, ENTITY_NAME_SOURCE
        )

    def get_source_by_id(self, source_id: str) -> Optional[HightouchSourceConnection]:
        return self._fetch_entity_by_id(
            API_ENDPOINT_SOURCES,
            source_id,
            HightouchSourceConnection,
            ENTITY_NAME_SOURCE,
        )

    def get_models(self) -> List[HightouchModel]:
        return self._fetch_entities(
            API_ENDPOINT_MODELS, HightouchModel, ENTITY_NAME_MODEL
        )

    def get_model_by_id(self, model_id: str) -> Optional[HightouchModel]:
        return self._fetch_entity_by_id(
            API_ENDPOINT_MODELS, model_id, HightouchModel, ENTITY_NAME_MODEL
        )

    def get_destinations(self) -> List[HightouchDestination]:
        return self._fetch_entities(
            API_ENDPOINT_DESTINATIONS, HightouchDestination, ENTITY_NAME_DESTINATION
        )

    def get_destination_by_id(
        self, destination_id: str
    ) -> Optional[HightouchDestination]:
        return self._fetch_entity_by_id(
            API_ENDPOINT_DESTINATIONS,
            destination_id,
            HightouchDestination,
            ENTITY_NAME_DESTINATION,
        )

    def get_syncs(self) -> List[HightouchSync]:
        return self._fetch_entities(API_ENDPOINT_SYNCS, HightouchSync, ENTITY_NAME_SYNC)

    def get_sync_by_id(self, sync_id: str) -> Optional[HightouchSync]:
        return self._fetch_entity_by_id(
            API_ENDPOINT_SYNCS, sync_id, HightouchSync, ENTITY_NAME_SYNC
        )

    def get_sync_runs(self, sync_id: str, limit: int = 10) -> List[HightouchSyncRun]:
        response = self._make_request(
            HTTP_METHOD_GET,
            f"{API_ENDPOINT_SYNCS}/{sync_id}/runs",
            params={"limit": limit},
        )
        runs = []

        for run_data in response.get(API_RESPONSE_FIELD_DATA, []):
            try:
                run = HightouchSyncRun.model_validate(run_data)
                runs.append(run)
            except ValidationError as e:
                self._report_parse_failure(ENTITY_NAME_SYNC_RUN, str(e))
                continue

        return runs

    def get_user_by_id(self, user_id: str) -> Optional[HightouchUser]:
        return self._fetch_entity_by_id(
            API_ENDPOINT_USERS, user_id, HightouchUser, ENTITY_NAME_USER
        )

    def get_contracts(self) -> List[HightouchContract]:
        # Contracts are optional (plan-gated) and a failure here must never abort the
        # rest of the run, so every request error (HTTP status, connection, timeout)
        # is downgraded to a warning + empty list.
        try:
            return self._fetch_entities(
                API_ENDPOINT_CONTRACTS, HightouchContract, ENTITY_NAME_CONTRACT
            )
        except requests.exceptions.RequestException as e:
            response = getattr(e, "response", None)
            status_code = response.status_code if response is not None else None
            if status_code == HTTP_STATUS_NOT_FOUND:
                message = (
                    "Contracts endpoint not found (404). Event Contracts may not be "
                    "enabled for your Hightouch account/plan. Skipping contract "
                    "ingestion."
                )
            else:
                detail = f"HTTP {status_code}" if status_code else type(e).__name__
                message = (
                    f"Failed to fetch Event Contracts ({detail}). Skipping contract "
                    "ingestion; the rest of the run is unaffected."
                )
            if self.report is not None:
                self.report.warning(
                    title="Could not fetch Event Contracts",
                    message=message,
                    exc=e,
                )
            else:
                logger.warning(message)
            return []

    def extract_field_mappings(
        self, sync: HightouchSync
    ) -> List[HightouchFieldMapping]:
        field_mappings: List[HightouchFieldMapping] = []
        config = sync.configuration if sync.configuration else {}

        if not config:
            return field_mappings

        mappings = config.get("mappings", [])
        if not isinstance(mappings, list):
            self._report_field_mappings_dropped(
                f"Sync {sync.id}: expected mappings to be a list, got "
                f"{type(mappings).__name__}; column-level lineage will be missing."
            )
            return field_mappings

        for i, mapping in enumerate(mappings):
            if not isinstance(mapping, dict):
                self._report_field_mappings_dropped(
                    f"Sync {sync.id}: skipping non-dict mapping at index {i} "
                    f"({type(mapping).__name__}); column-level lineage will be missing."
                )
                continue

            source = mapping.get("from")
            dest = mapping.get("to")

            if not source or not dest:
                self._report_field_mappings_dropped(
                    f"Sync {sync.id}: mapping at index {i} is missing a source "
                    "('from') or destination ('to') field; column-level lineage for "
                    "this column will be missing."
                )
                continue

            if not isinstance(source, str) or not isinstance(dest, str):
                logger.warning(
                    f"Sync {sync.id}: Field names must be strings, got from={type(source).__name__}, "
                    f"to={type(dest).__name__}. Converting to strings."
                )
                source = str(source)
                dest = str(dest)

            is_pk = mapping.get("isPrimaryKey", mapping.get("is_primary_key", False))

            field_mappings.append(
                HightouchFieldMapping(
                    source_field=source,
                    destination_field=dest,
                    is_primary_key=bool(is_pk),
                )
            )

        return field_mappings
