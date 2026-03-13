import json
import logging
from typing import Any, Dict, List, Optional, Type, TypeVar

import requests
from pydantic import BaseModel, ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.hightouch.config import HightouchAPIConfig
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
    API_RESPONSE_FIELD_DATA,
    API_RESPONSE_FIELD_HAS_MORE,
    ENTITY_NAME_CONTRACT,
    ENTITY_NAME_CONTRACT_RUN,
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
    HightouchContractRun,
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


class HightouchAPIClient:
    def __init__(self, config: HightouchAPIConfig):
        self.config = config
        self.session = self._create_session()

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
            logger.debug(f"Making {method} request to {url} with kwargs: {kwargs}")
            response = self.session.request(
                method, url, timeout=self.config.request_timeout_sec, **kwargs
            )
            response.raise_for_status()
            result = response.json()
            logger.debug(f"API response from {endpoint}: {json.dumps(result)}")
            return result
        except requests.exceptions.RequestException as e:
            logger.error(f"Error making request to {url}: {e}")
            raise

    def _make_paginated_request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        all_items = []
        offset = API_PAGINATION_INITIAL_OFFSET
        limit = (
            params.get("limit", API_PAGINATION_DEFAULT_LIMIT)
            if params
            else API_PAGINATION_DEFAULT_LIMIT
        )

        request_params = params.copy() if params else {}
        request_params["limit"] = limit

        while True:
            request_params["offset"] = offset

            logger.debug(f"Fetching {endpoint} with offset={offset}, limit={limit}")

            response = self._make_request(
                HTTP_METHOD_GET, endpoint, params=request_params
            )

            items = response.get(API_RESPONSE_FIELD_DATA, [])
            all_items.extend(items)

            logger.debug(
                f"Fetched {len(items)} items from {endpoint} (total so far: {len(all_items)})"
            )

            has_more = response.get(API_RESPONSE_FIELD_HAS_MORE, False)

            if not has_more or len(items) == 0:
                logger.info(
                    f"Completed fetching {endpoint}: {len(all_items)} total items"
                )
                break

            offset += len(items)

        return all_items

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
                logger.warning(f"Failed to parse {entity_name}: {e}, data: {item_data}")
                continue

        return entities

    def _fetch_entity_by_id(
        self, endpoint: str, entity_id: str, model_class: Type[T], entity_name: str
    ) -> Optional[T]:
        try:
            response = self._make_request(HTTP_METHOD_GET, f"{endpoint}/{entity_id}")
            return model_class.model_validate(response)
        except ValidationError as e:
            logger.warning(f"Failed to parse {entity_name} {entity_id}: {e}")
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
                logger.warning(
                    f"Failed to parse {ENTITY_NAME_SYNC_RUN}: {e}, data: {run_data}"
                )
                continue

        return runs

    def get_user_by_id(self, user_id: str) -> Optional[HightouchUser]:
        return self._fetch_entity_by_id(
            API_ENDPOINT_USERS, user_id, HightouchUser, ENTITY_NAME_USER
        )

    def get_contracts(self) -> List[HightouchContract]:
        try:
            return self._fetch_entities(
                API_ENDPOINT_CONTRACTS, HightouchContract, ENTITY_NAME_CONTRACT
            )
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(
                    "Contracts endpoint not found (404). This feature may not be available "
                    "for your Hightouch account/plan. Skipping contract ingestion."
                )
                return []
            raise

    def get_contract_by_id(self, contract_id: str) -> Optional[HightouchContract]:
        return self._fetch_entity_by_id(
            API_ENDPOINT_CONTRACTS, contract_id, HightouchContract, ENTITY_NAME_CONTRACT
        )

    def get_contract_runs(
        self, contract_id: str, limit: int = 10
    ) -> List[HightouchContractRun]:
        try:
            response = self._make_request(
                HTTP_METHOD_GET,
                f"{API_ENDPOINT_CONTRACTS}/{contract_id}/runs",
                params={"limit": limit},
            )
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(
                    f"Contract runs endpoint not found (404) for contract {contract_id}. "
                    "This contract may not exist or the feature may not be available. "
                    "Returning empty list."
                )
                return []
            raise

        runs = []

        for run_data in response.get(API_RESPONSE_FIELD_DATA, []):
            try:
                run = HightouchContractRun.model_validate(run_data)
                runs.append(run)
            except ValidationError as e:
                logger.warning(
                    f"Failed to parse {ENTITY_NAME_CONTRACT_RUN}: {e}, data: {run_data}"
                )
                continue

        return runs

    def extract_field_mappings(
        self, sync: HightouchSync
    ) -> List[HightouchFieldMapping]:
        field_mappings: List[HightouchFieldMapping] = []
        config = sync.configuration if sync.configuration else {}

        if not config:
            return field_mappings

        mappings = config.get("mappings", [])
        if not isinstance(mappings, list):
            logger.warning(
                f"Sync {sync.id}: Expected mappings to be a list, got {type(mappings).__name__}"
            )
            return field_mappings

        for i, mapping in enumerate(mappings):
            if not isinstance(mapping, dict):
                logger.warning(
                    f"Sync {sync.id}: Skipping non-dict mapping at index {i}: {type(mapping).__name__}"
                )
                continue

            source = mapping.get("from")
            dest = mapping.get("to")

            if not source or not dest:
                logger.debug(
                    f"Sync {sync.id}: Skipping incomplete mapping at index {i} "
                    f"(from={source}, to={dest})"
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

        logger.debug(
            f"Extracted {len(field_mappings)} field mappings for sync {sync.id}"
        )
        return field_mappings
