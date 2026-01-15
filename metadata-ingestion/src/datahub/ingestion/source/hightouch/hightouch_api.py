import json
import logging
from typing import Any, Dict, List, Optional

import requests
from pydantic import ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.hightouch.config import HightouchAPIConfig
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
)

logger = logging.getLogger(__name__)


class HightouchAPIClient:
    def __init__(self, config: HightouchAPIConfig):
        self.config = config
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update(
            {
                "Authorization": f"Bearer {self.config.api_key.get_secret_value()}",
                "Content-Type": "application/json",
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
        offset = 0
        limit = params.get("limit", 100) if params else 100

        request_params = params.copy() if params else {}
        request_params["limit"] = limit

        while True:
            request_params["offset"] = offset

            logger.debug(f"Fetching {endpoint} with offset={offset}, limit={limit}")

            response = self._make_request("GET", endpoint, params=request_params)

            items = response.get("data", [])
            all_items.extend(items)

            logger.debug(
                f"Fetched {len(items)} items from {endpoint} (total so far: {len(all_items)})"
            )

            has_more = response.get("hasMore", False)

            if not has_more or len(items) == 0:
                logger.info(
                    f"Completed fetching {endpoint}: {len(all_items)} total items"
                )
                break

            offset += len(items)

        return all_items

    def get_sources(self) -> List[HightouchSourceConnection]:
        all_data = self._make_paginated_request("sources")
        sources = []

        for source_data in all_data:
            try:
                source = HightouchSourceConnection.model_validate(source_data)
                sources.append(source)
            except ValidationError as e:
                logger.warning(f"Failed to parse source: {e}, data: {source_data}")
                continue

        return sources

    def get_source_by_id(self, source_id: str) -> Optional[HightouchSourceConnection]:
        try:
            response = self._make_request("GET", f"sources/{source_id}")
            return HightouchSourceConnection.model_validate(response)
        except ValidationError as e:
            logger.warning(f"Failed to parse source {source_id}: {e}")
            return None

    def get_models(self) -> List[HightouchModel]:
        all_data = self._make_paginated_request("models")
        models = []

        for model_data in all_data:
            try:
                model = HightouchModel.model_validate(model_data)
                models.append(model)
            except ValidationError as e:
                logger.warning(f"Failed to parse model: {e}, data: {model_data}")
                continue

        return models

    def get_model_by_id(self, model_id: str) -> Optional[HightouchModel]:
        try:
            response = self._make_request("GET", f"models/{model_id}")
            return HightouchModel.model_validate(response)
        except ValidationError as e:
            logger.warning(f"Failed to parse model {model_id}: {e}")
            return None

    def get_destinations(self) -> List[HightouchDestination]:
        all_data = self._make_paginated_request("destinations")
        destinations = []

        for dest_data in all_data:
            try:
                destination = HightouchDestination.model_validate(dest_data)
                destinations.append(destination)
            except ValidationError as e:
                logger.warning(f"Failed to parse destination: {e}, data: {dest_data}")
                continue

        return destinations

    def get_destination_by_id(
        self, destination_id: str
    ) -> Optional[HightouchDestination]:
        try:
            response = self._make_request("GET", f"destinations/{destination_id}")
            return HightouchDestination.model_validate(response)
        except ValidationError as e:
            logger.warning(f"Failed to parse destination {destination_id}: {e}")
            return None

    def get_syncs(self) -> List[HightouchSync]:
        all_data = self._make_paginated_request("syncs")
        syncs = []

        for sync_data in all_data:
            try:
                sync = HightouchSync.model_validate(sync_data)
                syncs.append(sync)
            except ValidationError as e:
                logger.warning(f"Failed to parse sync: {e}, data: {sync_data}")
                continue

        return syncs

    def get_sync_by_id(self, sync_id: str) -> Optional[HightouchSync]:
        try:
            response = self._make_request("GET", f"syncs/{sync_id}")
            return HightouchSync.model_validate(response)
        except ValidationError as e:
            logger.warning(f"Failed to parse sync {sync_id}: {e}")
            return None

    def get_sync_runs(self, sync_id: str, limit: int = 10) -> List[HightouchSyncRun]:
        response = self._make_request(
            "GET", f"syncs/{sync_id}/runs", params={"limit": limit}
        )
        runs = []

        for run_data in response.get("data", []):
            try:
                run = HightouchSyncRun.model_validate(run_data)
                runs.append(run)
            except ValidationError as e:
                logger.warning(f"Failed to parse sync run: {e}, data: {run_data}")
                continue

        return runs

    def get_user_by_id(self, user_id: str) -> Optional[HightouchUser]:
        try:
            response = self._make_request("GET", f"users/{user_id}")
            return HightouchUser.model_validate(response)
        except ValidationError as e:
            logger.warning(f"Failed to parse user {user_id}: {e}")
            return None

    def get_contracts(self) -> List[HightouchContract]:
        try:
            all_data = self._make_paginated_request("events/contracts")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(
                    "Contracts endpoint not found (404). This feature may not be available "
                    "for your Hightouch account/plan. Skipping contract ingestion."
                )
                return []
            raise

        contracts = []

        for contract_data in all_data:
            try:
                contract = HightouchContract.model_validate(contract_data)
                contracts.append(contract)
            except ValidationError as e:
                logger.warning(f"Failed to parse contract: {e}, data: {contract_data}")
                continue

        return contracts

    def get_contract_by_id(self, contract_id: str) -> Optional[HightouchContract]:
        try:
            response = self._make_request("GET", f"events/contracts/{contract_id}")
            return HightouchContract.model_validate(response)
        except ValidationError as e:
            logger.warning(f"Failed to parse contract {contract_id}: {e}")
            return None

    def get_contract_runs(
        self, contract_id: str, limit: int = 10
    ) -> List[HightouchContractRun]:
        try:
            response = self._make_request(
                "GET", f"events/contracts/{contract_id}/runs", params={"limit": limit}
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

        for run_data in response.get("data", []):
            try:
                run = HightouchContractRun.model_validate(run_data)
                runs.append(run)
            except ValidationError as e:
                logger.warning(f"Failed to parse contract run: {e}, data: {run_data}")
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
