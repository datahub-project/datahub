"""Client for interacting with the Hightouch REST API"""

import logging
from typing import Any, Dict, List, Optional

import requests
from pydantic import ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datahub.ingestion.source.hightouch.config import HightouchAPIConfig
from datahub.ingestion.source.hightouch.data_classes import (
    FieldMapping,
    HightouchDestination,
    HightouchModel,
    HightouchSource,
    HightouchSync,
    HightouchSyncRun,
    HightouchUser,
)

logger = logging.getLogger(__name__)


class HightouchAPIClient:
    """Client for the Hightouch REST API"""

    def __init__(self, config: HightouchAPIConfig):
        self.config = config
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic"""
        session = requests.Session()

        # Configure retries
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set headers
        session.headers.update(
            {
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json",
            }
        )

        return session

    def _make_request(
        self, method: str, endpoint: str, **kwargs: Any
    ) -> Dict[str, Any]:
        """Make a request to the Hightouch API"""
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

    def get_sources(self) -> List[HightouchSource]:
        """Get all sources from Hightouch"""
        response = self._make_request("GET", "sources")
        sources = []

        for source_data in response.get("data", []):
            try:
                # Pydantic automatically handles field aliases and type conversion
                source = HightouchSource.model_validate(source_data)
                sources.append(source)
            except ValidationError as e:
                logger.warning(f"Failed to parse source: {e}, data: {source_data}")
                continue

        return sources

    def get_source_by_id(self, source_id: str) -> Optional[HightouchSource]:
        """Get a specific source by ID"""
        try:
            response = self._make_request("GET", f"sources/{source_id}")
            return HightouchSource.model_validate(response)
        except ValidationError as e:
            logger.warning(f"Failed to parse source {source_id}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed to get source {source_id}: {e}")
            return None

    def get_models(self) -> List[HightouchModel]:
        """Get all models from Hightouch"""
        response = self._make_request("GET", "models")
        models = []

        for model_data in response.get("data", []):
            try:
                model = HightouchModel.model_validate(model_data)
                models.append(model)
            except ValidationError as e:
                logger.warning(f"Failed to parse model: {e}, data: {model_data}")
                continue

        return models

    def get_model_by_id(self, model_id: str) -> Optional[HightouchModel]:
        """Get a specific model by ID"""
        try:
            response = self._make_request("GET", f"models/{model_id}")
            return HightouchModel.model_validate(response)
        except ValidationError as e:
            logger.warning(f"Failed to parse model {model_id}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed to get model {model_id}: {e}")
            return None

    def get_destinations(self) -> List[HightouchDestination]:
        """Get all destinations from Hightouch"""
        response = self._make_request("GET", "destinations")
        destinations = []

        for dest_data in response.get("data", []):
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
        """Get a specific destination by ID"""
        try:
            response = self._make_request("GET", f"destinations/{destination_id}")
            return HightouchDestination.model_validate(response)
        except ValidationError as e:
            logger.warning(f"Failed to parse destination {destination_id}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed to get destination {destination_id}: {e}")
            return None

    def get_syncs(self) -> List[HightouchSync]:
        """Get all syncs from Hightouch"""
        response = self._make_request("GET", "syncs")
        syncs = []

        for sync_data in response.get("data", []):
            try:
                sync = HightouchSync.model_validate(sync_data)
                syncs.append(sync)
            except ValidationError as e:
                logger.warning(f"Failed to parse sync: {e}, data: {sync_data}")
                continue

        return syncs

    def get_sync_by_id(self, sync_id: str) -> Optional[HightouchSync]:
        """Get a specific sync by ID"""
        try:
            response = self._make_request("GET", f"syncs/{sync_id}")
            return HightouchSync.model_validate(response)
        except ValidationError as e:
            logger.warning(f"Failed to parse sync {sync_id}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed to get sync {sync_id}: {e}")
            return None

    def get_sync_runs(self, sync_id: str, limit: int = 10) -> List[HightouchSyncRun]:
        """Get sync runs for a specific sync"""
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
        """Get a specific user by ID"""
        try:
            response = self._make_request("GET", f"users/{user_id}")
            return HightouchUser.model_validate(response)
        except ValidationError as e:
            logger.warning(f"Failed to parse user {user_id}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed to get user {user_id}: {e}")
            return None

    def extract_field_mappings(self, sync: HightouchSync) -> List[FieldMapping]:
        """Extract field mappings from sync configuration"""
        field_mappings = []

        # Check different configuration formats
        # Hightouch stores field mappings in various formats depending on destination type
        # Defensive: handle missing configuration gracefully
        config = sync.configuration if sync.configuration else {}

        # Format 1: Direct field_mappings array
        if "fieldMappings" in config or "field_mappings" in config:
            mappings = config.get("fieldMappings", config.get("field_mappings", []))
            for mapping in mappings:
                if isinstance(mapping, dict):
                    source = mapping.get("sourceField") or mapping.get("source_field")
                    dest = mapping.get("destinationField") or mapping.get(
                        "destination_field"
                    )
                    is_pk = mapping.get(
                        "isPrimaryKey", mapping.get("is_primary_key", False)
                    )

                    if source and dest:
                        field_mappings.append(
                            FieldMapping(
                                source_field=source,
                                destination_field=dest,
                                is_primary_key=is_pk,
                            )
                        )

        # Format 2: Column mappings object
        elif "columnMappings" in config or "column_mappings" in config:
            mappings_dict = config.get(
                "columnMappings", config.get("column_mappings", {})
            )
            for dest_field, source_field in mappings_dict.items():
                field_mappings.append(
                    FieldMapping(
                        source_field=str(source_field),
                        destination_field=str(dest_field),
                        is_primary_key=False,
                    )
                )

        # Format 3: Columns array with to/from
        elif "columns" in config:
            for column in config.get("columns", []):
                if isinstance(column, dict):
                    source = column.get("from") or column.get("source")
                    dest = column.get("to") or column.get("destination")

                    if source and dest:
                        field_mappings.append(
                            FieldMapping(
                                source_field=source,
                                destination_field=dest,
                                is_primary_key=column.get("isPrimaryKey", False),
                            )
                        )

        return field_mappings
