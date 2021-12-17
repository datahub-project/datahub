import json
import logging
import urllib.parse
from json.decoder import JSONDecodeError
from typing import Any, Dict, List, Optional, Type, TypeVar

from avrogen.dict_wrapper import DictWrapper
from requests.adapters import Response
from requests.models import HTTPError

from datahub.configuration.common import ConfigModel, OperationalError
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import DatasetUsageStatisticsClass, OwnershipClass

# This bound isn't tight, but it's better than nothing.
Aspect = TypeVar("Aspect", bound=DictWrapper)

logger = logging.getLogger(__name__)


class DatahubClientConfig(ConfigModel):
    """Configuration class for holding connectivity to datahub gms"""

    server: str = "http://localhost:8080"
    token: Optional[str]
    timeout_sec: Optional[int]
    extra_headers: Optional[Dict[str, str]]
    ca_certificate_path: Optional[str]
    max_threads: int = 1


class DataHubGraph(DatahubRestEmitter):
    def __init__(self, config: DatahubClientConfig) -> None:
        self.config = config
        super().__init__(
            gms_server=self.config.server,
            token=self.config.token,
            connect_timeout_sec=self.config.timeout_sec,  # reuse timeout_sec for connect timeout
            read_timeout_sec=self.config.timeout_sec,
            extra_headers=self.config.extra_headers,
            ca_certificate_path=self.config.ca_certificate_path,
        )
        self.test_connection()

    def _get_generic(self, url: str) -> Dict:
        try:
            response = self._session.get(url)
            response.raise_for_status()
            return response.json()
        except HTTPError as e:
            try:
                info = response.json()
                raise OperationalError(
                    "Unable to get metadata from DataHub", info
                ) from e
            except JSONDecodeError:
                # If we can't parse the JSON, just raise the original error.
                raise OperationalError(
                    "Unable to get metadata from DataHub", {"message": str(e)}
                ) from e

    def _post_generic(self, url: str, payload_dict: Dict) -> Dict:
        payload = json.dumps(payload_dict)
        logger.debug(payload)
        try:
            response: Response = self._session.post(url, payload)
            response.raise_for_status()
            return response.json()
        except HTTPError as e:
            try:
                info = response.json()
                raise OperationalError(
                    "Unable to get metadata from DataHub", info
                ) from e
            except JSONDecodeError:
                # If we can't parse the JSON, just raise the original error.
                raise OperationalError(
                    "Unable to get metadata from DataHub", {"message": str(e)}
                ) from e

    @staticmethod
    def _guess_entity_type(urn: str) -> str:
        assert urn.startswith("urn:li:"), "urns must start with urn:li:"
        return urn.split(":")[2]

    def get_aspect(
        self,
        entity_urn: str,
        aspect: str,
        aspect_type_name: str,
        aspect_type: Type[Aspect],
    ) -> Optional[Aspect]:
        url = f"{self._gms_server}/aspects/{urllib.parse.quote(entity_urn)}?aspect={aspect}&version=0"
        response = self._session.get(url)
        if response.status_code == 404:
            # not found
            return None
        response.raise_for_status()
        response_json = response.json()
        aspect_json = response_json.get("aspect", {}).get(aspect_type_name)
        if aspect_json:
            return aspect_type.from_obj(aspect_json, tuples=True)
        else:
            raise OperationalError(
                f"Failed to find {aspect_type_name} in response {response_json}"
            )

    def get_config(self) -> Dict[str, Any]:
        return self._get_generic(f"{self.config.server}/config")

    def get_ownership(self, entity_urn: str) -> Optional[OwnershipClass]:
        return self.get_aspect(
            entity_urn=entity_urn,
            aspect="ownership",
            aspect_type_name="com.linkedin.common.Ownership",
            aspect_type=OwnershipClass,
        )

    def get_usage_aspects_from_urn(
        self, entity_urn: str, start_timestamp: int, end_timestamp: int
    ) -> Optional[List[DatasetUsageStatisticsClass]]:
        payload = {
            "urn": entity_urn,
            "entity": "dataset",
            "aspect": "datasetUsageStatistics",
            "startTimeMillis": start_timestamp,
            "endTimeMillis": end_timestamp,
        }
        headers: Dict[str, Any] = {}
        url = f"{self._gms_server}/aspects?action=getTimeseriesAspectValues"
        try:
            usage_aspects: List[DatasetUsageStatisticsClass] = []
            response = self._session.post(
                url, data=json.dumps(payload), headers=headers
            )
            if response.status_code != 200:
                logger.debug(
                    f"Non 200 status found while fetching usage aspects - {response.status_code}"
                )
                return None
            json_resp = response.json()
            all_aspects = json_resp.get("value", {}).get("values", [])
            for aspect in all_aspects:
                if aspect.get("aspect") and aspect.get("aspect").get("value"):
                    usage_aspects.append(
                        DatasetUsageStatisticsClass.from_obj(
                            json.loads(aspect.get("aspect").get("value")), tuples=True
                        )
                    )
            return usage_aspects
        except Exception as e:
            logger.error("Error while getting usage aspects.", e)
        return None

    def list_all_entity_urns(
        self, entity_type: str, start: int, count: int
    ) -> Optional[List[str]]:
        url = f"{self._gms_server}/entities?action=listUrns"
        payload = {"entity": entity_type, "start": start, "count": count}
        headers = {
            "X-RestLi-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        }
        try:
            response = self._session.post(
                url, data=json.dumps(payload), headers=headers
            )
            if response.status_code != 200:
                logger.debug(
                    f"Non 200 status found while fetching entity urns - {response.status_code}"
                )
                return None
            json_resp = response.json()
            return json_resp.get("value", {}).get("entities")
        except Exception as e:
            logger.error("Error while fetching entity urns.", e)
            return None

    def get_latest_timeseries_value(
        self,
        entity_urn: str,
        aspect_name: str,
        aspect_type: Type[Aspect],
        filter_criteria_map: Dict[str, str],
    ) -> Optional[Aspect]:
        filter_criteria = [
            {"field": k, "value": v, "condition": "EQUAL"}
            for k, v in filter_criteria_map.items()
        ]
        query_body = {
            "urn": entity_urn,
            "entity": self._guess_entity_type(entity_urn),
            "aspect": aspect_name,
            "latestValue": True,
            "filter": {"or": [{"and": filter_criteria}]},
        }
        end_point = f"{self.config.server}/aspects?action=getTimeseriesAspectValues"
        resp: Dict = self._post_generic(end_point, query_body)
        values: list = resp.get("value", {}).get("values")
        if values:
            assert len(values) == 1
            aspect_json: str = values[0].get("aspect", {}).get("value")
            if aspect_json:
                return aspect_type.from_obj(json.loads(aspect_json), tuples=False)
            else:
                raise OperationalError(
                    f"Failed to find {aspect_type} in response {aspect_json}"
                )
        return None
