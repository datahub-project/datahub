import json
import logging
import os
from json.decoder import JSONDecodeError
from typing import Any, Dict, Iterable, List, Optional, Type, Union

import pydantic
from avro.schema import RecordSchema
from deprecated import deprecated
from requests.adapters import Response
from requests.models import HTTPError

from datahub.configuration.common import ConfigModel, OperationalError
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.serialization_helper import post_json_transform
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    DomainPropertiesClass,
    DomainsClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    OwnershipClass,
    SchemaMetadataClass,
    TelemetryClientIdClass,
)
from datahub.utilities.urns.urn import Urn, guess_entity_type

logger = logging.getLogger(__name__)


telemetry_enabled = (
    os.environ.get("DATAHUB_TELEMETRY_ENABLED", "true").lower() == "true"
)


class DatahubClientConfig(ConfigModel):
    """Configuration class for holding connectivity to datahub gms"""

    server: str = "http://localhost:8080"
    token: Optional[str]
    timeout_sec: Optional[int]
    retry_status_codes: Optional[List[int]]
    retry_max_times: Optional[int]
    extra_headers: Optional[Dict[str, str]]
    ca_certificate_path: Optional[str]
    max_threads: int = 1
    disable_ssl_verification: bool = False


class DataHubGraphConfig(DatahubClientConfig):
    class Config:
        extra = (
            pydantic.Extra.allow
        )  # lossy to allow interop with DataHubRestSinkConfig


class DataHubGraph(DatahubRestEmitter):
    def __init__(self, config: Union[DatahubClientConfig, DataHubGraphConfig]) -> None:
        self.config = config
        super().__init__(
            gms_server=self.config.server,
            token=self.config.token,
            connect_timeout_sec=self.config.timeout_sec,  # reuse timeout_sec for connect timeout
            read_timeout_sec=self.config.timeout_sec,
            retry_status_codes=self.config.retry_status_codes,
            retry_max_times=self.config.retry_max_times,
            extra_headers=self.config.extra_headers,
            ca_certificate_path=self.config.ca_certificate_path,
        )
        self.test_connection()
        if not telemetry_enabled:
            self.server_id = "missing"
            return
        try:
            client_id: Optional[TelemetryClientIdClass] = self.get_aspect_v2(
                "urn:li:telemetry:clientId", TelemetryClientIdClass, "telemetryClientId"
            )
            self.server_id = client_id.clientId if client_id else "missing"
        except Exception as e:
            self.server_id = "missing"
            logger.debug(f"Failed to get server id due to {e}")

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

    @deprecated(
        reason="Use get_aspect_v2 instead which makes aspect_type_name truly optional"
    )
    def get_aspect(
        self,
        entity_urn: str,
        aspect: str,
        aspect_type_name: Optional[str],
        aspect_type: Type[Aspect],
    ) -> Optional[Aspect]:
        return self.get_aspect_v2(
            entity_urn=entity_urn,
            aspect=aspect,
            aspect_type=aspect_type,
            aspect_type_name=aspect_type_name,
        )

    def get_aspect_v2(
        self,
        entity_urn: str,
        aspect_type: Type[Aspect],
        aspect: str,
        aspect_type_name: Optional[str] = None,
    ) -> Optional[Aspect]:
        """
        Get an aspect for an entity.

        :param str entity_urn: The urn of the entity
        :param Type[Aspect] aspect_type: The type class of the aspect being requested (e.g. datahub.metadata.schema_classes.DatasetProperties)
        :param str aspect: The name of the aspect being requested (e.g. schemaMetadata, datasetProperties, etc.)
        :param Optional[str] aspect_type_name: The fully qualified classname of the aspect being requested. Typically not needed and extracted automatically from the class directly. (e.g. com.linkedin.common.DatasetProperties)
        :return: the Aspect as a dictionary if present, None if no aspect was found (HTTP status 404)
        :rtype: Optional[Aspect]
        :raises HttpError: if the HTTP response is not a 200 or a 404
        """
        url: str = f"{self._gms_server}/aspects/{Urn.url_encode(entity_urn)}?aspect={aspect}&version=0"
        response = self._session.get(url)
        if response.status_code == 404:
            # not found
            return None
        response.raise_for_status()
        response_json = response.json()
        if not aspect_type_name:
            record_schema: RecordSchema = aspect_type.__getattribute__(
                aspect_type, "RECORD_SCHEMA"
            )
            if not record_schema:
                logger.warning(
                    f"Failed to infer type name of the aspect from the aspect type class {aspect_type}. Please provide an aspect_type_name. Continuing, but this will fail."
                )
            else:
                aspect_type_name = record_schema.fullname.replace(".pegasus2avro", "")
        aspect_json = response_json.get("aspect", {}).get(aspect_type_name)
        if aspect_json:
            # need to apply a transform to the response to match rest.li and avro serialization
            post_json_obj = post_json_transform(aspect_json)
            return aspect_type.from_obj(post_json_obj)
        else:
            raise OperationalError(
                f"Failed to find {aspect_type_name} in response {response_json}"
            )

    def get_config(self) -> Dict[str, Any]:
        return self._get_generic(f"{self.config.server}/config")

    def get_ownership(self, entity_urn: str) -> Optional[OwnershipClass]:
        return self.get_aspect_v2(
            entity_urn=entity_urn,
            aspect="ownership",
            aspect_type=OwnershipClass,
        )

    def get_schema_metadata(self, entity_urn: str) -> Optional[SchemaMetadataClass]:
        return self.get_aspect_v2(
            entity_urn=entity_urn,
            aspect="schemaMetadata",
            aspect_type=SchemaMetadataClass,
        )

    def get_domain_properties(self, entity_urn: str) -> Optional[DomainPropertiesClass]:
        return self.get_aspect_v2(
            entity_urn=entity_urn,
            aspect="domainProperties",
            aspect_type=DomainPropertiesClass,
        )

    def get_dataset_properties(
        self, entity_urn: str
    ) -> Optional[DatasetPropertiesClass]:
        return self.get_aspect_v2(
            entity_urn=entity_urn,
            aspect="datasetProperties",
            aspect_type=DatasetPropertiesClass,
        )

    def get_tags(self, entity_urn: str) -> Optional[GlobalTagsClass]:
        return self.get_aspect_v2(
            entity_urn=entity_urn,
            aspect="globalTags",
            aspect_type=GlobalTagsClass,
        )

    def get_glossary_terms(self, entity_urn: str) -> Optional[GlossaryTermsClass]:
        return self.get_aspect_v2(
            entity_urn=entity_urn,
            aspect="glossaryTerms",
            aspect_type=GlossaryTermsClass,
        )

    def get_domain(self, entity_urn: str) -> Optional[DomainsClass]:
        return self.get_aspect_v2(
            entity_urn=entity_urn,
            aspect="domains",
            aspect_type=DomainsClass,
        )

    def get_browse_path(self, entity_urn: str) -> Optional[BrowsePathsClass]:
        return self.get_aspect_v2(
            entity_urn=entity_urn,
            aspect="browsePaths",
            aspect_type=BrowsePathsClass,
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
            "entity": guess_entity_type(entity_urn),
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

    def get_aspects_for_entity(
        self,
        entity_urn: str,
        aspects: List[str],
        aspect_types: List[Type[Aspect]],
    ) -> Optional[Dict[str, Optional[Aspect]]]:
        """
        Get multiple aspects for an entity. To get a single aspect for an entity, use the `get_aspect_v2` method.
        Warning: Do not use this method to determine if an entity exists!
        This method will always return an entity, even if it doesn't exist. This is an issue with how DataHub server
        responds to these calls, and will be fixed automatically when the server-side issue is fixed.

        :param str entity_urn: The urn of the entity
        :param List[Type[Aspect]] aspect_type_list: List of aspect type classes being requested (e.g. [datahub.metadata.schema_classes.DatasetProperties])
        :param List[str] aspects_list: List of aspect names being requested (e.g. [schemaMetadata, datasetProperties])
        :return: Optionally, a map of aspect_name to aspect_value as a dictionary if present, aspect_value will be set to None if that aspect was not found. Returns None on HTTP status 404.
        :rtype: Optional[Dict[str, Optional[Aspect]]]
        :raises HttpError: if the HTTP response is not a 200 or a 404
        """
        assert len(aspects) == len(
            aspect_types
        ), f"number of aspects requested ({len(aspects)}) should be the same as number of aspect types provided ({len(aspect_types)})"
        aspects_list = ",".join(aspects)
        url: str = f"{self._gms_server}/entitiesV2/{Urn.url_encode(entity_urn)}?aspects=List({aspects_list})"

        response = self._session.get(url)
        if response.status_code == 404:
            # not found
            return None
        response.raise_for_status()
        response_json = response.json()

        result: Dict[str, Optional[Aspect]] = {}
        for aspect_type in aspect_types:
            record_schema: RecordSchema = aspect_type.__getattribute__(
                aspect_type, "RECORD_SCHEMA"
            )
            if not record_schema:
                logger.warning(
                    f"Failed to infer type name of the aspect from the aspect type class {aspect_type}. Continuing, but this will fail."
                )
            else:
                aspect_type_name = record_schema.props["Aspect"]["name"]
            aspect_json = response_json.get("aspects", {}).get(aspect_type_name)
            if aspect_json:
                # need to apply a transform to the response to match rest.li and avro serialization
                post_json_obj = post_json_transform(aspect_json)
                result[aspect_type_name] = aspect_type.from_obj(post_json_obj["value"])
            else:
                result[aspect_type_name] = None

        return result

    def _get_search_endpoint(self):
        return f"{self.config.server}/entities?action=search"

    def _get_aspect_count_endpoint(self):
        return f"{self.config.server}/aspects?action=getCount"

    def get_domain_urn_by_name(self, domain_name: str) -> Optional[str]:
        """Retrieve a domain urn based on its name. Returns None if there is no match found"""

        filters = []
        filter_criteria = [
            {
                "field": "name",
                "value": domain_name,
                "condition": "EQUAL",
            }
        ]

        filters.append({"and": filter_criteria})
        search_body = {
            "input": "*",
            "entity": "domain",
            "start": 0,
            "count": 10,
            "filter": {"or": filters},
        }
        results: Dict = self._post_generic(self._get_search_endpoint(), search_body)
        num_entities = results.get("value", {}).get("numEntities", 0)
        if num_entities > 1:
            logger.warning(
                f"Got {num_entities} results for domain name {domain_name}. Will return the first match."
            )
        entities_yielded: int = 0
        entities = []
        for x in results["value"]["entities"]:
            entities_yielded += 1
            logger.debug(f"yielding {x['entity']}")
            entities.append(x["entity"])
        return entities[0] if entities_yielded else None

    def get_container_urns_by_filter(
        self,
        env: Optional[str] = None,
        search_query: str = "*",
    ) -> Iterable[str]:
        """Return container urns that match based on query"""
        url = self._get_search_endpoint()

        container_filters = []
        for container_subtype in ["Database", "Schema", "Project", "Dataset"]:
            filter_criteria = []

            filter_criteria.append(
                {
                    "field": "customProperties",
                    "value": f"instance={env}",
                    "condition": "EQUAL",
                }
            )

            filter_criteria.append(
                {
                    "field": "typeNames",
                    "value": container_subtype,
                    "condition": "EQUAL",
                }
            )
            container_filters.append({"and": filter_criteria})
        search_body = {
            "input": search_query,
            "entity": "container",
            "start": 0,
            "count": 10000,
            "filter": {"or": container_filters},
        }
        results: Dict = self._post_generic(url, search_body)
        num_entities = results["value"]["numEntities"]
        logger.debug(f"Matched {num_entities} containers")
        entities_yielded: int = 0
        for x in results["value"]["entities"]:
            entities_yielded += 1
            logger.debug(f"yielding {x['entity']}")
            yield x["entity"]

    def get_search_results(
        self, start: int = 0, count: int = 1, entity: str = "dataset"
    ) -> Dict:
        search_body = {"input": "*", "entity": entity, "start": start, "count": count}
        results: Dict = self._post_generic(self._get_search_endpoint(), search_body)
        return results

    def get_aspect_counts(self, aspect: str, urn_like: Optional[str] = None) -> int:
        args = {"aspect": aspect}
        if urn_like is not None:
            args["urnLike"] = urn_like
        results = self._post_generic(self._get_aspect_count_endpoint(), args)
        return results["value"]
