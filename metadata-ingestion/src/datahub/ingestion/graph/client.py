import json
import logging
import textwrap
import time
from dataclasses import dataclass
from enum import Enum
from json.decoder import JSONDecodeError
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Type, Union

from avro.schema import RecordSchema
from deprecated import deprecated
from requests.adapters import Response
from requests.models import HTTPError
from typing_extensions import Literal

from datahub.cli.cli_utils import get_boolean_env_variable, get_url_and_token
from datahub.configuration.common import ConfigModel, GraphError, OperationalError
from datahub.emitter.aspect import TIMESERIES_ASPECT_MAP
from datahub.emitter.mce_builder import Aspect, make_data_platform_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.serialization_helper import post_json_transform
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.metadata.schema_classes import (
    ASPECT_NAME_MAP,
    AspectBag,
    BrowsePathsClass,
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    DomainPropertiesClass,
    DomainsClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    OwnershipClass,
    SchemaMetadataClass,
    StatusClass,
    SystemMetadataClass,
    TelemetryClientIdClass,
)
from datahub.utilities.urns.urn import Urn, guess_entity_type

if TYPE_CHECKING:
    from datahub.ingestion.source.state.entity_removal_state import (
        GenericCheckpointState,
    )


logger = logging.getLogger(__name__)


telemetry_enabled = get_boolean_env_variable("DATAHUB_TELEMETRY_ENABLED", True)


class DatahubClientConfig(ConfigModel):
    """Configuration class for holding connectivity to datahub gms"""

    server: str = "http://localhost:8080"
    token: Optional[str]
    timeout_sec: Optional[int]
    retry_status_codes: Optional[List[int]]
    retry_max_times: Optional[int]
    extra_headers: Optional[Dict[str, str]]
    ca_certificate_path: Optional[str]
    max_threads: int = 15
    disable_ssl_verification: bool = False


# Alias for backwards compatibility.
# DEPRECATION: Remove in v0.10.2.
DataHubGraphConfig = DatahubClientConfig


def _graphql_entity_type(entity_type: str) -> str:
    """Convert the entity types into GraphQL "EntityType" enum values."""

    # Hard-coded special cases.
    if entity_type == "corpuser":
        return "CORP_USER"

    # Convert camelCase to UPPER_UNDERSCORE.
    entity_type = (
        "".join(["_" + c.lower() if c.isupper() else c for c in entity_type])
        .lstrip("_")
        .upper()
    )

    # Strip the "DATA_HUB_" prefix.
    if entity_type.startswith("DATA_HUB_"):
        entity_type = entity_type[len("DATA_HUB_") :]

    return entity_type


class DataHubGraph(DatahubRestEmitter):
    def __init__(self, config: DatahubClientConfig) -> None:
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
            disable_ssl_verification=self.config.disable_ssl_verification,
        )
        self.test_connection()
        if not telemetry_enabled:
            self.server_id = "missing"
            return
        try:
            client_id: Optional[TelemetryClientIdClass] = self.get_aspect(
                "urn:li:telemetry:clientId", TelemetryClientIdClass
            )
            self.server_id = client_id.clientId if client_id else "missing"
        except Exception as e:
            self.server_id = "missing"
            logger.debug(f"Failed to get server id due to {e}")

    def _get_generic(self, url: str, params: Optional[Dict] = None) -> Dict:
        try:
            response = self._session.get(url, params=params)
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

    def get_aspect(
        self,
        entity_urn: str,
        aspect_type: Type[Aspect],
        version: int = 0,
    ) -> Optional[Aspect]:
        """
        Get an aspect for an entity.

        :param entity_urn: The urn of the entity
        :param aspect_type: The type class of the aspect being requested (e.g. datahub.metadata.schema_classes.DatasetProperties)
        :param version: The version of the aspect to retrieve. The default of 0 means latest. Versions > 0 go from oldest to newest, so 1 is the oldest.
        :return: the Aspect as a dictionary if present, None if no aspect was found (HTTP status 404)

        :raises TypeError: if the aspect type is a timeseries aspect
        :raises HttpError: if the HTTP response is not a 200 or a 404
        """

        aspect = aspect_type.ASPECT_NAME
        if aspect in TIMESERIES_ASPECT_MAP:
            raise TypeError(
                'Cannot get a timeseries aspect using "get_aspect". Use "get_latest_timeseries_value" instead.'
            )

        url: str = f"{self._gms_server}/aspects/{Urn.url_encode(entity_urn)}?aspect={aspect}&version={version}"
        response = self._session.get(url)
        if response.status_code == 404:
            # not found
            return None
        response.raise_for_status()
        response_json = response.json()

        # Figure out what field to look in.
        record_schema: RecordSchema = aspect_type.RECORD_SCHEMA
        aspect_type_name = record_schema.fullname.replace(".pegasus2avro", "")

        # Deserialize the aspect json into the aspect type.
        aspect_json = response_json.get("aspect", {}).get(aspect_type_name)
        if aspect_json is not None:
            # need to apply a transform to the response to match rest.li and avro serialization
            post_json_obj = post_json_transform(aspect_json)
            return aspect_type.from_obj(post_json_obj)
        else:
            raise GraphError(
                f"Failed to find {aspect_type_name} in response {response_json}"
            )

    @deprecated(reason="Use get_aspect instead which makes aspect string name optional")
    def get_aspect_v2(
        self,
        entity_urn: str,
        aspect_type: Type[Aspect],
        aspect: str,
        aspect_type_name: Optional[str] = None,
        version: int = 0,
    ) -> Optional[Aspect]:
        assert aspect_type.ASPECT_NAME == aspect
        return self.get_aspect(
            entity_urn=entity_urn,
            aspect_type=aspect_type,
            version=version,
        )

    def get_config(self) -> Dict[str, Any]:
        return self._get_generic(f"{self.config.server}/config")

    def get_ownership(self, entity_urn: str) -> Optional[OwnershipClass]:
        return self.get_aspect(entity_urn=entity_urn, aspect_type=OwnershipClass)

    def get_schema_metadata(self, entity_urn: str) -> Optional[SchemaMetadataClass]:
        return self.get_aspect(entity_urn=entity_urn, aspect_type=SchemaMetadataClass)

    def get_domain_properties(self, entity_urn: str) -> Optional[DomainPropertiesClass]:
        return self.get_aspect(entity_urn=entity_urn, aspect_type=DomainPropertiesClass)

    def get_dataset_properties(
        self, entity_urn: str
    ) -> Optional[DatasetPropertiesClass]:
        return self.get_aspect(
            entity_urn=entity_urn, aspect_type=DatasetPropertiesClass
        )

    def get_tags(self, entity_urn: str) -> Optional[GlobalTagsClass]:
        return self.get_aspect(entity_urn=entity_urn, aspect_type=GlobalTagsClass)

    def get_glossary_terms(self, entity_urn: str) -> Optional[GlossaryTermsClass]:
        return self.get_aspect(entity_urn=entity_urn, aspect_type=GlossaryTermsClass)

    def get_domain(self, entity_urn: str) -> Optional[DomainsClass]:
        return self.get_aspect(entity_urn=entity_urn, aspect_type=DomainsClass)

    def get_browse_path(self, entity_urn: str) -> Optional[BrowsePathsClass]:
        return self.get_aspect(
            entity_urn=entity_urn,
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
            "aspect": aspect_type.ASPECT_NAME,
            "limit": 1,
            "filter": {"or": [{"and": filter_criteria}]},
        }
        end_point = f"{self.config.server}/aspects?action=getTimeseriesAspectValues"
        resp: Dict = self._post_generic(end_point, query_body)
        values: list = resp.get("value", {}).get("values")
        if values:
            assert len(values) == 1, len(values)
            aspect_json: str = values[0].get("aspect", {}).get("value")
            if aspect_json:
                return aspect_type.from_obj(json.loads(aspect_json), tuples=False)
            else:
                raise GraphError(
                    f"Failed to find {aspect_type} in response {aspect_json}"
                )
        return None

    def get_entity_raw(
        self, entity_urn: str, aspects: Optional[List[str]] = None
    ) -> Dict:
        endpoint: str = f"{self.config.server}/entitiesV2/{Urn.url_encode(entity_urn)}"
        if aspects is not None:
            assert aspects, "if provided, aspects must be a non-empty list"
            endpoint = f"{endpoint}?aspects=List(" + ",".join(aspects) + ")"

        response = self._session.get(endpoint)
        response.raise_for_status()
        return response.json()

    @deprecated(
        reason="Use get_aspect for a single aspect or get_entity_semityped for a full entity."
    )
    def get_aspects_for_entity(
        self,
        entity_urn: str,
        aspects: List[str],
        aspect_types: List[Type[Aspect]],
    ) -> Dict[str, Optional[Aspect]]:
        """
        Get multiple aspects for an entity.

        Deprecated in favor of `get_aspect` (single aspect) or `get_entity_semityped` (full
        entity without manually specifying a list of aspects).

        Warning: Do not use this method to determine if an entity exists!
        This method will always return an entity, even if it doesn't exist. This is an issue with how DataHub server
        responds to these calls, and will be fixed automatically when the server-side issue is fixed.

        :param str entity_urn: The urn of the entity
        :param List[Type[Aspect]] aspect_type_list: List of aspect type classes being requested (e.g. [datahub.metadata.schema_classes.DatasetProperties])
        :param List[str] aspects_list: List of aspect names being requested (e.g. [schemaMetadata, datasetProperties])
        :return: Optionally, a map of aspect_name to aspect_value as a dictionary if present, aspect_value will be set to None if that aspect was not found. Returns None on HTTP status 404.
        :raises HttpError: if the HTTP response is not a 200
        """
        assert len(aspects) == len(
            aspect_types
        ), f"number of aspects requested ({len(aspects)}) should be the same as number of aspect types provided ({len(aspect_types)})"

        # TODO: generate aspects list from type classes
        response_json = self.get_entity_raw(entity_urn, aspects)

        result: Dict[str, Optional[Aspect]] = {}
        for aspect_type in aspect_types:
            aspect_type_name = aspect_type.get_aspect_name()

            aspect_json = response_json.get("aspects", {}).get(aspect_type_name)
            if aspect_json:
                # need to apply a transform to the response to match rest.li and avro serialization
                post_json_obj = post_json_transform(aspect_json)
                result[aspect_type_name] = aspect_type.from_obj(post_json_obj["value"])
            else:
                result[aspect_type_name] = None

        return result

    def get_entity_semityped(self, entity_urn: str) -> AspectBag:
        """Get all non-timeseries aspects for an entity (experimental).

        This method is called "semityped" because it returns aspects as a dictionary of
        properly typed objects. While the returned dictionary is constrained using a TypedDict,
        the return type is still fairly loose.

        Warning: Do not use this method to determine if an entity exists! This method will always return
        something, even if the entity doesn't actually exist in DataHub.

        :param entity_urn: The urn of the entity
        :returns: A dictionary of aspect name to aspect value. If an aspect is not found, it will
            not be present in the dictionary. The entity's key aspect will always be present.
        """

        response_json = self.get_entity_raw(entity_urn)

        # Now, we parse the response into proper aspect objects.
        result: AspectBag = {}
        for aspect_name, aspect_json in response_json.get("aspects", {}).items():
            aspect_type = ASPECT_NAME_MAP.get(aspect_name)
            if aspect_type is None:
                logger.warning(f"Ignoring unknown aspect type {aspect_name}")
                continue

            post_json_obj = post_json_transform(aspect_json)
            aspect_value = aspect_type.from_obj(post_json_obj["value"])
            result[aspect_name] = aspect_value  # type: ignore

        return result

    @property
    def _search_endpoint(self):
        return f"{self.config.server}/entities?action=search"

    @property
    def _relationships_endpoint(self):
        return f"{self.config.server}/openapi/relationships/v1/"

    @property
    def _aspect_count_endpoint(self):
        return f"{self.config.server}/aspects?action=getCount"

    @property
    def _scroll_across_entities_endpoint(self):
        return f"{self.config.server}/entities?action=scrollAcrossEntities"

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
        results: Dict = self._post_generic(self._search_endpoint, search_body)
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
        url = self._search_endpoint

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

    def get_urns_by_filter(
        self,
        *,
        entity_types: Optional[List[str]] = None,
        platform: Optional[str] = None,
        batch_size: int = 10000,
    ) -> Iterable[str]:
        """Fetch all urns that match the given filters.

        Filters are combined conjunctively. If multiple filters are specified, the results will match all of them.
        Note that specifying a platform filter will automatically exclude all entity types that do not have a platform.

        :param entity_types: List of entity types to include. If None, all entity types will be returned.
        :param platform: Platform to filter on. If None, all platforms will be returned.
        """

        types: Optional[List[str]] = None
        if entity_types is not None:
            if not entity_types:
                raise ValueError("entity_types cannot be an empty list")

            types = [_graphql_entity_type(entity_type) for entity_type in entity_types]

        # Does not filter on env, because env is missing in dashboard / chart urns and custom properties
        # For containers, use { field: "customProperties", values: ["instance=env}"], condition:EQUAL }
        # For others, use { field: "origin", values: ["env"], condition:EQUAL }

        andFilters = []
        if platform:
            andFilters += [
                {
                    "field": "platform.keyword",
                    "values": [make_data_platform_urn(platform)],
                    "condition": "EQUAL",
                }
            ]
        orFilters = [{"and": andFilters}]

        query = textwrap.dedent(
            """
            query scrollUrnsWithFilters(
                $types: [EntityType!],
                $orFilters: [AndFilterInput!],
                $batchSize: Int!,
                $scrollId: String) {

                scrollAcrossEntities(input: {
                    query: "*",
                    count: $batchSize,
                    scrollId: $scrollId,
                    types: $types,
                    orFilters: $orFilters,
                    searchFlags: { skipHighlighting: true }
                }) {
                    nextScrollId
                    searchResults {
                        entity {
                            urn
                        }
                    }
                }
            }
            """
        )

        # Set scroll_id to False to enter while loop
        scroll_id: Union[Literal[False], str, None] = False
        while scroll_id is not None:
            response = self.execute_graphql(
                query,
                variables={
                    "types": types,
                    "orFilters": orFilters,
                    "batchSize": batch_size,
                    "scrollId": scroll_id or None,
                },
            )
            data = response["scrollAcrossEntities"]
            scroll_id = data["nextScrollId"]
            for entry in data["searchResults"]:
                yield entry["entity"]["urn"]

    def get_latest_pipeline_checkpoint(
        self, pipeline_name: str, platform: str
    ) -> Optional[Checkpoint["GenericCheckpointState"]]:
        from datahub.ingestion.source.state.entity_removal_state import (
            GenericCheckpointState,
        )
        from datahub.ingestion.source.state.stale_entity_removal_handler import (
            StaleEntityRemovalHandler,
        )
        from datahub.ingestion.source.state_provider.datahub_ingestion_checkpointing_provider import (
            DatahubIngestionCheckpointingProvider,
        )

        checkpoint_provider = DatahubIngestionCheckpointingProvider(self, "graph")
        job_name = StaleEntityRemovalHandler.compute_job_id(platform)

        raw_checkpoint = checkpoint_provider.get_latest_checkpoint(
            pipeline_name, job_name
        )
        if not raw_checkpoint:
            return None

        return Checkpoint.create_from_checkpoint_aspect(
            job_name=job_name,
            checkpoint_aspect=raw_checkpoint,
            state_class=GenericCheckpointState,
        )

    def get_search_results(
        self, start: int = 0, count: int = 1, entity: str = "dataset"
    ) -> Dict:
        search_body = {"input": "*", "entity": entity, "start": start, "count": count}
        results: Dict = self._post_generic(self._search_endpoint, search_body)
        return results

    def get_aspect_counts(self, aspect: str, urn_like: Optional[str] = None) -> int:
        args = {"aspect": aspect}
        if urn_like is not None:
            args["urnLike"] = urn_like
        results = self._post_generic(self._aspect_count_endpoint, args)
        return results["value"]

    def execute_graphql(self, query: str, variables: Optional[Dict] = None) -> Dict:
        url = f"{self.config.server}/api/graphql"
        body: Dict = {
            "query": query,
        }
        if variables:
            body["variables"] = variables

        result = self._post_generic(url, body)
        if result.get("errors"):
            raise GraphError(f"Error executing graphql query: {result['errors']}")

        return result["data"]

    class RelationshipDirection(str, Enum):
        INCOMING = "INCOMING"
        OUTGOING = "OUTGOING"

    @dataclass
    class RelatedEntity:
        urn: str
        relationship_type: str

    def get_related_entities(
        self,
        entity_urn: str,
        relationship_types: List[str],
        direction: RelationshipDirection,
    ) -> Iterable[RelatedEntity]:
        relationship_endpoint = self._relationships_endpoint
        done = False
        start = 0
        while not done:
            response = self._get_generic(
                url=relationship_endpoint,
                params={
                    "urn": entity_urn,
                    "direction": direction,
                    "relationshipTypes": relationship_types,
                    "start": start,
                },
            )
            for related_entity in response.get("entities", []):
                yield DataHubGraph.RelatedEntity(
                    urn=related_entity["urn"],
                    relationship_type=related_entity["relationshipType"],
                )
            done = response.get("count", 0) == 0 or response.get("count", 0) < len(
                response.get("entities", [])
            )
            start = start + response.get("count", 0)

    def soft_delete_urn(
        self,
        urn: str,
        run_id: str = "soft-delete-urns",
    ) -> None:
        timestamp = int(time.time() * 1000)
        self.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=StatusClass(removed=True),
                systemMetadata=SystemMetadataClass(
                    runId=run_id, lastObserved=timestamp
                ),
            )
        )


def get_default_graph() -> DataHubGraph:
    (url, token) = get_url_and_token()
    return DataHubGraph(DatahubClientConfig(server=url, token=token))
