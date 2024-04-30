import contextlib
import enum
import functools
import json
import logging
import textwrap
import time
from dataclasses import dataclass
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

from avro.schema import RecordSchema
from deprecated import deprecated
from requests.models import HTTPError

from datahub.cli.cli_utils import get_url_and_token
from datahub.configuration.common import ConfigModel, GraphError, OperationalError
from datahub.emitter.aspect import TIMESERIES_ASPECT_MAP
from datahub.emitter.mce_builder import DEFAULT_ENV, Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.serialization_helper import post_json_transform
from datahub.ingestion.graph.filters import (
    RemovedStatusFilter,
    SearchFilterRule,
    generate_filter,
)
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.schema_classes import (
    ASPECT_NAME_MAP,
    KEY_ASPECTS,
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
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.urns.urn import Urn, guess_entity_type

if TYPE_CHECKING:
    from datahub.ingestion.sink.datahub_rest import DatahubRestSink
    from datahub.ingestion.source.state.entity_removal_state import (
        GenericCheckpointState,
    )
    from datahub.sql_parsing.schema_resolver import (
        GraphQLSchemaMetadata,
        SchemaResolver,
    )
    from datahub.sql_parsing.sqlglot_lineage import SqlParsingResult


logger = logging.getLogger(__name__)
_MISSING_SERVER_ID = "missing"
_GRAPH_DUMMY_RUN_ID = "__datahub-graph-client"


class DatahubClientConfig(ConfigModel):
    """Configuration class for holding connectivity to datahub gms"""

    server: str = "http://localhost:8080"
    token: Optional[str] = None
    timeout_sec: Optional[int] = None
    retry_status_codes: Optional[List[int]] = None
    retry_max_times: Optional[int] = None
    extra_headers: Optional[Dict[str, str]] = None
    ca_certificate_path: Optional[str] = None
    client_certificate_path: Optional[str] = None
    disable_ssl_verification: bool = False


# Alias for backwards compatibility.
# DEPRECATION: Remove in v0.10.2.
DataHubGraphConfig = DatahubClientConfig


@dataclass
class RelatedEntity:
    urn: str
    relationship_type: str
    via: Optional[str] = None


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
            client_certificate_path=self.config.client_certificate_path,
            disable_ssl_verification=self.config.disable_ssl_verification,
        )

        self.server_id = _MISSING_SERVER_ID

    def test_connection(self) -> None:
        super().test_connection()

        # Cache the server id for telemetry.
        from datahub.telemetry.telemetry import telemetry_instance

        if not telemetry_instance.enabled:
            self.server_id = _MISSING_SERVER_ID
            return
        try:
            client_id: Optional[TelemetryClientIdClass] = self.get_aspect(
                "urn:li:telemetry:clientId", TelemetryClientIdClass
            )
            self.server_id = client_id.clientId if client_id else _MISSING_SERVER_ID
        except Exception as e:
            self.server_id = _MISSING_SERVER_ID
            logger.debug(f"Failed to get server id due to {e}")

    @classmethod
    def from_emitter(cls, emitter: DatahubRestEmitter) -> "DataHubGraph":
        return cls(
            DatahubClientConfig(
                server=emitter._gms_server,
                token=emitter._token,
                timeout_sec=emitter._read_timeout_sec,
                retry_status_codes=emitter._retry_status_codes,
                retry_max_times=emitter._retry_max_times,
                extra_headers=emitter._session.headers,
                disable_ssl_verification=emitter._session.verify is False,
                # TODO: Support these headers.
                # ca_certificate_path=emitter._ca_certificate_path,
                # client_certificate_path=emitter._client_certificate_path,
            )
        )

    def _send_restli_request(self, method: str, url: str, **kwargs: Any) -> Dict:
        try:
            response = self._session.request(method, url, **kwargs)
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

    def _get_generic(self, url: str, params: Optional[Dict] = None) -> Dict:
        return self._send_restli_request("GET", url, params=params)

    def _post_generic(self, url: str, payload_dict: Dict) -> Dict:
        return self._send_restli_request("POST", url, json=payload_dict)

    @contextlib.contextmanager
    def make_rest_sink(
        self, run_id: str = _GRAPH_DUMMY_RUN_ID
    ) -> Iterator["DatahubRestSink"]:
        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.sink.datahub_rest import (
            DatahubRestSink,
            DatahubRestSinkConfig,
            SyncOrAsync,
        )

        # This is a bit convoluted - this DataHubGraph class is a subclass of DatahubRestEmitter,
        # but initializing the rest sink creates another rest emitter.
        # TODO: We should refactor out the multithreading functionality of the sink
        # into a separate class that can be used by both the sink and the graph client
        # e.g. a DatahubBulkRestEmitter that both the sink and the graph client use.
        sink_config = DatahubRestSinkConfig(
            **self.config.dict(), mode=SyncOrAsync.ASYNC
        )

        with DatahubRestSink(PipelineContext(run_id=run_id), sink_config) as sink:
            yield sink
        if sink.report.failures:
            raise OperationalError(
                f"Failed to emit {len(sink.report.failures)} records",
                info=sink.report.as_obj(),
            )

    def emit_all(
        self,
        items: Iterable[
            Union[
                MetadataChangeEvent,
                MetadataChangeProposal,
                MetadataChangeProposalWrapper,
            ]
        ],
        run_id: str = _GRAPH_DUMMY_RUN_ID,
    ) -> None:
        """Emit all items in the iterable using multiple threads."""

        with self.make_rest_sink(run_id=run_id) as sink:
            for item in items:
                sink.emit_async(item)
        if sink.report.failures:
            raise OperationalError(
                f"Failed to emit {len(sink.report.failures)} records",
                info=sink.report.as_obj(),
            )

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

    @deprecated(
        reason='Use get_urns_by_filter(entity_types=["container"], ...) instead'
    )
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

    def _bulk_fetch_schema_info_by_filter(
        self,
        *,
        platform: Optional[str] = None,
        platform_instance: Optional[str] = None,
        env: Optional[str] = None,
        query: Optional[str] = None,
        container: Optional[str] = None,
        status: RemovedStatusFilter = RemovedStatusFilter.NOT_SOFT_DELETED,
        batch_size: int = 100,
        extraFilters: Optional[List[SearchFilterRule]] = None,
    ) -> Iterable[Tuple[str, "GraphQLSchemaMetadata"]]:
        """Fetch schema info for datasets that match all of the given filters.

        :return: An iterable of (urn, schema info) tuple that match the filters.
        """
        types = [_graphql_entity_type("dataset")]

        # Add the query default of * if no query is specified.
        query = query or "*"

        orFilters = generate_filter(
            platform, platform_instance, env, container, status, extraFilters
        )

        graphql_query = textwrap.dedent(
            """
            query scrollUrnsWithFilters(
                $types: [EntityType!],
                $query: String!,
                $orFilters: [AndFilterInput!],
                $batchSize: Int!,
                $scrollId: String) {

                scrollAcrossEntities(input: {
                    query: $query,
                    count: $batchSize,
                    scrollId: $scrollId,
                    types: $types,
                    orFilters: $orFilters,
                    searchFlags: {
                        skipHighlighting: true
                        skipAggregates: true
                    }
                }) {
                    nextScrollId
                    searchResults {
                        entity {
                            urn
                            ... on Dataset {
                                schemaMetadata(version: 0) {
                                    fields {
                                        fieldPath
                                        nativeDataType
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """
        )

        variables = {
            "types": types,
            "query": query,
            "orFilters": orFilters,
            "batchSize": batch_size,
        }

        for entity in self._scroll_across_entities(graphql_query, variables):
            if entity.get("schemaMetadata"):
                yield entity["urn"], entity["schemaMetadata"]

    def get_urns_by_filter(
        self,
        *,
        entity_types: Optional[List[str]] = None,
        platform: Optional[str] = None,
        platform_instance: Optional[str] = None,
        env: Optional[str] = None,
        query: Optional[str] = None,
        container: Optional[str] = None,
        status: RemovedStatusFilter = RemovedStatusFilter.NOT_SOFT_DELETED,
        batch_size: int = 10000,
        extraFilters: Optional[List[SearchFilterRule]] = None,
    ) -> Iterable[str]:
        """Fetch all urns that match all of the given filters.

        Filters are combined conjunctively. If multiple filters are specified, the results will match all of them.
        Note that specifying a platform filter will automatically exclude all entity types that do not have a platform.
        The same goes for the env filter.

        :param entity_types: List of entity types to include. If None, all entity types will be returned.
        :param platform: Platform to filter on. If None, all platforms will be returned.
        :param platform_instance: Platform instance to filter on. If None, all platform instances will be returned.
        :param env: Environment (e.g. PROD, DEV) to filter on. If None, all environments will be returned.
        :param query: Query string to filter on. If None, all entities will be returned.
        :param container: A container urn that entities must be within.
            This works recursively, so it will include entities within sub-containers as well.
            If None, all entities will be returned.
            Note that this requires browsePathV2 aspects (added in 0.10.4+).
        :param status: Filter on the deletion status of the entity. The default is only return non-soft-deleted entities.
        :param extraFilters: Additional filters to apply. If specified, the results will match all of the filters.

        :return: An iterable of urns that match the filters.
        """

        types = self._get_types(entity_types)

        # Add the query default of * if no query is specified.
        query = query or "*"

        # Env filter.
        orFilters = generate_filter(
            platform, platform_instance, env, container, status, extraFilters
        )

        graphql_query = textwrap.dedent(
            """
            query scrollUrnsWithFilters(
                $types: [EntityType!],
                $query: String!,
                $orFilters: [AndFilterInput!],
                $batchSize: Int!,
                $scrollId: String) {

                scrollAcrossEntities(input: {
                    query: $query,
                    count: $batchSize,
                    scrollId: $scrollId,
                    types: $types,
                    orFilters: $orFilters,
                    searchFlags: {
                        skipHighlighting: true
                        skipAggregates: true
                    }
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

        variables = {
            "types": types,
            "query": query,
            "orFilters": orFilters,
            "batchSize": batch_size,
        }

        for entity in self._scroll_across_entities(graphql_query, variables):
            yield entity["urn"]

    def _scroll_across_entities(
        self, graphql_query: str, variables_orig: dict
    ) -> Iterable[dict]:
        variables = variables_orig.copy()
        first_iter = True
        scroll_id: Optional[str] = None
        while first_iter or scroll_id:
            first_iter = False
            variables["scrollId"] = scroll_id

            response = self.execute_graphql(
                graphql_query,
                variables=variables,
            )
            data = response["scrollAcrossEntities"]
            scroll_id = data["nextScrollId"]
            for entry in data["searchResults"]:
                yield entry["entity"]

            if scroll_id:
                logger.debug(
                    f"Scrolling to next scrollAcrossEntities page: {scroll_id}"
                )

    def _get_types(self, entity_types: Optional[List[str]]) -> Optional[List[str]]:
        types: Optional[List[str]] = None
        if entity_types is not None:
            if not entity_types:
                raise ValueError(
                    "entity_types cannot be an empty list; use None for all entities"
                )

            types = [_graphql_entity_type(entity_type) for entity_type in entity_types]
        return types

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

        checkpoint_provider = DatahubIngestionCheckpointingProvider(self)
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

    def execute_graphql(
        self,
        query: str,
        variables: Optional[Dict] = None,
        operation_name: Optional[str] = None,
    ) -> Dict:
        url = f"{self.config.server}/api/graphql"

        body: Dict = {
            "query": query,
        }
        if variables:
            body["variables"] = variables
        if operation_name:
            body["operationName"] = operation_name

        logger.debug(
            f"Executing graphql query: {query} with variables: {json.dumps(variables)}"
        )
        result = self._post_generic(url, body)
        if result.get("errors"):
            raise GraphError(f"Error executing graphql query: {result['errors']}")

        return result["data"]

    class RelationshipDirection(str, enum.Enum):
        # FIXME: Upgrade to enum.StrEnum when we drop support for Python 3.10

        INCOMING = "INCOMING"
        OUTGOING = "OUTGOING"

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
                    "direction": direction.value,
                    "relationshipTypes": relationship_types,
                    "start": start,
                },
            )
            for related_entity in response.get("entities", []):
                yield RelatedEntity(
                    urn=related_entity["urn"],
                    relationship_type=related_entity["relationshipType"],
                    via=related_entity.get("via"),
                )
            done = response.get("count", 0) == 0 or response.get("count", 0) < len(
                response.get("entities", [])
            )
            start = start + response.get("count", 0)

    def exists(self, entity_urn: str) -> bool:
        entity_urn_parsed: Urn = Urn.from_string(entity_urn)
        try:
            key_aspect_class = KEY_ASPECTS.get(entity_urn_parsed.entity_type)
            if key_aspect_class:
                result = self.get_aspect(entity_urn, key_aspect_class)
                return result is not None
            else:
                raise Exception(
                    f"Failed to find key class for entity type {entity_urn_parsed.get_type()} for urn {entity_urn}"
                )
        except Exception as e:
            logger.debug(
                f"Failed to check for existence of urn {entity_urn}", exc_info=e
            )
            raise

    def soft_delete_entity(
        self,
        urn: str,
        run_id: str = _GRAPH_DUMMY_RUN_ID,
        deletion_timestamp: Optional[int] = None,
    ) -> None:
        """Soft-delete an entity by urn.

        Args:
            urn: The urn of the entity to soft-delete.
        """

        assert urn

        deletion_timestamp = deletion_timestamp or int(time.time() * 1000)
        self.emit(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=StatusClass(removed=True),
                systemMetadata=SystemMetadataClass(
                    runId=run_id, lastObserved=deletion_timestamp
                ),
            )
        )

    def hard_delete_entity(
        self,
        urn: str,
    ) -> Tuple[int, int]:
        """Hard delete an entity by urn.

        Args:
            urn: The urn of the entity to hard delete.

        Returns:
            A tuple of (rows_affected, timeseries_rows_affected).
        """

        assert urn

        payload_obj: Dict = {"urn": urn}
        summary = self._post_generic(
            f"{self._gms_server}/entities?action=delete", payload_obj
        ).get("value", {})

        rows_affected: int = summary.get("rows", 0)
        timeseries_rows_affected: int = summary.get("timeseriesRows", 0)
        return rows_affected, timeseries_rows_affected

    def delete_entity(self, urn: str, hard: bool = False) -> None:
        """Delete an entity by urn.

        Args:
            urn: The urn of the entity to delete.
            hard: Whether to hard delete the entity. If False (default), the entity will be soft deleted.
        """

        if hard:
            rows_affected, timeseries_rows_affected = self.hard_delete_entity(urn)
            logger.debug(
                f"Hard deleted entity {urn} with {rows_affected} rows affected and {timeseries_rows_affected} timeseries rows affected"
            )
        else:
            self.soft_delete_entity(urn)
            logger.debug(f"Soft deleted entity {urn}")

    # TODO: Create hard_delete_aspect once we support that in GMS.

    def hard_delete_timeseries_aspect(
        self,
        urn: str,
        aspect_name: str,
        start_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> int:
        """Hard delete timeseries aspects of an entity.

        Args:
            urn: The urn of the entity.
            aspect_name: The name of the timeseries aspect to delete.
            start_time: The start time of the timeseries data to delete. If not specified, defaults to the beginning of time.
            end_time: The end time of the timeseries data to delete. If not specified, defaults to the end of time.

        Returns:
            The number of timeseries rows affected.
        """

        assert urn
        assert aspect_name in TIMESERIES_ASPECT_MAP, "must be a timeseries aspect"

        payload_obj: Dict = {
            "urn": urn,
            "aspectName": aspect_name,
        }
        if start_time:
            payload_obj["startTimeMillis"] = int(start_time.timestamp() * 1000)
        if end_time:
            payload_obj["endTimeMillis"] = int(end_time.timestamp() * 1000)

        summary = self._post_generic(
            f"{self._gms_server}/entities?action=delete", payload_obj
        ).get("value", {})

        timeseries_rows_affected: int = summary.get("timeseriesRows", 0)
        return timeseries_rows_affected

    def delete_references_to_urn(
        self, urn: str, dry_run: bool = False
    ) -> Tuple[int, List[Dict]]:
        """Delete references to a given entity.

        This is useful for cleaning up references to an entity that is about to be deleted.
        For example, when deleting a tag, you might use this to remove that tag from all other
        entities that reference it.

        This does not delete the entity itself.

        Args:
            urn: The urn of the entity to delete references to.
            dry_run: If True, do not actually delete the references, just return the count of
                references and the list of related aspects.

        Returns:
            A tuple of (reference_count, sample of related_aspects).
        """

        assert urn

        payload_obj = {"urn": urn, "dryRun": dry_run}

        response = self._post_generic(
            f"{self._gms_server}/entities?action=deleteReferences", payload_obj
        ).get("value", {})
        reference_count = response.get("total", 0)
        related_aspects = response.get("relatedAspects", [])
        return reference_count, related_aspects

    @functools.lru_cache()
    def _make_schema_resolver(
        self,
        platform: str,
        platform_instance: Optional[str],
        env: str,
        include_graph: bool = True,
    ) -> "SchemaResolver":
        from datahub.sql_parsing.schema_resolver import SchemaResolver

        return SchemaResolver(
            platform=platform,
            platform_instance=platform_instance,
            env=env,
            graph=self if include_graph else None,
        )

    def initialize_schema_resolver_from_datahub(
        self,
        platform: str,
        platform_instance: Optional[str],
        env: str,
        batch_size: int = 100,
    ) -> "SchemaResolver":
        logger.info("Initializing schema resolver")
        schema_resolver = self._make_schema_resolver(
            platform, platform_instance, env, include_graph=False
        )

        logger.info(f"Fetching schemas for platform {platform}, env {env}")
        count = 0
        with PerfTimer() as timer:
            for urn, schema_info in self._bulk_fetch_schema_info_by_filter(
                platform=platform,
                platform_instance=platform_instance,
                env=env,
                batch_size=batch_size,
            ):
                try:
                    schema_resolver.add_graphql_schema_metadata(urn, schema_info)
                    count += 1
                except Exception:
                    logger.warning("Failed to add schema info", exc_info=True)

                if count % 1000 == 0:
                    logger.debug(
                        f"Loaded {count} schema info in {timer.elapsed_seconds()} seconds"
                    )
            logger.info(
                f"Finished loading total {count} schema info in {timer.elapsed_seconds()} seconds"
            )

        logger.info("Finished initializing schema resolver")
        return schema_resolver

    def parse_sql_lineage(
        self,
        sql: str,
        *,
        platform: str,
        platform_instance: Optional[str] = None,
        env: str = DEFAULT_ENV,
        default_db: Optional[str] = None,
        default_schema: Optional[str] = None,
    ) -> "SqlParsingResult":
        from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

        # Cache the schema resolver to make bulk parsing faster.
        schema_resolver = self._make_schema_resolver(
            platform=platform, platform_instance=platform_instance, env=env
        )

        return sqlglot_lineage(
            sql,
            schema_resolver=schema_resolver,
            default_db=default_db,
            default_schema=default_schema,
        )

    def create_tag(self, tag_name: str) -> str:
        graph_query: str = """
            mutation($tag_detail: CreateTagInput!) {
                createTag(input: $tag_detail)
            }
        """

        variables = {
            "tag_detail": {
                "name": tag_name,
                "id": tag_name,
            },
        }

        res = self.execute_graphql(
            query=graph_query,
            variables=variables,
        )

        # return urn
        return res["createTag"]

    def close(self) -> None:
        self._make_schema_resolver.cache_clear()
        super().close()


def get_default_graph() -> DataHubGraph:
    (url, token) = get_url_and_token()
    graph = DataHubGraph(DatahubClientConfig(server=url, token=token))
    graph.test_connection()
    return graph
