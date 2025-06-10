import contextlib
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
    Literal,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from avro.schema import RecordSchema
from pydantic import BaseModel
from requests.models import HTTPError
from typing_extensions import deprecated

from datahub._codegen.aspect import _Aspect
from datahub.cli import config_utils
from datahub.configuration.common import ConfigModel, GraphError, OperationalError
from datahub.emitter.aspect import TIMESERIES_ASPECT_MAP
from datahub.emitter.mce_builder import DEFAULT_ENV, Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import (
    DatahubRestEmitter,
)
from datahub.emitter.serialization_helper import post_json_transform
from datahub.ingestion.graph.config import (
    ClientMode,
    DatahubClientConfig as DatahubClientConfig,
)
from datahub.ingestion.graph.connections import (
    connections_gql,
    get_id_from_connection_urn,
)
from datahub.ingestion.graph.entity_versioning import EntityVersioningAPI
from datahub.ingestion.graph.filters import (
    RawSearchFilter,
    RawSearchFilterRule,
    RemovedStatusFilter,
    generate_filter,
)
from datahub.ingestion.graph.links import make_url_for_urn
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
from datahub.metadata.urns import CorpUserUrn, Urn
from datahub.telemetry.telemetry import telemetry_instance
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.str_enum import StrEnum
from datahub.utilities.urns.urn import guess_entity_type

if TYPE_CHECKING:
    from datahub.ingestion.sink.datahub_rest import (
        DatahubRestSink,
        DatahubRestSinkConfig,
    )
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


# Alias for backwards compatibility.
# DEPRECATION: Remove in v0.10.2.
DataHubGraphConfig = DatahubClientConfig


@dataclass
class RelatedEntity:
    urn: str
    relationship_type: str
    via: Optional[str] = None


def entity_type_to_graphql(entity_type: str) -> str:
    """Convert the entity types into GraphQL "EntityType" enum values."""

    # Hard-coded special cases.
    if entity_type == CorpUserUrn.ENTITY_TYPE:
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


def flexible_entity_type_to_graphql(entity_type: str) -> str:
    if entity_type.upper() == entity_type:
        # Assume that we were passed a graphql EntityType enum value,
        # so no conversion is needed.
        return entity_type
    return entity_type_to_graphql(entity_type)


class DataHubGraph(DatahubRestEmitter, EntityVersioningAPI):
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
            openapi_ingestion=self.config.openapi_ingestion,
            client_mode=config.client_mode,
            datahub_component=config.datahub_component,
        )
        self.server_id: str = _MISSING_SERVER_ID

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

    @property
    def frontend_base_url(self) -> str:
        """Get the public-facing base url of the frontend

        This url can be used to construct links to the frontend. The url will not include a trailing slash.

        Note: Only supported with DataHub Cloud.
        """

        if not self.server_config:
            self.test_connection()

        base_url = self.server_config.raw_config.get("baseUrl")
        if not base_url:
            raise ValueError("baseUrl not found in server config")
        return base_url

    def url_for(self, entity_urn: Union[str, Urn]) -> str:
        """Get the UI url for an entity.

        Note: Only supported with DataHub Cloud.

        Args:
            entity_urn: The urn of the entity to get the url for.

        Returns:
            The public-facing url for the entity.
        """

        return make_url_for_urn(self.frontend_base_url, str(entity_urn))

    @classmethod
    def from_emitter(cls, emitter: DatahubRestEmitter) -> "DataHubGraph":
        session_config = emitter._session_config

        if isinstance(session_config.timeout, tuple):
            # TODO: This is slightly lossy. Eventually, we want to modify the emitter
            # to accept a tuple for timeout_sec, and then we'll be able to remove this.
            timeout_sec: Optional[float] = session_config.timeout[0]
        else:
            timeout_sec = session_config.timeout
        return cls(
            DatahubClientConfig(
                server=emitter._gms_server,
                token=emitter._token,
                timeout_sec=timeout_sec,
                retry_status_codes=session_config.retry_status_codes,
                retry_max_times=session_config.retry_max_times,
                extra_headers=session_config.extra_headers,
                disable_ssl_verification=session_config.disable_ssl_verification,
                ca_certificate_path=session_config.ca_certificate_path,
                client_certificate_path=session_config.client_certificate_path,
                client_mode=session_config.client_mode,
                datahub_component=session_config.datahub_component,
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

    def _make_rest_sink_config(
        self, extra_config: Optional[Dict] = None
    ) -> "DatahubRestSinkConfig":
        from datahub.ingestion.sink.datahub_rest import DatahubRestSinkConfig

        # This is a bit convoluted - this DataHubGraph class is a subclass of DatahubRestEmitter,
        # but initializing the rest sink creates another rest emitter.
        # TODO: We should refactor out the multithreading functionality of the sink
        # into a separate class that can be used by both the sink and the graph client
        # e.g. a DatahubBulkRestEmitter that both the sink and the graph client use.
        return DatahubRestSinkConfig(**self.config.dict(), **(extra_config or {}))

    @contextlib.contextmanager
    def make_rest_sink(
        self,
        run_id: str = _GRAPH_DUMMY_RUN_ID,
        extra_sink_config: Optional[Dict] = None,
    ) -> Iterator["DatahubRestSink"]:
        from datahub.ingestion.api.common import PipelineContext
        from datahub.ingestion.sink.datahub_rest import DatahubRestSink

        sink_config = self._make_rest_sink_config(extra_config=extra_sink_config)
        with DatahubRestSink(PipelineContext(run_id=run_id), sink_config) as sink:
            yield sink
        if sink.report.failures:
            logger.error(
                f"Failed to emit {len(sink.report.failures)} records\n{sink.report.as_string()}"
            )
            raise OperationalError(
                f"Failed to emit {len(sink.report.failures)} records"
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

        # The context manager also ensures that we raise an error if a failure occurs.
        with self.make_rest_sink(run_id=run_id) as sink:
            for item in items:
                sink.emit_async(item)

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

    @deprecated("Use get_aspect instead which makes aspect string name optional")
    def get_aspect_v2(
        self,
        entity_urn: str,
        aspect_type: Type[Aspect],
        aspect: str,
        aspect_type_name: Optional[str] = None,
        version: int = 0,
    ) -> Optional[Aspect]:
        assert aspect == aspect_type.ASPECT_NAME
        return self.get_aspect(
            entity_urn=entity_urn,
            aspect_type=aspect_type,
            version=version,
        )

    def get_config(self) -> Dict[str, Any]:
        return self.server_config.raw_config

    def get_ownership(self, entity_urn: str) -> Optional[OwnershipClass]:
        return self.get_aspect(entity_urn=entity_urn, aspect_type=OwnershipClass)

    def get_schema_metadata(self, entity_urn: str) -> Optional[SchemaMetadataClass]:
        return self.get_aspect(entity_urn=entity_urn, aspect_type=SchemaMetadataClass)

    @deprecated("Use get_aspect directly.")
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

    @functools.lru_cache(maxsize=1)
    def get_domain(self, entity_urn: str) -> Optional[DomainsClass]:
        return self.get_aspect(entity_urn=entity_urn, aspect_type=DomainsClass)

    @deprecated("Use get_aspect directly.")
    def get_browse_path(self, entity_urn: str) -> Optional[BrowsePathsClass]:
        return self.get_aspect(entity_urn=entity_urn, aspect_type=BrowsePathsClass)

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
            {"field": k, "values": [v], "condition": "EQUAL"}
            for k, v in filter_criteria_map.items()
        ]
        filter = {"or": [{"and": filter_criteria}]}

        values = self.get_timeseries_values(
            entity_urn=entity_urn, aspect_type=aspect_type, filter=filter, limit=1
        )
        if not values:
            return None

        assert len(values) == 1, len(values)
        return values[0]

    def get_timeseries_values(
        self,
        entity_urn: str,
        aspect_type: Type[Aspect],
        filter: Dict[str, Any],
        limit: int = 10,
    ) -> List[Aspect]:
        query_body = {
            "urn": entity_urn,
            "entity": guess_entity_type(entity_urn),
            "aspect": aspect_type.ASPECT_NAME,
            "limit": limit,
            "filter": filter,
        }
        end_point = f"{self.config.server}/aspects?action=getTimeseriesAspectValues"
        resp: Dict = self._post_generic(end_point, query_body)

        values: Optional[List] = resp.get("value", {}).get("values")
        aspects: List[Aspect] = []
        for value in values or []:
            aspect_json: str = value.get("aspect", {}).get("value")
            if aspect_json:
                aspects.append(
                    aspect_type.from_obj(json.loads(aspect_json), tuples=False)
                )
            else:
                raise GraphError(
                    f"Failed to find {aspect_type} in response {aspect_json}"
                )
        return aspects

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
        "Use get_aspect for a single aspect or get_entity_semityped for a full entity."
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
        :param aspects: List of aspect names being requested (e.g. [schemaMetadata, datasetProperties])
        :param aspect_types: List of aspect type classes being requested (e.g. [datahub.metadata.schema_classes.DatasetProperties])
        :return: Optionally, a map of aspect_name to aspect_value as a dictionary if present, aspect_value will be set to None if that aspect was not found. Returns None on HTTP status 404.
        :raises HttpError: if the HTTP response is not a 200
        """
        assert len(aspects) == len(aspect_types), (
            f"number of aspects requested ({len(aspects)}) should be the same as number of aspect types provided ({len(aspect_types)})"
        )

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

    def get_entity_as_mcps(
        self, entity_urn: str, aspects: Optional[List[str]] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """Get all non-timeseries aspects for an entity.

        By formatting the entity's aspects as MCPWs, we can also include SystemMetadata.

        Warning: Do not use this method to determine if an entity exists! This method will always return
        something, even if the entity doesn't actually exist in DataHub.

        Args:
            entity_urn: The urn of the entity
            aspects: Optional list of aspect names being requested (e.g. ["schemaMetadata", "datasetProperties"])

        Returns:
            A list of MCPWs.
        """

        response_json = self.get_entity_raw(entity_urn, aspects)

        # Now, we parse the response into proper aspect objects.
        results: List[MetadataChangeProposalWrapper] = []
        for aspect_name, aspect_json in response_json.get("aspects", {}).items():
            aspect_type = ASPECT_NAME_MAP.get(aspect_name)
            if aspect_type is None:
                logger.warning(f"Ignoring unknown aspect type {aspect_name}")
                continue

            post_json_obj = post_json_transform(aspect_json)
            aspect_value = aspect_type.from_obj(post_json_obj["value"])

            system_metadata_raw = post_json_obj.get("systemMetadata")
            system_metadata = None
            if system_metadata_raw:
                system_metadata = SystemMetadataClass.from_obj(system_metadata_raw)

            mcpw = MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=aspect_value,
                systemMetadata=system_metadata,
            )

            results.append(mcpw)

        return results

    def get_entity_semityped(
        self, entity_urn: str, aspects: Optional[List[str]] = None
    ) -> AspectBag:
        """Get (all) non-timeseries aspects for an entity.

        This method is called "semityped" because it returns aspects as a dictionary of
        properly typed objects. While the returned dictionary is constrained using a TypedDict,
        the return type is still fairly loose.

        Warning: Do not use this method to determine if an entity exists! This method will always return
        something, even if the entity doesn't actually exist in DataHub.

        :param entity_urn: The urn of the entity
        :param aspects: Optional list of aspect names being requested (e.g. ["schemaMetadata", "datasetProperties"])
        :returns: A dictionary of aspect name to aspect value. If an aspect is not found, it will
            not be present in the dictionary. The entity's key aspect will always be present.
        """

        mcps = self.get_entity_as_mcps(entity_urn, aspects=aspects)

        result: AspectBag = {}
        for mcp in mcps:
            if mcp.aspect:
                result[mcp.aspect.get_aspect_name()] = mcp.aspect  # type: ignore

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
                "values": [domain_name],
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

    def get_connection_json(self, urn: str) -> Optional[dict]:
        """Retrieve a connection config.

        This is only supported with DataHub Cloud.

        Args:
            urn: The urn of the connection.

        Returns:
            The connection config as a dictionary, or None if the connection was not found.
        """

        # TODO: This should be capable of resolving secrets.

        res = self.execute_graphql(
            query=connections_gql,
            operation_name="GetConnection",
            variables={"urn": urn},
        )

        if not res["connection"]:
            return None

        connection_type = res["connection"]["details"]["type"]
        if connection_type != "JSON":
            logger.error(
                f"Expected connection details type to be 'JSON', but got {connection_type}"
            )
            return None

        blob = res["connection"]["details"]["json"]["blob"]
        obj = json.loads(blob)

        name = res["connection"]["details"].get("name")
        logger.info(f"Loaded connection {name or urn}")

        return obj

    def set_connection_json(
        self,
        urn: str,
        *,
        platform_urn: str,
        config: Union[ConfigModel, BaseModel, dict],
        name: Optional[str] = None,
    ) -> None:
        """Set a connection config.

        This is only supported with DataHub Cloud.

        Args:
            urn: The urn of the connection.
            platform_urn: The urn of the platform.
            config: The connection config as a dictionary or a ConfigModel.
            name: The name of the connection.
        """

        if isinstance(config, (ConfigModel, BaseModel)):
            blob = config.json()
        else:
            blob = json.dumps(config)

        id = get_id_from_connection_urn(urn)

        res = self.execute_graphql(
            query=connections_gql,
            operation_name="SetConnection",
            variables={
                "id": id,
                "platformUrn": platform_urn,
                "name": name,
                "blob": blob,
            },
        )

        assert res["upsertConnection"]["urn"] == urn

    @deprecated('Use get_urns_by_filter(entity_types=["container"], ...) instead')
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
                    "values": [f"instance={env}"],
                    "condition": "EQUAL",
                }
            )

            filter_criteria.append(
                {
                    "field": "typeNames",
                    "values": [container_subtype],
                    "condition": "EQUAL",
                }
            )
            container_filters.append({"and": filter_criteria})
        search_body = {
            "input": search_query,
            "entity": "container",
            "start": 0,
            "count": 5000,
            "filter": {"or": container_filters},
        }
        results: Dict = self._post_generic(url, search_body)
        num_entities = results["value"]["numEntities"]
        logger.debug(f"Matched {num_entities} containers")
        for x in results["value"]["entities"]:
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
        extraFilters: Optional[List[RawSearchFilterRule]] = None,
    ) -> Iterable[Tuple[str, "GraphQLSchemaMetadata"]]:
        """Fetch schema info for datasets that match all of the given filters.

        :return: An iterable of (urn, schema info) tuple that match the filters.
        """
        types = self._get_types(["dataset"])

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
        entity_types: Optional[Sequence[str]] = None,
        platform: Optional[str] = None,
        platform_instance: Optional[str] = None,
        env: Optional[str] = None,
        query: Optional[str] = None,
        container: Optional[str] = None,
        status: Optional[RemovedStatusFilter] = RemovedStatusFilter.NOT_SOFT_DELETED,
        batch_size: int = 5000,
        extraFilters: Optional[List[RawSearchFilterRule]] = None,
        extra_or_filters: Optional[RawSearchFilter] = None,
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
            platform,
            platform_instance,
            env,
            container,
            status,
            extraFilters,
            extra_or_filters=extra_or_filters,
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

    def get_results_by_filter(
        self,
        *,
        entity_types: Optional[List[str]] = None,
        platform: Optional[str] = None,
        platform_instance: Optional[str] = None,
        env: Optional[str] = None,
        query: Optional[str] = None,
        container: Optional[str] = None,
        status: RemovedStatusFilter = RemovedStatusFilter.NOT_SOFT_DELETED,
        batch_size: int = 5000,
        extra_and_filters: Optional[List[RawSearchFilterRule]] = None,
        extra_or_filters: Optional[RawSearchFilter] = None,
        extra_source_fields: Optional[List[str]] = None,
        skip_cache: bool = False,
    ) -> Iterable[dict]:
        """Fetch all results that match all of the given filters.

        Note: Only supported with DataHub Cloud.

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
        :param extra_and_filters: Additional filters to apply. If specified, the
            results will match all of the filters.
        :param extra_or_filters: Additional filters to apply. If specified, the
            results will match any of the filters.

        :return: An iterable of urns that match the filters.
        """

        types = self._get_types(entity_types)

        # Add the query default of * if no query is specified.
        query = query or "*"

        or_filters_final = generate_filter(
            platform,
            platform_instance,
            env,
            container,
            status,
            extra_and_filters,
            extra_or_filters,
        )
        graphql_query = textwrap.dedent(
            """
            query scrollUrnsWithFilters(
                $types: [EntityType!],
                $query: String!,
                $orFilters: [AndFilterInput!],
                $batchSize: Int!,
                $scrollId: String,
                $skipCache: Boolean!,
                $fetchExtraFields: [String!]) {

                scrollAcrossEntities(input: {
                    query: $query,
                    count: $batchSize,
                    scrollId: $scrollId,
                    types: $types,
                    orFilters: $orFilters,
                    searchFlags: {
                        skipHighlighting: true
                        skipAggregates: true
                        skipCache: $skipCache
                        fetchExtraFields: $fetchExtraFields
                    }
                }) {
                    nextScrollId
                    searchResults {
                        entity {
                            urn
                        }
                        extraProperties {
                            name
                            value
                        }
                    }
                }
            }
            """
        )

        variables = {
            "types": types,
            "query": query,
            "orFilters": or_filters_final,
            "batchSize": batch_size,
            "skipCache": "true" if skip_cache else "false",
            "fetchExtraFields": extra_source_fields,
        }

        for result in self._scroll_across_entities_results(graphql_query, variables):
            yield result

    def _scroll_across_entities_results(
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
                yield entry

            if scroll_id:
                logger.debug(
                    f"Scrolling to next scrollAcrossEntities page: {scroll_id}"
                )

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

    @classmethod
    def _get_types(cls, entity_types: Optional[Sequence[str]]) -> Optional[List[str]]:
        types: Optional[List[str]] = None
        if entity_types is not None:
            if not entity_types:
                raise ValueError(
                    "entity_types cannot be an empty list; use None for all entities"
                )

            types = [
                flexible_entity_type_to_graphql(entity_type)
                for entity_type in entity_types
            ]
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
        format_exception: bool = True,
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
            f"Executing {operation_name or ''} graphql query: {query} with variables: {json.dumps(variables)}"
        )
        result = self._post_generic(url, body)
        if result.get("errors"):
            if format_exception:
                raise GraphError(f"Error executing graphql query: {result['errors']}")
            else:
                raise GraphError(result["errors"])

        return result["data"]

    class RelationshipDirection(StrEnum):
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
        self.set_soft_delete_status(
            urn=urn, run_id=run_id, deletion_timestamp=deletion_timestamp, delete=True
        )

    def set_soft_delete_status(
        self,
        urn: str,
        delete: bool,
        run_id: str = _GRAPH_DUMMY_RUN_ID,
        deletion_timestamp: Optional[int] = None,
    ) -> None:
        """Change status of soft-delete an entity by urn.

        Args:
            urn: The urn of the entity to soft-delete.
        """
        assert urn

        deletion_timestamp = deletion_timestamp or int(time.time() * 1000)
        self.emit(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=StatusClass(removed=delete),
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

    @functools.lru_cache
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
        default_dialect: Optional[str] = None,
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
            default_dialect=default_dialect,
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

    def remove_tag(self, tag_urn: str, resource_urn: str) -> bool:
        graph_query = f"""
            mutation removeTag {{
                removeTag(
                input: {{
                    tagUrn: "{tag_urn}",
                    resourceUrn: "{resource_urn}"
                    }})
            }}
        """

        res = self.execute_graphql(query=graph_query)
        return res["removeTag"]

    def _assertion_result_shared(self) -> str:
        fragment: str = """
             fragment assertionResult on AssertionResult {
                 type
                 rowCount
                 missingCount
                 unexpectedCount
                 actualAggValue
                 externalUrl
                 nativeResults {
                     value
                 }
                 error {
                     type
                     properties {
                         value
                     }
                 }
             }
        """
        return fragment

    def _run_assertion_result_shared(self) -> str:
        fragment: str = """
            fragment runAssertionResult on RunAssertionResult {
                assertion {
                    urn
                }
                result {
                    ... assertionResult
                }
            }
        """
        return fragment

    def _run_assertion_build_params(
        self, params: Optional[Dict[str, str]] = None
    ) -> List[Any]:
        if params is None:
            return []

        results = []
        for key, value in params.items():
            result = {
                "key": key,
                "value": value,
            }
            results.append(result)

        return results

    def run_assertion(
        self,
        urn: str,
        save_result: bool = True,
        parameters: Optional[Dict[str, str]] = None,
        async_flag: bool = False,
    ) -> Dict:
        if parameters is None:
            parameters = {}
        params = self._run_assertion_build_params(parameters)
        graph_query: str = """
            %s
            mutation runAssertion($assertionUrn: String!, $saveResult: Boolean, $parameters: [StringMapEntryInput!], $async: Boolean!) {
                runAssertion(urn: $assertionUrn, saveResult: $saveResult, parameters: $parameters, async: $async) {
                    ... assertionResult
                }
            }
        """ % (self._assertion_result_shared())

        variables = {
            "assertionUrn": urn,
            "saveResult": save_result,
            "parameters": params,
            "async": async_flag,
        }

        res = self.execute_graphql(
            query=graph_query,
            variables=variables,
        )

        return res["runAssertion"]

    def run_assertions(
        self,
        urns: List[str],
        save_result: bool = True,
        parameters: Optional[Dict[str, str]] = None,
        async_flag: bool = False,
    ) -> Dict:
        if parameters is None:
            parameters = {}
        params = self._run_assertion_build_params(parameters)
        graph_query: str = """
            %s
            %s
            mutation runAssertions($assertionUrns: [String!]!, $saveResult: Boolean, $parameters: [StringMapEntryInput!], $async: Boolean!) {
                runAssertions(urns: $assertionUrns, saveResults: $saveResult, parameters: $parameters, async: $async) {
                    passingCount
                    failingCount
                    errorCount
                    results {
                        ... runAssertionResult
                    }
                }
            }
        """ % (
            self._assertion_result_shared(),
            self._run_assertion_result_shared(),
        )

        variables = {
            "assertionUrns": urns,
            "saveResult": save_result,
            "parameters": params,
            "async": async_flag,
        }

        res = self.execute_graphql(
            query=graph_query,
            variables=variables,
        )

        return res["runAssertions"]

    def run_assertions_for_asset(
        self,
        urn: str,
        tag_urns: Optional[List[str]] = None,
        parameters: Optional[Dict[str, str]] = None,
        async_flag: bool = False,
    ) -> Dict:
        if tag_urns is None:
            tag_urns = []
        if parameters is None:
            parameters = {}
        params = self._run_assertion_build_params(parameters)
        graph_query: str = """
            %s
            %s
            mutation runAssertionsForAsset($assetUrn: String!, $tagUrns: [String!], $parameters: [StringMapEntryInput!], $async: Boolean!) {
                runAssertionsForAsset(urn: $assetUrn, tagUrns: $tagUrns, parameters: $parameters, async: $async) {
                    passingCount
                    failingCount
                    errorCount
                    results {
                        ... runAssertionResult
                    }
                }
            }
        """ % (
            self._assertion_result_shared(),
            self._run_assertion_result_shared(),
        )

        variables = {
            "assetUrn": urn,
            "tagUrns": tag_urns,
            "parameters": params,
            "async": async_flag,
        }

        res = self.execute_graphql(
            query=graph_query,
            variables=variables,
        )

        return res["runAssertionsForAsset"]

    @deprecated("Use get_entities instead which returns typed aspects")
    def get_entities_v2(
        self,
        entity_name: str,
        urns: List[str],
        aspects: Optional[List[str]] = None,
        with_system_metadata: bool = False,
    ) -> Dict[str, Any]:
        aspects = aspects or []
        payload = {
            "urns": urns,
            "aspectNames": aspects,
            "withSystemMetadata": with_system_metadata,
        }
        headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        url = f"{self.config.server}/openapi/v2/entity/batch/{entity_name}"
        response = self._session.post(url, data=json.dumps(payload), headers=headers)
        response.raise_for_status()

        json_resp = response.json()
        entities = json_resp.get("entities", [])
        aspects_set = set(aspects)
        retval: Dict[str, Any] = {}

        for entity in entities:
            entity_aspects = entity.get("aspects", {})
            entity_urn = entity.get("urn", None)

            if entity_urn is None:
                continue
            for aspect_key, aspect_value in entity_aspects.items():
                # Include all aspects if aspect filter is empty
                if len(aspects) == 0 or aspect_key in aspects_set:
                    retval.setdefault(entity_urn, {})
                    retval[entity_urn][aspect_key] = aspect_value
        return retval

    def get_entities(
        self,
        entity_name: str,
        urns: List[str],
        aspects: Optional[List[str]] = None,
        with_system_metadata: bool = False,
    ) -> Dict[str, Dict[str, Tuple[_Aspect, Optional[SystemMetadataClass]]]]:
        """
        Get entities using the OpenAPI v3 endpoint, deserializing aspects into typed objects.

        Args:
            entity_name: The entity type name
            urns: List of entity URNs to fetch
            aspects: Optional list of aspect names to fetch. If None, all aspects will be fetched.
            with_system_metadata: If True, return system metadata along with each aspect.

        Returns:
            A dictionary mapping URNs to a dictionary of aspect name to tuples of
            (typed aspect object, system metadata). If with_system_metadata is False,
            the system metadata in the tuple will be None.
        """
        aspects = aspects or []

        request_payload = []
        for urn in urns:
            entity_request: Dict[str, Any] = {"urn": urn}
            for aspect_name in aspects:
                entity_request[aspect_name] = {}
            request_payload.append(entity_request)

        headers: Dict[str, Any] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        url = f"{self.config.server}/openapi/v3/entity/{entity_name}/batchGet"
        if with_system_metadata:
            url += "?systemMetadata=true"

        response = self._session.post(
            url, data=json.dumps(request_payload), headers=headers
        )
        response.raise_for_status()
        entities = response.json()

        result: Dict[str, Dict[str, Tuple[_Aspect, Optional[SystemMetadataClass]]]] = {}

        for entity in entities:
            entity_urn = entity.get("urn")
            if entity_urn is None:
                logger.warning(
                    f"Missing URN in entity response: {entity}, skipping deserialization"
                )
                continue

            entity_aspects: Dict[
                str, Tuple[_Aspect, Optional[SystemMetadataClass]]
            ] = {}

            for aspect_name, aspect_obj in entity.items():
                if aspect_name == "urn":
                    continue

                aspect_class = ASPECT_NAME_MAP.get(aspect_name)
                if aspect_class is None:
                    logger.warning(
                        f"Unknown aspect type {aspect_name}, skipping deserialization"
                    )
                    continue

                aspect_value = aspect_obj.get("value")
                if aspect_value is None:
                    logger.warning(
                        f"Unknown aspect value for aspect {aspect_name}, skipping deserialization"
                    )
                    continue

                try:
                    post_json_obj = post_json_transform(aspect_value)
                    typed_aspect = aspect_class.from_obj(post_json_obj)
                    assert isinstance(typed_aspect, aspect_class) and isinstance(
                        typed_aspect, _Aspect
                    )

                    system_metadata = None
                    if with_system_metadata:
                        system_metadata_obj = aspect_obj.get("systemMetadata")
                        if system_metadata_obj:
                            system_metadata = SystemMetadataClass.from_obj(
                                system_metadata_obj
                            )

                    entity_aspects[aspect_name] = (typed_aspect, system_metadata)
                except Exception as e:
                    logger.error(f"Error deserializing aspect {aspect_name}: {e}")
                    raise

            if entity_aspects:
                result[entity_urn] = entity_aspects

        return result

    def upsert_custom_assertion(
        self,
        urn: Optional[str],
        entity_urn: str,
        type: str,
        description: str,
        platform_name: Optional[str] = None,
        platform_urn: Optional[str] = None,
        field_path: Optional[str] = None,
        external_url: Optional[str] = None,
        logic: Optional[str] = None,
    ) -> Dict:
        graph_query: str = """
            mutation upsertCustomAssertion(
                $assertionUrn: String,
                $entityUrn: String!,
                $type: String!,
                $description: String!,
                $fieldPath: String,
                $platformName: String,
                $platformUrn: String,
                $externalUrl: String,
                $logic: String
            ) {
                upsertCustomAssertion(urn: $assertionUrn, input: {
                    entityUrn: $entityUrn
                    type: $type
                    description: $description
                    fieldPath: $fieldPath
                    platform: {
                        urn: $platformUrn
                        name: $platformName
                    }
                    externalUrl: $externalUrl
                    logic: $logic
                }) {
                        urn
                }
            }
        """

        variables = {
            "assertionUrn": urn,
            "entityUrn": entity_urn,
            "type": type,
            "description": description,
            "fieldPath": field_path,
            "platformName": platform_name,
            "platformUrn": platform_urn,
            "externalUrl": external_url,
            "logic": logic,
        }

        res = self.execute_graphql(
            query=graph_query,
            variables=variables,
        )

        return res["upsertCustomAssertion"]

    def report_assertion_result(
        self,
        urn: str,
        timestamp_millis: int,
        type: Literal["SUCCESS", "FAILURE", "ERROR", "INIT"],
        properties: Optional[List[Dict[str, str]]] = None,
        external_url: Optional[str] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        graph_query: str = """
            mutation reportAssertionResult(
                $assertionUrn: String!,
                $timestampMillis: Long!,
                $type: AssertionResultType!,
                $properties: [StringMapEntryInput!],
                $externalUrl: String,
                $error: AssertionResultErrorInput,
            ) {
                reportAssertionResult(urn: $assertionUrn, result: {
                    timestampMillis: $timestampMillis
                    type: $type
                    properties: $properties
                    externalUrl: $externalUrl
                    error: $error
                })
            }
        """

        variables = {
            "assertionUrn": urn,
            "timestampMillis": timestamp_millis,
            "type": type,
            "properties": properties,
            "externalUrl": external_url,
            "error": (
                {"type": error_type, "message": error_message} if error_type else None
            ),
        }

        res = self.execute_graphql(
            query=graph_query,
            variables=variables,
        )

        return res["reportAssertionResult"]

    def close(self) -> None:
        self._make_schema_resolver.cache_clear()
        super().close()


@functools.lru_cache(maxsize=None)
def get_default_graph(
    client_mode: Optional[ClientMode] = None,
    datahub_component: Optional[str] = None,
) -> DataHubGraph:
    graph_config = config_utils.load_client_config()
    graph_config.client_mode = client_mode
    graph_config.datahub_component = datahub_component
    graph = DataHubGraph(graph_config)
    graph.test_connection()
    telemetry_instance.set_context(server=graph)
    return graph
