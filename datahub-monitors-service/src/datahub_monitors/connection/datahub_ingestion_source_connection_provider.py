import logging
from typing import Dict, List, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.secret.secret_common import resolve_recipe
from datahub.secret.secret_store import SecretStore
from datahub.utilities.urns.urn import Urn, guess_entity_type

from datahub_monitors.connection.connection import Connection
from datahub_monitors.connection.ingest.bigquery import (
    extract_connection_from_bigquery_recipe,
)
from datahub_monitors.connection.ingest.redshift import (
    extract_connection_from_redshift_recipe,
)
from datahub_monitors.connection.ingest.snowflake import (
    extract_connection_from_snowflake_recipe,
)
from datahub_monitors.connection.provider import ConnectionProvider
from datahub_monitors.constants import (
    BIGQUERY_PLATFORM_NAME,
    CLI_EXECUTOR_ID,
    REDSHIFT_PLATFORM_NAME,
    SNOWFLAKE_PLATFORM_NAME,
)
from datahub_monitors.graphql.query import (
    GRAPHQL_INGESTION_SOURCE_FOR_ENTITY_QUERY,
    GRAPHQL_LIST_INGESTION_SOURCES_QUERY,
)

logger = logging.getLogger(__name__)

DATA_PLATFORM_ENTITY_TYPE = "dataPlatform"
INGESTION_SOURCE_ENTITY_TYPE = "dataHubIngestionSource"
DATA_PLATFORM_INSTANCE_ENTITY_TYPE = "dataPlatformInstance"
INGESTION_SOURCES_BATCH_SIZE = 10000

SUPPORTED_DATA_PLATFORM_TYPES = {
    SNOWFLAKE_PLATFORM_NAME,
    REDSHIFT_PLATFORM_NAME,
    BIGQUERY_PLATFORM_NAME,
}

PLATFORM_TO_CONNECTION_SUPPLIER = {
    SNOWFLAKE_PLATFORM_NAME: extract_connection_from_snowflake_recipe,
    BIGQUERY_PLATFORM_NAME: extract_connection_from_bigquery_recipe,
    REDSHIFT_PLATFORM_NAME: extract_connection_from_redshift_recipe,
}


def get_platform_from_platform_urn(urn: str) -> str:
    urn_obj = Urn.create_from_string(urn)
    return urn_obj.get_entity_id()[0]


def urn_to_entity_type(urn: str) -> str:
    return guess_entity_type(urn)


class DataHubIngestionSourceConnectionProvider(ConnectionProvider):
    """Ingestion source connection provider."""

    graph: DataHubGraph
    secret_stores: List[SecretStore]

    def __init__(self, graph: DataHubGraph, secret_stores: List[SecretStore]):
        self.graph = graph
        self.secret_stores = secret_stores

    def list_ingestion_sources(self) -> List[Dict]:
        result = self.graph.execute_graphql(
            GRAPHQL_LIST_INGESTION_SOURCES_QUERY,
            variables={"input": {"start": 0, "count": INGESTION_SOURCES_BATCH_SIZE}},
        )

        if "error" in result:
            # TODO: add either logging or throwing here.
            pass

        if (
            "listIngestionSources" not in result
            or "ingestionSources" not in result["listIngestionSources"]
        ):
            return []

        return result["listIngestionSources"]["ingestionSources"]

    def get_ingestion_source_for_entity(self, entity_urn: str) -> Optional[Dict]:
        result = self.graph.execute_graphql(
            GRAPHQL_INGESTION_SOURCE_FOR_ENTITY_QUERY,
            variables={"urn": entity_urn},
        )

        if "error" in result:
            # TODO: add either logging or throwing here.
            pass

        if "ingestionSourceForEntity" not in result:
            return None

        return result["ingestionSourceForEntity"]

    def extract_connection_from_ingestion_source(
        self, source_type: str, ingestion_source: Dict
    ) -> Optional[Connection]:
        # 1. Resolve the recipe itself.
        final_recipe_dict = resolve_recipe(
            ingestion_source["config"]["recipe"], self.secret_stores
        )

        if PLATFORM_TO_CONNECTION_SUPPLIER.get(source_type) is not None:
            supplier = PLATFORM_TO_CONNECTION_SUPPLIER.get(source_type)
            return supplier(f"urn:li:dataPlatform:{source_type}", final_recipe_dict, self.graph)  # type: ignore
        raise NotImplementedError(
            "Unable to extract connection from ingestion source. No extractor found."
        )

    def get_connection_from_data_platform(self, urn: str) -> Optional[Connection]:
        platform_type = get_platform_from_platform_urn(urn)
        if platform_type in SUPPORTED_DATA_PLATFORM_TYPES:
            # 1. List all ingestion sources in DataHub.
            ingestion_sources = self.list_ingestion_sources()

            # 2. Filter ingestion sources that are for the platform with given urn.
            for ingestion_source in ingestion_sources:
                source_type = ingestion_source["type"]
                source_executor = (
                    ingestion_source["config"]["executorId"]
                    if "executorId" in ingestion_source["config"]
                    else None
                )
                if (
                    platform_type == source_type and source_executor != CLI_EXECUTOR_ID
                ):  # Assumption Alert! This assumes that ingestion source name === data platform name.
                    # 3. Convert the recipe into a connection - currently only supports Snowflake, Redshift.
                    return self.extract_connection_from_ingestion_source(
                        source_type, ingestion_source
                    )
            # Did not find matching connection.
            logger.error(
                f"Failed to resolve connection from urn {urn}. No matching ingestion sources configured."
            )
            return None
        raise NotImplementedError(
            "platform type " + platform_type + " is not currently supported!"
        )

    def get_connection_from_entity(self, entity_urn: str) -> Optional[Connection]:
        ingestion_source = self.get_ingestion_source_for_entity(entity_urn)
        if ingestion_source is None:
            logger.error(
                f"Failed to resolve connection for entity with urn {entity_urn}. No matching ingestion sources configured."
            )
            return None
        source_type = ingestion_source["type"]
        (
            ingestion_source["config"]["executorId"]
            if "executorId" in ingestion_source["config"]
            else None
        )
        return self.extract_connection_from_ingestion_source(
            source_type, ingestion_source
        )

    def get_connection(self, urn: str) -> Optional[Connection]:
        # First, determine which type of urn we are working with.
        entity_type = urn_to_entity_type(urn)
        if entity_type == DATA_PLATFORM_ENTITY_TYPE:
            return self.get_connection_from_data_platform(urn)
        # TODO: Allow users to provide a connection along with the
        # monitor / assertion definition.
        raise NotImplementedError()

    def get_connection_for_entity(self, urn: str) -> Optional[Connection]:
        return self.get_connection_from_entity(urn)
