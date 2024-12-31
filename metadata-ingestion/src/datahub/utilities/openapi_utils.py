import logging
from typing import Iterable, Union

import datahub.metadata.schema_classes as models
from datahub.ingestion.graph.client import DataHubGraph
from datahub.utilities.search_utils import (
    ElasticDocumentQuery,
    ElasticsearchQueryBuilder,
)

logger = logging.getLogger(__name__)


class OpenAPIGraphClient:
    """
    An experimental client for the DataHubGraph that uses the OpenAPI endpoints
    to query entities and aspects.
    Does not support all features of the DataHubGraph.
    API is subject to change.

    DO NOT USE THIS UNLESS YOU KNOW WHAT YOU ARE DOING.
    """

    ENTITY_KEY_ASPECT_MAP = {
        aspect_type.ASPECT_INFO.get("keyForEntity"): name
        for name, aspect_type in models.ASPECT_NAME_MAP.items()
        if aspect_type.ASPECT_INFO.get("keyForEntity")
    }

    def __init__(self, graph: DataHubGraph):
        self.graph = graph
        self.openapi_base = graph._gms_server.rstrip("/") + "/openapi/v3"

    def scroll_urns_by_filter(
        self,
        entity_type: str,
        query: Union[ElasticDocumentQuery, ElasticsearchQueryBuilder],
    ) -> Iterable[str]:
        """
        Scroll through all urns that match the given filters.

        """

        key_aspect = self.ENTITY_KEY_ASPECT_MAP.get(entity_type)
        assert key_aspect, f"No key aspect found for entity type {entity_type}"

        count = 1000
        string_query = query.build()
        scroll_id = None
        logger.debug(f"Scrolling with query: {string_query}")
        while True:
            response = self.graph._get_generic(
                self.openapi_base + f"/entity/{entity_type.lower()}",
                params={
                    "systemMetadata": "false",
                    "includeSoftDelete": "false",
                    "skipCache": "false",
                    "aspects": [key_aspect],
                    "scrollId": scroll_id,
                    "count": count,
                    "query": string_query,
                },
            )
            entities = response.get("entities", [])
            scroll_id = response.get("scrollId")
            for entity in entities:
                yield entity["urn"]
            if not scroll_id:
                break
