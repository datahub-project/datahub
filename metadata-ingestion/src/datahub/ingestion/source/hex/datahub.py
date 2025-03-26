import logging
from dataclasses import dataclass, field
from typing import Any, Generator, List, Union

from datahub.ingestion.source.hex.constants import HEX_PLATFORM_URN
from datahub.metadata.urns import DatasetUrn, QueryUrn, SchemaFieldUrn
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_filters import FilterDsl as F

logger = logging.getLogger(__name__)


@dataclass
class QueryResponse:
    urn: QueryUrn
    hex_project_id: str
    subjects: List[Union[DatasetUrn, SchemaFieldUrn]] = field(default_factory=list)


class HexQueryFetcher:
    def __init__(self, datahub_client: DataHubClient, workspace_name: str):
        self.datahub_client = datahub_client
        self.workspace_name = workspace_name

    def fetch(self) -> Generator[QueryResponse, None, None]:
        # TODO: Implement this method
        # - fetch queries with origin = 'hex' and filter by time range
        # - filter out queries missing "Hex query metadata"
        # - parse "Hex query metadata" (workspace and project)
        # - filter queries with workspace != self.workspace_name
        # - yield QueryResponse objects

        hex_query_urns = self.datahub_client.search.get_urns(
            filter=F.and_(
                F.entity_type(QueryUrn.ENTITY_TYPE),
                F.custom_filter("origin", "EQUAL", [HEX_PLATFORM_URN.urn()]),  # TBC
                F.custom_filter(
                    "lastModified", "BIGGER_THAN", ["some value, in which format!?"]
                ),  # TBC
            ),
        )
        for urn in hex_query_urns:
            assert isinstance(urn, QueryUrn)
            # TBC: is there a batch get method?
            # TBC: there is no datahub.sdk.query.Query class?!

            query = self.datahub_client.entities.get(urn=urn)
            logger.debug(f"Processing query {query}")
            yield self._map_query(urn, query)

    def _map_query(self, urn: QueryUrn, query: Any) -> QueryResponse:
        return QueryResponse(
            urn=urn,
            hex_project_id="paser query statement and fetch project id",
            subjects=[],  # this should be got from the query object
        )
