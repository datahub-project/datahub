import pathlib
from typing import Any, Dict, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter, FilterDsl
from pydantic import BaseModel

from datahub_integrations.chat.tool import ToolRegistry

mcp = ToolRegistry()


def get_client() -> DataHubClient:
    return DataHubClient.from_env()


search_gql = (pathlib.Path(__file__).parent / "gql/search.gql").read_text()
entity_details_fragment_gql = (
    pathlib.Path(__file__).parent / "gql/entity_details.gql"
).read_text()

queries_gql = (pathlib.Path(__file__).parent / "gql/queries.gql").read_text()


def _clean_gql_response(response: Any) -> Any:
    if isinstance(response, dict):
        banned_keys = {
            "__typename",
        }
        return {
            k: _clean_gql_response(v)
            for k, v in response.items()
            if v is not None and k not in banned_keys
        }
    elif isinstance(response, list):
        return [_clean_gql_response(item) for item in response]
    else:
        return response


@mcp.tool(description="Get an entity by its DataHub URN.")
def get_entity(urn: str) -> dict:
    client = get_client()

    # Execute the GraphQL query
    variables = {"urn": urn}
    result = client._graph.execute_graphql(
        query=entity_details_fragment_gql,
        variables=variables,
        operation_name="GetEntity",
    )

    # Extract the entity data from the response
    if "entity" in result:
        return _clean_gql_response(result["entity"])

    # Return empty dict if entity not found
    return {}


@mcp.tool(
    description="""Search across DataHub entities.

Returns both a truncated list of results and facets/aggregations that can be used to iteratively refine the search filters.
To search for all entities, use the wildcard '*' as the query.

A typical workflow will involve multiple calls to this search tool, with each call refining the filters based on the facets/aggregations returned in the previous call.
After the final search is performed, you'll want to use the other tools to get more details about the relevant entities.

Here are some example filters:
- Production environment warehouse assets
```
{
  "and": [
    {"env": ["PROD"]},
    {"platform": ["snowflake", "bigquery", "redshift"]}
  ]
}
```

- All Snowflake tables
```
{
  "and_":[
    {"entity_type": ["DATASET"]},
    {"entity_type": "dataset", "entity_subtype": "Table"},
    {"platform": ["snowflake"]}
  ]
}
```
"""
)
def search(
    query: str = "*", filters: Optional[Filter] = None, num_results: int = 10
) -> dict:
    client = get_client()

    variables = {
        "query": query,
        "orFilters": compile_filters(filters),
        "batchSize": num_results,
    }

    response = client._graph.execute_graphql(
        search_gql,
        variables=variables,
        operation_name="search",
    )["scrollAcrossEntities"]

    # TODO: post process
    # e.g. strip all nulls?

    return _clean_gql_response(response)


@mcp.tool(description="Use this tool to get the SQL queries associated with a dataset.")
def get_dataset_queries(dataset_urn: str, start: int = 0, count: int = 10) -> dict:
    client = get_client()

    # Set up variables for the query
    variables = {"input": {"start": start, "count": count, "datasetUrn": dataset_urn}}

    # Execute the GraphQL query
    result = client._graph.execute_graphql(
        query=queries_gql, variables=variables, operation_name="listQueries"
    )
    return _clean_gql_response(result["listQueries"])


class AssetLineageDirective(BaseModel):
    urn: str
    upstream: bool
    downstream: bool
    num_hops: int


class AssetLineageAPI:
    def __init__(self, graph: DataHubGraph) -> None:
        self.graph = graph

    def get_degree_filter(self, num_hops: int) -> Optional[Filter]:
        """
        num_hops: Number of hops to search for lineage
        """
        if num_hops < 1:
            return None
        else:
            return FilterDsl.custom_filter(
                field="degree",
                condition="EQUAL",
                values=[str(i) for i in range(1, num_hops + 1)],
            )

    def get_lineage(
        self, asset_lineage_directive: AssetLineageDirective
    ) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {asset_lineage_directive.urn: {}}

        degree_filter = self.get_degree_filter(asset_lineage_directive.num_hops)
        variables = {
            "urn": asset_lineage_directive.urn,
            "start": 0,
            "count": 30,
            "orFilters": compile_filters(degree_filter) if degree_filter else None,
        }
        if asset_lineage_directive.upstream:
            result[asset_lineage_directive.urn]["upstreams"] = _clean_gql_response(
                self.graph.execute_graphql(
                    query=entity_details_fragment_gql,
                    variables={
                        "input": {
                            **variables,
                            "direction": "UPSTREAM",
                        }
                    },
                    operation_name="GetEntityLineage",
                )
            )
        if asset_lineage_directive.downstream:
            result[asset_lineage_directive.urn]["downstreams"] = _clean_gql_response(
                self.graph.execute_graphql(
                    query=entity_details_fragment_gql,
                    variables={
                        "input": {
                            **variables,
                            "direction": "DOWNSTREAM",
                        }
                    },
                    operation_name="GetEntityLineage",
                )
            )

        return result


@mcp.tool(
    description="""\
Use this tool to get upstream or downstream lineage for any entity. \
Set upstream to True for upstream lineage, False for downstream lineage."""
)
def get_lineage(urn: str, upstream: bool, num_hops: int = 1) -> dict:
    client = get_client()
    lineage_api = AssetLineageAPI(client._graph)
    asset_lineage_directive = AssetLineageDirective(
        urn=urn, upstream=upstream, downstream=not upstream, num_hops=num_hops
    )
    return lineage_api.get_lineage(asset_lineage_directive)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        urn_or_query = sys.argv[1]
    else:
        urn_or_query = "*"
        print("No query provided, will use '*' query")
    urn: Optional[str] = None
    if urn_or_query.startswith("urn:"):
        urn = urn_or_query
    else:
        urn = None
        query = urn_or_query
    if urn is None:
        search_data = search()
        for entity in search_data["searchResults"]:
            print(entity["entity"]["urn"])
            urn = entity["entity"]["urn"]
    assert urn is not None

    print("Getting entity:", urn)
    print(get_entity(urn))
    print("Getting lineage:", urn)
    print(get_lineage(urn, upstream=True))
    print("Getting queries", urn)
    print(get_dataset_queries(urn))
