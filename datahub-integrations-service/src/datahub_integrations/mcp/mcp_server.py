import contextlib
import contextvars
import functools
import inspect
import pathlib
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    ParamSpec,
    TypeVar,
)

import asyncer
import jmespath
from datahub.errors import ItemNotFoundError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter, FilterDsl
from datahub.utilities.ordered_set import OrderedSet
from fastmcp import FastMCP
from pydantic import BaseModel

_P = ParamSpec("_P")
_R = TypeVar("_R")


# See https://github.com/jlowin/fastmcp/issues/864#issuecomment-3103678258
# for why we need to wrap sync functions with asyncify.
def async_background(fn: Callable[_P, _R]) -> Callable[_P, Awaitable[_R]]:
    if inspect.iscoroutinefunction(fn):
        raise RuntimeError("async_background can only be used on non-async functions")

    @functools.wraps(fn)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        return await asyncer.asyncify(fn)(*args, **kwargs)

    return wrapper


mcp = FastMCP[None](name="datahub")


_mcp_dh_client = contextvars.ContextVar[DataHubClient]("_mcp_dh_client")


def get_datahub_client() -> DataHubClient:
    # Will raise a LookupError if no client is set.
    return _mcp_dh_client.get()


def set_datahub_client(client: DataHubClient) -> None:
    _mcp_dh_client.set(client)


@contextlib.contextmanager
def with_datahub_client(client: DataHubClient) -> Iterator[None]:
    token = _mcp_dh_client.set(client)
    try:
        yield
    finally:
        _mcp_dh_client.reset(token)


def _enable_cloud_fields(query: str) -> str:
    return query.replace("#[CLOUD]", "")


def _is_datahub_cloud(graph: DataHubGraph) -> bool:
    try:
        # Only DataHub Cloud has a frontend base url.
        _ = graph.frontend_base_url
    except ValueError:
        return False
    return True


def _execute_graphql(
    graph: DataHubGraph,
    *,
    query: str,
    operation_name: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
) -> Any:
    if _is_datahub_cloud(graph):
        query = _enable_cloud_fields(query)

    return graph.execute_graphql(
        query=query, variables=variables, operation_name=operation_name
    )


def _inject_urls_for_urns(
    graph: DataHubGraph, response: Any, json_paths: List[str]
) -> None:
    if not _is_datahub_cloud(graph):
        return

    for path in json_paths:
        for item in jmespath.search(path, response) if path else [response]:
            if isinstance(item, dict) and item.get("urn"):
                # Update item in place with url, ensuring that urn and url are first.
                new_item = {"urn": item["urn"], "url": graph.url_for(item["urn"])}
                new_item.update({k: v for k, v in item.items() if k != "urn"})
                item.clear()
                item.update(new_item)


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

        cleaned_response = {}
        for k, v in response.items():
            if k in banned_keys or v is None or v == []:
                continue
            cleaned_v = _clean_gql_response(v)
            if cleaned_v is not None and cleaned_v != {}:
                cleaned_response[k] = cleaned_v

        return cleaned_response
    elif isinstance(response, list):
        return [_clean_gql_response(item) for item in response]
    else:
        return response


def _clean_get_entity_response(raw_response: dict) -> dict:
    response = _clean_gql_response(raw_response)

    if response and (schema_metadata := response.get("schemaMetadata")):
        # Remove empty platformSchema to reduce response clutter
        if platform_schema := schema_metadata.get("platformSchema"):
            schema_value = platform_schema.get("schema")
            if not schema_value or schema_value == "":
                del schema_metadata["platformSchema"]

        # Remove default field attributes (false values) to keep only meaningful data
        if fields := schema_metadata.get("fields"):
            for field in fields:
                if field.get("recursive") is False:
                    field.pop("recursive", None)
                if field.get("isPartOfKey") is False:
                    field.pop("isPartOfKey", None)

    return response


@mcp.tool(description="Get an entity by its DataHub URN.")
@async_background
def get_entity(urn: str) -> dict:
    client = get_datahub_client()

    if not client._graph.exists(urn):
        # TODO: Ideally we use the `exists` field to check this, and also deal with soft-deleted entities.
        raise ItemNotFoundError(f"Entity {urn} not found")

    # Execute the GraphQL query
    variables = {"urn": urn}
    result = _execute_graphql(
        client._graph,
        query=entity_details_fragment_gql,
        variables=variables,
        operation_name="GetEntity",
    )["entity"]

    _inject_urls_for_urns(client._graph, result, [""])

    return _clean_get_entity_response(result)


@mcp.tool(
    description="""Search across DataHub entities.

Returns both a truncated list of results and facets/aggregations that can be used to iteratively refine the search filters.
To search for all entities, use the wildcard '*' as the query and set `filters: null`.

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
- All non-Snowflake tables
```
{
  "and":[
    {"entity_type": ["DATASET"]},
    {"entity_subtype": ["Table"]},
    {"not": {"platform": ["snowflake"]}}
  ]
}
```
"""
)
@async_background
def search(
    query: str = "*",
    filters: Optional[Filter] = None,
    num_results: int = 10,
) -> dict:
    client = get_datahub_client()

    types, compiled_filters = compile_filters(filters)
    variables = {
        "query": query,
        "types": types,
        "orFilters": compiled_filters,
        "count": max(num_results, 1),  # 0 is not a valid value for count.
    }

    response = _execute_graphql(
        client._graph,
        query=search_gql,
        variables=variables,
        operation_name="search",
    )["scrollAcrossEntities"]

    if num_results == 0 and isinstance(response, dict):
        # Hack to support num_results=0 without support for it in the backend.
        response.pop("searchResults", None)
        response.pop("count", None)

    return _clean_gql_response(response)


@mcp.tool(description="Use this tool to get the SQL queries associated with a dataset.")
@async_background
def get_dataset_queries(dataset_urn: str, start: int = 0, count: int = 10) -> dict:
    client = get_datahub_client()

    # Set up variables for the query
    variables = {"input": {"start": start, "count": count, "datasetUrn": dataset_urn}}

    # Execute the GraphQL query
    result = _execute_graphql(
        client._graph,
        query=queries_gql,
        variables=variables,
        operation_name="listQueries",
    )["listQueries"]

    for query in result["queries"]:
        if query.get("subjects"):
            query["subjects"] = _deduplicate_subjects(query["subjects"])

    return _clean_gql_response(result)


def _deduplicate_subjects(subjects: list[dict]) -> list[str]:
    # The "subjects" field returns every dataset and schema field associated with the query.
    # While this is useful for our backend to have, it's not useful here because
    # we can just look at the query directly. So we'll narrow it down to the unique
    # list of dataset urns.
    updated_subjects: OrderedSet[str] = OrderedSet()
    for subject in subjects:
        with contextlib.suppress(KeyError):
            updated_subjects.add(subject["dataset"]["urn"])
    return list(updated_subjects)


class AssetLineageDirective(BaseModel):
    urn: str
    upstream: bool
    downstream: bool
    max_hops: int


class AssetLineageAPI:
    def __init__(self, graph: DataHubGraph) -> None:
        self.graph = graph

    def get_degree_filter(self, max_hops: int) -> Optional[Filter]:
        """
        max_hops: Maximum number of hops to search for lineage
        """
        if max_hops == 1 or max_hops == 2:
            return FilterDsl.custom_filter(
                field="degree",
                condition="EQUAL",
                values=[str(i) for i in range(1, max_hops + 1)],
            )
        elif max_hops >= 3:
            return FilterDsl.custom_filter(
                field="degree",
                condition="EQUAL",
                values=["1", "2", "3+"],
            )
        else:
            raise ValueError(f"Invalid number of hops: {max_hops}")

    def get_lineage(
        self, asset_lineage_directive: AssetLineageDirective
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        degree_filter = self.get_degree_filter(asset_lineage_directive.max_hops)
        types, compiled_filters = compile_filters(degree_filter)
        variables = {
            "urn": asset_lineage_directive.urn,
            "start": 0,
            "count": 30,
            "types": types,
            "orFilters": compiled_filters,
            "searchFlags": {"skipHighlighting": True, "maxAggValues": 3},
        }
        if asset_lineage_directive.upstream:
            result["upstreams"] = _clean_gql_response(
                _execute_graphql(
                    self.graph,
                    query=entity_details_fragment_gql,
                    variables={
                        "input": {
                            **variables,
                            "direction": "UPSTREAM",
                        }
                    },
                    operation_name="GetEntityLineage",
                )["searchAcrossLineage"]
            )
        if asset_lineage_directive.downstream:
            result["downstreams"] = _clean_gql_response(
                _execute_graphql(
                    self.graph,
                    query=entity_details_fragment_gql,
                    variables={
                        "input": {
                            **variables,
                            "direction": "DOWNSTREAM",
                        }
                    },
                    operation_name="GetEntityLineage",
                )["searchAcrossLineage"]
            )

        return result


@mcp.tool(
    description="""\
Use this tool to get upstream or downstream lineage for any entity, including datasets, schemaFields, dashboards, charts, etc. \
Set upstream to True for upstream lineage, False for downstream lineage."""
)
@async_background
def get_lineage(urn: str, upstream: bool, max_hops: int = 1) -> dict:
    client = get_datahub_client()
    lineage_api = AssetLineageAPI(client._graph)
    asset_lineage_directive = AssetLineageDirective(
        urn=urn, upstream=upstream, downstream=not upstream, max_hops=max_hops
    )
    lineage = lineage_api.get_lineage(asset_lineage_directive)
    _inject_urls_for_urns(client._graph, lineage, ["*.searchResults[].entity"])
    return lineage
