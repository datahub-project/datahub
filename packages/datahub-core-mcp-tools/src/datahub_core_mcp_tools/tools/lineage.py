"""Lineage: get_lineage and get_lineage_paths_between."""

import copy
import inspect
import string
from dataclasses import dataclass
from typing import Annotated, Any, Dict, List, Literal, Optional

from datahub.metadata.urns import SchemaFieldUrn
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter, FilterDsl
from fastmcp.exceptions import ToolError
from loguru import logger
from pydantic import BeforeValidator, Field

from .. import arg_validation, entity_cleaning, graphql_helpers
from ..read_backend import JsonObject, ReadBackend, get_read_backend
from ..result_policy import (
    apply_decision,
    attach_header,
    current_decisions,
    label_entity_results,
    trust_labeled,
)
from ..search_filter_parser import FILTER_DOCS
from ..search_flags import request_search_flags
from ..version_requirements import read_only
from .entities import EntityField, project_entity

DEFAULT_LINEAGE_SEARCH_FLAGS: JsonObject = {
    "skipHighlighting": True,
    "maxAggValues": 3,
}

# Bounds of the upstream search behind get_lineage_paths_between. When the result
# cap cuts the search short, a "no path" answer says so instead of claiming the
# entities are unconnected.
PATH_SEARCH_MAX_HOPS = 10
PATH_SEARCH_MAX_RESULTS = 100

# Kept on a column-lineage node when its paths are folded into lineageColumns.
_LINEAGE_RESULT_KEYS = ("explored", "truncatedChildren", "ignoredAsHop")


@dataclass(frozen=True)
class LineageQuery:
    """One searchAcrossLineage request, before the direction is chosen."""

    urn: str
    max_hops: int
    max_results: int
    extra_filters: Optional[Filter] = None
    query: Optional[str] = None
    # Page start, sent straight to searchAcrossLineage. Paging has to happen
    # server-side: slicing a fetched page by offset only discards rows already
    # fetched, so any offset >= max_results would return nothing.
    offset: int = 0


def degree_filter(max_hops: int) -> Filter:
    """The `degree` filter for up to ``max_hops`` hops; 3 or more means unlimited."""
    if max_hops < 1:
        raise ToolError(f"max_hops must be at least 1, got {max_hops}")
    degrees = (
        [str(i) for i in range(1, max_hops + 1)] if max_hops < 3 else ["1", "2", "3+"]
    )
    return FilterDsl.custom_filter(field="degree", condition="EQUAL", values=degrees)


def lineage_variables(request: LineageQuery, *, upstream: bool) -> JsonObject:
    """The ``SearchAcrossLineageInput`` for one direction of ``request``.

    The request's SearchFlagsOverride, if any, is merged over the default flags.
    """
    filter = degree_filter(request.max_hops)
    if request.extra_filters is not None:
        filter = FilterDsl.and_(filter, request.extra_filters)
    types, compiled_filters = compile_filters(filter)
    return {
        "urn": request.urn,
        "query": request.query or "*",
        "start": request.offset,
        "count": request.max_results,
        "types": list(types) if types else None,
        "orFilters": compiled_filters,  # type: ignore[dict-item]
        "searchFlags": request_search_flags(DEFAULT_LINEAGE_SEARCH_FLAGS),
        "direction": "UPSTREAM" if upstream else "DOWNSTREAM",
    }


def _extract_lineage_columns_from_paths(search_results: List[dict]) -> List[dict]:
    """Fold column-lineage paths into each dataset's `lineageColumns`.

    Column lineage (a schemaField input) returns datasets, each with `paths`: the
    column chains from the input column to that dataset, e.g.
    ``source.user_id -> staging.uid -> target.customer_id``. The last node of each
    chain is the column in this dataset; those columns, deduplicated in order,
    become `lineageColumns` and the paths are dropped (get_lineage_paths_between
    returns them in full). Results without paths are returned unchanged.
    """
    if not any(result.get("paths") for result in search_results):
        return search_results

    processed: List[dict] = []
    for result in search_results:
        paths = result.get("paths") or []
        if not paths:
            processed.append(result)
            continue
        columns: List[str] = []
        for path_obj in paths:
            path = (path_obj or {}).get("path") or []
            last = path[-1] if path else None
            if isinstance(last, dict) and last.get("type") == "SCHEMA_FIELD":
                field_path = last.get("fieldPath")
                if field_path and field_path not in columns:
                    columns.append(field_path)
        new_result = {"entity": result["entity"], "degree": result.get("degree", 0)}
        if columns:
            new_result["lineageColumns"] = columns
        for key in _LINEAGE_RESULT_KEYS:
            if key in result:
                new_result[key] = result[key]
        processed.append(new_result)
    return processed


def _filter_restricts_to_non_datasets(parsed_filter: Optional[Filter]) -> bool:
    """True if the filter pins entity_type to a set that excludes datasets.

    Column lineage only resolves to datasets, so such a filter always returns 0.
    """
    if parsed_filter is None:
        return False
    try:
        types, _ = compile_filters(parsed_filter)
    except Exception:
        return False
    if not types:
        return False
    return not any(t.lower() == "dataset" for t in types)


def _require_existing(backend: ReadBackend, urn: str) -> None:
    """Raise unless ``urn`` exists.

    searchAcrossLineage returns zero results for a well-formed URN that does not
    exist, which reads as "no lineage". A schemaField has no key aspect of its own,
    so exists() is false for real columns: its dataset is checked instead.
    """
    existence_urn = urn
    if urn.startswith("urn:li:schemaField:"):
        existence_urn = str(SchemaFieldUrn.from_string(urn).parent)
    if not backend.exists(existence_urn):
        raise ToolError(f"Entity {existence_urn} not found")


def _page_direction(
    direction_results: dict,
    *,
    offset: int,
    max_results: int,
    fields: Optional[List[str]],
) -> None:
    """Clean, budget and label one direction's page in place, with paging metadata."""
    search_results = direction_results.get("searchResults") or []

    # `total` counts the whole result set, not the page: hasMore is measured against
    # it. Without it, nobody knows whether more pages exist; treat the page as all
    # of it rather than report 0, which would claim "returned N of 0".
    total_available = direction_results.get("total")
    if total_available is None:
        logger.warning(
            "get_lineage: backend omitted `total`; deriving it from the page, so "
            "hasMore only reflects the token budget"
        )
        total_available = offset + len(search_results)
        direction_results["total"] = total_available

    # Facets cover the whole graph, so every page would repeat the same ~1.4K-token
    # block. Page 1 carries it; continuation and empty pages don't.
    if offset > 0 or not search_results:
        direction_results.pop("facets", None)

    if not search_results:
        direction_results.update(
            searchResults=[], offset=offset, returned=0, hasMore=False
        )
        return

    search_results = _extract_lineage_columns_from_paths(search_results)

    # Projected before the budget measures each node, so leaving out unrequested
    # fields lets more nodes fit.
    def cleaned_entity(item: dict) -> dict:
        item["entity"] = project_entity(
            entity_cleaning.clean_get_entities_response(item.get("entity") or {}),
            fields,
        )
        return item["entity"]

    selected = list(
        graphql_helpers.select_results_within_budget(
            results=iter(search_results),
            fetch_entity=cleaned_entity,
            max_results=max_results,
        )
    )
    label_entity_results(selected)

    direction_results["searchResults"] = selected
    direction_results["offset"] = offset
    direction_results["returned"] = len(selected)
    direction_results["hasMore"] = offset + len(selected) < total_available
    # The next page starts after what was returned, not after max_results: the
    # token budget can cut a page short, and stepping by max_results skips the gap.
    if direction_results["hasMore"]:
        direction_results["nextOffset"] = offset + len(selected)
    if len(selected) < len(search_results):
        direction_results["truncatedDueToTokenBudget"] = True


_COLUMN_LINEAGE_METADATA = {
    "queryType": "column-level-lineage",
    "groupedBy": "dataset",
    "fields": {
        "lineageColumns": {
            "description": "Columns in each dataset that have a lineage relationship with the source column",
            "semantics": {
                "downstream": "Columns derived from the source column",
                "upstream": "Columns that the source column depends on",
            },
        }
    },
}


# TODO: sort_by / sort_order like search: SearchAcrossLineageInput takes sortInput.
@read_only
@trust_labeled
def get_lineage(
    urn: Annotated[str, Field(validation_alias=arg_validation.URN_ALIASES)],
    column: Optional[str] = None,
    query: Optional[str] = None,
    filter: Annotated[
        Optional[str], Field(validation_alias=arg_validation.FILTER_ALIASES)
    ] = None,
    upstream: Annotated[
        bool,
        BeforeValidator(arg_validation.coerce_lineage_direction),
        Field(validation_alias=arg_validation.UPSTREAM_ALIASES),
    ] = True,
    max_hops: Annotated[
        int, Field(validation_alias=arg_validation.MAX_HOPS_ALIASES)
    ] = 1,
    max_results: Annotated[
        int, Field(validation_alias=arg_validation.SIZE_ALIASES)
    ] = 30,
    offset: Annotated[int, Field(validation_alias=arg_validation.OFFSET_ALIASES)] = 0,
    fields: Annotated[
        Optional[List[EntityField]],
        BeforeValidator(arg_validation.coerce_str_list),
        Field(validation_alias=arg_validation.FIELDS_ALIASES),
    ] = None,
) -> dict:
    """Get upstream or downstream lineage for any entity, including datasets, schemaFields, dashboards, charts, etc.

    Set upstream to True for upstream lineage, False for downstream lineage.
    Set `column: null` to get lineage for entire dataset or for entity type other than dataset.
    When column is set, results are always datasets (only datasets have schema fields).
    Setting max_hops to 3 is equivalent to unlimited hops.

    $FILTER_DOCS

    PAGINATION:
    Pass the response's `nextOffset` back as `offset` to get the next page, and stop
    when `hasMore` is false. Do not step by max_results — the token budget can return
    fewer entities than requested, and stepping by max_results would skip the
    difference.

    QUERY PARAMETER - Search within lineage results:
    Use search's /q syntax.
    - /q my_schema -> find tables in specific schema
    - /q customer+transactions -> find entities with both terms
    - /q looker OR tableau -> find dashboards on either platform

    Examples:
    - Find a specific table in a large downstream set: query="my_db.my_schema.events"
    - Find Looker assets in lineage: filter="platform = looker"
    - Get all results: query="*" or omit parameter

    FIELDS - Control response size:
    Each node returns identity and description only. `fields` is an allowlist — name
    what the question needs, e.g. fields=["ownership"] for "who owns the downstream
    tables". Anything withheld is listed per node in `omittedFields`. Fewer fields per
    node also means more nodes fit within the token budget.
    """
    # Some models send the string "null" for JSON null, so a column literally named
    # "null" cannot be asked for.
    if column in ("null", ""):
        column = None

    urn = arg_validation.require_urn(urn, tool="get_lineage")
    backend = get_read_backend()
    _require_existing(backend, urn)

    try:
        parsed_filter = graphql_helpers.parse_filter_input(filter)
    except ValueError as e:
        raise ToolError(f"Invalid filter {filter!r}: {e}") from e

    # Column lineage only resolves to datasets, so a column= query filtered to
    # charts/dashboards always returns 0. Reject it with the two-hop recipe rather
    # than return a misleading empty result.
    if column is not None and _filter_restricts_to_non_datasets(parsed_filter):
        raise ToolError(
            "Column lineage only resolves to datasets, so a column= query filtered to "
            "charts/dashboards always returns 0. Call get_lineage(column=...) without an "
            "entity_type filter to get the downstream datasets, then follow those to the "
            "charts/dashboards."
        )

    request = LineageQuery(
        urn=arg_validation.schema_field_urn(urn, column, tool="get_lineage"),
        max_hops=max_hops,
        max_results=max_results,
        extra_filters=parsed_filter,
        query=query,
        offset=offset,
    )
    direction = "upstreams" if upstream else "downstreams"
    direction_results = graphql_helpers.clean_gql_response(
        backend.search_lineage(lineage_variables(request, upstream=upstream))
    )
    lineage: dict = {direction: direction_results}
    entity_cleaning.truncate_descriptions(lineage)
    _page_direction(
        direction_results, offset=offset, max_results=max_results, fields=fields
    )
    logger.info(
        f"get_lineage {direction}: returned {direction_results['returned']}/"
        f"{direction_results['total']} (offset={offset}, "
        f"hasMore={direction_results['hasMore']})"
    )

    if column is not None:
        lineage["metadata"] = copy.deepcopy(_COLUMN_LINEAGE_METADATA)

    returned_urns = [
        item["entity"]["urn"]
        for item in direction_results["searchResults"]
        if (item.get("entity") or {}).get("urn")
    ]
    attach_header(
        lineage, current_decisions(returned_urns), returned_urns, reader_only=True
    )
    return lineage


assert get_lineage.__doc__ is not None
# Dedent before substituting: FILTER_DOCS is flush-left, so interpolating it into an
# indented placeholder would leave its first line indented and the rest at column 0.
get_lineage.__doc__ = string.Template(inspect.cleandoc(get_lineage.__doc__)).substitute(
    FILTER_DOCS=FILTER_DOCS
)


def _find_result_with_target_urn(
    search_results: List[dict], target_urn: str, is_column_level: bool
) -> Optional[dict]:
    """The search result that reaches ``target_urn``.

    Column level: a result with a path ending at the target column. Dataset level:
    the result whose entity is the target.
    """
    for result in search_results:
        if is_column_level:
            for path_obj in result.get("paths") or []:
                path = (path_obj or {}).get("path") or []
                if path and (path[-1] or {}).get("urn") == target_urn:
                    return result
        elif (result.get("entity") or {}).get("urn") == target_urn:
            return result
    return None


def _path_trace_response(
    *,
    source_urn: str,
    target_urn: str,
    source_column: Optional[str],
    target_column: Optional[str],
    semantic_direction: str,
    paths: List[dict],
    metadata_extra: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "metadata": {
            "queryType": "lineage-path-trace",
            "direction": semantic_direction,
            "pathType": "column-level" if source_column else "dataset-level",
            **metadata_extra,
        },
        "source": {
            "urn": source_urn,
            **({"column": source_column} if source_column else {}),
        },
        "target": {
            "urn": target_urn,
            **({"column": target_column} if target_column else {}),
        },
        "pathCount": len(paths),
        "paths": paths,
    }


def _no_path_reason(upstreams: dict, queried: str, sought: str) -> Dict[str, Any]:
    """Why the upstream search of ``queried`` did not reach ``sought``, and whether
    that search was complete enough to prove there is no path."""
    searched = len(upstreams.get("searchResults") or [])
    total = upstreams.get("total", searched)
    # isPartial: GMS stopped the traversal early (timeout or relation limit), so
    # `total` counts only what it reached and a miss proves nothing.
    partial = bool(upstreams.get("isPartial"))
    if partial:
        reason = (
            f"The search of the {queried}'s upstream lineage stopped early "
            "(timeout or traversal limit), so a path may still exist."
        )
    elif total > searched:
        reason = (
            f"The search returned {searched} of {total} entities upstream of "
            f"the {queried} and the {sought} is not among them, so a path may "
            "still exist."
        )
    elif searched == 0:
        reason = f"The {queried} has no upstream lineage."
    else:
        reason = (
            f"The {sought} is not upstream of the {queried} "
            f"within {PATH_SEARCH_MAX_HOPS} hops."
        )
    return {"reason": reason, "searchComplete": not partial and total <= searched}


def _label_path_nodes(response: Dict[str, Any], cleaned_paths: List[dict]) -> None:
    """Apply reader decisions to the path nodes already returned.

    Label-only policies keep the paths-only contract: no further metadata read.
    """
    nodes = [
        node
        for path in cleaned_paths
        if path
        for node in (path.get("path") or [])
        if node and node.get("urn")
    ]
    urns = [node["urn"] for node in nodes]
    decisions = current_decisions(urns)
    header = decisions.header(urns) if decisions is not None else None
    if (
        decisions is None
        or not header
        or ("reader" not in header and "reader_policy_state" not in header)
    ):
        return
    for path in cleaned_paths:
        if path and path.get("path"):
            path["path"] = [
                apply_decision(node, node["urn"], decisions)
                if node and node.get("urn")
                else node
                for node in path["path"]
            ]
    attach_header(response, decisions, urns, reader_only=True)


def _find_upstream_lineage_path(
    backend: ReadBackend,
    query_urn: str,
    search_for_urn: str,
    source_urn: str,
    target_urn: str,
    source_column: Optional[str],
    target_column: Optional[str],
    semantic_direction: Literal["upstream", "downstream"],
) -> dict:
    """Look for ``search_for_urn`` in the upstream lineage of ``query_urn``.

    Lineage is symmetric (B downstream of A is A upstream of B), so a downstream
    question is answered by searching the target's upstreams and reversing the
    path. Upstream is the bounded side: tens of sources rather than thousands of
    consumers.
    """
    upstreams = graphql_helpers.clean_gql_response(
        backend.lineage_paths(
            lineage_variables(
                LineageQuery(
                    urn=query_urn,
                    max_hops=PATH_SEARCH_MAX_HOPS,
                    max_results=PATH_SEARCH_MAX_RESULTS,
                ),
                upstream=True,
            )
        )
    )
    search_results = upstreams.get("searchResults") or []

    # Finding no path is an answer, not a failed call: reported as a normal result
    # so clients and tool-call metrics don't count it as an error.
    queried, sought = (
        ("target", "source")
        if semantic_direction == "downstream"
        else ("source", "target")
    )
    response_args: Dict[str, Any] = {
        "source_urn": source_urn,
        "target_urn": target_urn,
        "source_column": source_column,
        "target_column": target_column,
        "semantic_direction": semantic_direction,
    }

    target_result = _find_result_with_target_urn(
        search_results, search_for_urn, is_column_level=target_column is not None
    )
    if not target_result:
        return _path_trace_response(
            **response_args,
            paths=[],
            metadata_extra={"noPath": _no_path_reason(upstreams, queried, sought)},
        )

    paths = target_result.get("paths") or []
    if not paths:
        degree = target_result.get("degree")
        return _path_trace_response(
            **response_args,
            paths=[],
            metadata_extra={
                "pathsUnavailable": (
                    f"The {sought} is upstream of the {queried}"
                    + (f" (degree {degree})" if degree else "")
                    + ", but DataHub returned no path details for it."
                )
            },
        )

    has_queries = any(
        (node or {}).get("type") == "QUERY"
        for path_obj in paths
        if path_obj
        for node in (path_obj.get("path") or [])
    )
    paths_metadata: Dict[str, str] = {
        "description": "Array of lineage paths showing transformation chains from source to target",
        "structure": "Each path contains alternating entities (SCHEMA_FIELD or DATASET) and optional transformation QUERY entities",
    }
    if has_queries:
        paths_metadata["queryEntities"] = (
            "QUERY entities are returned as URNs only. "
            "Use get_entities(query_urn) to fetch SQL statement and other query details."
        )

    response = _path_trace_response(
        **response_args,
        paths=paths,
        metadata_extra={"fields": {"paths": paths_metadata}},
    )
    _label_path_nodes(response, paths)
    return response


def _find_lineage_path(
    backend: ReadBackend,
    query_urn: str,
    target_full_urn: str,
    source_urn: str,
    target_urn: str,
    source_column: Optional[str],
    target_column: Optional[str],
    direction: Literal["upstream", "downstream"],
) -> dict:
    """The path in one semantic direction, always found by an upstream search.

    Downstream (source flows to target): search the target's upstreams for the
    source, then reverse each path to read source to target. Upstream (source
    depends on target): search the source's upstreams; the path already reads
    target to source.
    """
    common: Dict[str, Any] = {
        "source_urn": source_urn,
        "target_urn": target_urn,
        "source_column": source_column,
        "target_column": target_column,
        "semantic_direction": direction,
    }
    if direction == "upstream":
        return _find_upstream_lineage_path(
            backend, query_urn=query_urn, search_for_urn=target_full_urn, **common
        )
    result = _find_upstream_lineage_path(
        backend, query_urn=target_full_urn, search_for_urn=query_urn, **common
    )
    for path_obj in result.get("paths", []):
        if path_obj and "path" in path_obj:
            path_obj["path"] = list(reversed(path_obj["path"]))
    return result


@read_only
def get_lineage_paths_between(
    source_urn: str,
    target_urn: str,
    source_column: Optional[str] = None,
    target_column: Optional[str] = None,
    direction: Optional[Literal["upstream", "downstream"]] = None,
) -> dict:
    """Get detailed lineage path(s) between two specific entities or columns.

    Returns the paths array from searchAcrossLineage, showing the exact transformation
    chain(s) including intermediate entities, columns, and transformation query URNs.

    Unlike get_lineage() which returns all lineage targets with compact lineageColumns,
    this tool focuses on ONE specific target and returns detailed path information.

    Args:
        source_urn: URN of the source dataset
        target_urn: URN of the target dataset
        source_column: Optional column name in source dataset
        target_column: Optional column name in target dataset (required if source_column provided)
        direction: Optional direction to search. If None (default), automatically discovers
                  the path by trying downstream first, then upstream. Specify "downstream" or
                  "upstream" explicitly for better performance if you know the direction.

    Returns:
        Dictionary with:
        - source: Source entity/column info
        - target: Target entity/column info
        - paths: Array of path objects from GraphQL (with QUERY URNs)
        - pathCount: Number of paths found. 0 with metadata.noPath means no path
          was found; metadata.noPath.searchComplete is false when the search hit
          its result cap or DataHub stopped it early, so a path may still exist. 0 with
          metadata.pathsUnavailable means the entities are connected but no path
          details came back.

    Examples:
        # Column-level paths
        paths_result = get_lineage_paths_between(
            source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.base_table,PROD)",
            target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.final_table,PROD)",
            source_column="user_id",
            target_column="customer_id"
        )
        # Returns paths with QUERY URNs showing transformation chain

        # Fetch SQL for specific query of interest
        query_details = get_entities(paths_result["paths"][0]["path"][1]["urn"])

        # Dataset-level paths (auto-discover direction)
        get_lineage_paths_between(
            source_urn="urn:li:dataset:(...):base_table",
            target_urn="urn:li:dataset:(...):final_table"
        )

        # Explicit direction for better performance
        get_lineage_paths_between(
            source_urn="urn:li:dataset:(...):base_table",
            target_urn="urn:li:dataset:(...):final_table",
            direction="downstream"
        )
    """
    if source_column in ("null", ""):
        source_column = None
    if target_column in ("null", ""):
        target_column = None
    if (source_column is None) != (target_column is None):
        raise ToolError(
            "Both source_column and target_column must be provided for column-level "
            "lineage, or both must be None for dataset-level lineage"
        )

    tool = "get_lineage_paths_between"
    source_urn = arg_validation.require_urn(source_urn, tool=tool, param="source_urn")
    target_urn = arg_validation.require_urn(target_urn, tool=tool, param="target_urn")
    backend = get_read_backend()
    # A URN that does not exist has no lineage, which would read as "no path".
    _require_existing(backend, source_urn)
    _require_existing(backend, target_urn)

    query_urn = arg_validation.schema_field_urn(source_urn, source_column, tool=tool)
    target_full_urn = arg_validation.schema_field_urn(
        target_urn, target_column, tool=tool
    )
    args: Dict[str, Any] = {
        "query_urn": query_urn,
        "target_full_urn": target_full_urn,
        "source_urn": source_urn,
        "target_urn": target_urn,
        "source_column": source_column,
        "target_column": target_column,
    }
    if direction is not None:
        return _find_lineage_path(backend, direction=direction, **args)

    # Downstream first: the more common question.
    no_path: Dict[str, Dict[str, Any]] = {}
    attempts: tuple[Literal["downstream", "upstream"], ...] = ("downstream", "upstream")
    for attempt in attempts:
        result = _find_lineage_path(backend, direction=attempt, **args)
        if "noPath" in result["metadata"]:
            no_path[attempt] = result["metadata"]["noPath"]
            continue
        result["metadata"]["direction"] = f"auto-discovered-{attempt}"
        result["metadata"]["note"] = (
            "Direction was automatically discovered. Specify direction='downstream' "
            "or 'upstream' explicitly for better performance."
        )
        return result

    result["metadata"]["direction"] = "searched-both"
    result["metadata"]["noPath"] = {
        "reason": (
            "No lineage path found in either direction. "
            f"Downstream: {no_path['downstream']['reason']} "
            f"Upstream: {no_path['upstream']['reason']}"
        ),
        "searchComplete": all(n["searchComplete"] for n in no_path.values()),
    }
    return result
