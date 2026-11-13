"""get_aspect_history: retained aspect versions over OpenAPI v3 batchGet.

Most tests serve GMS through a fake graph session, so they exercise the default
GraphQLReadBackend and its OpenAPI client as well as the tool.
"""

import json
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from datahub.metadata.schema_classes import UpstreamLineageClass
from datahub.metadata.urns import SchemaFieldUrn
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import (
    DESCRIPTION_LENGTH_HARD_LIMIT,
    with_datahub_client,
)
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.openapi_client import RawVersionedBatch, VersionedAspectPair
from datahub_core_mcp_tools.read_backend import ReadBackend, ReadBackendBinding
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tools import aspect_history as history_module
from datahub_core_mcp_tools.tools.aspect_history import get_aspect_history
from datahub_core_mcp_tools.version_requirements import (
    get_version_requirement,
    tool_annotations,
)

URN_A = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders,PROD)"
URN_B = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customers,PROD)"
CHART_URN = "urn:li:chart:(looker,orders)"


def _aspect(value, *, run_id="run-1", newest=None, extra=None):
    """Build a batchGet envelope.

    ``newest`` populates ``systemMetadata.version``, which GMS sets on the v0
    envelope to the newest history version number, as a string.
    """
    system_metadata = {
        "runId": run_id,
        "pipelineName": "snowflake-prod",
        "properties": {"secret": "not-projected"},
    }
    if newest is not None:
        system_metadata["version"] = newest
    envelope = {
        "value": value,
        "systemMetadata": system_metadata,
        "auditStamp": {
            "time": 1_785_900_000_000,
            "actor": "urn:li:corpuser:__datahub_system",
            "message": "not-projected",
        },
    }
    if extra:
        envelope.update(extra)
    return envelope


def _entity(urn, **aspects):
    return {"urn": urn, **aspects}


def _response(body, *, status=200, error=None):
    response = MagicMock()
    response.status_code = status
    response.json.return_value = body
    if error:
        response.raise_for_status.side_effect = error
    return response


def _serve_retained_versions(graph, *, retained, newest=None):
    """Fake GMS modelled on measured DataHub Core 1.6 behaviour.

    Two properties of the real version space, both measured rather than assumed,
    and both invisible to a mock that echoes back whatever version was asked for:

    1. Retention prunes the *oldest* versions without renumbering, so the retained
       window is an arbitrary set rather than 1..N. Measured: after 26 writes the
       stored rows were 6..25.
    2. ``v0.systemMetadata.version`` reports the logical *next* version N, and a
       read at version N resolves to the same envelope as version 0 rather than
       returning empty. Measured on a clean dataset with 4 writes: v1..v3 held the
       first three writes, v4 was byte-identical to v0, v5+ were empty.

    ``newest`` is that logical N. Requests at N are therefore served the current
    envelope, exactly as the server does, so a tool that anchors at N instead of
    N-1 shows up here as a duplicate of ``current`` at ``history[0]``.
    """
    retained = set(retained)
    try:
        newest_logical = int(newest) if newest is not None else None
    except (TypeError, ValueError):
        # A non-numeric anchor is a case the tool has to survive, so the fake has
        # to serve it rather than crash: no version aliases to current.
        newest_logical = None

    def post(url, **kwargs):
        body = []
        for entity_request in json.loads(kwargs["data"]):
            urn = entity_request["urn"]
            entity: dict = {"urn": urn}
            for aspect_name, spec in entity_request.items():
                if aspect_name == "urn":
                    continue
                version = int(spec["headers"]["If-Version-Match"])
                if version == 0 or version == newest_logical:
                    # A read at the logical next version aliases to the current
                    # envelope on a real server. Measured, see the docstring.
                    entity[aspect_name] = _aspect(
                        {"v": 0}, run_id="run-current", newest=newest
                    )
                elif version in retained:
                    entity[aspect_name] = _aspect(
                        {"v": version}, run_id=f"run-{version}"
                    )
            body.append(entity)
        return _response(body)

    graph._session.post.side_effect = post


def _versions(item):
    return [entry["version"] for entry in item["history"]]


@pytest.fixture
def graph():
    value = MagicMock()
    value._gms_server = "https://datahub.example.test"
    value.exists.return_value = True
    return value


def _run(graph, *args, **kwargs):
    client = MagicMock()
    client._graph = graph
    with with_datahub_client(client):
        return get_aspect_history(*args, **kwargs)


def test_is_read_only_and_not_version_gated():
    """batchGet with a per-aspect If-Version-Match is served by GMS 1.7.0."""
    assert tool_annotations(get_aspect_history).read_only_hint is True
    assert get_version_requirement(get_aspect_history) is None


def test_pruned_prefix_window_is_reachable_newest_first(graph):
    """A high-churn aspect keeps versions 6..26; version 1 is pruned away.

    Measured against a real DataHub Core 1.6 instance after writing
    datasetProperties 26 times: v0.systemMetadata.version was "26" and the
    stored rows were 6..25, so a reader that starts at version 1 and stops at the
    first empty version returns nothing while 20 versions are retained. Version 26
    is not a row: it resolves to the same envelope as version 0.
    """
    _serve_retained_versions(graph, retained=range(6, 26), newest="26")

    result = _run(graph, URN_A, "datasetProperties", limit=10)
    item = result["results"][0]

    assert _versions(item) == [25, 24, 23, 22, 21, 20, 19, 18, 17, 16]
    assert item["page"]["anchorSource"] == "systemMetadata"
    assert item["page"]["fromVersion"] == 25
    assert item["page"]["hasMore"] is True
    assert item["page"]["nextFromVersion"] == 15


def test_history_never_duplicates_current(graph):
    """The anchor must be the newest stored row, not the logical next version.

    Measured on DataHub Core 1.6 with a clean 4-write dataset: v0.systemMetadata
    .version was "4", versions 1..3 held the first three writes, and a read at
    version 4 returned the same envelope as version 0. Anchoring at N therefore
    makes history[0] a silent duplicate of current.
    """
    _serve_retained_versions(graph, retained={1, 2, 3}, newest="4")

    item = _run(graph, URN_A, "datasetProperties", limit=10)["results"][0]

    assert _versions(item) == [3, 2, 1]
    assert item["page"]["fromVersion"] == 3
    current_value = item["current"]["value"]
    assert all(entry["value"] != current_value for entry in item["history"])


def test_single_write_has_no_positive_row(graph):
    """N < 2 means only v0 exists, so there is nothing to page and no cursor at 0."""
    _serve_retained_versions(graph, retained=set(), newest="1")

    item = _run(graph, URN_A, "datasetProperties", limit=10)["results"][0]

    assert item["history"] == []
    assert item["current"] is not None
    assert item["page"]["hasMore"] is False
    assert item["page"]["nextFromVersion"] is None


def test_version_one_is_empty_but_history_is_still_returned(graph):
    """The exact shape of the reproduction: asking for v1 alone yields nothing."""
    _serve_retained_versions(graph, retained=range(6, 26), newest="26")

    empty = _run(graph, URN_A, "datasetProperties", from_version=1, limit=10)
    assert empty["results"][0]["history"] == []

    anchored = _run(graph, URN_A, "datasetProperties", limit=10)
    assert anchored["results"][0]["history"] != []


def test_gap_inside_window_does_not_stop_paging(graph):
    """A hole inside the retained window must not be read as the end of history."""
    retained = set(range(6, 26)) - {14}
    _serve_retained_versions(graph, retained=retained, newest="26")

    item = _run(graph, URN_A, "datasetProperties", limit=20)["results"][0]

    assert 14 not in _versions(item)
    # Reaching the floor proves paging continued past the hole at 14.
    assert 6 in _versions(item)
    assert _versions(item) == sorted(retained, reverse=True)


def test_absent_system_metadata_version_falls_back_to_ascending_scan(graph):
    """systemMetadata.version is optional, so the anchor can be unresolvable."""
    _serve_retained_versions(graph, retained={1, 2, 3}, newest=None)

    item = _run(graph, URN_A, "ownership", limit=10)["results"][0]

    assert item["page"]["anchorSource"] == "fallback"
    assert _versions(item) == [3, 2, 1]
    assert item["page"]["hasMore"] is False


def test_non_numeric_system_metadata_version_falls_back(graph):
    _serve_retained_versions(graph, retained={1, 2}, newest="not-a-number")

    item = _run(graph, URN_A, "ownership", limit=10)["results"][0]

    assert item["page"]["anchorSource"] == "fallback"
    assert _versions(item) == [2, 1]


def test_aspect_never_written_costs_no_history_probes(graph):
    """No v0 means the aspect was never written, so there is nothing to page."""
    graph._session.post.return_value = _response([_entity(URN_A)])

    result = _run(graph, URN_A, "domains", limit=10)
    item = result["results"][0]

    assert item["current"] is None
    assert item["history"] == []
    assert item["error"] is None
    assert item["page"]["anchorSource"] == "aspectAbsent"
    assert item["page"]["hasMore"] is False
    # 1 exists() probe + 1 v0 read, and no version probes at all.
    assert result["batch"]["httpCalls"] == 2


def test_probe_budget_exhaustion_is_reported_as_more_available(graph):
    """Sparse windows can burn the probe budget; that must not read as "no more"."""
    # Every other version retained, so each kept version costs two probes.
    _serve_retained_versions(graph, retained=range(2, 201, 2), newest="200")

    item = _run(graph, URN_A, "datasetProperties", limit=20)["results"][0]

    assert len(_versions(item)) < 20
    assert item["page"]["hasMore"] is True
    assert item["page"]["nextFromVersion"] is not None
    # The cursor must resume below the last version already returned.
    assert item["page"]["nextFromVersion"] < _versions(item)[-1]


def test_include_current_false_still_resolves_history(graph):
    """v0 is still fetched for the anchor, just not emitted, and still counted."""
    _serve_retained_versions(graph, retained=range(6, 26), newest="26")

    result = _run(
        graph,
        URN_A,
        "datasetProperties",
        limit=3,
        include_current=False,
    )
    item = result["results"][0]

    assert item["current"] is None
    assert _versions(item) == [25, 24, 23]
    assert item["page"]["anchorSource"] == "systemMetadata"
    # 1 exists() probe + 1 v0 anchor read + 4 version reads (3 kept, 1 lookahead).
    assert result["batch"]["httpCalls"] == 6


def test_from_version_is_clamped_to_newest_retained_version(graph):
    _serve_retained_versions(graph, retained=range(6, 26), newest="26")

    item = _run(graph, URN_A, "datasetProperties", from_version=999, limit=3)[
        "results"
    ][0]

    assert item["page"]["anchorSource"] == "caller"
    assert item["page"]["fromVersion"] == 25
    assert _versions(item) == [25, 24, 23]


def test_from_version_pages_downward_from_the_cursor(graph):
    _serve_retained_versions(graph, retained=range(6, 26), newest="26")

    first = _run(graph, URN_A, "datasetProperties", limit=10)["results"][0]
    second = _run(
        graph,
        URN_A,
        "datasetProperties",
        from_version=first["page"]["nextFromVersion"],
        limit=10,
    )["results"][0]

    assert _versions(second) == [15, 14, 13, 12, 11, 10, 9, 8, 7, 6]
    assert set(_versions(first)).isdisjoint(_versions(second))


def test_cross_product_batches_pairs_once_per_version_and_orders_results(graph):
    _serve_retained_versions(graph, retained={1, 2}, newest=None)

    result = _run(graph, [URN_A, URN_B], ["ownership", "domains"], limit=3)

    assert [(item["urn"], item["aspectName"]) for item in result["results"]] == [
        (URN_A, "ownership"),
        (URN_A, "domains"),
        (URN_B, "ownership"),
        (URN_B, "domains"),
    ]
    assert result["results"][0]["current"]["value"]["v"] == 0
    assert _versions(result["results"][0]) == [2, 1]
    assert result["batch"]["urns"] == 2
    assert result["batch"]["pairs"] == 4
    assert result["batch"]["returnedPairs"] == 4
    assert result["batch"]["droppedPairs"] == []

    first = graph._session.post.call_args_list[0]
    assert first.args[0].endswith("/openapi/v3/entity/dataset/batchGet")
    assert first.kwargs["params"] == {"systemMetadata": "true"}
    payload = json.loads(first.kwargs["data"])
    assert payload == [
        {
            "urn": URN_A,
            "ownership": {"headers": {"If-Version-Match": "0"}},
            "domains": {"headers": {"If-Version-Match": "0"}},
        },
        {
            "urn": URN_B,
            "ownership": {"headers": {"If-Version-Match": "0"}},
            "domains": {"headers": {"If-Version-Match": "0"}},
        },
    ]


def test_pairs_with_different_anchors_share_one_request_per_step(graph):
    """Per-aspect version headers keep batching intact despite per-pair anchors."""

    def post(url, **kwargs):
        body = []
        for entity_request in json.loads(kwargs["data"]):
            urn = entity_request["urn"]
            entity: dict = {"urn": urn}
            newest = "26" if urn == URN_A else "9"
            for aspect_name, spec in entity_request.items():
                if aspect_name == "urn":
                    continue
                version = int(spec["headers"]["If-Version-Match"])
                if version == 0 or version == int(newest):
                    # A read at the logical next version aliases to current.
                    entity[aspect_name] = _aspect({"v": 0}, newest=newest)
                elif 1 <= version < int(newest):
                    entity[aspect_name] = _aspect({"v": version})
            body.append(entity)
        return _response(body)

    graph._session.post.side_effect = post

    result = _run(graph, [URN_A, URN_B], "datasetProperties", limit=2)
    by_urn = {item["urn"]: item for item in result["results"]}

    assert _versions(by_urn[URN_A]) == [25, 24]
    assert _versions(by_urn[URN_B]) == [8, 7]

    # Step 1 asks each URN for its own anchor inside a single request.
    step_one = json.loads(graph._session.post.call_args_list[1].kwargs["data"])
    versions = {
        entry["urn"]: entry["datasetProperties"]["headers"]["If-Version-Match"]
        for entry in step_one
    }
    assert versions == {URN_A: "25", URN_B: "8"}


@pytest.mark.parametrize(
    ("urns", "aspects"),
    [
        (URN_A, "ownership"),
        (json.dumps([URN_A]), json.dumps(["ownership"])),
        ([f" {URN_A} "], [" ownership"]),
    ],
)
def test_accepts_single_or_json_stringified_lists(graph, urns, aspects):
    _serve_retained_versions(graph, retained=set(), newest=None)
    result = _run(graph, urns, aspects)
    assert result["batch"]["pairs"] == 1
    assert result["results"][0]["error"] is None


def test_limit_and_paging_are_per_pair_with_honest_lookahead(graph):
    _serve_retained_versions(graph, retained=range(6, 26), newest="26")

    result = _run(graph, [URN_A, URN_B], "datasetProperties", limit=1)

    for item in result["results"]:
        assert _versions(item) == [25]
        assert item["page"]["hasMore"] is True
        assert item["page"]["nextFromVersion"] == 24
        assert item["page"]["requestedLimit"] == 1


def test_pair_local_validation_and_missing_entity_do_not_abort_batch(graph):
    graph.exists.side_effect = lambda urn: urn != URN_B
    _serve_retained_versions(graph, retained=set(), newest=None)
    result = _run(
        graph,
        [URN_A, URN_B, "not-a-urn"],
        ["ownership", "dataHubIngestionSourceInfo"],
    )
    by_pair = {(item["urn"], item["aspectName"]): item for item in result["results"]}
    assert by_pair[(URN_A, "ownership")]["error"] is None
    assert "has no history" in by_pair[(URN_A, "dataHubIngestionSourceInfo")]["error"]
    assert "not found" in by_pair[(URN_B, "ownership")]["error"]
    # A name gets the hint to look it up first.
    assert "search" in by_pair[("not-a-urn", "ownership")]["error"]


def test_a_call_with_no_readable_pair_fails(graph):
    graph.exists.return_value = False
    with pytest.raises(ToolError, match="not found"):
        _run(graph, URN_A, ["ownership", "schemaMetadata"])
    graph._session.post.assert_not_called()


def test_aspects_without_history_fail_before_any_request(graph):
    with pytest.raises(ToolError, match="ownership"):
        _run(graph, URN_A, ["dataHubIngestionSourceInfo"])
    graph.exists.assert_not_called()
    graph._session.post.assert_not_called()


def test_http_calls_include_per_urn_exists_probes(graph):
    _serve_retained_versions(graph, retained=set(), newest=None)

    result = _run(graph, [URN_A, URN_B], "ownership", limit=1)

    assert graph.exists.call_count == 2
    # 2 exists() probes + 1 v0 read + 9 fallback probes before the miss budget trips.
    assert result["batch"]["httpCalls"] == 2 + 1 + 9


def test_transport_failure_is_pair_local_across_entity_types(graph):
    def post(url, **kwargs):
        if "/dataset/" in url:
            return _response([], status=503, error=RuntimeError("provider detail"))
        return _response([_entity(CHART_URN, ownership=_aspect({"owners": []}))])

    graph._session.post.side_effect = post
    result = _run(graph, [URN_A, CHART_URN], "ownership", limit=1)
    dataset, chart = result["results"]
    assert dataset["error"] == "DataHub batchGet failed (RuntimeError)"
    assert "provider detail" not in dataset["error"]
    assert chart["error"] is None
    assert chart["current"] is not None


def test_provenance_is_allowlisted_and_values_are_bounded(graph):
    graph._session.post.return_value = _response(
        [
            _entity(
                URN_A,
                ownership=_aspect(
                    {"description": "word " * 2_000}, run_id="run-current"
                ),
            )
        ]
    )
    result = _run(graph, URN_A, "ownership")
    current = result["results"][0]["current"]
    description = current["value"]["description"]
    assert description.startswith("word word")
    assert len(description) <= DESCRIPTION_LENGTH_HARD_LIMIT
    assert current["systemMetadata"]["runId"] == "run-current"
    assert "properties" not in current["systemMetadata"]
    assert current["auditStamp"] == {
        "time": 1_785_900_000_000,
        "actor": "urn:li:corpuser:__datahub_system",
    }


def test_oversized_value_returns_preview_instead_of_raw_value(graph):
    graph._session.post.return_value = _response(
        [_entity(URN_A, ownership=_aspect({"text": "x" * 20_000}))]
    )
    result = _run(graph, URN_A, "ownership")
    current = result["results"][0]["current"]
    assert current["valueTruncated"] is True
    assert current["valueChars"] > history_module.MAX_ASPECT_VALUE_CHARS
    assert len(current["valuePreview"]) <= history_module.MAX_ASPECT_VALUE_CHARS
    assert "value" not in current
    assert "compactLineage" not in current


UPSTREAM_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.order_details,PROD)"


def _field(dataset_urn, field_path):
    return f"urn:li:schemaField:({dataset_urn},{field_path})"


def _wide_lineage(columns):
    """An upstreamLineage shaped like a wide BI model: one mapping per column."""
    return {
        "upstreams": [
            {
                "type": "TRANSFORMED",
                "dataset": UPSTREAM_URN,
                "auditStamp": {"time": 0, "actor": "urn:li:corpuser:datahub"},
            }
        ],
        "fineGrainedLineages": [
            {
                "upstreamType": "FIELD_SET",
                "upstreams": [_field(UPSTREAM_URN, f"col_{i}")],
                "downstreamType": "FIELD",
                "downstreams": [_field(URN_A, f"COL_{i}")],
                "confidenceScore": 1.0,
            }
            for i in range(columns)
        ],
    }


def _json_chars(value):
    """Size oracle independent of the module's own helper."""
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")))


def _compact(lineage: dict) -> dict:
    compact = history_module._compact_lineage(lineage)
    assert compact is not None
    return compact


def _resolve(compact, ref):
    dataset_index, field_path = ref
    dataset = compact["datasets"][dataset_index]
    return dataset if field_path is None else str(SchemaFieldUrn(dataset, field_path))


def test_oversized_lineage_returns_every_column_mapping_compactly(graph):
    lineage = _wide_lineage(200)
    # A single write: only v0 exists, so the response is the current value alone.
    graph._session.post.return_value = _response(
        [_entity(URN_A, upstreamLineage=_aspect(lineage, newest="1"))]
    )
    result = _run(graph, URN_A, "upstreamLineage")
    current = result["results"][0]["current"]
    assert current["valueTruncated"] is True
    assert current["valueChars"] > history_module.MAX_ASPECT_VALUE_CHARS
    assert "valuePreview" not in current

    compact = current["compactLineage"]
    assert compact["truncated"] is False
    assert compact["datasets"] == [UPSTREAM_URN, URN_A]
    assert compact["upstreams"] == [{"dataset": 0, "type": "TRANSFORMED"}]
    assert compact["fineGrainedLineagesTotal"] == 200
    assert len(compact["fineGrainedLineages"]) == 200
    for original, edge in zip(
        lineage["fineGrainedLineages"], compact["fineGrainedLineages"], strict=True
    ):
        assert [_resolve(compact, ref) for ref in edge["upstreams"]] == original[
            "upstreams"
        ]
        assert [_resolve(compact, ref) for ref in edge["downstreams"]] == original[
            "downstreams"
        ]
        assert edge["confidenceScore"] == 1.0
    assert _json_chars(compact) < current["valueChars"] / 2
    assert _json_chars(compact) <= history_module.MAX_COMPACT_LINEAGE_CHARS
    assert result["batch"]["truncatedByResponseBudget"] is False


def test_lineage_that_fits_returns_the_raw_value_only(graph):
    graph._session.post.return_value = _response(
        [_entity(URN_A, upstreamLineage=_aspect(_wide_lineage(3)))]
    )
    current = _run(graph, URN_A, "upstreamLineage")["results"][0]["current"]
    assert current["valueTruncated"] is False
    assert len(current["value"]["fineGrainedLineages"]) == 3
    assert "compactLineage" not in current


def test_compact_lineage_keeps_dataset_and_query_references(graph):
    lineage = _wide_lineage(200)
    query_urn = "urn:li:query:orders-model"
    lineage["upstreams"][0].update(
        {
            "query": "urn:li:query:orders-etl",
            "matchType": "NORMALIZED",
            "properties": {"source": "dbt"},
            "created": {"time": 1, "actor": "urn:li:corpuser:datahub"},
        }
    )
    lineage["fineGrainedLineages"][0] = {
        "upstreamType": "DATASET",
        "upstreams": [UPSTREAM_URN],
        "downstreamType": "FIELD",
        "downstreams": [_field(URN_A, "[version=2.0].[type=struct].address.city")],
        "transformOperation": "COALESCE",
        "query": query_urn,
        "matchType": "UNRESOLVED",
    }
    graph._session.post.return_value = _response(
        [_entity(URN_A, upstreamLineage=_aspect(lineage))]
    )
    current = _run(graph, URN_A, "upstreamLineage")["results"][0]["current"]
    compact = current["compactLineage"]
    assert compact["upstreams"] == [
        {
            "dataset": 0,
            "type": "TRANSFORMED",
            "query": "urn:li:query:orders-etl",
            "matchType": "NORMALIZED",
            "properties": {"source": "dbt"},
        }
    ]
    assert compact["fineGrainedLineages"][0] == {
        "upstreams": [[0, None]],
        "downstreams": [[1, "[version=2.0].[type=struct].address.city"]],
        "upstreamType": "DATASET",
        "transformOperation": "COALESCE",
        "query": query_urn,
        "matchType": "UNRESOLVED",
    }


def test_compact_lineage_copies_long_values_whole():
    lineage = _wide_lineage(3)
    expression = "CASE " + "WHEN a THEN b " * 200 + "END"
    lineage["fineGrainedLineages"][0]["transformOperation"] = expression
    compact = _compact(lineage)
    assert compact["truncated"] is False
    assert compact["fineGrainedLineages"][0]["transformOperation"] == expression


def test_compact_lineage_keeps_lineage_types_that_differ_from_the_usual():
    lineage = _wide_lineage(3)
    lineage["fineGrainedLineages"][1].update(
        {"upstreamType": "NONE", "upstreams": [], "downstreamType": "FIELD_SET"}
    )
    edges = _compact(lineage)["fineGrainedLineages"]
    assert "upstreamType" not in edges[0] and "downstreamType" not in edges[0]
    assert edges[1]["upstreamType"] == "NONE"
    assert edges[1]["downstreamType"] == "FIELD_SET"


def test_compact_lineage_is_built_from_the_raw_value(graph):
    """Response cleaning must not alter free-form Upstream.properties."""
    lineage = _wide_lineage(200)
    properties = {"description": "<b>" + "d" * 6_000 + "</b>", "__typename": "kept"}
    lineage["upstreams"][0]["properties"] = properties
    graph._session.post.return_value = _response(
        [_entity(URN_A, upstreamLineage=_aspect(lineage, newest="1"))]
    )
    current = _run(graph, URN_A, "upstreamLineage")["results"][0]["current"]
    assert current["compactLineage"]["upstreams"][0]["properties"] == properties


def test_compact_lineage_keeps_every_upstream_in_order_with_its_values():
    """Several upstreams, a non-default confidence and long values, schema-valid."""
    urns = [
        f"urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.{name},PROD)"
        for name in ("a", "b", "c")
    ]
    query = "urn:li:query:" + "q" * 650
    properties = {"description": "<b>" + "d" * 6_000 + "</b>", "__typename": "kept"}
    upstreams = [
        {"dataset": urns[0], "type": "COPY"},
        {"dataset": urns[1], "type": "VIEW", "query": query, "properties": properties},
    ]
    edge = {
        "upstreamType": "FIELD_SET",
        "upstreams": [
            str(SchemaFieldUrn(urns[0], "amount,usd")),
            str(SchemaFieldUrn(urns[1], "a\u241fb")),
        ],
        "downstreamType": "FIELD_SET",
        "downstreams": [str(SchemaFieldUrn(urns[2], "COL"))],
        "confidenceScore": 0.625,
        "query": query,
    }
    lineage: dict[str, Any] = {
        "upstreams": upstreams,
        "fineGrainedLineages": [edge] * 200,
    }
    assert UpstreamLineageClass.from_obj(lineage).validate()
    compact = history_module._format_aspect_version(
        0, {"value": lineage}, compact_lineage=True
    )["compactLineage"]
    assert compact["upstreamsTotal"] == 2
    assert [compact["datasets"][item["dataset"]] for item in compact["upstreams"]] == [
        urns[0],
        urns[1],
    ]
    assert compact["upstreams"][1]["query"] == query
    assert compact["upstreams"][1]["properties"] == properties
    mapping = compact["fineGrainedLineages"][0]
    assert mapping["confidenceScore"] == 0.625
    assert mapping["query"] == query
    assert mapping["downstreamType"] == "FIELD_SET"
    assert [_resolve(compact, ref) for ref in mapping["upstreams"]] == edge["upstreams"]


def test_compact_lineage_decodes_reserved_characters_in_field_paths():
    lineage = _wide_lineage(3)
    paths = ["amount,usd", "total(net)", "a\u241fb"]
    for edge, path in zip(lineage["fineGrainedLineages"], paths, strict=True):
        edge["downstreams"] = [str(SchemaFieldUrn(URN_A, path))]
    assert "amount%2Cusd" in lineage["fineGrainedLineages"][0]["downstreams"][0]
    compact = _compact(lineage)
    edges = compact["fineGrainedLineages"]
    assert [edge["downstreams"][0][1] for edge in edges] == paths
    for original, edge in zip(lineage["fineGrainedLineages"], edges, strict=True):
        assert _resolve(compact, edge["downstreams"][0]) == original["downstreams"][0]
    # Only the encoder's own uppercase form is decoded, as in the web app.
    assert history_module._decode_field_path("x%2cy%2Cz") == "x%2cy,z"


def test_compact_lineage_tolerates_malformed_references():
    lineage = _wide_lineage(3)
    chart_urn = "urn:li:chart:(looker,orders)"
    lineage["fineGrainedLineages"][0]["downstreams"] = _field(URN_A, "COL_0")
    lineage["fineGrainedLineages"][1]["downstreams"] = {"not": "a list"}
    lineage["fineGrainedLineages"][2]["upstreams"] = [chart_urn, "not a urn", 7]
    compact = _compact(lineage)
    edges = compact["fineGrainedLineages"]
    # A bare string is one reference, not a sequence of characters.
    assert [_resolve(compact, ref) for ref in edges[0]["downstreams"]] == [
        _field(URN_A, "COL_0")
    ]
    assert edges[1]["downstreams"] == []
    chart_ref, *unparsed = edges[2]["upstreams"]
    assert _resolve(compact, chart_ref) == chart_urn
    assert chart_ref[1] is None
    assert unparsed == ["not a urn", 7]


def test_compact_lineage_cut_keeps_a_consistent_prefix(graph, monkeypatch):
    lineage = _wide_lineage(200)
    # Every mapping introduces a dataset of its own, so the mapping that does not
    # fit has one too; it must not leak into ``datasets``.
    for i, edge in enumerate(lineage["fineGrainedLineages"]):
        edge["upstreams"] = [
            _field(f"urn:li:dataset:(urn:li:dataPlatform:s3,bucket/part_{i},PROD)", "x")
        ]
    monkeypatch.setattr(history_module, "MAX_COMPACT_LINEAGE_CHARS", 3_000)
    graph._session.post.return_value = _response(
        [_entity(URN_A, upstreamLineage=_aspect(lineage))]
    )
    current = _run(graph, URN_A, "upstreamLineage")["results"][0]["current"]
    compact = current["compactLineage"]
    returned = compact["fineGrainedLineages"]
    assert compact["truncated"] is True
    assert compact["fineGrainedLineagesTotal"] == 200
    assert 0 < len(returned) < 200
    referenced = {
        ref[0] for edge in returned for ref in edge["upstreams"] + edge["downstreams"]
    } | {upstream["dataset"] for upstream in compact["upstreams"]}
    assert referenced == set(range(len(compact["datasets"])))
    for original, edge in zip(lineage["fineGrainedLineages"], returned, strict=False):
        assert [_resolve(compact, ref) for ref in edge["upstreams"]] == original[
            "upstreams"
        ]
    assert _json_chars(compact) <= 3_000


@pytest.mark.parametrize("columns", [300, 500, 1_000])
def test_compact_lineage_cut_enforces_the_cap_on_the_whole_object(columns):
    """The cap holds for the encoded projection, separators and totals included."""
    lineage = _wide_lineage(columns)
    compact = _compact(lineage)
    cap = history_module.MAX_COMPACT_LINEAGE_CHARS
    chars = _json_chars(compact)
    assert compact["truncated"] is True
    assert chars <= cap
    # Still a tight fit: the cut leaves less room than one more mapping needs.
    largest = max(_json_chars(edge) for edge in compact["fineGrainedLineages"])
    assert chars > cap - largest - 1


def test_compact_lineage_cut_is_tight_and_rolls_back_new_datasets(monkeypatch):
    """The cut keeps every entry that fits, and forgets datasets a cut entry added."""
    lineage = _wide_lineage(200)
    for i, edge in enumerate(lineage["fineGrainedLineages"][120:], start=120):
        edge["upstreams"] = [
            _field(f"urn:li:dataset:(urn:li:dataPlatform:s3,part_{i}_{n},PROD)", "x")
            for n in range(3)
        ]
    prefix = _compact(
        {**lineage, "fineGrainedLineages": lineage["fineGrainedLineages"][:120]}
    )
    prefix.update({"fineGrainedLineagesTotal": 200, "truncated": True})
    size = _json_chars(prefix)
    monkeypatch.setattr(history_module, "MAX_COMPACT_LINEAGE_CHARS", size)
    assert history_module._compact_lineage(lineage) == prefix


def test_compact_lineage_needs_room_for_its_own_envelope(monkeypatch):
    monkeypatch.setattr(history_module, "MAX_COMPACT_LINEAGE_CHARS", 50)
    assert history_module._compact_lineage(_wide_lineage(3)) is None


def test_compact_lineage_cap_counts_every_character(monkeypatch):
    lineage = _wide_lineage(200)
    full = _compact(lineage)
    size = _json_chars(full)
    monkeypatch.setattr(history_module, "MAX_COMPACT_LINEAGE_CHARS", size)
    assert history_module._compact_lineage(lineage) == full
    monkeypatch.setattr(history_module, "MAX_COMPACT_LINEAGE_CHARS", size - 1)
    cut = _compact(lineage)
    assert cut["truncated"] is True
    assert _json_chars(cut) <= size - 1


def test_wide_lineage_history_keeps_previews_and_pages_forward(graph):
    """Only the current value is compacted, so every page has room for history."""

    def lineage_at(version):
        lineage = _wide_lineage(1_000)
        for i, edge in enumerate(lineage["fineGrainedLineages"]):
            edge["downstreams"] = [_field(URN_A, f"COL_{i}_v{version}")]
        return lineage

    lineages = {version: lineage_at(version) for version in range(6)}

    def post(url, **kwargs):
        body = []
        for entity_request in json.loads(kwargs["data"]):
            entity: dict = {"urn": entity_request["urn"]}
            for aspect_name, spec in entity_request.items():
                if aspect_name == "urn":
                    continue
                version = int(spec["headers"]["If-Version-Match"])
                if version in lineages:
                    entity[aspect_name] = _aspect(lineages[version], newest="6")
            body.append(entity)
        return _response(body)

    graph._session.post.side_effect = post
    seen: list[int] = []
    from_version = None
    for _page in range(6):
        item = _run(
            graph, URN_A, "upstreamLineage", from_version=from_version, limit=5
        )["results"][0]
        current = item["current"]["compactLineage"]
        assert current["fineGrainedLineages"][0]["downstreams"] == [[1, "COL_0_v0"]]
        assert item["history"], "a page without history cannot advance the cursor"
        for entry in item["history"]:
            assert "compactLineage" not in entry
            assert f"COL_0_v{entry['version']})" in entry["valuePreview"]
        seen.extend(_versions(item))
        from_version = item["page"]["nextFromVersion"]
        if from_version is None:
            break
    assert seen == [5, 4, 3, 2, 1]


def _serve_wide_lineage_versions(graph, *, system_metadata=None):
    """Versions 0..5 of a 1,000-mapping lineage; v0 reports logical version 6."""
    lineage = _wide_lineage(1_000)

    def post(url, **kwargs):
        body = []
        for entity_request in json.loads(kwargs["data"]):
            entity: dict = {"urn": entity_request["urn"]}
            for aspect_name, spec in entity_request.items():
                if aspect_name == "urn":
                    continue
                version = int(spec["headers"]["If-Version-Match"])
                if version <= 5:
                    aspect = _aspect(lineage, newest="6")
                    aspect["systemMetadata"].update(system_metadata or {})
                    entity[aspect_name] = aspect
            body.append(entity)
        return _response(body)

    graph._session.post.side_effect = post


def _walk_history(graph, pages=8):
    seen, items, from_version = [], [], None
    for _page in range(pages):
        item = _run(
            graph, URN_A, "upstreamLineage", from_version=from_version, limit=5
        )["results"][0]
        items.append(item)
        seen.extend(_versions(item))
        from_version = item["page"]["nextFromVersion"]
        if from_version is None:
            break
    return seen, items


def test_continuation_pages_give_history_priority_over_current(graph):
    # Schema-valid provenance whose JSON escaping (\u0001 is six characters) leaves
    # no room for current and a history entry together.
    _serve_wide_lineage_versions(
        graph,
        system_metadata={
            key: "\x01" * 512
            for key in ("runId", "lastRunId", "pipelineName", "registryName")
        },
    )
    seen, items = _walk_history(graph)
    assert seen == [5, 4, 3, 2, 1]
    first, *continuation = items
    assert "compactLineage" in first["current"]
    assert "currentOmittedByResponseBudget" not in first["page"]
    assert first["history"] == []
    for item in continuation:
        assert item["current"] is None
        assert item["page"]["currentOmittedByResponseBudget"] is True
        assert item["history"]


def test_continuation_pages_keep_current_when_it_fits(graph):
    _serve_wide_lineage_versions(graph)
    seen, items = _walk_history(graph)
    assert seen == [5, 4, 3, 2, 1]
    for item in items:
        assert "compactLineage" in item["current"]
        assert "currentOmittedByResponseBudget" not in item["page"]


_CONTROL_PROVENANCE = {
    # Schema-valid, and each escapes to 3,072 JSON characters (\u0001 is six).
    key: "\x01" * 512
    for key in ("runId", "lastRunId", "pipelineName", "registryName", "registryVersion")
}


def _extreme_envelope(value, *, newest):
    """Schema-valid provenance at its 512-character bounds, all control characters."""
    actor = "urn:li:corpuser:" + "\x01" * (512 - len("urn:li:corpuser:"))
    stamp = {"time": 1_785_900_000_000, "actor": actor, "impersonator": actor}
    return {
        "value": value,
        "systemMetadata": {
            **_CONTROL_PROVENANCE,
            "version": str(newest),
            "aspectCreated": stamp,
            "aspectModified": stamp,
        },
        "auditStamp": stamp,
    }


def _serve_versions(graph, versions):
    """Serve ``versions[(urn, aspect_name)][version]`` envelopes; others are absent."""

    def post(url, **kwargs):
        body = []
        for entity_request in json.loads(kwargs["data"]):
            urn = entity_request["urn"]
            entity: dict = {"urn": urn}
            for aspect_name, spec in entity_request.items():
                if aspect_name == "urn":
                    continue
                version = int(spec["headers"]["If-Version-Match"])
                envelope = versions.get((urn, aspect_name), {}).get(version)
                if envelope is not None:
                    entity[aspect_name] = envelope
            body.append(entity)
        return _response(body)

    graph._session.post.side_effect = post


def _wide_versions(newest, *, provenance=None, columns=1_000):
    envelope = _aspect(_wide_lineage(columns), newest=str(newest))
    envelope["systemMetadata"].update(provenance or {})
    return {version: envelope for version in range(newest)}


def _pairs_chars(result):
    return sum(_json_chars(item) for item in result["results"])


def test_continuation_drops_a_pair_with_no_room_for_any_history(graph):
    """A batch must not return history-less pairs whose cursor cannot move."""
    # Not in customProperties: response cleaning caps those values.
    big_history = _aspect({"qualifiedName": '"' * 20_000})
    _serve_versions(
        graph,
        {
            (URN_A, "upstreamLineage"): _wide_versions(2),
            (URN_A, "datasetProperties"): {
                0: _aspect({"name": "orders"}, newest="2"),
                1: big_history,
            },
        },
    )
    result = _run(
        graph,
        URN_A,
        ["upstreamLineage", "datasetProperties"],
        from_version=1,
        limit=1,
    )
    assert [item["aspectName"] for item in result["results"]] == ["upstreamLineage"]
    assert _versions(result["results"][0]) == [1]
    assert result["batch"]["droppedPairs"] == [
        {"urn": URN_A, "aspectName": "datasetProperties"}
    ]
    alone = _run(graph, URN_A, "datasetProperties", from_version=1, limit=1)
    assert _versions(alone["results"][0]) == [1]


def test_continuation_drops_a_history_entry_too_large_for_the_budget(graph):
    huge = _extreme_envelope({"qualifiedName": '"' * 20_000}, newest=2)
    _serve_versions(
        graph,
        {
            (URN_A, "datasetProperties"): {
                0: _aspect({"name": "orders"}, newest="2"),
                1: huge,
            }
        },
    )
    result = _run(graph, URN_A, "datasetProperties", from_version=1, limit=1)
    assert result["results"] == []
    assert result["batch"]["droppedPairs"] == [
        {"urn": URN_A, "aspectName": "datasetProperties"}
    ]


def test_first_page_falls_back_to_the_preview_when_compact_lineage_does_not_fit(
    graph,
):
    envelope = _extreme_envelope(_wide_lineage(1_000), newest=2)
    _serve_versions(graph, {(URN_A, "upstreamLineage"): {0: envelope, 1: envelope}})
    result = _run(graph, URN_A, "upstreamLineage", limit=1)
    item = result["results"][0]
    assert "compactLineage" not in item["current"]
    assert "valuePreview" in item["current"]
    assert item["page"]["truncatedByResponseBudget"] is True
    assert "currentOmittedByResponseBudget" not in item["page"]
    assert _pairs_chars(result) <= history_module.MAX_RESULTS_CHARS


def test_history_that_fits_once_current_is_left_out_ends_the_walk(graph):
    _serve_versions(
        graph,
        {(URN_A, "upstreamLineage"): _wide_versions(2, provenance=_CONTROL_PROVENANCE)},
    )
    item = _run(graph, URN_A, "upstreamLineage", from_version=1, limit=5)["results"][0]
    assert item["current"] is None
    assert _versions(item) == [1]
    assert item["page"] == {
        **item["page"],
        "returned": 1,
        "hasMore": False,
        "nextFromVersion": None,
        "truncatedByResponseBudget": True,
        "currentOmittedByResponseBudget": True,
    }


def test_pairs_that_leave_current_out_share_one_budget(graph):
    urns = [
        f"urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.t{i},PROD)"
        for i in range(3)
    ]
    _serve_versions(
        graph,
        {
            (urn, "upstreamLineage"): _wide_versions(3, provenance=_CONTROL_PROVENANCE)
            for urn in urns
        },
    )
    result = _run(graph, urns, "upstreamLineage", from_version=2, limit=5)
    assert _pairs_chars(result) <= history_module.MAX_RESULTS_CHARS
    returned = {item["urn"] for item in result["results"]}
    dropped = {pair["urn"] for pair in result["batch"]["droppedPairs"]}
    assert returned and dropped and returned | dropped == set(urns)
    for item in result["results"]:
        assert item["current"] is None and item["history"]


def test_global_budget_reports_dropped_pairs(graph, monkeypatch):
    graph._session.post.return_value = _response(
        [
            _entity(URN_A, ownership=_aspect({"text": "x" * 200})),
            _entity(URN_B, ownership=_aspect({"text": "y" * 200})),
        ]
    )
    monkeypatch.setattr(history_module, "MAX_RESULTS_CHARS", 700)
    result = _run(graph, [URN_A, URN_B], "ownership")
    assert result["batch"]["truncatedByResponseBudget"] is True
    assert result["batch"]["returnedPairs"] < 2
    assert result["batch"]["droppedPairs"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"from_version": 0}, "from_version"),
        ({"from_version": True}, "from_version"),
        ({"limit": 0}, "limit"),
        ({"limit": 21}, "limit"),
        ({"include_current": "true"}, "include_current"),
        ({"urns": []}, "urns"),
        ({"aspect_names": []}, "aspect_names"),
        ({"urns": [URN_A] * 11}, "urns"),
        ({"aspect_names": ["ownership"] * 9}, "aspect_names"),
        (
            {"urns": [URN_A] * 6, "aspect_names": ["ownership"] * 8},
            "must not exceed",
        ),
    ],
)
def test_rejects_unbounded_or_ambiguous_arguments(kwargs, message):
    arguments = {"urns": URN_A, "aspect_names": "ownership", **kwargs}
    with pytest.raises(ToolError, match=message):
        get_aspect_history(**arguments)


def test_response_explains_retention_and_untrusted_data(graph):
    _serve_retained_versions(graph, retained=set(), newest=None)
    result = _run(graph, URN_A, "domains")
    assert "retention policy" in result["provenance"]["boundedBy"]
    assert "newest to oldest" in result["provenance"]["versionSemantics"]["historical"]
    assert "untrusted catalog data" in result["dataHandling"]


def test_reported_response_budget_is_the_one_that_is_enforced(graph):
    _serve_retained_versions(graph, retained=set(), newest=None)
    result = _run(graph, URN_A, "domains")
    assert result["responseBudgetChars"] == history_module.MAX_RESULTS_CHARS


def test_404_at_versioned_seam_keeps_the_specific_diagnostic(graph):
    graph._session.post.return_value = _response([], status=404)
    result = _run(graph, URN_A, "ownership")
    error = result["results"][0]["error"]
    assert "versioned OpenAPI v3 batchGet is unavailable" in error
    # 1 exists() probe + 1 v0 read; the pair is abandoned after the seam fails.
    assert result["batch"]["httpCalls"] == 2


class _FixedBackend:
    """A ReadBackend serving fixed envelopes, as a host-bound backend would."""

    def __init__(self, envelopes: dict[tuple[str, str, int], dict]) -> None:
        self.envelopes = envelopes
        self.requested: list[VersionedAspectPair] = []

    def exists(self, urn: str) -> bool:
        return True

    def aspect_versions(self, pairs: list[VersionedAspectPair]) -> RawVersionedBatch:
        self.requested.extend(pairs)
        return RawVersionedBatch(
            aspects={
                (pair.urn, pair.aspect_name): envelope
                for pair in pairs
                if (
                    envelope := self.envelopes.get(
                        (pair.urn, pair.aspect_name, pair.version)
                    )
                )
                is not None
            },
            errors={},
            http_calls=1,
        )


def test_reads_go_through_a_bound_read_backend():
    backend = _FixedBackend(
        {
            (URN_A, "ownership", 0): _aspect({"owners": ["b"]}, newest="3"),
            (URN_A, "ownership", 2): _aspect({"owners": ["a"]}),
            (URN_A, "ownership", 1): _aspect({"owners": []}),
        }
    )
    client = MagicMock()
    bound = ReadBackendBinding(cast(ReadBackend, backend))
    with with_datahub_client(client, ToolContext([bound])):
        item = get_aspect_history(URN_A, "ownership")["results"][0]

    assert item["current"]["value"] == {"owners": ["b"]}
    assert [entry["value"] for entry in item["history"]] == [{"owners": ["a"]}, {}]
    assert [pair.version for pair in backend.requested] == [0, 2, 1]
    client._graph._session.post.assert_not_called()


async def test_registered_read_only_with_list_parameters() -> None:
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    async with Client(mcp) as client:
        tool = {t.name: t for t in await client.list_tools()}["get_aspect_history"]

    assert tool.annotations is not None
    assert tool.annotations.read_only_hint is True
    for name in ("urns", "aspect_names"):
        assert tool.input_schema["properties"][name]["type"] == "array"
