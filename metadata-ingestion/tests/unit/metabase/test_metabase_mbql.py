"""Unit tests for MBQL (query builder) lineage extraction in the Metabase source.

Covers:
- _extract_field_ids_from_mbql: recursive MBQL field ID extraction
- _get_table_urns_from_query_builder: join lineage path
- _get_cll_from_query_builder: full column-level lineage pipeline
  - direct field refs ["field", id, ...]
  - aggregations with explicit fields ["avg", ["field", id, ...]]
  - COUNT(*)-style aggregations with no field argument (fan-in all upstream columns)
  - calculated expression refs ["expression", "name"]
  - unknown ref types (should be silently skipped)
- _emit_model_workunits: CLL path wired for query-builder models
"""

from unittest.mock import MagicMock, patch

import pydantic

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.metabase import (
    DatasourceInfo,
    MetabaseCard,
    MetabaseCardListItem,
    MetabaseConfig,
    MetabaseDatasetQuery,
    MetabaseField,
    MetabaseQuery,
    MetabaseResultMetadata,
    MetabaseSource,
    _extract_field_ids_from_mbql,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    UpstreamLineageClass,
)

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

FILM_DATASOURCE = DatasourceInfo(
    platform="clickhouse",
    database_name="analytics",
    schema="default",
    platform_instance=None,
)

FILM_TABLE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:clickhouse,analytics.default.film,PROD)"
)
ACTOR_TABLE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:clickhouse,analytics.default.actor,PROD)"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_mock_response() -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"id": "session-token"}
    return resp


def _make_source(
    mock_post: MagicMock, mock_get: MagicMock, mock_delete: MagicMock
) -> MetabaseSource:
    """Create a MetabaseSource for unit tests using pre-configured outer mocks."""
    resp = _build_mock_response()
    mock_get.return_value = resp
    mock_post.return_value = resp
    mock_delete.return_value = MagicMock(status_code=204)

    config = MetabaseConfig(
        connect_uri="http://localhost:3000",
        username="test",
        password=pydantic.SecretStr("pwd"),
    )
    ctx = PipelineContext(run_id="mbql-unit-tests")
    ctx.graph = None
    return MetabaseSource(ctx, config)


def _field(fid: int, name: str, table_id: int = 21) -> MetabaseField:
    return MetabaseField(id=fid, name=name, table_id=table_id)


def _card_with_cll(
    result_metadata: list,
    aggregation: list | None = None,
    breakout: list | None = None,
    expressions: dict | None = None,
    source_table: int = 21,
    database_id: int = 5,
) -> MetabaseCard:
    query: dict = {"source-table": source_table}
    if aggregation is not None:
        query["aggregation"] = aggregation
    if breakout is not None:
        query["breakout"] = breakout
    if expressions is not None:
        query["expressions"] = expressions

    return MetabaseCard(
        id=6,
        name="CLL Test Model",
        database_id=database_id,
        dataset_query=MetabaseDatasetQuery(type="query", query=query),
        result_metadata=[MetabaseResultMetadata(**m) for m in result_metadata],
    )


# ---------------------------------------------------------------------------
# _extract_field_ids_from_mbql — module-level function, no source needed
# ---------------------------------------------------------------------------


def test_extract_field_ids_direct_ref():
    assert _extract_field_ids_from_mbql(["field", 100, None]) == [100]


def test_extract_field_ids_nested_in_aggregation():
    assert _extract_field_ids_from_mbql(["avg", ["field", 132, None]]) == [132]


def test_extract_field_ids_multiple_from_expression():
    ids = _extract_field_ids_from_mbql(["-", ["field", 10, None], ["field", 20, None]])
    assert sorted(ids) == [10, 20]


def test_extract_field_ids_count_star_returns_empty():
    assert _extract_field_ids_from_mbql(["count"]) == []


def test_extract_field_ids_named_ref_ignored():
    # String field IDs (named references) must not be returned as integers
    assert (
        _extract_field_ids_from_mbql(["field", "name", {"base-type": "type/Text"}])
        == []
    )


def test_extract_field_ids_non_list_returns_empty():
    assert _extract_field_ids_from_mbql(None) == []  # type: ignore[arg-type]
    assert _extract_field_ids_from_mbql([]) == []
    assert _extract_field_ids_from_mbql(42) == []  # type: ignore[arg-type]


def test_extract_field_ids_deeply_nested():
    ids = _extract_field_ids_from_mbql(
        ["and", ["=", ["field", 5, None], 1], ["field", 6, None]]
    )
    assert sorted(ids) == [5, 6]


# ---------------------------------------------------------------------------
# MetabaseQuery Pydantic model — join and expression fields parsed correctly
# ---------------------------------------------------------------------------


def test_metabase_query_parses_joins():
    query = MetabaseQuery.model_validate(
        {
            "source-table": 21,
            "joins": [
                {
                    "source-table": 22,
                    "alias": "Actor",
                    "condition": [
                        "=",
                        ["field", 1, None],
                        ["field", 2, {"join-alias": "Actor"}],
                    ],
                }
            ],
        }
    )
    assert query.source_table == 21
    assert query.joins is not None
    assert len(query.joins) == 1
    assert query.joins[0].source_table == 22
    assert query.joins[0].alias == "Actor"


def test_metabase_query_parses_expressions():
    query = MetabaseQuery.model_validate(
        {
            "source-table": 10,
            "expressions": {"profit": ["-", ["field", 5, None], ["field", 6, None]]},
        }
    )
    assert query.expressions is not None
    assert "profit" in query.expressions
    assert query.expressions["profit"] == ["-", ["field", 5, None], ["field", 6, None]]


# ---------------------------------------------------------------------------
# _get_table_urns_from_query_builder — join path
# ---------------------------------------------------------------------------


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_join_lineage_returns_both_tables(mock_post, mock_get, mock_delete):
    """A card with source-table + one join should produce two table URNs."""
    src = _make_source(mock_post, mock_get, mock_delete)
    src.get_datasource_from_id = MagicMock(return_value=FILM_DATASOURCE)

    def resolve_table(table_id):
        return {21: ("default", "film"), 22: ("default", "actor")}.get(
            table_id, (None, None)
        )  # type: ignore[arg-type]

    src.get_source_table_from_id = MagicMock(side_effect=resolve_table)  # type: ignore[method-assign]

    card = MetabaseCard(
        id=10,
        name="Film-Actor Join",
        database_id=5,
        dataset_query=MetabaseDatasetQuery(
            type="query",
            query={
                "source-table": 21,
                "joins": [{"source-table": 22, "alias": "Actor", "condition": []}],
            },
        ),
    )

    urns = src._get_table_urns_from_query_builder(card)
    assert set(urns) == {FILM_TABLE_URN, ACTOR_TABLE_URN}
    src.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_multiple_joins_all_captured(mock_post, mock_get, mock_delete):
    """Three tables (primary + 2 joins) should produce three URNs."""
    src = _make_source(mock_post, mock_get, mock_delete)
    src.get_datasource_from_id = MagicMock(return_value=FILM_DATASOURCE)

    def resolve_table(table_id):
        mapping = {
            21: ("default", "film"),
            22: ("default", "actor"),
            23: ("default", "category"),
        }
        return mapping.get(table_id, (None, None))  # type: ignore[arg-type]

    src.get_source_table_from_id = MagicMock(side_effect=resolve_table)  # type: ignore[method-assign]

    card = MetabaseCard(
        id=11,
        name="Three-Way Join",
        database_id=5,
        dataset_query=MetabaseDatasetQuery(
            type="query",
            query={
                "source-table": 21,
                "joins": [
                    {"source-table": 22, "alias": "A", "condition": []},
                    {"source-table": 23, "alias": "C", "condition": []},
                ],
            },
        ),
    )

    urns = src._get_table_urns_from_query_builder(card)
    category_urn = "urn:li:dataset:(urn:li:dataPlatform:clickhouse,analytics.default.category,PROD)"
    assert set(urns) == {FILM_TABLE_URN, ACTOR_TABLE_URN, category_urn}
    src.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_join_with_card_ref_resolves_transitively(mock_post, mock_get, mock_delete):
    """A join whose source-table is a card__ref should resolve through the referenced card."""
    src = _make_source(mock_post, mock_get, mock_delete)
    src.get_datasource_from_id = MagicMock(return_value=FILM_DATASOURCE)
    src.get_source_table_from_id = MagicMock(return_value=("default", "film"))  # type: ignore[method-assign]

    referenced_card = MetabaseCard(
        id=99,
        name="Inner Card",
        database_id=5,
        dataset_query=MetabaseDatasetQuery(type="query", query={"source-table": 21}),
    )
    src.get_card_details_by_id = MagicMock(return_value=referenced_card)  # type: ignore[method-assign]

    card = MetabaseCard(
        id=10,
        name="Joined Via Card",
        database_id=5,
        dataset_query=MetabaseDatasetQuery(
            type="query",
            query={
                "source-table": 21,
                "joins": [
                    {"source-table": "card__99", "alias": "Inner", "condition": []}
                ],
            },
        ),
    )

    urns = src._get_table_urns_from_query_builder(card)
    # Both the primary table and the join-via-card should resolve to film
    assert urns.count(FILM_TABLE_URN) == 2
    src.close()


# ---------------------------------------------------------------------------
# _get_cll_from_query_builder — direct field ref
# ---------------------------------------------------------------------------


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_cll_direct_field_ref(mock_post, mock_get, mock_delete):
    """field_ref: ["field", 131, null] → CLL from film.rating to output column."""
    src = _make_source(mock_post, mock_get, mock_delete)
    src.get_datasource_from_id = MagicMock(return_value=FILM_DATASOURCE)
    src.get_field_from_id = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda fid: _field(131, "rating") if fid == 131 else None
    )
    src.get_source_table_from_id = MagicMock(return_value=("default", "film"))  # type: ignore[method-assign]

    card = _card_with_cll(
        result_metadata=[{"name": "rating", "field_ref": ["field", 131, None]}],
        breakout=[["field", 131, None]],
    )

    result = src._get_cll_from_query_builder(
        card,
        "urn:li:dataset:(urn:li:dataPlatform:metabase,model.6,PROD)",
    )
    assert result is not None
    assert len(result.upstreams) == 1
    assert result.upstreams[0].dataset == FILM_TABLE_URN
    assert result.fineGrainedLineages is not None
    assert len(result.fineGrainedLineages) == 1

    fg = result.fineGrainedLineages[0]
    assert isinstance(fg, FineGrainedLineageClass)
    assert fg.upstreams is not None
    assert fg.downstreams is not None
    assert any("rating" in u for u in fg.upstreams)
    assert any("rating" in d for d in fg.downstreams)
    src.close()


# ---------------------------------------------------------------------------
# _get_cll_from_query_builder — aggregation with explicit field
# ---------------------------------------------------------------------------


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_cll_aggregation_with_explicit_field(mock_post, mock_get, mock_delete):
    """["aggregation", 1] where aggregation[1]=["avg", ["field", 132, null]]
    → CLL from film.rental_rate to avg_rental_rate output column."""
    src = _make_source(mock_post, mock_get, mock_delete)
    src.get_datasource_from_id = MagicMock(return_value=FILM_DATASOURCE)
    src.get_field_from_id = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda fid: _field(132, "rental_rate") if fid == 132 else None
    )
    src.get_source_table_from_id = MagicMock(return_value=("default", "film"))  # type: ignore[method-assign]

    card = _card_with_cll(
        result_metadata=[{"name": "avg_rental_rate", "field_ref": ["aggregation", 1]}],
        aggregation=[["count"], ["avg", ["field", 132, None]]],
    )

    result = src._get_cll_from_query_builder(
        card,
        "urn:li:dataset:(urn:li:dataPlatform:metabase,model.6,PROD)",
    )
    assert result is not None
    assert result.fineGrainedLineages is not None
    assert len(result.fineGrainedLineages) == 1

    fg = result.fineGrainedLineages[0]
    assert fg.upstreams is not None
    assert fg.downstreams is not None
    assert any("rental_rate" in u for u in fg.upstreams)
    assert any("avg_rental_rate" in d for d in fg.downstreams)
    src.close()


# ---------------------------------------------------------------------------
# _get_cll_from_query_builder — COUNT(*) fan-in
# ---------------------------------------------------------------------------


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_cll_count_star_fans_in_all_upstream_columns(mock_post, mock_get, mock_delete):
    """["aggregation", 0] where aggregation[0]=["count"] (COUNT(*))
    → all resolved upstream columns become inputs to the count column."""
    fields = {
        131: _field(131, "rating"),
        132: _field(132, "rental_rate"),
    }
    src = _make_source(mock_post, mock_get, mock_delete)
    src.get_datasource_from_id = MagicMock(return_value=FILM_DATASOURCE)
    src.get_field_from_id = MagicMock(side_effect=lambda fid: fields.get(fid))  # type: ignore[method-assign]
    src.get_source_table_from_id = MagicMock(return_value=("default", "film"))  # type: ignore[method-assign]

    # breakout resolves field 131; aggregation has COUNT(*) at index 0 and AVG(132) at 1
    card = _card_with_cll(
        result_metadata=[
            {"name": "rating", "field_ref": ["field", 131, None]},
            {"name": "film_count", "field_ref": ["aggregation", 0]},
        ],
        breakout=[["field", 131, None]],
        aggregation=[["count"], ["avg", ["field", 132, None]]],
    )

    result = src._get_cll_from_query_builder(
        card,
        "urn:li:dataset:(urn:li:dataPlatform:metabase,model.6,PROD)",
    )
    assert result is not None
    assert result.fineGrainedLineages is not None

    count_fg = next(
        fg
        for fg in result.fineGrainedLineages
        if fg.downstreams and any("film_count" in d for d in fg.downstreams)
    )
    assert count_fg.upstreams is not None
    upstream_cols = {u.split(",")[-1].rstrip(")") for u in count_fg.upstreams}
    assert "rating" in upstream_cols
    assert "rental_rate" in upstream_cols
    src.close()


# ---------------------------------------------------------------------------
# _get_cll_from_query_builder — expression ref
# ---------------------------------------------------------------------------


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_cll_expression_ref_resolves_constituent_fields(
    mock_post, mock_get, mock_delete
):
    """["expression", "profit"] where expressions["profit"] involves fields 10 and 20
    → CLL from both fields to the profit output column."""
    fields = {
        10: _field(10, "revenue"),
        20: _field(20, "cost"),
    }
    src = _make_source(mock_post, mock_get, mock_delete)
    src.get_datasource_from_id = MagicMock(return_value=FILM_DATASOURCE)
    src.get_field_from_id = MagicMock(side_effect=lambda fid: fields.get(fid))  # type: ignore[method-assign]
    src.get_source_table_from_id = MagicMock(return_value=("default", "film"))  # type: ignore[method-assign]

    card = _card_with_cll(
        result_metadata=[{"name": "profit", "field_ref": ["expression", "profit"]}],
        expressions={"profit": ["-", ["field", 10, None], ["field", 20, None]]},
    )

    result = src._get_cll_from_query_builder(
        card,
        "urn:li:dataset:(urn:li:dataPlatform:metabase,model.6,PROD)",
    )
    assert result is not None
    assert result.fineGrainedLineages is not None
    assert len(result.fineGrainedLineages) == 1

    fg = result.fineGrainedLineages[0]
    assert fg.upstreams is not None
    assert fg.downstreams is not None
    upstream_cols = {u.split(",")[-1].rstrip(")") for u in fg.upstreams}
    assert "revenue" in upstream_cols
    assert "cost" in upstream_cols
    assert any("profit" in d for d in fg.downstreams)
    src.close()


# ---------------------------------------------------------------------------
# _get_cll_from_query_builder — unknown ref type
# ---------------------------------------------------------------------------


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_cll_unknown_ref_type_produces_no_cll_entry(mock_post, mock_get, mock_delete):
    """An unrecognised field_ref type should be silently skipped (no CLL entry)."""
    src = _make_source(mock_post, mock_get, mock_delete)
    src.get_datasource_from_id = MagicMock(return_value=FILM_DATASOURCE)
    src.get_field_from_id = MagicMock(return_value=None)  # type: ignore[method-assign]
    src.get_source_table_from_id = MagicMock(return_value=("default", "film"))  # type: ignore[method-assign]

    card = _card_with_cll(
        result_metadata=[
            # "fk->" is not handled by the resolver
            {
                "name": "some_col",
                "field_ref": ["fk->", ["field", 99, None], ["field", 100, None]],
            },
        ],
    )

    result = src._get_cll_from_query_builder(
        card,
        "urn:li:dataset:(urn:li:dataPlatform:metabase,model.6,PROD)",
    )
    # Table-level lineage is present, but no column-level entries
    assert result is not None
    assert not result.fineGrainedLineages
    src.close()


# ---------------------------------------------------------------------------
# _get_cll_from_query_builder — guard conditions
# ---------------------------------------------------------------------------


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_cll_returns_none_when_no_result_metadata(mock_post, mock_get, mock_delete):
    src = _make_source(mock_post, mock_get, mock_delete)
    src.get_datasource_from_id = MagicMock(return_value=FILM_DATASOURCE)
    src.get_field_from_id = MagicMock(return_value=None)  # type: ignore[method-assign]
    src.get_source_table_from_id = MagicMock(return_value=("default", "film"))  # type: ignore[method-assign]

    card = MetabaseCard(
        id=6,
        name="No metadata",
        database_id=5,
        dataset_query=MetabaseDatasetQuery(type="query", query={"source-table": 21}),
        result_metadata=[],
    )

    result = src._get_cll_from_query_builder(
        card,
        "urn:li:dataset:(urn:li:dataPlatform:metabase,model.6,PROD)",
    )
    assert result is None
    src.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_cll_returns_none_when_no_database_id(mock_post, mock_get, mock_delete):
    src = _make_source(mock_post, mock_get, mock_delete)

    card = MetabaseCard(
        id=6,
        name="No db id",
        dataset_query=MetabaseDatasetQuery(type="query", query={"source-table": 21}),
        result_metadata=[
            MetabaseResultMetadata(name="x", field_ref=["field", 1, None])
        ],
    )

    result = src._get_cll_from_query_builder(
        card,
        "urn:li:dataset:(urn:li:dataPlatform:metabase,model.6,PROD)",
    )
    assert result is None
    src.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_cll_returns_none_when_upstream_table_not_found(
    mock_post, mock_get, mock_delete
):
    """If source-table lookup fails (returns None name), CLL should return None."""
    src = _make_source(mock_post, mock_get, mock_delete)
    src.get_datasource_from_id = MagicMock(return_value=FILM_DATASOURCE)
    src.get_field_from_id = MagicMock(return_value=None)  # type: ignore[method-assign]
    # All table lookups fail
    src.get_source_table_from_id = MagicMock(return_value=(None, None))  # type: ignore[method-assign]

    card = _card_with_cll(
        result_metadata=[{"name": "rating", "field_ref": ["field", 131, None]}],
        breakout=[["field", 131, None]],
    )

    result = src._get_cll_from_query_builder(
        card,
        "urn:li:dataset:(urn:li:dataPlatform:metabase,model.6,PROD)",
    )
    assert result is None
    src.close()


# ---------------------------------------------------------------------------
# result_metadata entries with None name / None field_ref
# ---------------------------------------------------------------------------


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_cll_skips_metadata_with_missing_name_or_field_ref(
    mock_post, mock_get, mock_delete
):
    """result_metadata entries with no name or no field_ref should be skipped without crashing."""
    src = _make_source(mock_post, mock_get, mock_delete)
    src.get_datasource_from_id = MagicMock(return_value=FILM_DATASOURCE)
    src.get_field_from_id = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda fid: _field(131, "rating") if fid == 131 else None
    )
    src.get_source_table_from_id = MagicMock(return_value=("default", "film"))  # type: ignore[method-assign]

    card = _card_with_cll(
        result_metadata=[
            {"name": None, "field_ref": ["field", 131, None]},  # missing name → skip
            {"name": "ok_col", "field_ref": None},  # missing field_ref → skip
            {"name": "rating", "field_ref": ["field", 131, None]},  # valid → include
        ],
        breakout=[["field", 131, None]],
    )

    result = src._get_cll_from_query_builder(
        card,
        "urn:li:dataset:(urn:li:dataPlatform:metabase,model.6,PROD)",
    )
    assert result is not None
    assert result.fineGrainedLineages is not None
    # Only the valid entry should produce a CLL entry
    assert len(result.fineGrainedLineages) == 1
    fg = result.fineGrainedLineages[0]
    assert fg.downstreams is not None
    assert any("rating" in d for d in fg.downstreams)
    src.close()


# ---------------------------------------------------------------------------
# _emit_model_workunits — CLL path for query-builder models
# ---------------------------------------------------------------------------


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_emit_model_uses_cll_for_query_builder(mock_post, mock_get, mock_delete):
    """A query-builder model should emit UpstreamLineageClass with fineGrainedLineages."""
    src = _make_source(mock_post, mock_get, mock_delete)
    src.get_datasource_from_id = MagicMock(return_value=FILM_DATASOURCE)
    src.get_field_from_id = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda fid: _field(131, "rating") if fid == 131 else None
    )
    src.get_source_table_from_id = MagicMock(return_value=("default", "film"))  # type: ignore[method-assign]
    src._get_collections_map = MagicMock(return_value={})  # type: ignore[method-assign]

    card_data = {
        "id": 6,
        "name": "Film Rating Model",
        "type": "model",
        "database_id": 5,
        "query_type": "query",
        "dataset_query": {
            "type": "query",
            "database": 5,
            "query": {
                "source-table": 21,
                "breakout": [["field", 131, None]],
                "aggregation": [["count"]],
            },
        },
        "result_metadata": [
            {
                "name": "rating",
                "display_name": "Rating",
                "base_type": "type/Text",
                "field_ref": ["field", 131, None],
            },
            {
                "name": "count",
                "display_name": "Count",
                "base_type": "type/BigInteger",
                "field_ref": ["aggregation", 0],
            },
        ],
        "collection_id": None,
        "creator_id": None,
    }

    src.get_card_details_by_id = MagicMock(  # type: ignore[method-assign]
        return_value=MetabaseCard.model_validate(card_data)
    )

    model_item = MetabaseCardListItem(id=6, type="model", name="Film Rating Model")
    workunits = list(src._emit_model_workunits(model_item))

    lineage_wu = next(
        (
            wu
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, UpstreamLineageClass)
        ),
        None,
    )
    assert lineage_wu is not None
    assert isinstance(lineage_wu.metadata, MetadataChangeProposalWrapper)
    assert isinstance(lineage_wu.metadata.aspect, UpstreamLineageClass)
    upstream_lineage = lineage_wu.metadata.aspect
    assert len(upstream_lineage.upstreams) == 1
    assert upstream_lineage.fineGrainedLineages is not None
    assert len(upstream_lineage.fineGrainedLineages) >= 1

    # Direct field ref (rating) must have a CLL entry with film.rating as upstream
    rating_fgl = next(
        (
            fg
            for fg in upstream_lineage.fineGrainedLineages
            if fg.downstreams and any("rating" in d for d in fg.downstreams)
        ),
        None,
    )
    assert rating_fgl is not None
    assert rating_fgl.upstreams is not None
    assert any("rating" in u for u in rating_fgl.upstreams)
    src.close()


@patch("requests.delete")
@patch("requests.Session.get")
@patch("requests.post")
def test_emit_model_uses_plain_lineage_for_native_sql(mock_post, mock_get, mock_delete):
    """Native SQL models emit UpstreamLineageClass with fineGrainedLineages."""
    src = _make_source(mock_post, mock_get, mock_delete)
    src.get_datasource_from_id = MagicMock(return_value=FILM_DATASOURCE)
    src._get_collections_map = MagicMock(return_value={})  # type: ignore[method-assign]

    card_data = {
        "id": 55,
        "name": "SQL Model",
        "type": "model",
        "database_id": 5,
        "query_type": "native",
        "dataset_query": {
            "type": "native",
            "native": {"query": "SELECT rating FROM film"},
        },
        "result_metadata": [],
        "collection_id": None,
        "creator_id": None,
    }

    src.get_card_details_by_id = MagicMock(  # type: ignore[method-assign]
        return_value=MetabaseCard.model_validate(card_data)
    )

    model_item = MetabaseCardListItem(id=55, type="model", name="SQL Model")
    workunits = list(src._emit_model_workunits(model_item))

    lineage_wu = next(
        (
            wu
            for wu in workunits
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and isinstance(wu.metadata.aspect, UpstreamLineageClass)
        ),
        None,
    )
    assert lineage_wu is not None
    assert isinstance(lineage_wu.metadata, MetadataChangeProposalWrapper)
    assert isinstance(lineage_wu.metadata.aspect, UpstreamLineageClass)
    lineage = lineage_wu.metadata.aspect
    assert len(lineage.upstreams) == 1
    assert lineage.fineGrainedLineages is not None
    assert len(lineage.fineGrainedLineages) >= 1
    rating_fgl = next(
        (
            fg
            for fg in lineage.fineGrainedLineages
            if fg.downstreams and any("rating" in d for d in fg.downstreams)
        ),
        None,
    )
    assert rating_fgl is not None
    assert rating_fgl.upstreams is not None
    assert any("rating" in u for u in rating_fgl.upstreams)
    src.close()
