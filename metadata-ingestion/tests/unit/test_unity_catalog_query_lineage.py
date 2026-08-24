from datetime import datetime, timezone
from typing import Optional

from datahub.ingestion.source.unity.proxy_types import Query
from datahub.ingestion.source.unity.query_lineage import QueryLineageResolver


def _urn(full_name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:databricks,{full_name},PROD)"


def _resolve(full_name: str) -> Optional[str]:
    return _urn(full_name)


def _query(text: str, day: int, source: str, target: str) -> Query:
    return Query(
        query_id=f"stmt-{day}",
        query_text=text,
        statement_type=None,
        start_time=None,
        end_time=datetime(2026, 1, day, tzinfo=timezone.utc),
        user_id=None,
        user_name=None,
        executed_as_user_id=None,
        executed_as_user_name=None,
        source_table_full_names=[source],
        target_table_full_names=[target],
    )


def test_links_edge_to_query_urn():
    resolver = QueryLineageResolver(resolve_urn=_resolve)
    resolver.add_query(
        _query(
            "INSERT INTO my_catalog.my_schema.tgt SELECT col_a FROM my_catalog.my_schema.src",
            1,
            "my_catalog.my_schema.src",
            "my_catalog.my_schema.tgt",
        )
    )

    urn = resolver.query_urn_for(
        _urn("my_catalog.my_schema.src"), _urn("my_catalog.my_schema.tgt")
    )

    assert urn is not None
    assert urn.startswith("urn:li:query:")
    assert resolver.queries_to_emit() == [
        (
            urn,
            "INSERT INTO my_catalog.my_schema.tgt SELECT col_a FROM my_catalog.my_schema.src",
        )
    ]
    assert resolver.num_edges_linked == 1


def test_latest_statement_wins_for_same_edge():
    resolver = QueryLineageResolver(resolve_urn=_resolve)
    older = _query(
        "INSERT INTO my_catalog.my_schema.tgt SELECT col_a FROM my_catalog.my_schema.src",
        1,
        "my_catalog.my_schema.src",
        "my_catalog.my_schema.tgt",
    )
    newer = _query(
        "INSERT INTO my_catalog.my_schema.tgt SELECT col_b FROM my_catalog.my_schema.src",
        2,
        "my_catalog.my_schema.src",
        "my_catalog.my_schema.tgt",
    )
    resolver.add_query(older)
    resolver.add_query(newer)

    urn = resolver.query_urn_for(
        _urn("my_catalog.my_schema.src"), _urn("my_catalog.my_schema.tgt")
    )
    emitted = dict(resolver.queries_to_emit())

    assert emitted[urn] == newer.query_text


def test_literal_only_variants_share_one_urn():
    resolver = QueryLineageResolver(resolve_urn=_resolve)
    for day, literal in ((1, "2026-01-01"), (2, "2026-01-02")):
        resolver.add_query(
            _query(
                f"INSERT INTO my_catalog.my_schema.tgt SELECT col_a FROM my_catalog.my_schema.src WHERE d = '{literal}'",
                day,
                "my_catalog.my_schema.src",
                "my_catalog.my_schema.tgt",
            )
        )

    assert len(resolver.queries_to_emit()) == 1


def test_unknown_edge_returns_none():
    resolver = QueryLineageResolver(resolve_urn=_resolve)

    assert resolver.query_urn_for(_urn("a.b.c"), _urn("d.e.f")) is None


def test_blank_statement_text_is_skipped():
    resolver = QueryLineageResolver(resolve_urn=_resolve)
    resolver.add_query(
        _query("   ", 1, "my_catalog.my_schema.src", "my_catalog.my_schema.tgt")
    )

    assert resolver.queries_to_emit() == []
    assert resolver.num_statements_skipped == 1


def test_unresolvable_table_name_is_skipped():
    resolver = QueryLineageResolver(resolve_urn=lambda _full_name: None)
    resolver.add_query(
        _query(
            "INSERT INTO my_catalog.my_schema.tgt SELECT col_a FROM my_catalog.my_schema.src",
            1,
            "my_catalog.my_schema.src",
            "my_catalog.my_schema.tgt",
        )
    )

    assert resolver.queries_to_emit() == []
    assert resolver.num_edges_linked == 0
