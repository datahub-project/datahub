from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Optional

import datahub.metadata.schema_classes as models
from datahub.ingestion.source.unity.proxy_types import Query
from datahub.ingestion.source.unity.query_lineage import (
    QueryLineageResolver,
    build_query_entity_aspects,
)
from datahub.ingestion.source.unity.usage import UnityCatalogUsageExtractor


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
    assert urn is not None
    emitted = dict(resolver.queries_to_emit())

    assert emitted[urn] == newer.query_text


def test_tie_break_is_stable_regardless_of_insertion_order():
    # Same edge, same end_time: _is_newer's primary key ties, so the fingerprint
    # tie-break is what decides the winner. Real data averages 126 distinct
    # statements per edge, so this must not depend on add_query() call order.
    stmt_a = _query(
        "INSERT INTO my_catalog.my_schema.tgt SELECT col_a FROM my_catalog.my_schema.src",
        1,
        "my_catalog.my_schema.src",
        "my_catalog.my_schema.tgt",
    )
    stmt_b = _query(
        "INSERT INTO my_catalog.my_schema.tgt SELECT col_b FROM my_catalog.my_schema.src",
        1,
        "my_catalog.my_schema.src",
        "my_catalog.my_schema.tgt",
    )
    edge = (
        _urn("my_catalog.my_schema.src"),
        _urn("my_catalog.my_schema.tgt"),
    )

    forward = QueryLineageResolver(resolve_urn=_resolve)
    forward.add_query(stmt_a)
    forward.add_query(stmt_b)

    reverse = QueryLineageResolver(resolve_urn=_resolve)
    reverse.add_query(stmt_b)
    reverse.add_query(stmt_a)

    forward_urn = forward.query_urn_for(*edge)
    reverse_urn = reverse.query_urn_for(*edge)

    assert forward_urn == reverse_urn
    assert dict(forward.queries_to_emit()) == dict(reverse.queries_to_emit())


def test_literal_only_variants_share_one_urn():
    # Two different edges whose statements differ only in a literal value must
    # still collapse to one Query, since query_text linkage is independent of
    # which edge it was attached to.
    resolver = QueryLineageResolver(resolve_urn=_resolve)
    edges = (
        ("my_catalog.my_schema.src1", "my_catalog.my_schema.tgt1"),
        ("my_catalog.my_schema.src2", "my_catalog.my_schema.tgt2"),
    )
    for day, (literal, (source, target)) in enumerate(
        (("2026-01-01", edges[0]), ("2026-01-02", edges[1])), start=1
    ):
        resolver.add_query(
            _query(
                f"SELECT * FROM t WHERE d = '{literal}'",
                day,
                source,
                target,
            )
        )

    urns = {
        resolver.query_urn_for(_urn(source), _urn(target)) for source, target in edges
    }

    assert len(urns) == 1
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


def test_single_target_urn_matches_usage_path_fingerprint():
    # The system-tables usage path (usage.py's _query_fingerprint) already emits
    # Query entities for these statements. Our resolver must derive the exact
    # same URN for a single-target statement, or we mint a duplicate Query.
    query = _query(
        "INSERT INTO my_catalog.my_schema.tgt SELECT col_a FROM my_catalog.my_schema.src",
        1,
        "my_catalog.my_schema.src",
        "my_catalog.my_schema.tgt",
    )

    resolver = QueryLineageResolver(resolve_urn=_resolve)
    resolver.add_query(query)
    urn = resolver.query_urn_for(
        _urn("my_catalog.my_schema.src"), _urn("my_catalog.my_schema.tgt")
    )

    usage_path_stub = SimpleNamespace(
        platform="databricks",
        report=SimpleNamespace(num_queries_preparsed_fingerprint_fallback=0),
    )
    # _query_fingerprint only touches self.platform/self.report; a real
    # UnityCatalogUsageExtractor needs an unrelated config/proxy/schema_resolver
    # graph that has nothing to do with fingerprinting.
    expected_fingerprint = UnityCatalogUsageExtractor._query_fingerprint(
        usage_path_stub,  # type: ignore[arg-type]
        query,
    )

    assert urn == f"urn:li:query:{expected_fingerprint}"


def test_build_query_entity_aspects_shape():
    aspects = build_query_entity_aspects(
        query_urn="urn:li:query:abc123",
        query_text="SELECT col_a FROM my_catalog.my_schema.src",
        subject_urns=[
            _urn("my_catalog.my_schema.src"),
            _urn("my_catalog.my_schema.tgt"),
        ],
    )

    names = {type(a).__name__ for a in aspects}
    assert names == {"QueryPropertiesClass", "QuerySubjectsClass"}

    props = next(a for a in aspects if isinstance(a, models.QueryPropertiesClass))
    assert props.statement.value == "SELECT col_a FROM my_catalog.my_schema.src"
    assert props.statement.language == "SQL"
    assert props.source == "SYSTEM"

    subjects = next(a for a in aspects if isinstance(a, models.QuerySubjectsClass))
    assert len(subjects.subjects) == 2


def test_subject_urns_for_returns_all_distinct_datasets_for_a_query():
    resolver = QueryLineageResolver(resolve_urn=_resolve)
    text = "INSERT INTO my_catalog.my_schema.tgt SELECT col_a FROM my_catalog.my_schema.src"
    resolver.add_query(
        _query(text, 1, "my_catalog.my_schema.src", "my_catalog.my_schema.tgt")
    )
    resolver.add_query(
        _query(text, 1, "my_catalog.my_schema.src2", "my_catalog.my_schema.tgt")
    )

    urn = resolver.query_urn_for(
        _urn("my_catalog.my_schema.src"), _urn("my_catalog.my_schema.tgt")
    )
    assert urn is not None
    assert urn == resolver.query_urn_for(
        _urn("my_catalog.my_schema.src2"), _urn("my_catalog.my_schema.tgt")
    )

    assert resolver.subject_urns_for(urn) == sorted(
        [
            _urn("my_catalog.my_schema.src"),
            _urn("my_catalog.my_schema.src2"),
            _urn("my_catalog.my_schema.tgt"),
        ]
    )


def test_subject_urns_for_reflects_edges_added_after_a_prior_call():
    # Guards the subjects cache added for perf (avoids an O(edges) scan per
    # query_urn): a lazily-built cache that isn't invalidated on add_query
    # would keep serving this pre-mutation subject list forever.
    resolver = QueryLineageResolver(resolve_urn=_resolve)
    text = "INSERT INTO my_catalog.my_schema.tgt SELECT col_a FROM my_catalog.my_schema.src"
    resolver.add_query(
        _query(text, 1, "my_catalog.my_schema.src", "my_catalog.my_schema.tgt")
    )
    urn = resolver.query_urn_for(
        _urn("my_catalog.my_schema.src"), _urn("my_catalog.my_schema.tgt")
    )
    assert urn is not None

    resolver.subject_urns_for(urn)  # populate the cache before the mutation below

    resolver.add_query(
        _query(text, 1, "my_catalog.my_schema.src2", "my_catalog.my_schema.tgt")
    )

    assert resolver.subject_urns_for(urn) == sorted(
        [
            _urn("my_catalog.my_schema.src"),
            _urn("my_catalog.my_schema.src2"),
            _urn("my_catalog.my_schema.tgt"),
        ]
    )
