from typing import Dict, List, Set

from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_sql_parsed_result_from_statements,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEFAULTS = dict(
    default_db="dev",
    platform="postgres",
    platform_instance=None,
    env="PROD",
    default_schema="public",
)


def _parse(queries: List[str], **overrides: object) -> SqlParsingResult:
    kwargs = {**_DEFAULTS, **overrides}
    return create_lineage_sql_parsed_result_from_statements(queries=queries, **kwargs)  # type: ignore[arg-type]


def _table_short_names(urns: list) -> Set[str]:
    """Extract the bare table name from each URN.

    URN format: urn:li:dataset:(urn:li:dataPlatform:postgres,dev.public.staging,PROD)
    Returns: {"staging"}
    """
    names: Set[str] = set()
    for urn in urns:
        # the dataset name sits between the last comma-separated segments
        # e.g. "dev.public.staging" → "staging"
        dataset_part = str(urn).split(",")[-2]
        names.add(dataset_part.split(".")[-1])
    return names


def _cll_map(result: SqlParsingResult) -> Dict[str, Dict[str, List[str]]]:
    """Build {downstream_table: {downstream_col: [upstream_table.col, ...]}} from CLL.

    Useful for concise assertions on column lineage.
    """
    out: Dict[str, Dict[str, List[str]]] = {}
    for cll in result.column_lineage or []:
        ds_table = str(cll.downstream.table or "").split(".")[-1].rstrip(",PROD)")
        ds_col = cll.downstream.column
        ups = [
            f"{str(u.table).split('.')[-1].rstrip(',PROD)')}.{u.column}"
            for u in cll.upstreams
        ]
        out.setdefault(ds_table, {})[ds_col] = ups
    return out


# ---------------------------------------------------------------------------
# Basic / boundary tests
# ---------------------------------------------------------------------------


class TestBasics:
    def test_empty_list(self) -> None:
        result = _parse([])
        assert result.in_tables == []
        assert result.out_tables == []
        assert result.debug_info.error is not None

    def test_single_statement(self) -> None:
        result = _parse(["INSERT INTO target SELECT id, name FROM source"])
        assert _table_short_names(result.in_tables) == {"source"}
        assert _table_short_names(result.out_tables) == {"target"}

    def test_whitespace_and_empty_queries_ignored(self) -> None:
        result = _parse(["", "   ", "INSERT INTO target SELECT id FROM source"])
        assert _table_short_names(result.in_tables) == {"source"}
        assert _table_short_names(result.out_tables) == {"target"}

    def test_multiple_independent_statements(self) -> None:
        result = _parse(
            [
                "INSERT INTO target1 SELECT id FROM source1",
                "INSERT INTO target2 SELECT name FROM source2",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source1", "source2"}
        assert _table_short_names(result.out_tables) == {"target1", "target2"}

    def test_confidence_score_is_positive(self) -> None:
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id FROM source",
                "INSERT INTO target SELECT id FROM staging",
            ]
        )
        assert result.debug_info.confidence > 0


# ---------------------------------------------------------------------------
# Promoted temp tables (created but NOT dropped)
# ---------------------------------------------------------------------------


class TestPromotedTempTables:
    """Temp tables that are created but never dropped should be visible in lineage."""

    def test_simple_promotion_table_and_column_lineage(self) -> None:
        """source → staging (promoted) → target

        Table-level: staging appears in both in_tables and out_tables.
        Column-level: staging cols trace to source; target cols trace to staging.
        """
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id, name FROM source",
                "INSERT INTO target SELECT id, name FROM staging",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source", "staging"}
        assert _table_short_names(result.out_tables) == {"staging", "target"}

        cll = _cll_map(result)
        # staging.id ← source.id, staging.name ← source.name
        assert "source.id" in cll["staging"]["id"]
        assert "source.name" in cll["staging"]["name"]
        # target.id ← staging.id, target.name ← staging.name
        assert "staging.id" in cll["target"]["id"]
        assert "staging.name" in cll["target"]["name"]

    def test_multi_hop_all_promoted(self) -> None:
        """source → t1 → t2 → target — none dropped, full chain visible."""
        result = _parse(
            [
                "CREATE TEMP TABLE t1 AS SELECT id FROM source",
                "CREATE TEMP TABLE t2 AS SELECT id FROM t1",
                "INSERT INTO target SELECT id FROM t2",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source", "t1", "t2"}
        assert _table_short_names(result.out_tables) == {"t1", "t2", "target"}

    def test_fan_out_one_temp_multiple_targets(self) -> None:
        """source → staging → target1, staging → target2"""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id FROM source",
                "INSERT INTO target1 SELECT id FROM staging",
                "INSERT INTO target2 SELECT id FROM staging",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source", "staging"}
        assert _table_short_names(result.out_tables) == {
            "staging",
            "target1",
            "target2",
        }

    def test_transformation_preserves_column_mapping(self) -> None:
        """Column transformations are tracked per-hop, not collapsed."""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id, 2 * val AS doubled FROM source",
                "INSERT INTO target SELECT id, doubled FROM staging",
            ]
        )
        cll = _cll_map(result)
        # staging.doubled traces back to source.val (the transformation origin)
        assert "source.val" in cll["staging"]["doubled"]
        # target.doubled traces to staging.doubled (one hop, not collapsed to source)
        assert "staging.doubled" in cll["target"]["doubled"]

    def test_temp_table_created_but_never_consumed(self) -> None:
        """An orphan temp table (created, never referenced) still appears as output."""
        result = _parse(
            [
                "CREATE TEMP TABLE orphan AS SELECT id FROM source",
                "INSERT INTO target SELECT id FROM other_table",
            ]
        )
        assert "orphan" in _table_short_names(result.out_tables)
        assert "target" in _table_short_names(result.out_tables)


# ---------------------------------------------------------------------------
# Dropped temp tables (created AND dropped → resolved away)
# ---------------------------------------------------------------------------


class TestDroppedTempTables:
    """Temp tables that are explicitly dropped are resolved away."""

    def test_drop_resolves_table_and_column_lineage(self) -> None:
        """source → staging (dropped) → target becomes source → target."""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id, name FROM source",
                "INSERT INTO target SELECT id, name FROM staging",
                "DROP TABLE staging",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source"}
        assert _table_short_names(result.out_tables) == {"target"}

        # Column lineage resolves through staging directly to source
        cll = _cll_map(result)
        assert "source.id" in cll["target"]["id"]
        assert "source.name" in cll["target"]["name"]

    def test_drop_table_if_exists(self) -> None:
        """IF EXISTS variant of DROP still resolves the temp table."""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id FROM source",
                "INSERT INTO target SELECT id FROM staging",
                "DROP TABLE IF EXISTS staging",
            ]
        )
        assert "staging" not in _table_short_names(result.in_tables)
        assert "staging" not in _table_short_names(result.out_tables)
        assert _table_short_names(result.in_tables) == {"source"}

    def test_multi_hop_partial_drop(self) -> None:
        """source → t1 (dropped) → t2 (promoted) → target

        t1 is resolved, t2 gets source as upstream via t1 resolution.
        """
        result = _parse(
            [
                "CREATE TEMP TABLE t1 AS SELECT id FROM source",
                "CREATE TEMP TABLE t2 AS SELECT id FROM t1",
                "INSERT INTO target SELECT id FROM t2",
                "DROP TABLE t1",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        out_names = _table_short_names(result.out_tables)

        assert "t1" not in in_names and "t1" not in out_names
        assert "t2" in out_names and "t2" in in_names
        assert "source" in in_names
        assert "target" in out_names

    def test_drop_of_nonexistent_table_is_harmless(self) -> None:
        """DROP of a table that was never created as temp doesn't break real lineage."""
        result = _parse(
            [
                "INSERT INTO target SELECT id FROM source",
                "DROP TABLE IF EXISTS some_random_table",
            ]
        )
        assert "source" in _table_short_names(result.in_tables)
        assert "target" in _table_short_names(result.out_tables)

    def test_mixed_dropped_and_promoted(self) -> None:
        """Two temp tables: one dropped, one kept.

        source → tmp_dropped (dropped) → target1
        source → tmp_kept (promoted) → target2
        """
        result = _parse(
            [
                "CREATE TEMP TABLE tmp_dropped AS SELECT id FROM source",
                "CREATE TEMP TABLE tmp_kept AS SELECT id FROM source",
                "INSERT INTO target1 SELECT id FROM tmp_dropped",
                "INSERT INTO target2 SELECT id FROM tmp_kept",
                "DROP TABLE tmp_dropped",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        out_names = _table_short_names(result.out_tables)

        # tmp_dropped resolved away
        assert "tmp_dropped" not in in_names
        assert "tmp_dropped" not in out_names

        # tmp_kept promoted
        assert "tmp_kept" in in_names
        assert "tmp_kept" in out_names

        # Both targets present
        assert {"target1", "target2"}.issubset(out_names)


# ---------------------------------------------------------------------------
# JOINs, aggregations, and complex column-level lineage
# ---------------------------------------------------------------------------


class TestJoinsAndComplexColumnLineage:
    """Tests for JOIN queries, aggregations, and other complex SQL patterns
    combined with multi-statement temp table handling."""

    def test_join_two_temp_tables(self) -> None:
        """Two promoted temp tables joined into a target.

        source1 → t_left ──┐
                            ├─ JOIN → target
        source2 → t_right ─┘

        Column lineage: target.id ← t_left.id, target.val ← t_right.val
        """
        result = _parse(
            [
                "CREATE TEMP TABLE t_left AS SELECT id FROM source1",
                "CREATE TEMP TABLE t_right AS SELECT id, val FROM source2",
                "INSERT INTO target SELECT t_left.id, t_right.val FROM t_left JOIN t_right ON t_left.id = t_right.id",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        out_names = _table_short_names(result.out_tables)

        assert {"source1", "source2", "t_left", "t_right"}.issubset(in_names)
        assert {"t_left", "t_right", "target"}.issubset(out_names)

        cll = _cll_map(result)
        assert "t_left.id" in cll["target"]["id"]
        assert "t_right.val" in cll["target"]["val"]

    def test_temp_table_joined_with_real_table(self) -> None:
        """One promoted temp table joined with a real (non-temp) table.

        source → staging ──┐
                            ├─ JOIN → target
        dimensions ─────────┘
        """
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id, amount FROM source",
                "INSERT INTO target SELECT staging.id, staging.amount, dimensions.label FROM staging JOIN dimensions ON staging.id = dimensions.id",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        out_names = _table_short_names(result.out_tables)

        assert {"source", "staging", "dimensions"}.issubset(in_names)
        assert {"staging", "target"}.issubset(out_names)

        cll = _cll_map(result)
        assert "staging.id" in cll["target"]["id"]
        assert "staging.amount" in cll["target"]["amount"]
        assert "dimensions.label" in cll["target"]["label"]

    def test_temp_table_from_join(self) -> None:
        """Fan-in: temp table created from a JOIN of two real tables.

        source1 ──┐
                   ├─ JOIN → staging → target
        source2 ──┘

        Column lineage: staging gets columns from both sources.
        """
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT s1.id, s2.val FROM source1 s1 JOIN source2 s2 ON s1.id = s2.id",
                "INSERT INTO target SELECT id, val FROM staging",
            ]
        )
        in_names = _table_short_names(result.in_tables)

        assert {"source1", "source2", "staging"}.issubset(in_names)

        cll = _cll_map(result)
        # staging.id ← source1.id, staging.val ← source2.val
        assert "source1.id" in cll["staging"]["id"]
        assert "source2.val" in cll["staging"]["val"]
        # target traces to staging
        assert "staging.id" in cll["target"]["id"]
        assert "staging.val" in cll["target"]["val"]

    def test_aggregation_through_temp_table(self) -> None:
        """Aggregation (GROUP BY + COUNT) in temp table, then consumed downstream.

        Column lineage for aggregated columns traces to the source column
        used in the aggregation function.
        """
        result = _parse(
            [
                "CREATE TEMP TABLE agg AS SELECT category, COUNT(id) AS cnt FROM source GROUP BY category",
                "INSERT INTO target SELECT category, cnt FROM agg",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        out_names = _table_short_names(result.out_tables)

        assert {"source", "agg"}.issubset(in_names)
        assert {"agg", "target"}.issubset(out_names)

        cll = _cll_map(result)
        # agg.category ← source.category
        assert "source.category" in cll["agg"]["category"]
        # agg.cnt ← source.id (the COUNT argument)
        assert "source.id" in cll["agg"]["cnt"]
        # target traces one hop to agg
        assert "agg.category" in cll["target"]["category"]
        assert "agg.cnt" in cll["target"]["cnt"]

    def test_cte_inside_temp_table_creation(self) -> None:
        """CTE (WITH clause) used within a CREATE TEMP TABLE statement."""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS WITH cte AS (SELECT id, name FROM source) SELECT id, name FROM cte",
                "INSERT INTO target SELECT id, name FROM staging",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        out_names = _table_short_names(result.out_tables)

        assert "source" in in_names
        assert {"staging", "target"}.issubset(out_names)

        cll = _cll_map(result)
        # CTE should be transparent — staging traces to source
        assert "source.id" in cll["staging"]["id"]
        assert "source.name" in cll["staging"]["name"]

    def test_dropped_join_resolves_column_lineage(self) -> None:
        """Two dropped temp tables joined → column lineage resolves to real sources.

        source1 → t_left (dropped) ──┐
                                      ├─ JOIN → target
        source2 → t_right (dropped) ─┘

        Column lineage: target.id ← source1.id, target.val ← source2.val
        """
        result = _parse(
            [
                "CREATE TEMP TABLE t_left AS SELECT id FROM source1",
                "CREATE TEMP TABLE t_right AS SELECT id, val FROM source2",
                "INSERT INTO target SELECT t_left.id, t_right.val FROM t_left JOIN t_right ON t_left.id = t_right.id",
                "DROP TABLE t_left",
                "DROP TABLE t_right",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        out_names = _table_short_names(result.out_tables)

        # Both temp tables resolved away
        assert "t_left" not in in_names and "t_left" not in out_names
        assert "t_right" not in in_names and "t_right" not in out_names

        assert _table_short_names(result.in_tables) == {"source1", "source2"}
        assert _table_short_names(result.out_tables) == {"target"}

        # Column lineage traces all the way to real sources
        cll = _cll_map(result)
        assert "source1.id" in cll["target"]["id"]
        assert "source2.val" in cll["target"]["val"]
