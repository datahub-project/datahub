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
# CREATE TEMP TABLE — resolved away (session-scoped, dies when task ends)
# ---------------------------------------------------------------------------


class TestTempTableResolution:
    """CREATE TEMP TABLE produces session-scoped tables that don't survive
    beyond the Airflow task. They should be resolved away so lineage shows
    the real upstream sources."""

    def test_simple_temp_resolved_table_and_column_lineage(self) -> None:
        """source → staging (temp, resolved) → target  becomes  source → target."""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id, name FROM source",
                "INSERT INTO target SELECT id, name FROM staging",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source"}
        assert _table_short_names(result.out_tables) == {"target"}

        # Column lineage resolves through staging directly to source
        cll = _cll_map(result)
        assert "source.id" in cll["target"]["id"]
        assert "source.name" in cll["target"]["name"]

    def test_multi_hop_temp_chain_fully_resolved(self) -> None:
        """source → t1 (temp) → t2 (temp) → target  becomes  source → target."""
        result = _parse(
            [
                "CREATE TEMP TABLE t1 AS SELECT id FROM source",
                "CREATE TEMP TABLE t2 AS SELECT id FROM t1",
                "INSERT INTO target SELECT id FROM t2",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source"}
        assert _table_short_names(result.out_tables) == {"target"}

        cll = _cll_map(result)
        assert "source.id" in cll["target"]["id"]

    def test_fan_out_temp_to_multiple_targets(self) -> None:
        """source → staging (temp) → target1, staging → target2.
        Both targets should trace to source."""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id FROM source",
                "INSERT INTO target1 SELECT id FROM staging",
                "INSERT INTO target2 SELECT id FROM staging",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source"}
        assert _table_short_names(result.out_tables) == {"target1", "target2"}

    def test_transformation_column_lineage_resolved(self) -> None:
        """Expression transformations resolve through temp table.
        target.doubled should trace back to source.val."""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id, 2 * val AS doubled FROM source",
                "INSERT INTO target SELECT id, doubled FROM staging",
            ]
        )
        cll = _cll_map(result)
        assert "source.val" in cll["target"]["doubled"]
        assert "source.id" in cll["target"]["id"]

    def test_join_two_temp_tables_resolved(self) -> None:
        """Two temp tables joined → both resolved to their real sources.

        source1 → t_left (temp) ──┐
                                   ├─ JOIN → target
        source2 → t_right (temp) ─┘

        Column lineage: target.id ← source1.id, target.val ← source2.val
        """
        result = _parse(
            [
                "CREATE TEMP TABLE t_left AS SELECT id FROM source1",
                "CREATE TEMP TABLE t_right AS SELECT id, val FROM source2",
                "INSERT INTO target SELECT t_left.id, t_right.val FROM t_left JOIN t_right ON t_left.id = t_right.id",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source1", "source2"}
        assert _table_short_names(result.out_tables) == {"target"}

        cll = _cll_map(result)
        assert "source1.id" in cll["target"]["id"]
        assert "source2.val" in cll["target"]["val"]

    def test_temp_from_join_then_insert(self) -> None:
        """Fan-in: temp table created from a JOIN of two real tables.

        source1 ──┐
                   ├─ JOIN → staging (temp) → target
        source2 ──┘

        Column lineage resolves through staging to both sources.
        """
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT s1.id, s2.val FROM source1 s1 JOIN source2 s2 ON s1.id = s2.id",
                "INSERT INTO target SELECT id, val FROM staging",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source1", "source2"}
        assert _table_short_names(result.out_tables) == {"target"}

        cll = _cll_map(result)
        assert "source1.id" in cll["target"]["id"]
        assert "source2.val" in cll["target"]["val"]

    def test_aggregation_through_temp_resolved(self) -> None:
        """GROUP BY + COUNT in temp table, column lineage resolves to source."""
        result = _parse(
            [
                "CREATE TEMP TABLE agg AS SELECT category, COUNT(id) AS cnt FROM source GROUP BY category",
                "INSERT INTO target SELECT category, cnt FROM agg",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source"}
        assert _table_short_names(result.out_tables) == {"target"}

        cll = _cll_map(result)
        assert "source.category" in cll["target"]["category"]
        assert "source.id" in cll["target"]["cnt"]

    def test_cte_inside_temp_table(self) -> None:
        """CTE within CREATE TEMP TABLE — both CTE and temp are transparent."""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS WITH cte AS (SELECT id, name FROM source) SELECT id, name FROM cte",
                "INSERT INTO target SELECT id, name FROM staging",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source"}
        assert _table_short_names(result.out_tables) == {"target"}

        cll = _cll_map(result)
        assert "source.id" in cll["target"]["id"]
        assert "source.name" in cll["target"]["name"]

    def test_temp_joined_with_real_table(self) -> None:
        """Temp table joined with a real (non-temp) table.
        Temp resolves away; real table stays as upstream.

        source → staging (temp) ──┐
                                   ├─ JOIN → target
        dimensions (real) ─────────┘
        """
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id, amount FROM source",
                "INSERT INTO target SELECT staging.id, staging.amount, dimensions.label FROM staging JOIN dimensions ON staging.id = dimensions.id",
            ]
        )
        in_names = _table_short_names(result.in_tables)

        # staging resolved away, source takes its place; dimensions stays
        assert "source" in in_names
        assert "dimensions" in in_names
        assert "staging" not in in_names

        cll = _cll_map(result)
        assert "source.id" in cll["target"]["id"]
        assert "source.amount" in cll["target"]["amount"]
        assert "dimensions.label" in cll["target"]["label"]


# ---------------------------------------------------------------------------
# CREATE TABLE (non-temp) — kept as real intermediate table
# ---------------------------------------------------------------------------


class TestNonTempIntermediateTable:
    """CREATE TABLE (without TEMP) produces a persistent table visible to
    other Airflow tasks. It should appear in lineage as an intermediate node,
    NOT be resolved away."""

    def test_non_temp_intermediate_preserved(self) -> None:
        """source → staging (real, kept) → target.
        staging appears in both in_tables and out_tables."""
        result = _parse(
            [
                "CREATE TABLE staging AS SELECT id, name FROM source",
                "INSERT INTO target SELECT id, name FROM staging",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        out_names = _table_short_names(result.out_tables)

        # staging is a real table — visible as both output (created) and input (consumed)
        assert "staging" in out_names
        assert "staging" in in_names
        assert "source" in in_names
        assert "target" in out_names

    def test_non_temp_intermediate_column_lineage(self) -> None:
        """Column lineage preserves hops through non-temp intermediate.

        staging.id ← source.id  (first hop)
        target.id ← staging.id  (second hop, NOT collapsed to source.id)
        """
        result = _parse(
            [
                "CREATE TABLE staging AS SELECT id, name FROM source",
                "INSERT INTO target SELECT id, name FROM staging",
            ]
        )
        cll = _cll_map(result)

        # staging columns trace to source
        assert "source.id" in cll["staging"]["id"]
        assert "source.name" in cll["staging"]["name"]

        # target columns trace to staging (NOT resolved to source)
        assert "staging.id" in cll["target"]["id"]
        assert "staging.name" in cll["target"]["name"]

    def test_non_temp_joined_with_temp(self) -> None:
        """Mix of temp and non-temp intermediates.

        source1 → staging (real, kept) ──┐
                                          ├─ JOIN → target
        source2 → tmp (temp, resolved) ──┘

        staging visible in lineage; tmp resolved to source2.
        """
        result = _parse(
            [
                "CREATE TABLE staging AS SELECT id, amount FROM source1",
                "CREATE TEMP TABLE tmp AS SELECT id, label FROM source2",
                "INSERT INTO target SELECT staging.id, staging.amount, tmp.label FROM staging JOIN tmp ON staging.id = tmp.id",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        out_names = _table_short_names(result.out_tables)

        # staging kept (non-temp)
        assert "staging" in in_names
        assert "staging" in out_names

        # tmp resolved away
        assert "tmp" not in in_names
        assert "tmp" not in out_names

        # source2 appears as direct upstream (resolved through tmp)
        assert "source2" in in_names

        cll = _cll_map(result)
        assert "staging.id" in cll["target"]["id"]
        assert "staging.amount" in cll["target"]["amount"]
        assert "source2.label" in cll["target"]["label"]

    def test_non_temp_multi_hop_chain(self) -> None:
        """source → t1 (real) → t2 (real) → target — full chain visible."""
        result = _parse(
            [
                "CREATE TABLE t1 AS SELECT id FROM source",
                "CREATE TABLE t2 AS SELECT id FROM t1",
                "INSERT INTO target SELECT id FROM t2",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        out_names = _table_short_names(result.out_tables)

        assert in_names == {"source", "t1", "t2"}
        assert out_names == {"t1", "t2", "target"}

    def test_non_temp_fan_out(self) -> None:
        """Non-temp intermediate consumed by multiple targets."""
        result = _parse(
            [
                "CREATE TABLE staging AS SELECT id FROM source",
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

    def test_non_temp_from_join(self) -> None:
        """Non-temp intermediate created from JOIN — preserved with correct CLL."""
        result = _parse(
            [
                "CREATE TABLE staging AS SELECT s1.id, s2.val FROM source1 s1 JOIN source2 s2 ON s1.id = s2.id",
                "INSERT INTO target SELECT id, val FROM staging",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        assert {"source1", "source2", "staging"}.issubset(in_names)

        cll = _cll_map(result)
        # staging gets columns from both sources
        assert "source1.id" in cll["staging"]["id"]
        assert "source2.val" in cll["staging"]["val"]
        # target traces one hop to staging
        assert "staging.id" in cll["target"]["id"]
        assert "staging.val" in cll["target"]["val"]

    def test_non_temp_aggregation_preserved(self) -> None:
        """Aggregation in non-temp intermediate — column lineage preserved per-hop."""
        result = _parse(
            [
                "CREATE TABLE agg AS SELECT category, COUNT(id) AS cnt FROM source GROUP BY category",
                "INSERT INTO target SELECT category, cnt FROM agg",
            ]
        )
        assert "agg" in _table_short_names(result.out_tables)

        cll = _cll_map(result)
        # agg.cnt ← source.id (COUNT argument)
        assert "source.id" in cll["agg"]["cnt"]
        assert "source.category" in cll["agg"]["category"]
        # target traces to agg, not collapsed to source
        assert "agg.cnt" in cll["target"]["cnt"]
        assert "agg.category" in cll["target"]["category"]
