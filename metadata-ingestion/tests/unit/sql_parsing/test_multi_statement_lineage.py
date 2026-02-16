from typing import Dict, List, Set

import pytest

from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_lineage_from_sql_statements,
)

_DEFAULTS = dict(
    default_db="dev",
    platform="postgres",
    platform_instance=None,
    env="PROD",
    default_schema="public",
)


def _parse(queries: List[str], **overrides: object) -> SqlParsingResult:
    kwargs = {**_DEFAULTS, **overrides}
    return create_lineage_from_sql_statements(queries=queries, **kwargs)  # type: ignore[arg-type]


def _urn_to_table_name(urn: str) -> str:
    """Extract the bare table name from a DataHub URN.

    URN format: urn:li:dataset:(urn:li:dataPlatform:postgres,dev.public.staging,PROD)
    Returns: "staging"
    """
    parts = str(urn).split(",")
    if len(parts) >= 2:
        return parts[-2].split(".")[-1]
    return str(urn).split(".")[-1]


def _table_short_names(urns: list) -> Set[str]:
    """Extract bare table names from a list of URNs."""
    return {_urn_to_table_name(urn) for urn in urns}


def _cll_map(result: SqlParsingResult) -> Dict[str, Dict[str, List[str]]]:
    """Build {downstream_table: {downstream_col: [upstream_table.col, ...]}} from CLL.

    Example output: {"target": {"id": ["source.id"], "name": ["source.name"]}}
    """
    out: Dict[str, Dict[str, List[str]]] = {}
    for cll in result.column_lineage or []:
        ds_table = _urn_to_table_name(str(cll.downstream.table or ""))
        ds_col = cll.downstream.column
        ups = sorted(
            f"{_urn_to_table_name(str(u.table))}.{u.column}" for u in cll.upstreams
        )
        out.setdefault(ds_table, {})[ds_col] = ups
    return out


@pytest.fixture(scope="function", autouse=True)
def _disable_cooperative_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable SQL lineage timeout to avoid flaky test failures."""
    monkeypatch.setattr(
        "datahub.sql_parsing.sqlglot_lineage.SQL_LINEAGE_TIMEOUT_ENABLED", False
    )


class TestInputValidation:
    """Boundary conditions: empty input, whitespace-only queries, single statement."""

    def test_empty_query_list_returns_error(self) -> None:
        result = _parse([])
        assert result.in_tables == []
        assert result.out_tables == []
        assert result.debug_info.error is not None

    def test_whitespace_only_queries_are_skipped(self) -> None:
        result = _parse(["", "   ", "INSERT INTO target SELECT id FROM source"])
        assert _table_short_names(result.in_tables) == {"source"}
        assert _table_short_names(result.out_tables) == {"target"}

    def test_single_insert_produces_correct_lineage(self) -> None:
        result = _parse(["INSERT INTO target SELECT id, name FROM source"])
        assert _table_short_names(result.in_tables) == {"source"}
        assert _table_short_names(result.out_tables) == {"target"}

    def test_multiple_independent_inserts_produce_separate_lineage(self) -> None:
        result = _parse(
            [
                "INSERT INTO target1 SELECT id FROM source1",
                "INSERT INTO target2 SELECT name FROM source2",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source1", "source2"}
        assert _table_short_names(result.out_tables) == {"target1", "target2"}

    def test_confidence_is_positive_for_valid_queries(self) -> None:
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id FROM source",
                "INSERT INTO target SELECT id FROM staging",
            ]
        )
        assert result.debug_info.confidence > 0


class TestTempTableLineage:
    """Temp tables should be resolved away so lineage shows
    only real upstream sources and downstream targets.

    Pattern: source -> temp (resolved away) -> target
    Expected: in_tables={source}, out_tables={target}
    """

    def test_single_temp_resolved_to_source(self) -> None:
        """source -> staging(temp) -> target  =>  source -> target"""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id, name FROM source",
                "INSERT INTO target SELECT id, name FROM staging",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source"}
        assert _table_short_names(result.out_tables) == {"target"}

    def test_multi_hop_temp_chain_resolves_to_original(self) -> None:
        """source -> t1(temp) -> t2(temp) -> target  =>  source -> target"""
        result = _parse(
            [
                "CREATE TEMP TABLE t1 AS SELECT id FROM source",
                "CREATE TEMP TABLE t2 AS SELECT id FROM t1",
                "INSERT INTO target SELECT id FROM t2",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source"}
        assert _table_short_names(result.out_tables) == {"target"}

    def test_fan_out_temp_to_multiple_targets(self) -> None:
        """source -> staging(temp) -> {target1, target2}"""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id FROM source",
                "INSERT INTO target1 SELECT id FROM staging",
                "INSERT INTO target2 SELECT id FROM staging",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source"}
        assert _table_short_names(result.out_tables) == {"target1", "target2"}

    def test_temp_from_join_resolves_both_sources(self) -> None:
        """source1 + source2 -> staging(temp) -> target  =>  {source1, source2} -> target"""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT s1.id, s2.val FROM source1 s1 JOIN source2 s2 ON s1.id = s2.id",
                "INSERT INTO target SELECT id, val FROM staging",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source1", "source2"}
        assert _table_short_names(result.out_tables) == {"target"}

    def test_temp_joined_with_real_table_resolves_temp_only(self) -> None:
        """source -> staging(temp) + dimensions(real) -> target
        Temp resolved; dimensions stays as upstream."""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id, amount FROM source",
                "INSERT INTO target SELECT staging.id, staging.amount, dimensions.label FROM staging JOIN dimensions ON staging.id = dimensions.id",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        assert "source" in in_names
        assert "dimensions" in in_names
        assert "staging" not in in_names


class TestTempTableColumnLineage:
    """Column-level lineage through temp tables should resolve to the original
    source columns, as if the temp table never existed.

    Each test verifies that downstream columns trace back to the correct
    source columns after temp table resolution.
    """

    def test_direct_column_copy_resolves_through_temp(self) -> None:
        """target.id <- staging(temp).id <- source.id  =>  target.id <- source.id"""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id, name FROM source",
                "INSERT INTO target SELECT id, name FROM staging",
            ]
        )
        cll = _cll_map(result)
        assert "source.id" in cll["target"]["id"]
        assert "source.name" in cll["target"]["name"]

    def test_expression_transform_resolves_to_source_column(self) -> None:
        """target.doubled <- staging(temp).doubled <- 2*source.val  =>  target.doubled <- source.val"""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id, 2 * val AS doubled FROM source",
                "INSERT INTO target SELECT id, doubled FROM staging",
            ]
        )
        cll = _cll_map(result)
        assert "source.val" in cll["target"]["doubled"]
        assert "source.id" in cll["target"]["id"]

    def test_aggregation_count_resolves_to_source_column(self) -> None:
        """target.cnt <- agg(temp).cnt <- COUNT(source.id)  =>  target.cnt <- source.id"""
        result = _parse(
            [
                "CREATE TEMP TABLE agg AS SELECT category, COUNT(id) AS cnt FROM source GROUP BY category",
                "INSERT INTO target SELECT category, cnt FROM agg",
            ]
        )
        cll = _cll_map(result)
        assert "source.category" in cll["target"]["category"]
        assert "source.id" in cll["target"]["cnt"]

    def test_join_of_two_temps_resolves_each_to_its_source(self) -> None:
        """target.id <- t_left(temp).id <- source1.id
        target.val <- t_right(temp).val <- source2.val"""
        result = _parse(
            [
                "CREATE TEMP TABLE t_left AS SELECT id FROM source1",
                "CREATE TEMP TABLE t_right AS SELECT id, val FROM source2",
                "INSERT INTO target SELECT t_left.id, t_right.val FROM t_left JOIN t_right ON t_left.id = t_right.id",
            ]
        )
        cll = _cll_map(result)
        assert "source1.id" in cll["target"]["id"]
        assert "source2.val" in cll["target"]["val"]

    def test_temp_from_join_resolves_columns_to_both_sources(self) -> None:
        """staging(temp) created from JOIN of source1 + source2.
        target.id <- staging.id <- source1.id
        target.val <- staging.val <- source2.val"""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT s1.id, s2.val FROM source1 s1 JOIN source2 s2 ON s1.id = s2.id",
                "INSERT INTO target SELECT id, val FROM staging",
            ]
        )
        cll = _cll_map(result)
        assert "source1.id" in cll["target"]["id"]
        assert "source2.val" in cll["target"]["val"]

    def test_cte_inside_temp_resolves_correctly(self) -> None:
        """CTE within CREATE TEMP TABLE — both CTE and temp are transparent.
        target.id <- staging(temp).id <- cte.id <- source.id  =>  target.id <- source.id"""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS WITH cte AS (SELECT id, name FROM source) SELECT id, name FROM cte",
                "INSERT INTO target SELECT id, name FROM staging",
            ]
        )
        cll = _cll_map(result)
        assert "source.id" in cll["target"]["id"]
        assert "source.name" in cll["target"]["name"]

    def test_multi_hop_temp_chain_column_resolves_to_original(self) -> None:
        """source -> t1(temp) -> t2(temp) -> target
        target.id resolves all the way back to source.id"""
        result = _parse(
            [
                "CREATE TEMP TABLE t1 AS SELECT id FROM source",
                "CREATE TEMP TABLE t2 AS SELECT id FROM t1",
                "INSERT INTO target SELECT id FROM t2",
            ]
        )
        cll = _cll_map(result)
        assert "source.id" in cll["target"]["id"]

    def test_column_rename_in_temp_tracks_original_source(self) -> None:
        """Column renamed in temp: source.id -> staging.user_id -> target.user_id
        target.user_id should trace back to source.id"""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id AS user_id FROM source",
                "INSERT INTO target SELECT user_id FROM staging",
            ]
        )
        cll = _cll_map(result)
        assert "source.id" in cll["target"]["user_id"]

    def test_temp_joined_with_real_table_column_lineage(self) -> None:
        """Temp columns resolve to source; real table columns stay as-is.
        target.id <- source.id (via resolved temp)
        target.label <- dimensions.label (direct, not through temp)"""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id, amount FROM source",
                "INSERT INTO target SELECT staging.id, staging.amount, dimensions.label FROM staging JOIN dimensions ON staging.id = dimensions.id",
            ]
        )
        cll = _cll_map(result)
        assert "source.id" in cll["target"]["id"]
        assert "source.amount" in cll["target"]["amount"]
        assert "dimensions.label" in cll["target"]["label"]

    def test_multiple_expressions_in_temp_all_resolve(self) -> None:
        """Multiple transformed columns in one temp table all resolve correctly.
        target.full_name <- source.first_name + source.last_name
        target.age_group <- source.age"""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT first_name || ' ' || last_name AS full_name, CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END AS age_group FROM source",
                "INSERT INTO target SELECT full_name, age_group FROM staging",
            ]
        )
        cll = _cll_map(result)
        target_cll = cll.get("target", {})
        # full_name is derived from first_name and last_name
        assert "full_name" in target_cll
        assert "source.first_name" in target_cll["full_name"]
        assert "source.last_name" in target_cll["full_name"]
        # age_group is derived from age
        assert "age_group" in target_cll
        assert "source.age" in target_cll["age_group"]

    def test_fan_out_temp_column_lineage_resolves_for_each_target(self) -> None:
        """Same temp consumed by two targets — CLL resolves for both.
        target1.id <- source.id  AND  target2.id <- source.id"""
        result = _parse(
            [
                "CREATE TEMP TABLE staging AS SELECT id FROM source",
                "INSERT INTO target1 SELECT id FROM staging",
                "INSERT INTO target2 SELECT id FROM staging",
            ]
        )
        cll = _cll_map(result)
        assert "source.id" in cll["target1"]["id"]
        assert "source.id" in cll["target2"]["id"]


class TestPersistentTableLineage:
    """CREATE TABLE (without TEMP) produces a persistent table visible to
    other Airflow tasks. It should appear in lineage as an intermediate node,
    NOT be resolved away.

    Pattern: source -> staging(real) -> target
    Expected: in_tables={source, staging}, out_tables={staging, target}
    """

    def test_non_temp_appears_as_both_input_and_output(self) -> None:
        """source -> staging(real) -> target
        staging appears in out_tables (created) AND in_tables (read from)."""
        result = _parse(
            [
                "CREATE TABLE staging AS SELECT id, name FROM source",
                "INSERT INTO target SELECT id, name FROM staging",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        out_names = _table_short_names(result.out_tables)
        assert "staging" in out_names
        assert "staging" in in_names
        assert "source" in in_names
        assert "target" in out_names

    def test_multi_hop_chain_preserves_all_intermediate_nodes(self) -> None:
        """source -> t1(real) -> t2(real) -> target — full chain visible."""
        result = _parse(
            [
                "CREATE TABLE t1 AS SELECT id FROM source",
                "CREATE TABLE t2 AS SELECT id FROM t1",
                "INSERT INTO target SELECT id FROM t2",
            ]
        )
        assert _table_short_names(result.in_tables) == {"source", "t1", "t2"}
        assert _table_short_names(result.out_tables) == {"t1", "t2", "target"}

    def test_fan_out_preserves_intermediate_as_shared_source(self) -> None:
        """staging(real) consumed by multiple targets."""
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

    def test_non_temp_from_join_preserves_both_sources(self) -> None:
        """Non-temp intermediate created from JOIN — preserved with both sources."""
        result = _parse(
            [
                "CREATE TABLE staging AS SELECT s1.id, s2.val FROM source1 s1 JOIN source2 s2 ON s1.id = s2.id",
                "INSERT INTO target SELECT id, val FROM staging",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        assert {"source1", "source2", "staging"}.issubset(in_names)


class TestPersistentTableColumnLineage:
    """CLL through persistent intermediate tables is NOT collapsed.
    Each hop is preserved individually:
      staging.id <- source.id  (first hop)
      target.id <- staging.id  (second hop, NOT source.id)
    """

    def test_column_lineage_preserves_each_hop_separately(self) -> None:
        """staging.id <- source.id  AND  target.id <- staging.id (NOT source.id)"""
        result = _parse(
            [
                "CREATE TABLE staging AS SELECT id, name FROM source",
                "INSERT INTO target SELECT id, name FROM staging",
            ]
        )
        cll = _cll_map(result)
        # First hop: staging <- source
        assert "source.id" in cll["staging"]["id"]
        assert "source.name" in cll["staging"]["name"]
        # Second hop: target <- staging (NOT collapsed to source)
        assert "staging.id" in cll["target"]["id"]
        assert "staging.name" in cll["target"]["name"]

    def test_join_sourced_intermediate_preserves_column_origins(self) -> None:
        """staging.id <- source1.id, staging.val <- source2.val
        target.id <- staging.id, target.val <- staging.val"""
        result = _parse(
            [
                "CREATE TABLE staging AS SELECT s1.id, s2.val FROM source1 s1 JOIN source2 s2 ON s1.id = s2.id",
                "INSERT INTO target SELECT id, val FROM staging",
            ]
        )
        cll = _cll_map(result)
        # staging columns trace to their respective sources
        assert "source1.id" in cll["staging"]["id"]
        assert "source2.val" in cll["staging"]["val"]
        # target traces one hop to staging
        assert "staging.id" in cll["target"]["id"]
        assert "staging.val" in cll["target"]["val"]

    def test_aggregation_in_intermediate_preserves_per_hop(self) -> None:
        """agg.cnt <- COUNT(source.id)  AND  target.cnt <- agg.cnt (NOT source.id)"""
        result = _parse(
            [
                "CREATE TABLE agg AS SELECT category, COUNT(id) AS cnt FROM source GROUP BY category",
                "INSERT INTO target SELECT category, cnt FROM agg",
            ]
        )
        cll = _cll_map(result)
        # agg columns trace to source
        assert "source.id" in cll["agg"]["cnt"]
        assert "source.category" in cll["agg"]["category"]
        # target traces to agg, not collapsed to source
        assert "agg.cnt" in cll["target"]["cnt"]
        assert "agg.category" in cll["target"]["category"]


class TestMixed_TempAndPersistentLineage:
    """Scenarios combining temp (resolved away) and persistent (preserved)
    intermediate tables. Verifies correct behavior for both in the same query set."""

    def test_temp_resolved_and_persistent_preserved_table_lineage(self) -> None:
        """source1 -> staging(real, kept)
        source2 -> tmp(temp, resolved)
        staging + tmp -> target
        staging visible in lineage; tmp resolved to source2."""
        result = _parse(
            [
                "CREATE TABLE staging AS SELECT id, amount FROM source1",
                "CREATE TEMP TABLE tmp AS SELECT id, label FROM source2",
                "INSERT INTO target SELECT staging.id, staging.amount, tmp.label FROM staging JOIN tmp ON staging.id = tmp.id",
            ]
        )
        in_names = _table_short_names(result.in_tables)
        out_names = _table_short_names(result.out_tables)
        assert "staging" in in_names
        assert "staging" in out_names
        assert "tmp" not in in_names
        assert "tmp" not in out_names
        assert "source2" in in_names

    def test_mixed_column_lineage_temp_resolved_persistent_preserved(self) -> None:
        """target.amount <- staging.amount (one hop to persistent, NOT collapsed)
        target.label <- source2.label (resolved through temp tmp)"""
        result = _parse(
            [
                "CREATE TABLE staging AS SELECT id, amount FROM source1",
                "CREATE TEMP TABLE tmp AS SELECT id, label FROM source2",
                "INSERT INTO target SELECT staging.id, staging.amount, tmp.label FROM staging JOIN tmp ON staging.id = tmp.id",
            ]
        )
        cll = _cll_map(result)
        # Persistent intermediate: target traces to staging (not collapsed)
        assert "staging.id" in cll["target"]["id"]
        assert "staging.amount" in cll["target"]["amount"]
        # Temp resolved: target.label traces through tmp to source2
        assert "source2.label" in cll["target"]["label"]
