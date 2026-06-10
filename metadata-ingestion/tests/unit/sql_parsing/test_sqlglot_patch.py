from datahub.sql_parsing._sqlglot_patch import SQLGLOT_PATCHED

import time

import pytest
import sqlglot
import sqlglot.errors
import sqlglot.lineage
import sqlglot.optimizer
import sqlglot.optimizer.unnest_subqueries
from sqlglot import exp

from datahub.utilities.cooperative_timeout import (
    CooperativeTimeoutError,
    cooperative_timeout,
)
from datahub.utilities.perf_timer import PerfTimer

assert SQLGLOT_PATCHED


@pytest.mark.flaky(reruns=3)
def test_cooperative_timeout_sql() -> None:
    statement = sqlglot.parse_one("SELECT pg_sleep(3)", dialect="postgres")
    with (
        pytest.raises(CooperativeTimeoutError),
        PerfTimer() as timer,
        cooperative_timeout(timeout=0.6),
    ):
        while True:
            # sql() implicitly calls copy(), which is where we check for the timeout.
            assert statement.sql() is not None
            time.sleep(0.0001)

    assert 0.6 <= timer.elapsed_seconds() <= 1.2


def test_scope_circular_dependency() -> None:
    scope = sqlglot.optimizer.build_scope(
        sqlglot.parse_one("WITH w AS (SELECT * FROM q) SELECT * FROM w")
    )
    assert scope is not None

    cte_scope = scope.cte_scopes[0]
    cte_scope.cte_scopes.append(cte_scope)

    with pytest.raises(sqlglot.errors.OptimizeError, match="circular scope dependency"):
        list(scope.traverse())


def test_lineage_node_subfield() -> None:
    expression = sqlglot.parse_one("SELECT 1 AS test")
    node = sqlglot.lineage.Node("test", expression, expression, subfield="subfield")  # type: ignore
    assert node.subfield == "subfield"  # type: ignore


def test_decorrelate_null_parent_predicate() -> None:
    # Regression guard for the removed _patch_unnest_subqueries() patch.
    #
    # When a correlated subquery is wrapped in a function (e.g. COALESCE) in the
    # SELECT list, it is neither a direct projection nor inside a predicate, so
    # decorrelate()'s `parent_predicate` is None. A non-EQ correlation (y.c > x.c)
    # then routes into the ARRAY_ANY branch that interpolates `parent_predicate`,
    # which used to crash with "'NoneType' object has no attribute 'replace'".
    #
    # sqlglot v30+ guards this with an early return; if a future upgrade drops
    # that guard, this test fails instead of silently reintroducing the crash.
    sql = "SELECT COALESCE((SELECT y.b FROM y WHERE y.a = x.a AND y.c > x.c), 0) AS c FROM x"
    optimized = sqlglot.optimizer.unnest_subqueries.unnest_subqueries(
        sqlglot.parse_one(sql)
    )
    # The guard leaves the un-decorrelatable subquery in place rather than crashing.
    assert optimized.find(exp.Subquery) is not None


def test_decorrelate_subquery_projection() -> None:
    # The other shape where decorrelate()'s `parent_predicate` is None: a correlated
    # subquery used directly as a SELECT projection. Here `is_subquery_projection` is
    # True, so the rewrite loop takes the `continue` path and never touches the
    # patched `parent_predicate` branches. This case is decorrelated into a LEFT JOIN
    # (rather than left in place), so it complements the COALESCE case above.
    sql = "SELECT x.a, (SELECT y.b FROM y WHERE y.a = x.a) AS sub FROM x"
    optimized = sqlglot.optimizer.unnest_subqueries.unnest_subqueries(
        sqlglot.parse_one(sql)
    )
    # The correlated projection is rewritten into a LEFT JOIN against an aggregated
    # derived table; reaching this without crashing is the regression guard.
    assert optimized.find(exp.Join) is not None
