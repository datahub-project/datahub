from datahub.sql_parsing._sqlglot_patch import SQLGLOT_PATCHED

import time

import pytest
import sqlglot
import sqlglot.errors
import sqlglot.lineage
import sqlglot.optimizer

from datahub.utilities.cooperative_timeout import (
    CooperativeTimeoutError,
    cooperative_timeout,
)
from datahub.utilities.perf_timer import PerfTimer

assert SQLGLOT_PATCHED


def test_cooperative_timeout_sql() -> None:
    statement = sqlglot.parse_one("SELECT pg_sleep(3)", dialect="postgres")
    with pytest.raises(
        CooperativeTimeoutError
    ), PerfTimer() as timer, cooperative_timeout(timeout=0.6):
        while True:
            # sql() implicitly calls copy(), which is where we check for the timeout.
            assert statement.sql() is not None
            time.sleep(0.0001)
    assert 0.6 <= timer.elapsed_seconds() <= 1.0


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
