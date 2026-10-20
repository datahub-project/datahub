"""Unit tests for the TeradataOperator OpenLineage patch."""

import sys
import types
from collections.abc import Mapping
from typing import Optional
from unittest import mock

import pytest

from datahub_airflow_plugin._constants import DATAHUB_SQL_PARSING_RESULT_KEY
from datahub_airflow_plugin.airflow3._teradata_openlineage_patch import (
    _create_teradata_openlineage_wrapper,
    _enhance_teradata_lineage_with_sql_parsing,
)

_EXTRACTORS = "airflow.providers.openlineage.extractors"


class _FakeOperatorLineage:
    """Stand-in for airflow.providers.openlineage OperatorLineage."""

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


def _restore_modules(
    keys: tuple[str, ...], saved: Mapping[str, types.ModuleType]
) -> None:
    for key in keys:
        if key in saved:
            sys.modules[key] = saved[key]
        elif key in sys.modules:
            del sys.modules[key]


@pytest.fixture
def openlineage_sys_modules_sandbox():
    """Save and restore OpenLineage-related entries in sys.modules."""
    keys = (_EXTRACTORS,)
    saved: dict[str, types.ModuleType] = {
        k: sys.modules[k] for k in keys if k in sys.modules
    }
    yield
    _restore_modules(keys, saved)


def test_create_wrapper_uses_operator_lineage_import(
    openlineage_sys_modules_sandbox: None,
) -> None:
    """Primary import path: OperatorLineage resolves from extractors package."""
    ext = types.ModuleType(_EXTRACTORS)
    ext.OperatorLineage = _FakeOperatorLineage  # type: ignore[attr-defined]
    sys.modules[_EXTRACTORS] = ext

    original = mock.Mock()
    wrapper = _create_teradata_openlineage_wrapper(original)

    assert wrapper is not original


def test_create_wrapper_returns_original_when_import_fails(
    openlineage_sys_modules_sandbox: None,
) -> None:
    """If OperatorLineage cannot be resolved, the original callable is returned."""
    ext = types.ModuleType(_EXTRACTORS)
    sys.modules[_EXTRACTORS] = ext

    original = mock.Mock()
    wrapper = _create_teradata_openlineage_wrapper(original)

    assert wrapper is original


@pytest.mark.parametrize(
    "schema, expected_unqualified",
    [
        pytest.param("dw", "dw.u", id="operator_schema"),
        pytest.param(None, "u", id="no_schema"),
    ],
)
def test_operator_schema_qualifies_only_unqualified_tables(
    schema: Optional[str], expected_unqualified: str
) -> None:
    # TeradataOperator forwards `schema` to its hook as hook_params["database"].
    # Teradata dataset names are two-tier (database.table), so that database
    # must qualify unqualified tables only, not prefix qualified ones.
    teradata = pytest.importorskip("airflow.providers.teradata.operators.teradata")
    from airflow.providers.openlineage.extractors import OperatorLineage

    sql = "INSERT INTO dw.t SELECT s.a, u.b FROM other_db.s JOIN u ON s.id = u.id"
    operator = teradata.TeradataOperator(task_id="teradata", schema=schema, sql=sql)

    lineage = _enhance_teradata_lineage_with_sql_parsing(
        OperatorLineage(), sql, operator
    )

    result = lineage.run_facets[DATAHUB_SQL_PARSING_RESULT_KEY]
    assert sorted(result.in_tables) == sorted(
        [
            f"urn:li:dataset:(urn:li:dataPlatform:teradata,{expected_unqualified},PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:teradata,other_db.s,PROD)",
        ]
    )
    assert result.out_tables == [
        "urn:li:dataset:(urn:li:dataPlatform:teradata,dw.t,PROD)"
    ]
