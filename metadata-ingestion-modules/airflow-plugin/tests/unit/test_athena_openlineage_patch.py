"""Unit tests for the AthenaOperator OpenLineage patch."""

from typing import Dict, Optional

import pytest
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.openlineage.extractors import OperatorLineage

from datahub_airflow_plugin._constants import DATAHUB_SQL_PARSING_RESULT_KEY
from datahub_airflow_plugin.airflow3._athena_openlineage_patch import (
    patch_athena_operator,
)


@pytest.fixture
def patched_athena_operator(monkeypatch: pytest.MonkeyPatch) -> None:
    # Stub the provider's implementation, which would call AWS, and re-apply
    # the DataHub patch on top of it. monkeypatch restores both on teardown.
    monkeypatch.setattr(
        AthenaOperator,
        "get_openlineage_facets_on_complete",
        lambda self, task_instance: OperatorLineage(),
    )
    monkeypatch.setattr(
        AthenaOperator, "_datahub_openlineage_patched", False, raising=False
    )
    monkeypatch.delattr(AthenaOperator, "_datahub_openlineage_patched")
    patch_athena_operator()


@pytest.mark.parametrize(
    "database, query_execution_context",
    [
        pytest.param("dw", None, id="database"),
        pytest.param(None, {"Database": "dw"}, id="query_execution_context"),
    ],
)
def test_operator_database_is_the_default_schema(
    patched_athena_operator: None,
    database: Optional[str],
    query_execution_context: Optional[Dict[str, str]],
) -> None:
    # Athena dataset names are two-tier (database.table), so the operator's
    # database must only qualify unqualified tables, not prefix qualified ones.
    operator = AthenaOperator(
        task_id="athena_lineage",
        database=database,
        query_execution_context=query_execution_context,
        query="INSERT INTO dw.t SELECT s.a, u.b FROM other_db.s JOIN u ON s.id = u.id",
        output_location="s3://bucket/results/",
    )

    lineage = operator.get_openlineage_facets_on_complete(None)

    result = lineage.run_facets[DATAHUB_SQL_PARSING_RESULT_KEY]
    assert sorted(result.in_tables) == [
        "urn:li:dataset:(urn:li:dataPlatform:athena,dw.u,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:athena,other_db.s,PROD)",
    ]
    assert result.out_tables == [
        "urn:li:dataset:(urn:li:dataPlatform:athena,dw.t,PROD)"
    ]
