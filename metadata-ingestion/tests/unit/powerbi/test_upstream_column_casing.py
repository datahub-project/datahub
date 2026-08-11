from typing import List, Optional
from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    ResolvePlatformInstanceFromDatasetTypeMapping,
)
from datahub.ingestion.source.powerbi.m_query import pattern_handler
from datahub.ingestion.source.powerbi.m_query.pattern_handler import MSSqlLineage
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    Column,
    Table,
)
from datahub.metadata.schema_classes import StringTypeClass
from datahub.sql_parsing.schema_resolver import SchemaInfo

UPSTREAM_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:mssql,warehouse.dbo.fact_orders,PROD)"
)


def _handler(column_names: List[str]) -> MSSqlLineage:
    table = Table(
        name="fact_orders",
        full_name="warehouse.fact_orders",
        columns=[
            Column(
                name=name,
                dataType="string",
                isHidden=False,
                datahubDataType=StringTypeClass(),
            )
            for name in column_names
        ],
    )
    return MSSqlLineage(
        ctx=PipelineContext(run_id="test"),
        table=table,
        config=PowerBiDashboardSourceConfig(
            tenant_id="t", client_id="c", client_secret="s"
        ),
        reporter=PowerBiDashboardSourceReport(),
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            PowerBiDashboardSourceConfig(
                tenant_id="t", client_id="c", client_secret="s"
            )
        ),
    )


def _upstream_columns(
    column_names: List[str], schema_info: Optional[SchemaInfo]
) -> List[str]:
    with patch.object(
        pattern_handler, "_fetch_upstream_schema", return_value=schema_info
    ):
        cll = _handler(column_names).create_table_column_lineage(UPSTREAM_URN)
    return [upstream.column for info in cll for upstream in info.upstreams]


def test_upstream_casing_follows_the_warehouse_not_powerbi() -> None:
    # The warehouse was ingested with convert_column_urns_to_lowercase, so its
    # schemaField URNs are lowercased. Emitting PowerBI's own casing would point
    # the edge at a field that does not exist.
    assert _upstream_columns(
        ["LeadId", "Amount"], {"leadid": "VARCHAR", "amount": "DECIMAL"}
    ) == ["leadid", "amount"]


def test_upstream_casing_wins_over_powerbi_casing() -> None:
    # PowerBI and the warehouse can disagree on casing in either direction; the
    # warehouse is the side that owns the schemaField URN.
    assert _upstream_columns(["Leadid"], {"LeadId": "VARCHAR"}) == ["LeadId"]


def test_falls_back_to_powerbi_casing_when_schema_is_unresolvable() -> None:
    # No graph, or the upstream has not been ingested yet: keep today's behaviour
    # rather than dropping the edge.
    assert _upstream_columns(["LeadId"], None) == ["LeadId"]


def test_columns_absent_from_the_upstream_schema_are_left_alone() -> None:
    # A PowerBI-only computed column has no warehouse counterpart to match.
    assert _upstream_columns(["LeadId", "Computed"], {"leadid": "VARCHAR"}) == [
        "leadid",
        "Computed",
    ]
