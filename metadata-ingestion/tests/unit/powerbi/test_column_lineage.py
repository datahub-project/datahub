"""Unit tests for Mapper.make_fine_grained_lineage_class (powerbi.py).

PowerBI builds ColumnLineageInfo directly in m_query/pattern_handler.py rather
than via sql_parsing.sqlglot_lineage's internal translation step, so it isn't
covered by that module's own empty-column filtering and needs its own guard.
"""

from unittest.mock import MagicMock

from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.m_query.data_classes import Lineage
from datahub.ingestion.source.powerbi.powerbi import Mapper
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
)

UPSTREAM_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.upstream,PROD)"
DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:powerbi,ds-1,PROD)"


def _config(**overrides: object) -> PowerBiDashboardSourceConfig:
    base = {
        "client_id": "foo",
        "client_secret": "bar",
        "tenant_id": "0B0C960B-FCDF-4D0F-8C45-2E03BB59DDEB",
        "convert_lineage_urns_to_lowercase": False,
    }
    base.update(overrides)
    return PowerBiDashboardSourceConfig.parse_obj(base)


def _mapper(config: PowerBiDashboardSourceConfig) -> Mapper:
    return Mapper(
        ctx=MagicMock(),
        config=config,
        reporter=PowerBiDashboardSourceReport(),
        dataplatform_instance_resolver=create_dataplatform_instance_resolver(config),
    )


def test_make_fine_grained_lineage_class_skips_unresolved_columns() -> None:
    mapper = _mapper(_config())
    lineage = Lineage(
        upstreams=[],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(column="my_col"),
                upstreams=[
                    ColumnRef(table=UPSTREAM_URN, column=""),
                    ColumnRef(table=UPSTREAM_URN, column="resolved_col"),
                ],
            ),
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(column=""),
                upstreams=[ColumnRef(table=UPSTREAM_URN, column="other_col")],
            ),
        ],
    )

    result = mapper.make_fine_grained_lineage_class(lineage, DATASET_URN)

    assert len(result) == 2
    with_unresolved_upstream, with_empty_downstream = result

    assert with_unresolved_upstream.upstreams is not None
    assert len(with_unresolved_upstream.upstreams) == 1
    assert "resolved_col" in with_unresolved_upstream.upstreams[0]

    assert with_empty_downstream.downstreams == []
