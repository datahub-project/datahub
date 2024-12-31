import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    ResolvePlatformInstanceFromDatasetTypeMapping,
)
from datahub.ingestion.source.powerbi.m_query.pattern_handler import MSSqlLineage
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table


@pytest.fixture
def creator():
    config = PowerBiDashboardSourceConfig(
        tenant_id="test-tenant-id",
        client_id="test-client-id",
        client_secret="test-client-secret",
    )

    table = Table(
        name="test_table",
        full_name="db.schema.test_table",
    )

    return MSSqlLineage(
        ctx=PipelineContext(run_id="test-run-id"),
        table=table,
        reporter=PowerBiDashboardSourceReport(),
        config=config,
        platform_instance_resolver=ResolvePlatformInstanceFromDatasetTypeMapping(
            config
        ),
    )


def test_parse_three_part_table_reference(creator):
    v = creator.create_urn_using_old_parser(
        "SELECT * FROM [dwhdbt].[dbo2].[my_table] where oper_day_date > getdate() - 5",
        db_name="default_db",
        server="server",
    )
    assert len(v) == 1
    assert (
        v[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,dwhdbt.dbo2.my_table,PROD)"
    )


def test_parse_two_part_table_reference(creator):
    v = creator.create_urn_using_old_parser(
        "SELECT * FROM my_schema.my_table",
        db_name="default_db",
        server="server",
    )
    assert len(v) == 1
    assert (
        v[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,default_db.my_schema.my_table,PROD)"
    )


def test_parse_one_part_table_reference(creator):
    v = creator.create_urn_using_old_parser(
        "SELECT * FROM my_table",
        db_name="default_db",
        server="server",
    )
    assert len(v) == 1
    assert (
        v[0].urn
        == "urn:li:dataset:(urn:li:dataPlatform:mssql,default_db.dbo.my_table,PROD)"
    )
