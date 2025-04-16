import json
import pathlib
from typing import Any, Dict, List, cast
from unittest import mock

import pytest
from freezegun import freeze_time
from tableauserverclient import Server

import datahub.ingestion.source.tableau.tableau_constant as c
from datahub.emitter.mce_builder import DEFAULT_ENV, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import TestConnectionReport
from datahub.ingestion.source.tableau.tableau import (
    DEFAULT_PAGE_SIZE,
    SiteIdContentUrl,
    TableauConfig,
    TableauPageSizeConfig,
    TableauSiteSource,
    TableauSource,
    TableauSourceReport,
)
from datahub.ingestion.source.tableau.tableau_common import (
    TableauLineageOverrides,
    TableauUpstreamReference,
    get_filter_pages,
    make_filter,
    optimize_query_filter,
    tableau_field_to_schema_field,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from tests.test_helpers import test_connection_helpers
from tests.unit.tableau.test_tableau_config import default_config

FROZEN_TIME = "2021-12-07 07:00:00"

test_resources_dir = pathlib.Path(__file__).parent


def read_response(file_name):
    response_json_path = f"{test_resources_dir}/setup/{file_name}"
    with open(response_json_path) as file:
        data = json.loads(file.read())
        return data


@freeze_time(FROZEN_TIME)
def test_tableau_test_connection_success():
    with mock.patch("datahub.ingestion.source.tableau.tableau.Server"):
        report = test_connection_helpers.run_test_connection(
            TableauSource, default_config
        )
        test_connection_helpers.assert_basic_connectivity_success(report)


@freeze_time(FROZEN_TIME)
def test_tableau_test_connection_failure():
    report = test_connection_helpers.run_test_connection(TableauSource, default_config)
    test_connection_helpers.assert_basic_connectivity_failure(report, "Unable to login")


def test_connection_report_test(requests_mock):
    server_info_response = """
        <tsResponse xmlns:t="http://tableau.com/api">
            <t:serverInfo>
                <t:productVersion build="build-number">foo</t:productVersion>
                <t:restApiVersion>2.4</t:restApiVersion>
            </t:serverInfo>
        </tsResponse>

    """

    requests_mock.register_uri(
        "GET",
        "https://do-not-connect/api/2.4/serverInfo",
        text=server_info_response,
        status_code=200,
        headers={"Content-Type": "application/xml"},
    )

    signin_response = """
        <tsResponse xmlns:t="http://tableau.com/api">
            <t:credentials token="fake_token">
                <t:site id="fake_site_luid" contentUrl="fake_site_content_url"/>
                <t:user id="fake_user_id"/>
            </t:credentials>
        </tsResponse>
    """

    requests_mock.register_uri(
        "POST",
        "https://do-not-connect/api/2.4/auth/signin",
        text=signin_response,
        status_code=200,
        headers={"Content-Type": "application/xml"},
    )

    user_by_id_response = """
        <tsResponse xmlns:t="http://tableau.com/api">
          <t:user id="user-id" name="foo@abc.com" siteRole="SiteAdministratorExplorer" />
        </tsResponse>
    """

    requests_mock.register_uri(
        "GET",
        "https://do-not-connect/api/2.4/sites/fake_site_luid/users/fake_user_id",
        text=user_by_id_response,
        status_code=200,
        headers={"Content-Type": "application/xml"},
    )

    report: TestConnectionReport = TableauSource.test_connection(default_config)

    assert report
    assert report.capability_report
    assert report.capability_report.get(c.SITE_PERMISSION)
    assert report.capability_report[c.SITE_PERMISSION].capable

    # Role other than SiteAdministratorExplorer
    user_by_id_response = """
        <tsResponse xmlns:t="http://tableau.com/api">
          <t:user id="user-id" name="foo@abc.com" siteRole="Explorer" />
        </tsResponse>
    """

    requests_mock.register_uri(
        "GET",
        "https://do-not-connect/api/2.4/sites/fake_site_luid/users/fake_user_id",
        text=user_by_id_response,
        status_code=200,
        headers={"Content-Type": "application/xml"},
    )

    report = TableauSource.test_connection(default_config)

    assert report
    assert report.capability_report
    assert report.capability_report.get(c.SITE_PERMISSION)
    assert report.capability_report[c.SITE_PERMISSION].capable is False
    assert (
        report.capability_report[c.SITE_PERMISSION].failure_reason
        == "The user does not have the `Site Administrator Explorer` role. Their current role is Explorer."
    )


def test_tableau_no_verify():
    # This test ensures that we can connect to a self-signed certificate
    # when ssl_verify is set to False.

    source = TableauSource.create(
        {
            "connect_uri": "https://self-signed.badssl.com/",
            "ssl_verify": False,
            "site": "bogus",
            # Credentials
            "username": "bogus",
            "password": "bogus",
        },
        PipelineContext(run_id="0"),
    )
    list(source.get_workunits())

    report = source.get_report().as_string()
    assert "SSL" not in report
    assert "Unable to login" in report


def test_tableau_unsupported_csql():
    context = PipelineContext(run_id="0", pipeline_name="test_tableau")
    config_dict = default_config.copy()
    del config_dict["stateful_ingestion"]
    config = TableauConfig.parse_obj(config_dict)
    config.extract_lineage_from_unsupported_custom_sql_queries = True
    config.lineage_overrides = TableauLineageOverrides(
        database_override_map={"production database": "prod"}
    )

    def check_lineage_metadata(
        lineage, expected_entity_urn, expected_upstream_table, expected_cll
    ):
        mcp = cast(MetadataChangeProposalWrapper, list(lineage)[0].metadata)

        expected = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=expected_upstream_table,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            ],
            fineGrainedLineages=[
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=[
                        make_schema_field_urn(expected_upstream_table, upstream_column)
                    ],
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[
                        make_schema_field_urn(expected_entity_urn, downstream_column)
                    ],
                )
                for upstream_column, downstream_column in expected_cll.items()
            ],
        )
        assert mcp.entityUrn == expected_entity_urn

        actual_aspect = mcp.aspect
        assert actual_aspect == expected

    csql_urn = "urn:li:dataset:(urn:li:dataPlatform:tableau,09988088-05ad-173c-a2f1-f33ba3a13d1a,PROD)"
    expected_upstream_table = "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_bigquery_project.invent_dw.UserDetail,PROD)"
    expected_cll = {
        "user_id": "user_id",
        "source": "source",
        "user_source": "user_source",
    }

    site_source = TableauSiteSource(
        config=config,
        ctx=context,
        platform="tableau",
        site=SiteIdContentUrl(site_id="id1", site_content_url="site1"),
        report=TableauSourceReport(),
        server=Server("https://test-tableau-server.com"),
    )

    lineage = site_source._create_lineage_from_unsupported_csql(
        csql_urn=csql_urn,
        csql={
            "query": "SELECT user_id, source, user_source FROM (SELECT *, ROW_NUMBER() OVER (partition BY user_id ORDER BY __partition_day DESC) AS rank_ FROM invent_dw.UserDetail ) source_user WHERE rank_ = 1",
            "isUnsupportedCustomSql": "true",
            "connectionType": "bigquery",
            "database": {
                "name": "my_bigquery_project",
                "connectionType": "bigquery",
            },
        },
        out_columns=[],
    )
    check_lineage_metadata(
        lineage=lineage,
        expected_entity_urn=csql_urn,
        expected_upstream_table=expected_upstream_table,
        expected_cll=expected_cll,
    )

    # With database as None
    lineage = site_source._create_lineage_from_unsupported_csql(
        csql_urn=csql_urn,
        csql={
            "query": "SELECT user_id, source, user_source FROM (SELECT *, ROW_NUMBER() OVER (partition BY user_id ORDER BY __partition_day DESC) AS rank_ FROM my_bigquery_project.invent_dw.UserDetail ) source_user WHERE rank_ = 1",
            "isUnsupportedCustomSql": "true",
            "connectionType": "bigquery",
            "database": None,
        },
        out_columns=[],
    )
    check_lineage_metadata(
        lineage=lineage,
        expected_entity_urn=csql_urn,
        expected_upstream_table=expected_upstream_table,
        expected_cll=expected_cll,
    )


def test_lineage_overrides():
    # Simple - specify platform instance to presto table
    assert (
        TableauUpstreamReference(
            "presto_catalog",
            "test-database-id",
            "test-schema",
            "test-table",
            "presto",
        ).make_dataset_urn(
            env=DEFAULT_ENV, platform_instance_map={"presto": "my_presto_instance"}
        )
        == "urn:li:dataset:(urn:li:dataPlatform:presto,my_presto_instance.presto_catalog.test-schema.test-table,PROD)"
    )

    # Transform presto urn to hive urn
    # resulting platform instance for hive = mapped platform instance + presto_catalog
    assert (
        TableauUpstreamReference(
            "presto_catalog",
            "test-database-id",
            "test-schema",
            "test-table",
            "presto",
        ).make_dataset_urn(
            env=DEFAULT_ENV,
            platform_instance_map={"presto": "my_instance"},
            lineage_overrides=TableauLineageOverrides(
                platform_override_map={"presto": "hive"},
            ),
        )
        == "urn:li:dataset:(urn:li:dataPlatform:hive,my_instance.presto_catalog.test-schema.test-table,PROD)"
    )

    # transform hive urn to presto urn
    assert (
        TableauUpstreamReference(
            None,
            None,
            "test-schema",
            "test-table",
            "hive",
        ).make_dataset_urn(
            env=DEFAULT_ENV,
            platform_instance_map={"hive": "my_presto_instance.presto_catalog"},
            lineage_overrides=TableauLineageOverrides(
                platform_override_map={"hive": "presto"},
            ),
        )
        == "urn:li:dataset:(urn:li:dataPlatform:presto,my_presto_instance.presto_catalog.test-schema.test-table,PROD)"
    )


def test_database_hostname_to_platform_instance_map():
    # Simple - snowflake table
    assert (
        TableauUpstreamReference(
            "test-database-name",
            "test-database-id",
            "test-schema",
            "test-table",
            "snowflake",
        ).make_dataset_urn(env=DEFAULT_ENV, platform_instance_map={})
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,test-database-name.test-schema.test-table,PROD)"
    )

    # Finding platform instance based off hostname to platform instance mappings
    assert (
        TableauUpstreamReference(
            "test-database-name",
            "test-database-id",
            "test-schema",
            "test-table",
            "snowflake",
        ).make_dataset_urn(
            env=DEFAULT_ENV,
            platform_instance_map={},
            database_hostname_to_platform_instance_map={
                "test-hostname": "test-platform-instance"
            },
            database_server_hostname_map={"test-database-id": "test-hostname"},
        )
        == "urn:li:dataset:(urn:li:dataPlatform:snowflake,test-platform-instance.test-database-name.test-schema.test-table,PROD)"
    )


def test_tableau_source_handles_none_nativedatatype():
    field: Dict[str, Any] = {
        "__typename": "CalculatedField",
        "id": "abcd",
        "name": "Test Field",
        "description": None,
        "isHidden": False,
        "folderName": None,
        "upstreamFields": [],
        "upstreamColumns": [],
        "role": None,
        "dataType": None,
        "defaultFormat": "s",
        "aggregation": None,
        "formula": "a/b + d",
    }
    schema_field: SchemaField = tableau_field_to_schema_field(
        field=field, ingest_tags=False
    )
    assert schema_field.nativeDataType == "UNKNOWN"


def test_tableau_source_unescapes_lt():
    res = TableauSiteSource._clean_tableau_query_parameters(
        "select * from t where c1 << 135"
    )

    assert res == "select * from t where c1 < 135"


def test_tableau_source_unescapes_gt():
    res = TableauSiteSource._clean_tableau_query_parameters(
        "select * from t where c1 >> 135"
    )

    assert res == "select * from t where c1 > 135"


def test_tableau_source_unescapes_gte():
    res = TableauSiteSource._clean_tableau_query_parameters(
        "select * from t where c1 >>= 135"
    )

    assert res == "select * from t where c1 >= 135"


def test_tableau_source_unescapeslgte():
    res = TableauSiteSource._clean_tableau_query_parameters(
        "select * from t where c1 <<= 135"
    )

    assert res == "select * from t where c1 <= 135"


def test_tableau_source_doesnt_touch_not_escaped():
    res = TableauSiteSource._clean_tableau_query_parameters(
        "select * from t where c1 < 135 and c2 > 15"
    )

    assert res == "select * from t where c1 < 135 and c2 > 15"


TABLEAU_PARAMS = [
    "<Parameters.MyParam>",
    "<Parameters.MyParam_1>",
    "<Parameters.My Param _ 1>",
    "<Parameters.My Param 1 !@\"',.#$%^:;&*()-_+={}|\\ /<>",
    "<[Parameters].MyParam>",
    "<[Parameters].MyParam_1>",
    "<[Parameters].My Param _ 1>",
    "<[Parameters].My Param 1 !@\"',.#$%^:;&*()-_+={}|\\ /<>",
    "<Parameters.[MyParam]>",
    "<Parameters.[MyParam_1]>",
    "<Parameters.[My Param _ 1]>",
    "<Parameters.[My Param 1 !@\"',.#$%^:;&*()-_+={}|\\ /<]>",
    "<[Parameters].[MyParam]>",
    "<[Parameters].[MyParam_1]>",
    "<[Parameters].[My Param _ 1]>",
    "<[Parameters].[My Param 1 !@\"',.#$%^:;&*()-_+={}|\\ /<]>",
    "<Parameters.[My Param 1 !@\"',.#$%^:;&*()-_+={}|\\ /<>]>",
    "<[Parameters].[My Param 1 !@\"',.#$%^:;&*()-_+={}|\\ /<>]>",
]


@pytest.mark.parametrize("p", TABLEAU_PARAMS)
def test_tableau_source_cleanups_tableau_parameters_in_equi_predicates(p):
    assert (
        TableauSiteSource._clean_tableau_query_parameters(
            f"select * from t where c1 = {p} and c2 = {p} and c3 = 7"
        )
        == "select * from t where c1 = 1 and c2 = 1 and c3 = 7"
    )


@pytest.mark.parametrize("p", TABLEAU_PARAMS)
def test_tableau_source_cleanups_tableau_parameters_in_lt_gt_predicates(p):
    assert (
        TableauSiteSource._clean_tableau_query_parameters(
            f"select * from t where c1 << {p} and c2<<{p} and c3 >> {p} and c4>>{p} or {p} >> c1 and {p}>>c2 and {p} << c3 and {p}<<c4"
        )
        == "select * from t where c1 < 1 and c2<1 and c3 > 1 and c4>1 or 1 > c1 and 1>c2 and 1 < c3 and 1<c4"
    )


@pytest.mark.parametrize("p", TABLEAU_PARAMS)
def test_tableau_source_cleanups_tableau_parameters_in_lte_gte_predicates(p):
    assert (
        TableauSiteSource._clean_tableau_query_parameters(
            f"select * from t where c1 <<= {p} and c2<<={p} and c3 >>= {p} and c4>>={p} or {p} >>= c1 and {p}>>=c2 and {p} <<= c3 and {p}<<=c4"
        )
        == "select * from t where c1 <= 1 and c2<=1 and c3 >= 1 and c4>=1 or 1 >= c1 and 1>=c2 and 1 <= c3 and 1<=c4"
    )


@pytest.mark.parametrize("p", TABLEAU_PARAMS)
def test_tableau_source_cleanups_tableau_parameters_in_join_predicate(p):
    assert (
        TableauSiteSource._clean_tableau_query_parameters(
            f"select * from t1 inner join t2 on t1.id = t2.id and t2.c21 = {p} and t1.c11 = 123 + {p}"
        )
        == "select * from t1 inner join t2 on t1.id = t2.id and t2.c21 = 1 and t1.c11 = 123 + 1"
    )


@pytest.mark.parametrize("p", TABLEAU_PARAMS)
def test_tableau_source_cleanups_tableau_parameters_in_complex_expressions(p):
    assert (
        TableauSiteSource._clean_tableau_query_parameters(
            f"select myudf1(c1, {p}, c2) / myudf2({p}) > ({p} + 3 * {p} * c5) * {p} - c4"
        )
        == "select myudf1(c1, 1, c2) / myudf2(1) > (1 + 3 * 1 * c5) * 1 - c4"
    )


@pytest.mark.parametrize("p", TABLEAU_PARAMS)
def test_tableau_source_cleanups_tableau_parameters_in_udfs(p):
    assert (
        TableauSiteSource._clean_tableau_query_parameters(f"select myudf({p}) from t")
        == "select myudf(1) from t"
    )


def test_make_id_filter():
    ids = [i for i in range(1, 6)]
    filter_dict = {c.ID_WITH_IN: ids}
    assert make_filter(filter_dict) == f"{c.ID_WITH_IN}: [1, 2, 3, 4, 5]"


def test_make_project_filter():
    projects = ["x", "y", "z"]
    filter_dict = {c.PROJECT_NAME_WITH_IN: projects}
    assert make_filter(filter_dict) == f'{c.PROJECT_NAME_WITH_IN}: ["x", "y", "z"]'


def test_make_multiple_filters():
    ids = [i for i in range(1, 6)]
    projects = ["x", "y", "z"]
    filter_dict = {c.ID_WITH_IN: ids, c.PROJECT_NAME_WITH_IN: projects}
    assert (
        make_filter(filter_dict)
        == f'{c.ID_WITH_IN}: [1, 2, 3, 4, 5], {c.PROJECT_NAME_WITH_IN}: ["x", "y", "z"]'
    )


def test_get_filter_pages_simple():
    ids = [i for i in range(5)]
    filter_dict = {c.ID_WITH_IN: ids}
    assert get_filter_pages(filter_dict, 10) == [filter_dict]


def test_get_filter_pages_non_id_large_filter():
    projects = [f"project{i}" for i in range(10)]
    filter_dict = {c.PROJECT_NAME_WITH_IN: projects}
    assert get_filter_pages(filter_dict, 10) == [filter_dict]


def test_get_filter_pages_for_single_key():
    projects = ["project1"]
    filter_dict = {c.PROJECT_NAME_WITH_IN: projects}
    assert get_filter_pages(filter_dict, 10) == [filter_dict]


def test_get_filter_pages_id_filter_splits_into_multiple_filters():
    page_size = 10
    num_ids = 20000
    ids = [f"id_{i}" for i in range(num_ids)]
    filter_dict = {c.ID_WITH_IN: ids}
    assert get_filter_pages(filter_dict, page_size) == [
        {c.ID_WITH_IN: filter_dict[c.ID_WITH_IN][i : i + page_size]}
        for i in range(0, num_ids, page_size)
    ]


def test_optimize_query_filter_removes_duplicates():
    query_filter = {
        c.ID_WITH_IN: ["id1", "id2", "id1"],
        c.PROJECT_NAME_WITH_IN: ["project1", "project2", "project1"],
    }
    result = optimize_query_filter(query_filter)
    assert len(result) == 2
    assert result[c.ID_WITH_IN] == ["id1", "id2"]
    assert result[c.PROJECT_NAME_WITH_IN] == ["project1", "project2"]


def test_optimize_query_filter_handles_empty_lists():
    query_filter: Dict[str, List[str]] = {c.ID_WITH_IN: [], c.PROJECT_NAME_WITH_IN: []}
    result = optimize_query_filter(query_filter)
    assert len(result) == 2
    assert result[c.ID_WITH_IN] == []
    assert result[c.PROJECT_NAME_WITH_IN] == []


def test_optimize_query_filter_handles_missing_keys():
    query_filter: Dict[str, List[str]] = {}
    result = optimize_query_filter(query_filter)
    assert result == {}


def test_optimize_query_filter_handles_other_keys():
    query_filter = {"any_other_key": ["id1", "id2", "id1"]}
    result = optimize_query_filter(query_filter)
    assert len(result) == 1
    assert result["any_other_key"] == ["id1", "id2", "id1"]


def test_optimize_query_filter_handles_no_duplicates():
    query_filter = {
        c.ID_WITH_IN: ["id1", "id2"],
        c.PROJECT_NAME_WITH_IN: ["project1", "project2"],
    }
    result = optimize_query_filter(query_filter)
    assert len(result) == 2
    assert result[c.ID_WITH_IN] == ["id1", "id2"]
    assert result[c.PROJECT_NAME_WITH_IN] == ["project1", "project2"]


def test_tableau_upstream_reference():
    d = {
        "id": "7127b695-3df5-4a3a-4837-eb0f4b572337",
        "name": "TABLE1",
        "database": None,
        "schema": "SCHEMA1",
        "fullName": "DB1.SCHEMA1.TABLE1",
        "connectionType": "snowflake",
        "description": "",
        "columnsConnection": {"totalCount": 0},
    }
    ref = TableauUpstreamReference.create(d)
    assert ref

    assert ref.database == "DB1"
    assert ref.schema == "SCHEMA1"
    assert ref.table == "TABLE1"
    assert ref.connection_type == "snowflake"

    try:
        ref = TableauUpstreamReference.create(None)  # type: ignore[arg-type]
        raise AssertionError(
            "TableauUpstreamReference.create with None should have raised exception"
        )
    except ValueError:
        assert True


class TestTableauPageSizeConfig:
    def test_defaults(self):
        config = TableauPageSizeConfig()
        assert config.effective_database_server_page_size == DEFAULT_PAGE_SIZE
        assert config.effective_workbook_page_size == 1
        assert config.effective_sheet_page_size == DEFAULT_PAGE_SIZE
        assert config.effective_dashboard_page_size == DEFAULT_PAGE_SIZE
        assert config.effective_embedded_datasource_page_size == DEFAULT_PAGE_SIZE
        assert (
            config.effective_embedded_datasource_field_upstream_page_size
            == DEFAULT_PAGE_SIZE * 10
        )
        assert config.effective_published_datasource_page_size == DEFAULT_PAGE_SIZE
        assert (
            config.effective_published_datasource_field_upstream_page_size
            == DEFAULT_PAGE_SIZE * 10
        )
        assert config.effective_custom_sql_table_page_size == DEFAULT_PAGE_SIZE
        assert config.effective_database_table_page_size == DEFAULT_PAGE_SIZE

    def test_page_size_fallbacks(self):
        page_size = 33
        config = TableauPageSizeConfig(page_size=page_size)
        assert config.effective_database_server_page_size == page_size
        assert config.effective_workbook_page_size == 1
        assert config.effective_sheet_page_size == page_size
        assert config.effective_dashboard_page_size == page_size
        assert config.effective_embedded_datasource_page_size == page_size
        assert (
            config.effective_embedded_datasource_field_upstream_page_size
            == page_size * 10
        )
        assert config.effective_published_datasource_page_size == page_size
        assert (
            config.effective_published_datasource_field_upstream_page_size
            == page_size * 10
        )
        assert config.effective_custom_sql_table_page_size == page_size
        assert config.effective_database_table_page_size == page_size

    def test_fine_grained(self):
        any_page_size = 55
        config = TableauPageSizeConfig(database_server_page_size=any_page_size)
        assert config.page_size == DEFAULT_PAGE_SIZE
        assert config.effective_database_server_page_size == any_page_size

        config = TableauPageSizeConfig(workbook_page_size=any_page_size)
        assert config.page_size == DEFAULT_PAGE_SIZE
        assert config.effective_workbook_page_size == any_page_size

        config = TableauPageSizeConfig(workbook_page_size=None)
        assert config.page_size == DEFAULT_PAGE_SIZE
        assert config.effective_workbook_page_size == DEFAULT_PAGE_SIZE

        config = TableauPageSizeConfig(sheet_page_size=any_page_size)
        assert config.page_size == DEFAULT_PAGE_SIZE
        assert config.effective_sheet_page_size == any_page_size

        config = TableauPageSizeConfig(dashboard_page_size=any_page_size)
        assert config.page_size == DEFAULT_PAGE_SIZE
        assert config.effective_dashboard_page_size == any_page_size

        config = TableauPageSizeConfig(embedded_datasource_page_size=any_page_size)
        assert config.page_size == DEFAULT_PAGE_SIZE
        assert config.effective_embedded_datasource_page_size == any_page_size

        config = TableauPageSizeConfig(
            embedded_datasource_field_upstream_page_size=any_page_size
        )
        assert config.page_size == DEFAULT_PAGE_SIZE
        assert (
            config.effective_embedded_datasource_field_upstream_page_size
            == any_page_size
        )

        config = TableauPageSizeConfig(published_datasource_page_size=any_page_size)
        assert config.page_size == DEFAULT_PAGE_SIZE
        assert config.effective_published_datasource_page_size == any_page_size

        config = TableauPageSizeConfig(
            published_datasource_field_upstream_page_size=any_page_size
        )
        assert config.page_size == DEFAULT_PAGE_SIZE
        assert (
            config.effective_published_datasource_field_upstream_page_size
            == any_page_size
        )

        config = TableauPageSizeConfig(custom_sql_table_page_size=any_page_size)
        assert config.page_size == DEFAULT_PAGE_SIZE
        assert config.effective_custom_sql_table_page_size == any_page_size

        config = TableauPageSizeConfig(database_table_page_size=any_page_size)
        assert config.page_size == DEFAULT_PAGE_SIZE
        assert config.effective_database_table_page_size == any_page_size
