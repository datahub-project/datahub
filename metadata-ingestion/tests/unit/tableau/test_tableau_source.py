import json
import pathlib
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple, cast
from unittest import mock

import pytest
import time_machine
from tableauserverclient import Server, SiteItem

import datahub.ingestion.source.tableau.tableau_constant as c
from datahub.emitter.mce_builder import DEFAULT_ENV, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import TestConnectionReport
from datahub.ingestion.source.tableau.tableau import (
    DEFAULT_PAGE_SIZE,
    CustomSqlParseResult,
    LineageResult,
    SiteIdContentUrl,
    TableauConfig,
    TableauPageSizeConfig,
    TableauProject,
    TableauSiteSource,
    TableauSource,
    TableauSourceReport,
    UpstreamTablesResult,
)
from datahub.ingestion.source.tableau.tableau_common import (
    TableauLineageOverrides,
    TableauUpstreamReference,
    get_filter_pages,
    make_filter,
    make_fine_grained_lineage_class,
    optimize_query_filter,
    tableau_field_to_schema_field,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
    SqlParsingResult,
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


@time_machine.travel(FROZEN_TIME, tick=False)
def test_tableau_test_connection_success():
    with mock.patch("datahub.ingestion.source.tableau.tableau.Server"):
        report = test_connection_helpers.run_test_connection(
            TableauSource, default_config
        )
        test_connection_helpers.assert_basic_connectivity_success(report)


@time_machine.travel(FROZEN_TIME, tick=False)
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


@pytest.mark.parametrize(
    "ssl_verify",
    [
        True,
        False,
        "/etc/ssl/certs/custom-ca-bundle.pem",
    ],
)
def test_tableau_ssl_verify_passed_to_http_options(ssl_verify):
    config = TableauConfig.model_validate(
        {
            **default_config,
            "ssl_verify": ssl_verify,
        }
    )

    with mock.patch("datahub.ingestion.source.tableau.tableau.Server") as mock_server:
        config.make_tableau_client(config.site)

    http_options = mock_server.call_args.kwargs["http_options"]
    assert http_options["verify"] == ssl_verify
    assert "cert" not in http_options


def test_tableau_unsupported_csql():
    context = PipelineContext(run_id="0", pipeline_name="test_tableau")
    config_dict = default_config.copy()
    del config_dict["stateful_ingestion"]
    config = TableauConfig.model_validate(config_dict)
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


def test_make_fine_grained_lineage_class_skips_upstreams_with_unresolved_column():
    # Simulates sqlglot failing to resolve an upstream column (e.g. because
    # lineage_overrides.platform_override_map swapped the platform and the
    # schema-aware column resolver no longer matches), which surfaces as an
    # empty column name on the ColumnRef.
    upstream_table_urn = "urn:li:dataset:(urn:li:dataPlatform:athena,db.table,PROD)"
    parsed_result = SqlParsingResult(
        in_tables=[upstream_table_urn],
        out_tables=[],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(column="my_col"),
                upstreams=[
                    ColumnRef(table=upstream_table_urn, column=""),
                    ColumnRef(table=upstream_table_urn, column="resolved_col"),
                ],
            )
        ],
    )

    result = make_fine_grained_lineage_class(
        parsed_result,
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,ds-1,PROD)",
        out_columns=[],
    )

    assert len(result) == 1
    assert result[0].upstreams == [
        f"urn:li:schemaField:({upstream_table_urn},resolved_col)"
    ]


def test_make_fine_grained_lineage_class_skips_unresolved_downstream_column():
    # An empty downstream column (same root cause as the upstream case above)
    # must not produce an invalid schemaField URN with an empty field path.
    upstream_table_urn = "urn:li:dataset:(urn:li:dataPlatform:athena,db.table,PROD)"
    parsed_result = SqlParsingResult(
        in_tables=[upstream_table_urn],
        out_tables=[],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(column=""),
                upstreams=[ColumnRef(table=upstream_table_urn, column="resolved_col")],
            )
        ],
    )

    result = make_fine_grained_lineage_class(
        parsed_result,
        dataset_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,ds-1,PROD)",
        out_columns=[],
    )

    assert len(result) == 1
    assert result[0].downstreams == []


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

    with pytest.raises(ValueError):
        TableauUpstreamReference.create(None)  # type: ignore[arg-type]


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


def test_get_owner_identifier_username():
    """Test owner identifier extraction using username."""
    config_dict = default_config.copy()
    config_dict["use_email_as_username"] = False
    config = TableauConfig.model_validate(config_dict)

    context = PipelineContext(run_id="test", pipeline_name="test")
    site_source = TableauSiteSource(
        config=config,
        ctx=context,
        site=SiteIdContentUrl(site_id="site1", site_content_url="site1"),
        report=TableauSourceReport(),
        server=mock.MagicMock(spec=Server),
        platform="tableau",
    )

    owner_dict = {"username": "testuser", "email": "test@example.com"}
    result = site_source._get_owner_identifier(owner_dict)
    assert result == "testuser"


def test_get_owner_identifier_email():
    """Test owner identifier extraction using email."""
    config_dict = default_config.copy()
    config_dict["use_email_as_username"] = True
    config = TableauConfig.model_validate(config_dict)

    context = PipelineContext(run_id="test", pipeline_name="test")
    site_source = TableauSiteSource(
        config=config,
        ctx=context,
        site=SiteIdContentUrl(site_id="site1", site_content_url="site1"),
        report=TableauSourceReport(),
        server=mock.MagicMock(spec=Server),
        platform="tableau",
    )

    owner_dict = {"username": "testuser", "email": "test@example.com"}
    result = site_source._get_owner_identifier(owner_dict)
    assert result == "test@example.com"


def test_get_owner_identifier_email_fallback():
    """Test owner identifier extraction falls back to username when email is not available."""
    config_dict = default_config.copy()
    config_dict["use_email_as_username"] = True
    config = TableauConfig.model_validate(config_dict)

    context = PipelineContext(run_id="test", pipeline_name="test")
    site_source = TableauSiteSource(
        config=config,
        ctx=context,
        site=SiteIdContentUrl(site_id="site1", site_content_url="site1"),
        report=TableauSourceReport(),
        server=mock.MagicMock(spec=Server),
        platform="tableau",
    )

    owner_dict = {"username": "testuser"}  # No email
    result = site_source._get_owner_identifier(owner_dict)
    assert result == "testuser"


def test_get_owner_identifier_empty_dict():
    """Test owner identifier extraction with empty owner dict."""
    config_dict = default_config.copy()
    config_dict["use_email_as_username"] = True
    config = TableauConfig.model_validate(config_dict)

    context = PipelineContext(run_id="test", pipeline_name="test")
    site_source = TableauSiteSource(
        config=config,
        ctx=context,
        site=SiteIdContentUrl(site_id="site1", site_content_url="site1"),
        report=TableauSourceReport(),
        server=mock.MagicMock(spec=Server),
        platform="tableau",
    )

    result = site_source._get_owner_identifier({})
    assert result is None


class TestTableauSourceNewFeatures:
    """Test suite for new Tableau source features including VC support and column normalization."""

    def setup_method(self, method):
        """Set up test fixtures."""
        self.config = TableauConfig.parse_obj(default_config)
        self.ctx = PipelineContext(run_id="test")

        with mock.patch("datahub.ingestion.source.tableau.tableau.Server"):
            mock_site = mock.MagicMock(
                spec=SiteItem, id="test-site-id", content_url="test-site"
            )

            self.tableau_source = TableauSiteSource(
                config=self.config,
                ctx=self.ctx,
                platform="tableau",
                site=mock_site,
                server=mock.MagicMock(),
                report=TableauSourceReport(),
            )

    def test_get_upstream_vc_tables_with_relationships(self):
        """Test get_upstream_vc_tables with existing VC relationships."""
        datasource_id = "ds-123"

        # Set up VC relationships
        self.tableau_source.vc_processor.datasource_vc_relationships[datasource_id] = [
            {
                "vc_id": "vc-456",
                "vc_table_id": "vc-table-1",
                "vc_table_name": "test_table",
                "column_name": "test_column",
                "field_name": "test_field",
            },
            {
                "vc_id": "vc-456",
                "vc_table_id": "vc-table-2",
                "vc_table_name": "another_table",
                "column_name": "another_column",
                "field_name": "another_field",
            },
        ]

        result = self.tableau_source.get_upstream_vc_tables(datasource_id)

        # Should return UpstreamTablesResult dataclass
        assert isinstance(result, UpstreamTablesResult)
        assert len(result.upstream_tables) == 2  # Two unique VC tables
        assert len(result.table_id_to_urn) == 2

        # Check that URNs are properly formatted
        for urn in result.table_id_to_urn.values():
            assert "urn:li:dataset:(urn:li:dataPlatform:tableau," in urn
            assert "vc-456." in urn  # Should contain VC ID

    def test_create_upstream_table_lineage_with_vc_tables(self):
        """Test _create_upstream_table_lineage prioritizes VC tables for embedded datasources."""
        datasource_id = "ds-123"
        datasource = {
            c.ID: datasource_id,
            c.NAME: "Test Embedded Datasource",
            c.FIELDS: [],
            c.UPSTREAM_TABLES: [
                {"id": "regular-table-1", "name": "regular_table"}
            ],  # Regular tables
            c.UPSTREAM_DATA_SOURCES: [],
        }

        # Set up VC relationships to simulate VC tables being available
        self.tableau_source.vc_processor.datasource_vc_relationships[datasource_id] = [
            {
                "vc_id": "vc-456",
                "vc_table_id": "vc-table-1",
                "vc_table_name": "vc_test_table",
                "column_name": "test_column",
                "field_name": "test_field",
            }
        ]

        result = self.tableau_source._create_upstream_table_lineage(
            datasource, browse_path=None, is_embedded_ds=True
        )

        # Should return LineageResult with VC tables, not regular tables
        assert isinstance(result, LineageResult)
        assert len(result.upstream_tables) == 1

        # Check that the upstream URN contains VC information
        upstream_urn = result.upstream_tables[0].dataset
        assert "vc-456.vc_test_table" in upstream_urn

    def test_snowflake_column_normalization_in_lineage(self):
        """Test that Snowflake column normalization is applied during lineage creation."""
        datasource = {
            c.ID: "ds-123",
            c.NAME: "Test Datasource",
            c.FIELDS: [
                {
                    c.NAME: "test_field",
                    c.UPSTREAM_COLUMNS: [
                        {
                            c.NAME: "Customer Name",  # Should be normalized to customer_name
                            c.TABLE: {c.ID: "table-1", c.TYPE_NAME: "DatabaseTable"},
                        }
                    ],
                }
            ],
        }

        # Mock Snowflake URN detection and ensure ingest_tables_external is False for normalization
        with (
            mock.patch.object(
                self.tableau_source, "is_snowflake_urn", return_value=True
            ),
            mock.patch.object(
                self.tableau_source.config, "ingest_tables_external", False
            ),
        ):
            # Mock table ID to URN mapping
            table_id_to_urn = {
                "table-1": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.schema.table,PROD)"
            }

            fine_grained_lineages = (
                self.tableau_source.get_upstream_columns_of_fields_in_datasource(
                    datasource, "urn:li:dataset:test", table_id_to_urn
                )
            )

            # Should have created fine-grained lineage with normalized column name
            assert fine_grained_lineages is not None
            assert len(fine_grained_lineages) == 1
            lineage = fine_grained_lineages[0]

            # Check that the upstream field URN contains the normalized column name
            assert lineage.upstreams is not None
            assert len(lineage.upstreams) > 0
            upstream_field_urn = lineage.upstreams[0]
            assert (
                "customer_name" in upstream_field_urn
            )  # Normalized from "Customer Name"

    def test_get_upstream_fields_from_custom_sql_skips_unresolved_downstream_column(
        self,
    ):
        upstream_table_urn = "urn:li:dataset:(urn:li:dataPlatform:athena,db.table,PROD)"
        parsed_result = SqlParsingResult(
            in_tables=[upstream_table_urn],
            out_tables=[],
            column_lineage=[
                ColumnLineageInfo(
                    downstream=DownstreamColumnRef(column=""),
                    upstreams=[
                        ColumnRef(table=upstream_table_urn, column="resolved_col")
                    ],
                )
            ],
        )

        with mock.patch.object(
            self.tableau_source,
            "parse_custom_sql",
            return_value=CustomSqlParseResult(
                result=parsed_result, platform_instance=None
            ),
        ):
            fine_grained_lineages = (
                self.tableau_source.get_upstream_fields_from_custom_sql(
                    datasource={}, datasource_urn="urn:li:dataset:(...,csql,PROD)"
                )
            )

        assert len(fine_grained_lineages) == 1
        assert fine_grained_lineages[0].downstreams == []


def _extract_dataset_properties(work_units):
    for wu in work_units:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper):
            if hasattr(wu.metadata, "aspect") and isinstance(
                wu.metadata.aspect, DatasetPropertiesClass
            ):
                return wu.metadata.aspect
        elif hasattr(wu.metadata, "proposedSnapshot"):
            for aspect in wu.metadata.proposedSnapshot.aspects:
                if isinstance(aspect, DatasetPropertiesClass):
                    return aspect
    return None


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.parametrize(
    "datasource_type,datasource_name,csql_id,ds_id,project_luid,has_project_luid_patch,expected_name,workbook_data",
    [
        pytest.param(
            "PublishedDatasource",
            "My Datasource",
            "csql-123",
            "ds-123",
            "project-luid-123",
            True,
            "My Datasource",
            None,
            id="with_datasource_name",
        ),
        pytest.param(
            "PublishedDatasource",
            None,
            "csql-789",
            "ds-789",
            "project-luid-789",
            True,
            None,
            None,
            id="with_none_datasource_name",
        ),
        pytest.param(
            "PublishedDatasource",
            "",
            "csql-999",
            "ds-999",
            "project-luid-999",
            True,
            None,
            None,
            id="with_empty_string_datasource_name",
        ),
        pytest.param(
            "EmbeddedDatasource",
            "Original Datasource Name",
            "csql-embedded",
            "ds-embedded",
            None,
            False,
            "Original Datasource Name",
            {
                "id": "wb-123",
                "name": "My Workbook",
                "projectName": "default",
                "luid": "wb-luid-123",
            },
            id="with_embedded_datasource",
        ),
    ],
)
def test_custom_sql_datasource_naming_scenarios(
    datasource_type,
    datasource_name,
    csql_id,
    ds_id,
    project_luid,
    has_project_luid_patch,
    expected_name,
    workbook_data,
):
    context = PipelineContext(run_id="0", pipeline_name="test_tableau")
    config_dict = default_config.copy()
    del config_dict["stateful_ingestion"]
    config = TableauConfig.model_validate(config_dict)

    mock_server = mock.MagicMock(spec=Server)
    mock_server.user_id = "test-user-id"
    mock_server.users = mock.MagicMock()
    mock_server.users.get_by_id = mock.MagicMock(
        return_value=mock.MagicMock(site_role="SiteAdministratorExplorer")
    )

    site_source = TableauSiteSource(
        config=config,
        ctx=context,
        platform="tableau",
        site=SiteIdContentUrl(site_id="id1", site_content_url="site1"),
        report=TableauSourceReport(),
        server=mock_server,
    )

    columns = []
    if datasource_type:
        datasource = {
            "__typename": datasource_type,
            "id": ds_id,
            "name": datasource_name,
        }
        if datasource_type == "PublishedDatasource":
            datasource["projectName"] = "default"
            datasource["luid"] = f"ds-luid-{ds_id.split('-')[1]}"
        elif datasource_type == "EmbeddedDatasource" and workbook_data:
            datasource["workbook"] = workbook_data

        columns = [
            {
                "id": "col-1",
                "name": "column1",
                "referencedByFields": [{"datasource": datasource}],
            }
        ]

    custom_sql_data = [
        {
            "id": csql_id,
            "name": f"Custom SQL Query {csql_id}",
            "query": f"SELECT * FROM table{csql_id}",
            "connectionType": "postgres",
            "columns": columns,
            "tables": [],
            "database": {"name": "test_db", "connectionType": "postgres"},
        }
    ]

    patch_connection = mock.patch.object(
        site_source, "get_connection_objects", return_value=custom_sql_data
    )
    patch_browse_path = mock.patch.object(
        site_source, "_get_project_browse_path_name", return_value="default"
    )

    if has_project_luid_patch:
        patch_project_luid = mock.patch.object(
            site_source,
            "_get_datasource_project_luid",
            return_value=project_luid,
        )
        with patch_connection, patch_browse_path, patch_project_luid:
            work_units = list(site_source.emit_custom_sql_datasources())
            dataset_properties = _extract_dataset_properties(work_units)
    else:
        with patch_connection, patch_browse_path:
            work_units = list(site_source.emit_custom_sql_datasources())
            dataset_properties = _extract_dataset_properties(work_units)

    if expected_name is None:
        assert dataset_properties is None
    else:
        assert dataset_properties is not None
        assert dataset_properties.name == expected_name


def test_table_lineage_without_columns():
    """Test that tables without column metadata still create table-level lineage"""
    config = TableauConfig(
        connect_uri="http://test",
        username="test",
        password="test",
        site="test",
    )

    ctx = PipelineContext(run_id="test")
    site = SiteItem(name="test-site", content_url="test")
    site._id = "test-site-id"
    report = TableauSourceReport()

    with mock.patch("datahub.ingestion.source.tableau.tableau.Server"):
        source = TableauSiteSource(
            config=config,
            ctx=ctx,
            site=site,
            report=report,
            server=mock.MagicMock(),
            platform="tableau",
        )

        with mock.patch.object(
            TableauUpstreamReference,
            "create",
            return_value=mock.MagicMock(
                make_dataset_urn=mock.MagicMock(
                    return_value="urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.table,PROD)"
                )
            ),
        ):
            tables = [
                {
                    c.ID: "table-123",
                    c.NAME: "my_table",
                    c.COLUMNS_CONNECTION: {"totalCount": 0},  # NO COLUMNS
                    c.DATABASE: {"name": "test_db"},
                    c.SCHEMA: "test_schema",
                    c.CONNECTION_TYPE: "snowflake",
                }
            ]

            upstream_tables, table_id_to_urn = source.get_upstream_tables(
                tables=tables,
                datasource_name="test_datasource",
                browse_path="/test",
                is_custom_sql=False,
            )

            # Tables without columns should still create table-level lineage
            assert len(upstream_tables) == 1
            assert len(table_id_to_urn) == 1
            assert report.num_upstream_table_processed_without_columns == 1


@time_machine.travel(FROZEN_TIME, tick=False)
def test_emit_datasource_does_not_add_embedded_ids_to_published_list():
    """Embedded datasource IDs should not leak into datasource_ids_being_used,
    which is the tracking list for published datasources.

    Regression test for CUS-8144: embedded datasource IDs were incorrectly
    added to the published datasource filter, causing phantom datasources
    to appear during published datasource ingestion.
    """
    config = TableauConfig.parse_obj(
        {
            **default_config,
            "extract_project_hierarchy": False,
        }
    )
    ctx = PipelineContext(run_id="test")
    site = SiteItem(name="test-site", content_url="test")
    site._id = "test-site-id"
    report = TableauSourceReport()

    with mock.patch("datahub.ingestion.source.tableau.tableau.Server"):
        source = TableauSiteSource(
            config=config,
            ctx=ctx,
            site=site,
            report=report,
            server=mock.MagicMock(),
            platform="tableau",
        )

        # Set up project registry and mappings so _get_datasource_project_luid succeeds
        source.tableau_project_registry = {
            "project-luid-1": TableauProject(
                id="project-luid-1",
                name="default",
                description="",
                parent_id=None,
                parent_name=None,
                path=["default"],
            )
        }
        source.workbook_project_map = {"workbook-luid-1": "project-luid-1"}
        source.datasource_project_map = {"published-ds-luid-1": "project-luid-1"}

        embedded_datasource = {
            c.ID: "embedded-ds-1",
            c.NAME: "Embedded DS 1",
            c.TYPE_NAME: c.EMBEDDED_DATA_SOURCE,
            c.LUID: "embedded-ds-luid-1",
            "hasExtracts": False,
            "extractLastRefreshTime": None,
            "extractLastIncrementalUpdateTime": None,
            "extractLastUpdateTime": None,
            "downstreamSheets": [],
            "fields": [],
            c.UPSTREAM_TABLES: [],
            c.UPSTREAM_DATA_SOURCES: [],
            c.WORKBOOK: {
                c.ID: "workbook-1",
                c.NAME: "Test Workbook",
                c.LUID: "workbook-luid-1",
                c.PROJECT_NAME: "default",
                c.PROJECT_LUID: "project-luid-1",
            },
        }

        published_datasource = {
            c.ID: "published-ds-1",
            c.NAME: "Published DS 1",
            c.TYPE_NAME: c.PUBLISHED_DATA_SOURCE,
            c.LUID: "published-ds-luid-1",
            c.PROJECT_NAME: "default",
            "hasExtracts": False,
            "extractLastRefreshTime": None,
            "extractLastIncrementalUpdateTime": None,
            "extractLastUpdateTime": None,
            "downstreamSheets": [],
            "fields": [],
            c.UPSTREAM_TABLES: [],
            c.UPSTREAM_DATA_SOURCES: [],
        }

        workbook = {
            c.ID: "workbook-1",
            c.NAME: "Test Workbook",
            c.LUID: "workbook-luid-1",
            c.PROJECT_NAME: "default",
            c.PROJECT_LUID: "project-luid-1",
            "owner": {c.ID: "owner-1", "username": "testuser"},
        }

        # Emit an embedded datasource
        list(
            source.emit_datasource(
                embedded_datasource, workbook=workbook, is_embedded_ds=True
            )
        )

        # The embedded datasource ID should NOT appear in the published list
        assert "embedded-ds-1" not in source.datasource_ids_being_used

        # Emit a published datasource
        list(
            source.emit_datasource(
                published_datasource, workbook=None, is_embedded_ds=False
            )
        )

        # The published datasource ID SHOULD appear in the published list
        assert "published-ds-1" in source.datasource_ids_being_used
        # And the embedded one should still not be there
        assert "embedded-ds-1" not in source.datasource_ids_being_used


def _make_site_source() -> TableauSiteSource:
    """Create a minimal TableauSiteSource for unit testing."""
    config = TableauConfig.model_validate(default_config)
    ctx = PipelineContext(run_id="test")
    with mock.patch("datahub.ingestion.source.tableau.tableau.Server"):
        return TableauSiteSource(
            config=config,
            ctx=ctx,
            platform="tableau",
            site=SiteIdContentUrl(site_id="s1", site_content_url="s1"),
            report=TableauSourceReport(),
            server=mock.MagicMock(spec=Server),
        )


class ProjectSpec(NamedTuple):
    id: str
    name: str
    parent_id: Optional[str]


def _tsc_project(spec: ProjectSpec) -> mock.MagicMock:
    """Minimal stand-in for a tableauserverclient ProjectItem, as returned by the
    projects Pager."""
    project = mock.MagicMock()
    project.id = spec.id
    project.name = spec.name
    project.parent_id = spec.parent_id
    project.description = None
    return project


def _collect_container_tree(
    source: TableauSiteSource,
    all_project_map: Dict[str, TableauProject],
) -> Dict[str, Dict[str, Optional[str]]]:
    """Run emit_project_containers and return {project_id: {name, parent}} so
    assertions read in terms of project ids rather than opaque container urns. A
    root project's parent resolves to the "site" sentinel (its container nests under
    the site container, which emit_site_container emits separately)."""
    urn_to_id = {
        source.gen_project_key(p.id).as_urn(): p.id for p in all_project_map.values()
    }
    urn_to_id[source.gen_site_key(source.site_id).as_urn()] = "site"

    tree: Dict[str, Dict[str, Optional[str]]] = {}
    for wu in source.emit_project_containers(all_project_map):
        project_id = urn_to_id[wu.get_urn()]
        entry = tree.setdefault(project_id, {"name": None, "parent": None})
        props = wu.get_aspect_of_type(ContainerPropertiesClass)
        if props is not None:
            entry["name"] = props.name
        parent = wu.get_aspect_of_type(ContainerClass)
        if parent is not None:
            entry["parent"] = urn_to_id.get(parent.container)
    return tree


class TestProjectContainerHierarchy:
    """End-to-end tests for project container emission under project_pattern
    filtering.

    Each test declares a source project hierarchy plus a filter, then runs the real
    pipeline -- fetch (via a mocked projects Pager) -> project_pattern filtering ->
    registry building -> container emission -- and asserts the resulting browse
    tree. The guiding invariant: every ingested project keeps its full folder path
    up to the site, regardless of which ancestors matched the filter.
    """

    @staticmethod
    def _run(
        projects: List[ProjectSpec],
        *,
        allow: Optional[List[str]] = None,
        deny: Optional[List[str]] = None,
        extract_project_hierarchy: bool = True,
    ) -> Tuple[TableauSiteSource, Dict[str, Dict[str, Optional[str]]]]:
        """Feed ``projects`` through the real projects Pager and run filtering +
        registry building + container emission.

        Returns (source, tree), where tree is {project_id: {name, parent}} and a
        root project's parent resolves to the "site" sentinel (its container nests
        under the site container). Sites are always added as containers here to
        mirror a typical deployment.
        """
        project_pattern: Dict[str, List[str]] = {}
        if allow is not None:
            project_pattern["allow"] = allow
        if deny is not None:
            project_pattern["deny"] = deny

        config_dict = {k: v for k, v in default_config.items() if k != "projects"}
        config_dict.update(
            project_pattern=project_pattern or {"allow": [".*"]},
            extract_project_hierarchy=extract_project_hierarchy,
            add_site_container=True,
        )
        config = TableauConfig.model_validate(config_dict)

        with mock.patch("datahub.ingestion.source.tableau.tableau.Server"):
            source = TableauSiteSource(
                config=config,
                ctx=PipelineContext(run_id="test"),
                platform="tableau",
                site=SiteIdContentUrl(site_id="s1", site_content_url="s1"),
                report=TableauSourceReport(),
                server=mock.MagicMock(),
            )

        project_items = [_tsc_project(p) for p in projects]

        def fake_pager(endpoint: Any, **kwargs: Any) -> Any:
            # Only the projects endpoint has data; the datasource/workbook registries
            # are irrelevant to project-container emission.
            if endpoint is source.server.projects:
                return iter(project_items)
            return iter([])

        with mock.patch(
            "datahub.ingestion.source.tableau.tableau.TSC.Pager", side_effect=fake_pager
        ):
            all_project_map = source._populate_projects_registry()

        return source, _collect_container_tree(source, all_project_map)

    def test_reconstructs_unmatched_ancestors(self) -> None:
        """A matched leaf keeps its full folder path when every ancestor is filtered
        out.

            Site
            └── Project_1        filtered out
                └── Project_2    filtered out
                    └── Project_3    filtered out
                        └── Project_4    MATCHES (leaf)
        """
        _, tree = self._run(
            projects=[
                ProjectSpec("p1", "Project_1", None),
                ProjectSpec("p2", "Project_2", "p1"),
                ProjectSpec("p3", "Project_3", "p2"),
                ProjectSpec("p4", "Project_4", "p3"),
            ],
            allow=["^Project_4$"],
        )

        assert set(tree) == {"p1", "p2", "p3", "p4"}
        assert tree["p4"]["parent"] == "p3"
        assert tree["p3"]["parent"] == "p2"
        assert tree["p2"]["parent"] == "p1"
        assert tree["p1"]["parent"] == "site"
        assert tree["p1"]["name"] == "Project_1"

    def test_shares_common_ancestors_once(self) -> None:
        """Two matched leaves keep their full paths and share ancestors exactly once,
        with each branch nested correctly.

            Site
            └── Project_1                shared ancestor
                └── Project_2            shared ancestor
                    ├── Project_3
                    │   └── Project_4        MATCHES (leaf)
                    └── Project_5
                        └── Project_6        MATCHES (leaf)
        """
        _, tree = self._run(
            projects=[
                ProjectSpec("p1", "Project_1", None),
                ProjectSpec("p2", "Project_2", "p1"),
                ProjectSpec("p3", "Project_3", "p2"),
                ProjectSpec("p4", "Project_4", "p3"),
                ProjectSpec("p5", "Project_5", "p2"),
                ProjectSpec("p6", "Project_6", "p5"),
            ],
            allow=["^Project_4$", "^Project_6$"],
        )

        # Shared ancestors (p1, p2) appear once each -- keying by project id
        # collapses duplicates, so an equal set means no ancestor was emitted twice
        # under a different identity.
        assert set(tree) == {"p1", "p2", "p3", "p4", "p5", "p6"}
        assert tree["p4"]["parent"] == "p3"
        assert tree["p3"]["parent"] == "p2"
        assert tree["p6"]["parent"] == "p5"
        assert tree["p5"]["parent"] == "p2"
        assert tree["p2"]["parent"] == "p1"
        assert tree["p1"]["parent"] == "site"

    def test_middle_project_with_parent_and_child(self) -> None:
        """A matched middle project (unmatched parent above, child below) links up to
        its reconstructed parent and down to its child.

            Site
            └── Project_1        filtered out    -> path-only ancestor
                └── Project_2    MATCHES         -> content container
                    └── Project_3  child of match -> content container
        """
        source, tree = self._run(
            projects=[
                ProjectSpec("p1", "Project_1", None),
                ProjectSpec("p2", "Project_2", "p1"),
                ProjectSpec("p3", "Project_3", "p2"),
            ],
            allow=["^Project_2$"],
        )

        # Project_2 matched directly; Project_3 was pulled in as its child; the
        # unmatched ancestor Project_1 stayed out of the content registry.
        assert set(source.tableau_project_registry) == {"p2", "p3"}
        assert set(tree) == {"p1", "p2", "p3"}
        assert tree["p3"]["parent"] == "p2"
        assert tree["p2"]["parent"] == "p1"
        assert tree["p1"]["parent"] == "site"

    def test_denied_ancestor_still_emitted_as_path_container(self) -> None:
        """An explicitly denied ancestor carries no content but is still emitted as a
        path container so the matched descendant's path stays unbroken."""
        source, tree = self._run(
            projects=[
                ProjectSpec("p1", "Project_1", None),
                ProjectSpec("p2", "Project_2", "p1"),
                ProjectSpec("p3", "Project_3", "p2"),
            ],
            allow=["^Project_3$"],
            deny=["^Project_1$"],
        )

        # Denied ancestor is not content-bearing...
        assert "p1" not in source.tableau_project_registry
        # ...but still appears in the browse tree, correctly nested.
        assert set(tree) == {"p1", "p2", "p3"}
        assert tree["p3"]["parent"] == "p2"
        assert tree["p2"]["parent"] == "p1"
        assert tree["p1"]["parent"] == "site"

    def test_denied_child_excluded_while_sibling_emits(self) -> None:
        """Under a matched parent, extract_project_hierarchy pulls in children --
        except ones matching a deny rule. A denied child that is not an ancestor of
        anything ingested emits no container at all, while its sibling emits
        normally.

            Site
            └── Project_1    MATCHES (parent)
                ├── Project_2    child, re-admitted -> emits
                └── Project_3    DENIED             -> not emitted
        """
        source, tree = self._run(
            projects=[
                ProjectSpec("p1", "Project_1", None),
                ProjectSpec("p2", "Project_2", "p1"),
                ProjectSpec("p3", "Project_3", "p1"),
            ],
            allow=["^Project_1$"],
            deny=["^Project_3$"],
        )

        # The denied child is excluded entirely; the parent and its other child emit.
        assert set(source.tableau_project_registry) == {"p1", "p2"}
        assert set(tree) == {"p1", "p2"}
        assert "p3" not in tree
        assert tree["p2"]["parent"] == "p1"
        assert tree["p1"]["parent"] == "site"

    def test_path_reconstructed_when_hierarchy_flag_disabled(self) -> None:
        """Path correctness does not depend on extract_project_hierarchy: with the
        flag off, no descendants are re-admitted, yet the matched project's full
        ancestor path is still reconstructed."""
        source, tree = self._run(
            projects=[
                ProjectSpec("p1", "Project_1", None),
                ProjectSpec("p2", "Project_2", "p1"),
                ProjectSpec("p3", "Project_3", "p2"),
            ],
            allow=["^Project_3$"],
            extract_project_hierarchy=False,
        )

        # Only the matched leaf is content-bearing (no downward re-admission)...
        assert set(source.tableau_project_registry) == {"p3"}
        # ...yet the full ancestor path is still reconstructed.
        assert set(tree) == {"p1", "p2", "p3"}
        assert tree["p3"]["parent"] == "p2"
        assert tree["p2"]["parent"] == "p1"
        assert tree["p1"]["parent"] == "site"

    def test_ancestor_absent_from_map_is_reparented_to_site(self) -> None:
        """An ancestor entirely absent from the fetched project map (e.g. the API
        omits it because of insufficient permissions) has no entry to recurse into.
        _get_all_project nulls the dangling parent_id, so the orphaned project is
        reparented under the site instead of dangling -- and container emission must
        not fail looking up the missing ancestor.

            Site
            (Project_1)          NOT fetched -- absent from the map
                └── Project_2    orphaned -> reparented under site
                    └── Project_3    MATCHES (leaf)
        """
        source, tree = self._run(
            # Project_1, the parent of Project_2, is intentionally not fetched.
            projects=[
                ProjectSpec("p2", "Project_2", "p1"),
                ProjectSpec("p3", "Project_3", "p2"),
            ],
            allow=["^Project_3$"],
        )

        # The missing ancestor never emits a container...
        assert "p1" not in tree
        # ...and its orphaned child is reparented directly under the site.
        assert set(tree) == {"p2", "p3"}
        assert tree["p3"]["parent"] == "p2"
        assert tree["p2"]["parent"] == "site"
        # The dangling parent reference is surfaced to operators, not swallowed.
        assert "Incomplete project hierarchy" in source.report.as_string()

    def test_dangling_parent_id_raises_actionable_error(self) -> None:
        """emit_project_containers relies on _get_all_project having nulled out any
        parent_id absent from the project map. If a future regression breaks that
        normalization, the lookup should fail with an actionable error naming the
        offending project -- not a bare KeyError."""
        source = _make_site_source()
        # A project pointing at a parent id that is not in the map -- the exact state
        # _get_all_project is supposed to prevent.
        orphan = TableauProject(
            id="p2",
            name="Project_2",
            description=None,
            parent_id="p1",
            parent_name=None,
            path=["Project_2"],
        )
        source.tableau_project_registry = {"p2": orphan}

        with pytest.raises(ValueError, match="p1"):
            list(source.emit_project_containers({"p2": orphan}))


class TestNullApiResponseHandling:
    """Tableau's Metadata API occasionally returns None entries inside list fields
    (upstreamTables, fields, upstreamColumns).  Each affected function should
    silently skip non-dict entries and continue processing the rest of the list.
    """

    def setup_method(self) -> None:
        self.source = _make_site_source()

    def test_get_upstream_tables_skips_non_dict_entries(self) -> None:
        tables: Sequence[Optional[dict]] = [None, None, {c.ID: "t1", c.NAME: None}]

        upstream_tables, table_id_to_urn = self.source.get_upstream_tables(
            tables=tables,
            datasource_name="test_datasource",
            browse_path=None,
            is_custom_sql=False,
        )

        assert upstream_tables == []
        assert table_id_to_urn == {}

    def test_get_upstream_tables_processes_valid_entries_after_none(self) -> None:
        valid_table = {
            c.ID: "t-valid",
            c.NAME: "my_table",
            c.SCHEMA: "my_schema",
            c.FULL_NAME: "db.my_schema.my_table",
            c.CONNECTION_TYPE: "snowflake",
            c.DATABASE: {"name": "my_db", "connectionType": "snowflake"},
            c.COLUMNS_CONNECTION: {"totalCount": 1},
        }

        with mock.patch.object(
            TableauUpstreamReference,
            "create",
            return_value=mock.MagicMock(
                make_dataset_urn=mock.MagicMock(
                    return_value="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.my_schema.my_table,PROD)"
                )
            ),
        ):
            tables_with_none: Sequence[Optional[dict]] = [None, valid_table, None]
            upstream_tables, table_id_to_urn = self.source.get_upstream_tables(
                tables=tables_with_none,
                datasource_name="ds",
                browse_path=None,
                is_custom_sql=False,
            )

        assert len(upstream_tables) == 1
        assert len(table_id_to_urn) == 1

    def test_get_upstream_csql_tables_skips_non_dict_fields(self) -> None:
        fields: Sequence[Optional[dict]] = [None, None, {c.UPSTREAM_COLUMNS: None}]

        upstream_csql, csql_id_to_urn = self.source.get_upstream_csql_tables(fields)

        assert upstream_csql == []
        assert csql_id_to_urn == {}

    def test_get_upstream_csql_tables_processes_valid_after_none(self) -> None:
        valid_field = {
            c.UPSTREAM_COLUMNS: [
                {
                    c.TABLE: {c.TYPE_NAME: c.CUSTOM_SQL_TABLE, c.ID: "csql-1"},
                    c.NAME: "col1",
                }
            ]
        }

        fields_with_none: Sequence[Optional[dict]] = [None, valid_field, None]
        upstream_csql, csql_id_to_urn = self.source.get_upstream_csql_tables(
            fields_with_none
        )

        assert len(upstream_csql) == 1
        assert "csql-1" in csql_id_to_urn

    def test_get_upstream_columns_skips_non_dict_fields(self) -> None:
        datasource = {c.ID: "ds-1", c.FIELDS: [None, "not_a_dict"]}

        result = self.source.get_upstream_columns_of_fields_in_datasource(
            datasource=datasource,
            datasource_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,ds-1,PROD)",
            table_id_to_urn={
                "t1": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.t,PROD)"
            },
        )

        assert result == []

    def test_get_upstream_columns_processes_valid_after_none(self) -> None:
        datasource = {
            c.ID: "ds-2",
            c.FIELDS: [
                None,
                {
                    c.NAME: "my_field",
                    c.UPSTREAM_COLUMNS: [{c.NAME: "col1", c.TABLE: {c.ID: "t1"}}],
                },
            ],
        }

        result = self.source.get_upstream_columns_of_fields_in_datasource(
            datasource=datasource,
            datasource_urn="urn:li:dataset:(urn:li:dataPlatform:tableau,ds-2,PROD)",
            table_id_to_urn={
                "t1": "urn:li:dataset:(urn:li:dataPlatform:postgres,db.s.t,PROD)"
            },
        )

        assert len(result) == 1
        assert result[0].downstreams == [
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:tableau,ds-2,PROD),my_field)"
        ]

    def test_get_schema_metadata_skips_non_dict_fields(self) -> None:
        bad_fields: Sequence[Optional[dict]] = [None, None]
        result = self.source._get_schema_metadata_for_datasource(bad_fields)

        assert result is None

    def test_get_schema_metadata_processes_valid_after_none(self) -> None:
        valid_field = {
            c.ID: "f1",
            c.NAME: "my_column",
            "__typename": "DimensionField",
            c.UPSTREAM_COLUMNS: [],
            "isHidden": False,
            "folderName": None,
            "description": None,
            "dataType": None,
            "defaultFormat": None,
            "aggregation": None,
            "role": None,
        }

        fields_with_none: Sequence[Optional[dict]] = [None, valid_field]
        result = self.source._get_schema_metadata_for_datasource(fields_with_none)

        assert result is not None
        assert len(result.fields) == 1
        assert result.fields[0].fieldPath == "my_column"
