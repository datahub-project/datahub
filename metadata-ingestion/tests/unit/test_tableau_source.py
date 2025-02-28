from typing import Any, Dict, List

import pytest

import datahub.ingestion.source.tableau.tableau_constant as c
from datahub.ingestion.source.tableau.tableau import (
    DEFAULT_PAGE_SIZE,
    TableauPageSizeConfig,
    TableauSiteSource,
)
from datahub.ingestion.source.tableau.tableau_common import (
    TableauUpstreamReference,
    get_filter_pages,
    make_filter,
    optimize_query_filter,
    tableau_field_to_schema_field,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField


def test_tablea_source_handles_none_nativedatatype():
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
