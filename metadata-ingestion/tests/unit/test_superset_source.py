from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.superset import SupersetConfig, SupersetSource
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result


def test_default_values():
    config = SupersetConfig.parse_obj({})

    assert config.connect_uri == "http://localhost:8088"
    assert config.display_uri == "http://localhost:8088"
    assert config.provider == "db"
    assert config.env == "PROD"
    assert config.username is None
    assert config.password is None
    assert config.dataset_pattern == AllowDenyPattern.allow_all()
    assert config.chart_pattern == AllowDenyPattern.allow_all()
    assert config.dashboard_pattern == AllowDenyPattern.allow_all()
    assert config.database_pattern == AllowDenyPattern.allow_all()


def test_set_display_uri():
    display_uri = "some_host:1234"

    config = SupersetConfig.parse_obj({"display_uri": display_uri})

    assert config.connect_uri == "http://localhost:8088"
    assert config.display_uri == display_uri


def test_superset_login(requests_mock):
    login_url = "http://localhost:8088/api/v1/security/login"
    requests_mock.post(login_url, json={"access_token": "dummy_token"}, status_code=200)

    dashboard_url = "http://localhost:8088/api/v1/dashboard/"
    requests_mock.get(dashboard_url, json={}, status_code=200)

    for entity in ["dataset", "dashboard", "chart"]:
        requests_mock.get(
            f"http://localhost:8088/api/v1/{entity}/related/owners",
            json={},
            status_code=200,
        )

    source = SupersetSource(
        ctx=PipelineContext(run_id="superset-source-test"), config=SupersetConfig()
    )
    assert source.platform == "superset"


def test_superset_build_owners_info(requests_mock):
    login_url = "http://localhost:8088/api/v1/security/login"
    requests_mock.post(login_url, json={"access_token": "dummy_token"}, status_code=200)

    dashboard_url = "http://localhost:8088/api/v1/dashboard/"
    requests_mock.get(dashboard_url, json={}, status_code=200)

    for entity in ["dataset", "dashboard", "chart"]:
        requests_mock.get(
            f"http://localhost:8088/api/v1/{entity}/related/owners",
            json={
                "count": 2,
                "result": [
                    {
                        "extra": {"active": "false", "email": "test_user1@example.com"},
                        "text": "Test User1",
                        "value": 1,
                    },
                    {
                        "extra": {"active": "false", "email": "test_user2@example.com"},
                        "text": "Test User2",
                        "value": 2,
                    },
                ],
            },
            status_code=200,
        )

    source = SupersetSource(
        ctx=PipelineContext(run_id="superset-source-owner-info-test"),
        config=SupersetConfig(),
    )
    assert source.owner_info == {
        1: "test_user1@example.com",
        2: "test_user2@example.com",
    }


def test_column_level_lineage(requests_mock):
    login_url = "http://localhost:8088/api/v1/security/login"
    requests_mock.post(login_url, json={"access_token": "dummy_token"}, status_code=200)

    dashboard_url = "http://localhost:8088/api/v1/dashboard/"
    requests_mock.get(dashboard_url, json={}, status_code=200)

    for entity in ["dataset", "dashboard", "chart"]:
        requests_mock.get(
            f"http://localhost:8088/api/v1/{entity}/related/owners",
            json={
                "count": 2,
                "result": [
                    {
                        "extra": {"active": "false", "email": "test_user1@example.com"},
                        "text": "Test User1",
                        "value": 1,
                    },
                    {
                        "extra": {"active": "false", "email": "test_user2@example.com"},
                        "text": "Test User2",
                        "value": 2,
                    },
                ],
            },
            status_code=200,
        )

    sql = """
    SELECT tt2.id, tt2.name, tt2.description, db.database_name 
    FROM test_table2 tt2
    JOIN databases db ON tt2.database_id = db.id
    WHERE tt2.kind = 'virtual'
    ORDER BY tt2.id DESC;
    """

    source = SupersetSource(
        ctx=PipelineContext(run_id="superset-source-owner-info-test"),
        config=SupersetConfig(),
    )

    parsed_query_object = create_lineage_sql_parsed_result(
        query=sql,
        default_db="test-db",
        platform="postgres",
        platform_instance=None,
        env="TEST",
    )

    virtual_lineage = source.generate_virtual_dataset_lineage(
        parsed_query_object=parsed_query_object,
        datasource_urn="urn:li:dataset:(urn:li:dataPlatform:superset,test_database_name.test_schema_name.test_table_name,PROD)",
    )

    assert any(
        "urn:li:dataset:(urn:li:dataPlatform:postgres,test-db.databases,TEST)"
        in virtual_lineage.upstreams[i].get("dataset")
        for i in range(len(virtual_lineage.upstreams))
    )


def test_construct_chart_cll_aggregate_mode(requests_mock):
    login_url = "http://localhost:8088/api/v1/security/login"
    requests_mock.post(login_url, json={"access_token": "dummy_token"}, status_code=200)
    requests_mock.get(
        "http://localhost:8088/api/v1/dashboard/", json={}, status_code=200
    )

    for entity in ["dataset", "dashboard", "chart"]:
        requests_mock.get(
            f"http://localhost:8088/api/v1/{entity}/related/owners",
            json={},
            status_code=200,
        )

    requests_mock.get(
        "http://localhost:8088/api/v1/dataset/1",
        json={
            "result": {
                "columns": [
                    {
                        "column_name": "time",
                        "type": "TIMESTAMP",
                        "description": "Event time",
                    },
                    {
                        "column_name": "network",
                        "type": "STRING",
                        "description": "Network used",
                    },
                    {
                        "column_name": "amount_usd",
                        "type": "NUMERIC",
                        "description": "USD amount",
                    },
                    {
                        "column_name": "creation_time",
                        "type": "TIMESTAMP",
                        "description": "Creation time",
                    },
                ],
                "metrics": [
                    {
                        "metric_name": "sum_usd",
                        "expression": "SUM(amount_usd)",
                        "metric_type": "SUM",
                        "description": "Total USD volume",
                    },
                    {
                        "metric_name": "total_requests",
                        "expression": "COUNT(DISTINCT request_id)",
                        "metric_type": "COUNT_DISTINCT",
                    },
                ],
                "schema": "test_schema",
                "table_name": "test_table",
            }
        },
        status_code=200,
    )

    source = SupersetSource(ctx=PipelineContext(run_id="test"), config=SupersetConfig())

    # Realistic chart data based on provided examples
    chart_data = {
        "form_data": {
            "query_mode": "aggregate",
            "viz_type": "ag-grid-table",
            "groupby": [
                "network",
                {
                    "sqlExpression": "CASE WHEN amount_usd > 1000 THEN 'Large' ELSE 'Small' END",
                    "label": "transaction_size",
                },
            ],
            "x_axis": "creation_time",
            "metrics": [
                "sum_usd",
                {
                    "aggregate": "SUM",
                    "column": {"column_name": "amount_usd", "type": "NUMERIC"},
                    "expressionType": "SIMPLE",
                    "label": "Total Amount",
                },
                {
                    "expressionType": "SQL",
                    "sqlExpression": "AVG(CASE WHEN network = 'Test Network' THEN amount_usd * 1.5 ELSE amount_usd END)",
                    "label": "Weighted Average",
                },
            ],
            "adhoc_filters": [
                {
                    "clause": "WHERE",
                    "comparator": "Test Network",
                    "expressionType": "SIMPLE",
                    "operator": "==",
                    "subject": "network",
                }
            ],
        },
        "datasource_type": "table",
        "datasource_id": 1,
    }

    datasource_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:superset,test_schema.test_table,PROD)"
    )
    input_fields = source.construct_chart_cll(chart_data, datasource_urn, 1)

    assert len(input_fields) == 4

    field_types = {}
    for f in input_fields:
        if f.schemaField:
            field_types[f.schemaField.fieldPath] = f.schemaField.nativeDataType

    assert field_types["network"] == "STRING"
    assert field_types["sum_usd"] == "SUM"
    assert field_types["amount_usd"] == "NUMERIC"
    assert field_types["creation_time"] == "TIMESTAMP"
