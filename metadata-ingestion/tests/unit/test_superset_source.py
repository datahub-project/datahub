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
