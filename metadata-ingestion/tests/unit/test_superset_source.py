from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.superset import SupersetConfig, SupersetSource


def test_default_values():
    config = SupersetConfig.parse_obj({})

    assert config.connect_uri == "http://localhost:8088"
    assert config.display_uri == "http://localhost:8088"
    assert config.provider == "db"
    assert config.env == "PROD"
    assert config.username is None
    assert config.password is None


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
        ctx=PipelineContext(run_id="superset-source-test"), config=SupersetConfig()
    )
    assert source.owner_info == {
        1: "test_user1@example.com",
        2: "test_user2@example.com",
    }
