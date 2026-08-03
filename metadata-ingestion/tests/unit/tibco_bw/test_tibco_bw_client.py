import pytest
from requests_mock import Mocker

from datahub.ingestion.source.tibco_bw.client import (
    BwAgentClient,
    TciClient,
    _app_properties,
    _as_object_list,
    create_client,
)
from datahub.ingestion.source.tibco_bw.config import TibcoBwSourceConfig
from datahub.ingestion.source.tibco_bw.constants import (
    AUTH_BEARER_PREFIX,
    HEADER_AUTHORIZATION,
)


def _on_prem_config() -> TibcoBwSourceConfig:
    return TibcoBwSourceConfig.model_validate(
        {
            "deployment": "on_prem",
            "base_url": "http://bw:8079",
            "username": "user",
            "password": "secret",
        }
    )


def _cloud_config() -> TibcoBwSourceConfig:
    return TibcoBwSourceConfig.model_validate({"deployment": "cloud", "token": "abc"})


def test_create_client_selects_implementation() -> None:
    assert isinstance(create_client(_on_prem_config()), BwAgentClient)
    assert isinstance(create_client(_cloud_config()), TciClient)


def test_session_uses_bearer_token_for_cloud() -> None:
    client = TciClient(_cloud_config())
    assert client.session.headers[HEADER_AUTHORIZATION] == f"{AUTH_BEARER_PREFIX}abc"
    assert client.session.auth is None


def test_session_uses_basic_auth_for_on_prem() -> None:
    client = BwAgentClient(_on_prem_config())
    assert client.session.auth == ("user", "secret")
    assert HEADER_AUTHORIZATION not in client.session.headers


def test_ca_certificate_path_sets_verify() -> None:
    config = _on_prem_config()
    config.ca_certificate_path = "/etc/ssl/ca.pem"
    assert BwAgentClient(config).session.verify == "/etc/ssl/ca.pem"


@pytest.mark.parametrize(
    "payload,expected",
    [
        ([{"a": 1}], [{"a": 1}]),
        ({"items": [{"a": 1}], "count": 1}, [{"a": 1}]),
        ("unexpected", []),
        ({"a": 1}, []),
    ],
)
def test_as_object_list(payload: object, expected: list) -> None:
    assert _as_object_list(payload) == expected


def test_app_properties_omits_missing() -> None:
    assert _app_properties(None, None, None) == {}
    assert _app_properties("1.0", "Running", None) == {
        "version": "1.0",
        "state": "Running",
    }


def test_bwagent_fetch_scopes(requests_mock: Mocker) -> None:
    base = "http://bw:8079"
    requests_mock.get(f"{base}/bw/v1/domains", json=[{"name": "D1"}])
    requests_mock.get(
        f"{base}/bw/v1/domains/D1/appspaces",
        json=[{"name": "AS1", "description": "primary space"}],
    )
    requests_mock.get(
        f"{base}/bw/v1/domains/D1/appspaces/AS1/appnodes",
        json=[{"name": "node1", "status": "Running"}],
    )
    requests_mock.get(
        f"{base}/bw/v1/domains/D1/appspaces/AS1/applications",
        json=[{"name": "orders", "version": "1.0", "state": "Running"}],
    )

    scopes = BwAgentClient(_on_prem_config()).fetch_scopes()

    assert len(scopes) == 1
    scope = scopes[0]
    assert scope.id == "D1/AS1"
    assert scope.name == "AS1"
    assert scope.properties["domain"] == "D1"
    assert scope.properties["appnode_count"] == "1"
    assert scope.properties["appnodes"] == "node1 (Running)"
    assert [app.name for app in scope.applications] == ["orders"]
    assert scope.applications[0].properties["version"] == "1.0"


def test_tci_fetch_scopes(requests_mock: Mocker) -> None:
    base = "https://api.cloud.tibco.com"
    requests_mock.get(
        f"{base}/v1/userinfo",
        json={
            "subscriptions": [
                {"subscriptionId": "sub1", "name": "Prod", "orgDisplayName": "Acme"}
            ]
        },
    )
    requests_mock.get(
        f"{base}/v1/subscriptions/sub1/apps",
        json=[{"name": "sync", "type": "flogo", "status": "STARTED"}],
    )

    scopes = TciClient(_cloud_config()).fetch_scopes()

    assert len(scopes) == 1
    scope = scopes[0]
    assert scope.id == "sub1"
    assert scope.name == "Prod"
    assert scope.properties["organization"] == "Acme"
    assert scope.applications[0].properties["app_type"] == "flogo"
