import pytest
from datahub.emitter.mce_builder import make_dataset_urn

from tests.utils import get_frontend_url, get_sleep_info, wait_for_healthcheck_util

restli_default_headers = {
    "X-RestLi-Protocol-Version": "2.0.0",
}
sleep_sec, sleep_times = get_sleep_info()

TEST_DATASET_URN = make_dataset_urn(platform="postgres", name="foo")


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_create_update_delete_dataset_assertion(frontend_session):
    pass


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_create_update_delete_freshness_assertion(frontend_session):
    json = {
        "query": """mutation createFreshnessAssertion($input: CreateFreshnessAssertionInput!) {\n
            createFreshnessAssertion(input: $input) {\n
                urn\n
            }\n
        }""",
        "variables": {
            "input": {
                "entityUrn": TEST_DATASET_URN,
                "type": "DATASET_CHANGE",
                "schedule": {
                    "type": "CRON",
                    "cron": {"cron": "* * * * *", "timezone": "America / Los Angeles"},
                },
                "actions": {
                    "onSuccess": [{"type": "RESOLVE_INCIDENT"}],
                    "onFailure": [{"type": "RAISE_INCIDENT"}],
                },
            }
        },
    }

    response = frontend_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createFreshnessAssertion"]

    assertion_urn = res_data["data"]["createFreshnessAssertion"]["urn"]

    # Update the assertion
    json = {
        "query": """mutation updateFreshnessAssertion($urn: String!, $input: UpdateFreshnessAssertionInput!) {\n
            updateFreshnessAssertion(urn: $urn, input: $input) {\n
                urn\n
            }\n
        }""",
        "variables": {
            "urn": assertion_urn,
            "input": {
                "schedule": {
                    "type": "FIXED_INTERVAL",
                    "fixedInterval": {"unit": "DAY", "multiple": 2},
                },
                "actions": {
                    "onSuccess": [{"type": "RESOLVE_INCIDENT"}],
                    "onFailure": [{"type": "RAISE_INCIDENT"}],
                },
            },
        },
    }

    response = frontend_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["updateFreshnessAssertion"]

    assertion_urn = res_data["data"]["updateFreshnessAssertion"]["urn"]

    # Delete the assertion
    json = {
        "query": """mutation deleteAssertion($urn: String!) {\n
            deleteAssertion(urn: $urn)
        },""",
        "variables": {"urn": assertion_urn},
    }

    response = frontend_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["deleteAssertion"] is True
