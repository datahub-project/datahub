import time

import pytest
import requests
import urllib
from datahub.cli.docker import check_local_docker_containers
from datahub.ingestion.run.pipeline import Pipeline

GMS_ENDPOINT = "http://localhost:8080"
FRONTEND_ENDPOINT = "http://localhost:9002"
KAFKA_BROKER = "localhost:9092"

bootstrap_sample_data = "../metadata-ingestion/examples/mce_files/bootstrap_mce.json"
usage_sample_data = (
    "../metadata-ingestion/tests/integration/bigquery-usage/bigquery_usages_golden.json"
)
bq_sample_data = "./sample_bq_data.json"
restli_default_headers = {
    "X-RestLi-Protocol-Version": "2.0.0",
}
kafka_post_ingestion_wait_sec = 60


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    # Simply assert that everything is healthy, but don't wait.
    assert not check_local_docker_containers()
    yield


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


def ingest_file(filename: str):
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "file",
                "config": {"filename": filename},
            },
            "sink": {
                "type": "datahub-rest",
                "config": {"server": GMS_ENDPOINT},
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_ingestion_via_rest(wait_for_healthchecks):
    ingest_file(bootstrap_sample_data)


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_ingestion_usage_via_rest(wait_for_healthchecks):
    ingest_file(usage_sample_data)


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_ingestion_via_kafka(wait_for_healthchecks):
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "file",
                "config": {"filename": bq_sample_data},
            },
            "sink": {
                "type": "datahub-kafka",
                "config": {
                    "connection": {
                        "bootstrap": KAFKA_BROKER,
                    }
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()

    # Since Kafka emission is asynchronous, we must wait a little bit so that
    # the changes are actually processed.
    time.sleep(kafka_post_ingestion_wait_sec)


@pytest.mark.dependency(
    depends=[
        "test_ingestion_via_rest",
        "test_ingestion_via_kafka",
        "test_ingestion_usage_via_rest",
    ]
)
def test_run_ingestion(wait_for_healthchecks):
    # Dummy test so that future ones can just depend on this one.
    pass


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_gms_get_user():
    username = "jdoe"
    urn = f"urn:li:corpuser:{username}"
    response = requests.get(
        f"{GMS_ENDPOINT}/entities/{urllib.parse.quote(urn)}",
        headers={
            **restli_default_headers,
        },
    )
    response.raise_for_status()
    data = response.json()

    assert data["value"]
    assert data["value"]["com.linkedin.metadata.snapshot.CorpUserSnapshot"]
    assert data["value"]["com.linkedin.metadata.snapshot.CorpUserSnapshot"]["urn"] == urn


@pytest.mark.parametrize(
    "platform,dataset_name,env",
    [
        (
            # This one tests the bootstrap sample data.
            "urn:li:dataPlatform:kafka",
            "SampleKafkaDataset",
            "PROD",
        ),
        (
            # This one tests BigQuery ingestion.
            "urn:li:dataPlatform:bigquery",
            "bigquery-public-data.covid19_geotab_mobility_impact.us_border_wait_times",
            "PROD",
        ),
    ],
)
@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_gms_get_dataset(platform, dataset_name, env):
    platform = "urn:li:dataPlatform:bigquery"
    dataset_name = (
        "bigquery-public-data.covid19_geotab_mobility_impact.us_border_wait_times"
    )
    env = "PROD"
    urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    response = requests.get(
        f"{GMS_ENDPOINT}/entities/{urllib.parse.quote(urn)}",
        headers={
            **restli_default_headers,
            "X-RestLi-Method": "get",
        },
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data["value"]
    assert res_data["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]
    assert res_data["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]["urn"] == urn


@pytest.mark.parametrize(
    "query,min_expected_results",
    [
        ("covid", 1),
        ("sample", 3),
    ],
)
@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_gms_search_dataset(query, min_expected_results):


    json = {
            "input": f"{query}",
            "entity": "dataset",
            "start": 0,
            "count": 10
    }
    print(json)
    response = requests.post(
        f"{GMS_ENDPOINT}/entities?action=search",
        headers=restli_default_headers,
        json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data["value"]
    assert res_data["value"]["numEntities"] >= min_expected_results
    assert len(res_data["value"]["entities"]) >= min_expected_results


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_gms_usage_fetch():
    response = requests.post(
        f"{GMS_ENDPOINT}/usageStats?action=queryRange",
        headers=restli_default_headers,
        json={
            "resource": "urn:li:dataset:(urn:li:dataPlatform:bigquery,harshal-playground-306419.test_schema.excess_deaths_derived,PROD)",
            "duration": "DAY",
            "rangeFromEnd": "ALL",
        },
    )
    response.raise_for_status()

    data = response.json()["value"]

    assert len(data["buckets"]) == 3
    assert data["buckets"][0]["metrics"]["topSqlQueries"]

    fields = data["aggregations"].pop("fields")
    assert len(fields) == 12
    assert fields[0]["count"] == 7

    users = data["aggregations"].pop("users")
    assert len(users) == 1
    assert users[0]["count"] == 7

    assert data["aggregations"] == {
        # "fields" and "users" already popped out
        "totalSqlQueries": 7,
        "uniqueUserCount": 1,
    }


@pytest.fixture(scope="session")
def frontend_session(wait_for_healthchecks):
    session = requests.Session()

    headers = {
        "Content-Type": "application/json",
    }
    data = '{"username":"datahub", "password":"datahub"}'
    response = session.post(
        f"{FRONTEND_ENDPOINT}/logIn", headers=headers, data=data
    )
    response.raise_for_status()

    yield session


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_frontend_auth(frontend_session):
    pass


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_frontend_browse_datasets(frontend_session):

    json = {
        "query": """query browse($input: BrowseInput!) {\n
                        browse(input: $input) {\n
                            start\n
                            count\n
                            total\n
                            groups {
                                name
                            }
                            entities {\n
                                ... on Dataset {\n
                                    urn\n
                                    name\n
                                }\n
                            }\n
                        }\n
                    }""",
        "variables": {
            "input": {
                "type": "DATASET",
                "path": ["prod"]
            }
        }
    }

    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json
    )

    response.raise_for_status()
    res_data = response.json()
    assert res_data
    assert res_data["data"]
    assert res_data["data"]["browse"]
    assert len(res_data["data"]["browse"]["entities"]) == 0
    assert len(res_data["data"]["browse"]["groups"]) > 0


@pytest.mark.parametrize(
    "query,min_expected_results",
    [
        ("covid", 1),
        ("sample", 3),
    ],
)
@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_frontend_search_datasets(frontend_session, query, min_expected_results):

    json = {
        "query": """query search($input: SearchInput!) {\n
            search(input: $input) {\n
                start\n
                count\n
                total\n 
                searchResults {\n
                    entity {\n
                        ... on Dataset {\n
                            urn\n
                            name\n
                        }\n
                    }\n
                }\n
            }\n
        }""",
        "variables": {
            "input": {
                "type": "DATASET",
                "query": f"{query}",
                "start": 0,
                "count": 10
            }
        }
    }

    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["search"]
    assert res_data["data"]["search"]["total"] >= min_expected_results
    assert len(res_data["data"]["search"]["searchResults"]) >= min_expected_results


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_frontend_user_info(frontend_session):

    urn = f"urn:li:corpuser:datahub"
    json = {
        "query": """query corpUser($urn: String!) {\n
            corpUser(urn: $urn) {\n
                urn\n
                username\n
                editableInfo {\n
                    pictureLink\n
                }\n
                info {\n
                    firstName\n
                    fullName\n
                    title\n
                    email\n
                }\n
            }\n
        }""",
        "variables": {
            "urn": urn
        }
    }
    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data 
    assert res_data["data"]
    assert res_data["data"]["corpUser"]
    assert res_data["data"]["corpUser"]["urn"] == urn


@pytest.mark.parametrize(
    "platform,dataset_name,env",
    [
        (
            # This one tests the bootstrap sample data.
            "urn:li:dataPlatform:kafka",
            "SampleKafkaDataset",
            "PROD",
        ),
        (
            # This one tests BigQuery ingestion.
            "urn:li:dataPlatform:bigquery",
            "bigquery-public-data.covid19_geotab_mobility_impact.us_border_wait_times",
            "PROD",
        ),
    ],
)
@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_frontend_datasets(frontend_session, platform, dataset_name, env):
    urn = f"urn:li:dataset:({platform},{dataset_name},{env})"
    json = {
        "query": """query getDataset($urn: String!) {\n
            dataset(urn: $urn) {\n
                urn\n
                name\n
                description\n
                platform {\n
                    urn\n
                }\n
                schemaMetadata {\n
                    name\n
                    version\n
                    createdAt\n
                }\n
            }\n
        }""",
        "variables": {
            "urn": urn
        }
    }
    # Basic dataset info.
    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["urn"] == urn
    assert res_data["data"]["dataset"]["name"] == dataset_name
    assert res_data["data"]["dataset"]["platform"]["urn"] == platform

@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_ingest_with_system_metadata():
    response = requests.post(
        f"{GMS_ENDPOINT}/entities?action=ingest",
        headers=restli_default_headers,
        json={
          'entity':
            {
              'value':
                {'com.linkedin.metadata.snapshot.CorpUserSnapshot':
                  {'urn': 'urn:li:corpuser:datahub', 'aspects':
                    [{'com.linkedin.identity.CorpUserInfo': {'active': True, 'displayName': 'Data Hub', 'email': 'datahub@linkedin.com', 'title': 'CEO', 'fullName': 'Data Hub'}}]
                   }
                  }
                },
              'systemMetadata': {'lastObserved': 1628097379571, 'runId': 'af0fe6e4-f547-11eb-81b2-acde48001122'}
        },
    )
    response.raise_for_status()

@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_ingest_with_blank_system_metadata():
    response = requests.post(
        f"{GMS_ENDPOINT}/entities?action=ingest",
        headers=restli_default_headers,
        json={
          'entity':
            {
              'value':
                {'com.linkedin.metadata.snapshot.CorpUserSnapshot':
                  {'urn': 'urn:li:corpuser:datahub', 'aspects':
                    [{'com.linkedin.identity.CorpUserInfo': {'active': True, 'displayName': 'Data Hub', 'email': 'datahub@linkedin.com', 'title': 'CEO', 'fullName': 'Data Hub'}}]
                   }
                  }
                },
              'systemMetadata': {}
        },
    )
    response.raise_for_status()

@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_ingest_without_system_metadata():
    response = requests.post(
        f"{GMS_ENDPOINT}/entities?action=ingest",
        headers=restli_default_headers,
        json={
          'entity':
            {
              'value':
                {'com.linkedin.metadata.snapshot.CorpUserSnapshot':
                  {'urn': 'urn:li:corpuser:datahub', 'aspects':
                    [{'com.linkedin.identity.CorpUserInfo': {'active': True, 'displayName': 'Data Hub', 'email': 'datahub@linkedin.com', 'title': 'CEO', 'fullName': 'Data Hub'}}]
                   }
                  }
             },
        },
    )
    response.raise_for_status()


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_frontend_list_policies(frontend_session):

    json = {
        "query": """query listPolicies($input: ListPoliciesInput!) {\n
            listPolicies(input: $input) {\n
                start\n
                count\n
                total\n
                policies {\n
                    urn\n
                    type\n
                    name\n
                    description\n
                    state\n
                    resources {\n
                      type\n
                      allResources\n
                      resources\n
                    }\n
                    privileges\n
                    actors {\n
                      users\n
                      groups\n
                      allUsers\n
                      allGroups\n
                      resourceOwners\n
                    }\n
                    editable\n
                }\n
            }\n
        }""",
        "variables": {
            "input": {
              "start": "0",
              "count": "20",
            }
        }
    }
    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listPolicies"]
    assert res_data["data"]["listPolicies"]["start"] is 0
    assert res_data["data"]["listPolicies"]["count"] is 8
    assert len(res_data["data"]["listPolicies"]["policies"]) is 8 # Length of default policies.

@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion", "test_frontend_list_policies"])
def test_frontend_update_policy(frontend_session):

    json = {
        "query": """mutation updatePolicy($urn: String!, $input: PolicyInput!) {\n
            updatePolicy(urn: $urn, input: $input) }""",
        "variables": {
            "urn": "urn:li:dataHubPolicy:7",
            "input": {
                "type": "PLATFORM",
                "state": "INACTIVE",
                "name": "Updated Platform Policy",
                "description": "My Metadaata Policy",
                "privileges": [ "MANAGE_POLICIES" ],
                "actors": {
                  "users": ["urn:li:corpuser:datahub"],
                  "resourceOwners": False,
                  "allUsers": False,
                  "allGroups": False
                }
            }
        }
    }

    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["updatePolicy"]
    assert res_data["data"]["updatePolicy"] == "urn:li:dataHubPolicy:7"

@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion", "test_frontend_list_policies", "test_frontend_update_policy"])
def test_frontend_delete_policy(frontend_session):

    json = {
        "query": """mutation deletePolicy($urn: String!) {\n
            deletePolicy(urn: $urn) }""",
        "variables": {
            "urn": "urn:li:dataHubPolicy:7"
        }
    }

    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    # Now verify the policy has been removed.
    json = {
        "query": """query listPolicies($input: ListPoliciesInput!) {\n
            listPolicies(input: $input) {\n
                start\n
                count\n
                total\n
                policies {\n
                    urn\n
                }\n
            }\n
        }""",
        "variables": {
            "input": {
              "start": "0",
              "count": "20",
            }
        }
    }
    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listPolicies"]
    assert len(res_data["data"]["listPolicies"]["policies"]) is 7 # Length of default policies - 1

@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion", "test_frontend_list_policies", "test_frontend_delete_policy"])
def test_frontend_create_policy(frontend_session):

    # Policy tests are not idempotent. If you rerun this test it will be wrong.
    json = {
        "query": """mutation createPolicy($input: PolicyInput!) {\n
            createPolicy(input: $input) }""",
        "variables": {
            "input": {
                "type": "METADATA",
                "name": "Test Metadata Policy",
                "description": "My Metadaata Policy",
                "state": "ACTIVE",
                "resources": {
                  "type": "dataset",
                  "allResources": True
                },
                "privileges": [ "EDIT_ENTITY_TAGS" ],
                "actors": {
                  "users": ["urn:li:corpuser:datahub"],
                  "resourceOwners": False,
                  "allUsers": False,
                  "allGroups": False
                }
            }
        }
    }

    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createPolicy"]

    # Now verify the policy has been added.
    json = {
        "query": """query listPolicies($input: ListPoliciesInput!) {\n
            listPolicies(input: $input) {\n
                start\n
                count\n
                total\n
                policies {\n
                    urn\n
                }\n
            }\n
        }""",
        "variables": {
            "input": {
              "start": "0",
              "count": "20",
            }
        }
    }
    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listPolicies"]
    # TODO: Check to see that the policy we created in inside the array.
    assert len(res_data["data"]["listPolicies"]["policies"]) is 8 # Back up to 8

@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_frontend_app_config(frontend_session):

    json = {
        "query": """query appConfig {\n
            appConfig {\n
                analyticsConfig {\n
                  enabled\n
                }\n
                policiesConfig {\n
                  enabled\n
                  platformPrivileges {\n
                    type\n
                    displayName\n
                    description\n
                  }\n
                  resourcePrivileges {\n
                    resourceType\n
                    resourceTypeDisplayName\n
                    entityType\n
                    privileges {\n
                      type\n
                      displayName\n
                      description\n
                    }\n
                  }\n
                }\n
            }\n
        }"""
    }

    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["appConfig"]
    assert res_data["data"]["appConfig"]["analyticsConfig"]["enabled"] is True
    assert res_data["data"]["appConfig"]["policiesConfig"]["enabled"] is True

@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_frontend_me_query(frontend_session):

    json = {
        "query": """query me {\n
            me {\n
                corpUser {\n
                  urn\n
                  username\n
                  editableInfo {\n
                      pictureLink\n
                  }\n
                  info {\n
                      firstName\n
                      fullName\n
                      title\n
                      email\n
                  }\n
                }\n
                platformPrivileges {\n
                  viewAnalytics
                  managePolicies
                }\n
            }\n
        }"""
    }

    response = frontend_session.post(
        f"{FRONTEND_ENDPOINT}/api/v2/graphql", json=json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["me"]["corpUser"]["urn"] == "urn:li:corpuser:datahub"
    assert res_data["data"]["me"]["platformPrivileges"]["viewAnalytics"] is True
    assert res_data["data"]["me"]["platformPrivileges"]["managePolicies"] is True