import logging
import time
import urllib
from http import HTTPStatus
from typing import Any, Optional

import concurrent.futures
import pytest
import requests
import tenacity
from datahub.ingestion.run.pipeline import Pipeline

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_cypress_suite1

from tests.utils import (
    get_kafka_broker_url,
    get_kafka_schema_registry,
    get_sleep_info,
    ingest_file_via_rest,
    get_frontend_session,
    get_admin_credentials,
    get_root_urn,
    wait_for_writes_to_sync,
)

bootstrap_sample_data = "../metadata-ingestion/examples/mce_files/bootstrap_mce.json"
usage_sample_data = "./test_resources/bigquery_usages_golden.json"
bq_sample_data = "./sample_bq_data.json"
restli_default_headers = {
    "X-RestLi-Protocol-Version": "2.0.0",
}
kafka_post_ingestion_wait_sec = 30

sleep_sec, sleep_times = get_sleep_info()


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_user_present(auth_session, urn: str):
    response = auth_session.get(
        f"{auth_session.gms_url()}/entities/{urllib.parse.quote(urn)}",
        headers={
            **restli_default_headers,
        },
    )
    response.raise_for_status()
    data = response.json()

    user_key = "com.linkedin.metadata.snapshot.CorpUserSnapshot"
    assert data["value"]
    assert data["value"][user_key]
    assert data["value"][user_key]["urn"] == urn
    return data


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_user_relationship_present(auth_session, urn, relationships):
    json = {
        "query": """query corpUser($urn: String!) {\n
            corpUser(urn: $urn) {\n
                urn\n
                relationships(input: { types: ["IsMemberOfNativeGroup"], direction: OUTGOING, start: 0, count: 1 }) {\n
                    total\n
                }\n
            }\n
        }""",
        "variables": {"urn": urn},
    }
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["corpUser"]
    assert res_data["data"]["corpUser"]["relationships"]
    assert res_data["data"]["corpUser"]["relationships"]["total"] == relationships


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_dataset_present(
        auth_session: Any,
    urn: str,
    aspects: Optional[str] = "datasetProperties",
) -> Any:
    response = auth_session.get(
        f"{auth_session.gms_url()}/entitiesV2?ids=List({urllib.parse.quote(urn)})&aspects=List({aspects})",
        headers={
            **restli_default_headers,
            "X-RestLi-Method": "batch_get",
        },
    )
    response.raise_for_status()
    res_data = response.json()
    assert res_data["results"]
    assert res_data["results"][urn]
    assert res_data["results"][urn]["aspects"]["datasetProperties"]
    return res_data


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_group_not_present(auth_session, urn: str) -> Any:
    json = {
        "query": """query corpGroup($urn: String!) {\n
            corpGroup(urn: $urn) {\n
                urn\n
                properties {\n
                    displayName\n
                }\n
            }\n
        }""",
        "variables": {"urn": urn},
    }
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["corpGroup"]
    assert res_data["data"]["corpGroup"]["properties"] is None


def fixture_ingestion_via_rest(auth_session):
    ingest_file_via_rest(auth_session, bootstrap_sample_data)
    _ensure_user_present(auth_session, urn=get_root_urn())


def fixture_ingestion_usage_via_rest(auth_session):
    ingest_file_via_rest(auth_session, usage_sample_data)


def fixture_ingestion_via_kafka(auth_session):
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
                        "bootstrap": get_kafka_broker_url(),
                        "schema_registry_url": get_kafka_schema_registry(),
                    }
                },
            },
        }
    )
    pipeline.run()
    pipeline.raise_from_status()
    _ensure_dataset_present(auth_session,
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,bigquery-public-data.covid19_geotab_mobility_impact.us_border_wait_times,PROD)"
    )

    # Since Kafka emission is asynchronous, we must wait a little bit so that
    # the changes are actually processed.
    time.sleep(kafka_post_ingestion_wait_sec)


@pytest.fixture(scope='module', autouse=True)
def test_run_ingestion(auth_session):
    # Dummy test so that future ones can just depend on this one.

    # The rest sink fixtures cannot run at the same time, limitation of the Pipeline code
    fixture_ingestion_usage_via_rest(auth_session)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        for ingestion_fixture in ["fixture_ingestion_via_kafka", "fixture_ingestion_via_rest"]:
            futures.append(
                executor.submit(globals()[ingestion_fixture], auth_session)
            )

        for future in concurrent.futures.as_completed(futures):
            logger.info(future.result())
    wait_for_writes_to_sync()


def test_gms_get_user(auth_session):
    username = "jdoe"
    urn = f"urn:li:corpuser:{username}"
    _ensure_user_present(auth_session, urn=urn)


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
def test_gms_get_dataset(auth_session, platform, dataset_name, env):
    platform = "urn:li:dataPlatform:bigquery"
    dataset_name = (
        "bigquery-public-data.covid19_geotab_mobility_impact.us_border_wait_times"
    )
    env = "PROD"
    urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    response = auth_session.get(
        f"{auth_session.gms_url()}/entities/{urllib.parse.quote(urn)}",
        headers={
            **restli_default_headers,
            "X-RestLi-Method": "get",
        },
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data["value"]
    assert res_data["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]
    assert (
        res_data["value"]["com.linkedin.metadata.snapshot.DatasetSnapshot"]["urn"]
        == urn
    )


def test_gms_batch_get_v2(auth_session):
    platform = "urn:li:dataPlatform:bigquery"
    env = "PROD"
    name_1 = "bigquery-public-data.covid19_geotab_mobility_impact.us_border_wait_times"
    name_2 = "bigquery-public-data.covid19_geotab_mobility_impact.ca_border_wait_times"
    urn1 = f"urn:li:dataset:({platform},{name_1},{env})"
    urn2 = f"urn:li:dataset:({platform},{name_2},{env})"

    resp1 = _ensure_dataset_present(auth_session, urn1, aspects="datasetProperties,ownership")
    assert resp1["results"][urn1]["aspects"]["ownership"]

    resp2 = _ensure_dataset_present(auth_session, urn2, aspects="datasetProperties,ownership")
    assert (
        "ownership" not in resp2["results"][urn2]["aspects"]
    )  # Aspect does not exist.


@pytest.mark.parametrize(
    "query,min_expected_results",
    [
        ("covid", 1),
        ("sample", 3),
    ],
)
def test_gms_search_dataset(auth_session, query, min_expected_results):

    json = {"input": f"{query}", "entity": "dataset", "start": 0, "count": 10}
    print(json)
    response = auth_session.post(
        f"{auth_session.gms_url()}/entities?action=search",
        headers=restli_default_headers,
        json=json,
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data["value"]
    assert res_data["value"]["numEntities"] >= min_expected_results
    assert len(res_data["value"]["entities"]) >= min_expected_results


@pytest.mark.parametrize(
    "query,min_expected_results",
    [
        ("covid", 1),
        ("sample", 3),
    ],
)
def test_gms_search_across_entities(auth_session, query, min_expected_results):

    json = {"input": f"{query}", "entities": [], "start": 0, "count": 10}
    print(json)
    response = auth_session.post(
        f"{auth_session.gms_url()}/entities?action=searchAcrossEntities",
        headers=restli_default_headers,
        json=json,
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data["value"]
    assert res_data["value"]["numEntities"] >= min_expected_results
    assert len(res_data["value"]["entities"]) >= min_expected_results


def test_gms_usage_fetch(auth_session):
    response = auth_session.post(
        f"{auth_session.gms_url()}/usageStats?action=queryRange",
        headers=restli_default_headers,
        json={
            "resource": "urn:li:dataset:(urn:li:dataPlatform:bigquery,harshal-playground-306419.test_schema.excess_deaths_derived,PROD)",
            "duration": "DAY",
            "rangeFromEnd": "ALL",
        },
    )
    response.raise_for_status()

    data = response.json()["value"]

    assert len(data["buckets"]) == 6
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


def test_frontend_auth(auth_session):
    pass


def test_frontend_browse_datasets(auth_session):

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
        "variables": {"input": {"type": "DATASET", "path": ["prod"]}},
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)

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
        ("", 1),
    ],
)
def test_frontend_search_datasets(auth_session, query, min_expected_results):

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
            "input": {"type": "DATASET", "query": f"{query}", "start": 0, "count": 10}
        },
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["search"]
    assert res_data["data"]["search"]["total"] >= min_expected_results
    assert len(res_data["data"]["search"]["searchResults"]) >= min_expected_results


@pytest.mark.parametrize(
    "query,min_expected_results",
    [
        ("covid", 1),
        ("sample", 3),
        ("", 1),
    ],
)
def test_frontend_search_across_entities(auth_session, query, min_expected_results):

    json = {
        "query": """query searchAcrossEntities($input: SearchAcrossEntitiesInput!) {\n
            searchAcrossEntities(input: $input) {\n
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
            "input": {"types": [], "query": f"{query}", "start": 0, "count": 10}
        },
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["searchAcrossEntities"]
    assert res_data["data"]["searchAcrossEntities"]["total"] >= min_expected_results
    assert (
        len(res_data["data"]["searchAcrossEntities"]["searchResults"])
        >= min_expected_results
    )


def test_frontend_user_info(auth_session):

    urn = get_root_urn()
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
        "variables": {"urn": urn},
    }
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
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
def test_frontend_datasets(auth_session, platform, dataset_name, env):
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
        "variables": {"urn": urn},
    }
    # Basic dataset info.
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["urn"] == urn
    assert res_data["data"]["dataset"]["name"] == dataset_name
    assert res_data["data"]["dataset"]["platform"]["urn"] == platform


def test_ingest_with_system_metadata(auth_session, test_run_ingestion):
    response = auth_session.post(
        f"{auth_session.gms_url()}/entities?action=ingest",
        headers=restli_default_headers,
        json={
            "entity": {
                "value": {
                    "com.linkedin.metadata.snapshot.CorpUserSnapshot": {
                        "urn": get_root_urn(),
                        "aspects": [
                            {
                                "com.linkedin.identity.CorpUserInfo": {
                                    "active": True,
                                    "displayName": "DataHub",
                                    "email": "datahub@linkedin.com",
                                    "title": "CEO",
                                    "fullName": "DataHub",
                                }
                            }
                        ],
                    }
                }
            },
            "systemMetadata": {
                "lastObserved": 1628097379571,
                "runId": "af0fe6e4-f547-11eb-81b2-acde48001122",
            },
        },
    )
    response.raise_for_status()


def test_ingest_with_blank_system_metadata(auth_session):
    response = auth_session.post(
        f"{auth_session.gms_url()}/entities?action=ingest",
        headers=restli_default_headers,
        json={
            "entity": {
                "value": {
                    "com.linkedin.metadata.snapshot.CorpUserSnapshot": {
                        "urn": get_root_urn(),
                        "aspects": [
                            {
                                "com.linkedin.identity.CorpUserInfo": {
                                    "active": True,
                                    "displayName": "DataHub",
                                    "email": "datahub@linkedin.com",
                                    "title": "CEO",
                                    "fullName": "DataHub",
                                }
                            }
                        ],
                    }
                }
            },
            "systemMetadata": {},
        },
    )
    response.raise_for_status()


def test_ingest_without_system_metadata(auth_session):
    response = auth_session.post(
        f"{auth_session.gms_url()}/entities?action=ingest",
        headers=restli_default_headers,
        json={
            "entity": {
                "value": {
                    "com.linkedin.metadata.snapshot.CorpUserSnapshot": {
                        "urn": get_root_urn(),
                        "aspects": [
                            {
                                "com.linkedin.identity.CorpUserInfo": {
                                    "active": True,
                                    "displayName": "DataHub",
                                    "email": "datahub@linkedin.com",
                                    "title": "CEO",
                                    "fullName": "DataHub",
                                }
                            }
                        ],
                    }
                }
            },
        },
    )
    response.raise_for_status()


def test_frontend_app_config(auth_session):

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

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["appConfig"]
    assert res_data["data"]["appConfig"]["analyticsConfig"]["enabled"] is True
    assert res_data["data"]["appConfig"]["policiesConfig"]["enabled"] is True


def test_frontend_me_query(auth_session):

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
                  manageIdentities
                  manageUserCredentials
                  generatePersonalAccessTokens 
                }\n
            }\n
        }"""
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["me"]["corpUser"]["urn"] == get_root_urn()
    assert res_data["data"]["me"]["platformPrivileges"]["viewAnalytics"] is True
    assert res_data["data"]["me"]["platformPrivileges"]["managePolicies"] is True
    assert res_data["data"]["me"]["platformPrivileges"]["manageUserCredentials"] is True
    assert res_data["data"]["me"]["platformPrivileges"]["manageIdentities"] is True
    assert (
        res_data["data"]["me"]["platformPrivileges"]["generatePersonalAccessTokens"]
        is True
    )


def test_list_users(auth_session):

    json = {
        "query": """query listUsers($input: ListUsersInput!) {\n
            listUsers(input: $input) {\n
                start\n
                count\n
                total\n
                users {\n
                    urn\n
                    type\n
                    username\n
                    properties {\n
                      firstName
                    }\n
                }\n
            }\n
        }""",
        "variables": {
            "input": {
                "start": "0",
                "count": "2",
            }
        },
    }
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listUsers"]
    assert res_data["data"]["listUsers"]["start"] == 0
    assert res_data["data"]["listUsers"]["count"] == 2
    assert (
        len(res_data["data"]["listUsers"]["users"]) >= 2
    )  # Length of default user set.


@pytest.mark.dependency()
def test_list_groups(auth_session):

    json = {
        "query": """query listGroups($input: ListGroupsInput!) {\n
            listGroups(input: $input) {\n
                start\n
                count\n
                total\n
                groups {\n
                    urn\n
                    type\n
                    name\n
                    properties {\n
                      displayName
                    }\n
                }\n
            }\n
        }""",
        "variables": {
            "input": {
                "start": "0",
                "count": "2",
            }
        },
    }
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listGroups"]
    assert res_data["data"]["listGroups"]["start"] == 0
    assert res_data["data"]["listGroups"]["count"] == 2
    assert (
        len(res_data["data"]["listGroups"]["groups"]) >= 2
    )  # Length of default group set.


@pytest.mark.dependency(
    depends=["test_list_groups"]
)
def test_add_remove_members_from_group(auth_session):

    # Assert no group edges for user jdoe
    json = {
        "query": """query corpUser($urn: String!) {\n
            corpUser(urn: $urn) {\n
                urn\n
                relationships(input: { types: ["IsMemberOfNativeGroup"], direction: OUTGOING, start: 0, count: 1 }) {\n
                    total\n
                }\n
            }\n
        }""",
        "variables": {"urn": "urn:li:corpuser:jdoe"},
    }
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["corpUser"]
    assert res_data["data"]["corpUser"]["relationships"]["total"] == 0

    # Add jdoe to group
    json = {
        "query": """mutation addGroupMembers($input: AddGroupMembersInput!) {\n
            addGroupMembers(input: $input) }""",
        "variables": {
            "input": {
                "groupUrn": "urn:li:corpGroup:bfoo",
                "userUrns": ["urn:li:corpuser:jdoe"],
            }
        },
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()

    # Verify the member has been added
    _ensure_user_relationship_present(auth_session, "urn:li:corpuser:jdoe", 1)

    # Now remove jdoe from the group
    json = {
        "query": """mutation removeGroupMembers($input: RemoveGroupMembersInput!) {\n
            removeGroupMembers(input: $input) }""",
        "variables": {
            "input": {
                "groupUrn": "urn:li:corpGroup:bfoo",
                "userUrns": ["urn:li:corpuser:jdoe"],
            }
        },
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()

    # Verify the member has been removed
    _ensure_user_relationship_present(auth_session, "urn:li:corpuser:jdoe", 0)


@pytest.mark.dependency()
def test_update_corp_group_properties(auth_session):

    group_urn = "urn:li:corpGroup:bfoo"

    # Update Corp Group Description
    json = {
        "query": """mutation updateCorpGroupProperties($urn: String!, $input: CorpGroupUpdateInput!) {\n
            updateCorpGroupProperties(urn: $urn, input: $input) { urn } }""",
        "variables": {
            "urn": group_urn,
            "input": {
                "description": "My test description",
                "slack": "test_group_slack",
                "email": "test_group_email@email.com",
            },
        },
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    print(res_data)
    assert "errors" not in res_data
    assert res_data["data"]["updateCorpGroupProperties"] is not None

    # Verify the description has been updated
    json = {
        "query": """query corpGroup($urn: String!) {\n
            corpGroup(urn: $urn) {\n
                urn\n
                editableProperties {\n
                    description\n
                    slack\n
                    email\n
                }\n
            }\n
        }""",
        "variables": {"urn": group_urn},
    }
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["corpGroup"]
    assert res_data["data"]["corpGroup"]["editableProperties"]
    assert res_data["data"]["corpGroup"]["editableProperties"] == {
        "description": "My test description",
        "slack": "test_group_slack",
        "email": "test_group_email@email.com",
    }

    # Reset the editable properties
    json = {
        "query": """mutation updateCorpGroupProperties($urn: String!, $input: CorpGroupUpdateInput!) {\n
            updateCorpGroupProperties(urn: $urn, input: $input) { urn } }""",
        "variables": {
            "urn": group_urn,
            "input": {"description": "", "slack": "", "email": ""},
        },
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()


@pytest.mark.dependency(
    depends=[
        "test_update_corp_group_properties",
    ]
)
def test_update_corp_group_description(auth_session):

    group_urn = "urn:li:corpGroup:bfoo"

    # Update Corp Group Description
    json = {
        "query": """mutation updateDescription($input: DescriptionUpdateInput!) {\n
            updateDescription(input: $input) }""",
        "variables": {
            "input": {"description": "My test description", "resourceUrn": group_urn},
        },
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    print(res_data)
    assert "errors" not in res_data
    assert res_data["data"]["updateDescription"] is True

    # Verify the description has been updated
    json = {
        "query": """query corpGroup($urn: String!) {\n
            corpGroup(urn: $urn) {\n
                urn\n
                editableProperties {\n
                    description\n
                }\n
            }\n
        }""",
        "variables": {"urn": group_urn},
    }
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["corpGroup"]
    assert res_data["data"]["corpGroup"]["editableProperties"]
    assert (
        res_data["data"]["corpGroup"]["editableProperties"]["description"]
        == "My test description"
    )

    # Reset Corp Group Description
    json = {
        "query": """mutation updateDescription($input: DescriptionUpdateInput!) {\n
            updateDescription(input: $input) }""",
        "variables": {
            "input": {"description": "", "resourceUrn": group_urn},
        },
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()


@pytest.mark.dependency(
    depends=[
        "test_list_groups",
        "test_add_remove_members_from_group",
    ]
)
def test_remove_user(auth_session):

    json = {
        "query": """mutation removeUser($urn: String!) {\n
            removeUser(urn: $urn) }""",
        "variables": {"urn": "urn:li:corpuser:jdoe"},
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()

    json = {
        "query": """query corpUser($urn: String!) {\n
            corpUser(urn: $urn) {\n
                urn\n
                properties {\n
                    firstName\n
                }\n
            }\n
        }""",
        "variables": {"urn": "urn:li:corpuser:jdoe"},
    }
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["corpUser"]
    assert res_data["data"]["corpUser"]["properties"] is None


@pytest.mark.dependency(
    depends=[
        "test_list_groups",
        "test_add_remove_members_from_group",
    ]
)
def test_remove_group(auth_session):
    group_urn = "urn:li:corpGroup:bfoo"

    json = {
        "query": """mutation removeGroup($urn: String!) {\n
            removeGroup(urn: $urn) }""",
        "variables": {"urn": group_urn},
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()

    _ensure_group_not_present(auth_session, group_urn)


@pytest.mark.dependency(
    depends=[
        "test_list_groups",
        "test_remove_group",
    ]
)
def test_create_group(auth_session):

    json = {
        "query": """mutation createGroup($input: CreateGroupInput!) {\n
            createGroup(input: $input) }""",
        "variables": {
            "input": {
                "id": "test-id",
                "name": "Test Group",
                "description": "My test group",
            }
        },
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()

    json = {
        "query": """query corpGroup($urn: String!) {\n
            corpGroup(urn: $urn) {\n
                urn\n
                properties {\n
                    displayName\n
                }\n
            }\n
        }""",
        "variables": {"urn": "urn:li:corpGroup:test-id"},
    }
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["corpGroup"]
    assert res_data["data"]["corpGroup"]["properties"]["displayName"] == "Test Group"


def test_home_page_recommendations(auth_session):

    min_expected_recommendation_modules = 0

    json = {
        "query": """query listRecommendations($input: ListRecommendationsInput!) {\n
            listRecommendations(input: $input) { modules { title } } }""",
        "variables": {
            "input": {
                "userUrn": get_root_urn(),
                "requestContext": {"scenario": "HOME"},
                "limit": 5,
            }
        },
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    print(res_data)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listRecommendations"]
    assert "errors" not in res_data
    assert (
        len(res_data["data"]["listRecommendations"]["modules"])
        > min_expected_recommendation_modules
    )


def test_search_results_recommendations(auth_session):

    # This test simply ensures that the recommendations endpoint does not return an error.
    json = {
        "query": """query listRecommendations($input: ListRecommendationsInput!) {\n
            listRecommendations(input: $input) { modules { title }  } }""",
        "variables": {
            "input": {
                "userUrn": get_root_urn(),
                "requestContext": {
                    "scenario": "SEARCH_RESULTS",
                    "searchRequestContext": {"query": "asdsdsdds", "filters": []},
                },
                "limit": 5,
            }
        },
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert "errors" not in res_data


def test_generate_personal_access_token(auth_session):

    # Test success case
    json = {
        "query": """query getAccessToken($input: GetAccessTokenInput!) {\n
            getAccessToken(input: $input) {\n
              accessToken\n
            }\n
        }""",
        "variables": {
            "input": {
                "type": "PERSONAL",
                "actorUrn": get_root_urn(),
                "duration": "ONE_MONTH",
            }
        },
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["getAccessToken"]["accessToken"] is not None
    assert "errors" not in res_data

    # Test unauthenticated case
    json = {
        "query": """query getAccessToken($input: GetAccessTokenInput!) {\n
            getAccessToken(input: $input) {\n
              accessToken\n
            }\n
        }""",
        "variables": {
            "input": {
                "type": "PERSONAL",
                "actorUrn": "urn:li:corpuser:jsmith",
                "duration": "ONE_DAY",
            }
        },
    }

    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert "errors" in res_data  # Assert the request fails


def test_native_user_endpoints(auth_session):
    # Sign up tests
    frontend_session = get_frontend_session()

    # Test getting the invite token
    get_invite_token_json = {
        "query": """query getInviteToken($input: GetInviteTokenInput!) {\n
            getInviteToken(input: $input){\n
              inviteToken\n
            }\n
        }""",
        "variables": {"input": {}},
    }

    get_invite_token_response = frontend_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=get_invite_token_json
    )
    get_invite_token_response.raise_for_status()
    get_invite_token_res_data = get_invite_token_response.json()

    assert get_invite_token_res_data
    assert get_invite_token_res_data["data"]
    invite_token = get_invite_token_res_data["data"]["getInviteToken"]["inviteToken"]
    assert invite_token is not None
    assert "errors" not in get_invite_token_res_data

    # Pass the invite token when creating the user
    sign_up_json = {
        "fullName": "Test User",
        "email": "test@email.com",
        "password": "password",
        "title": "Date Engineer",
        "inviteToken": invite_token,
    }

    sign_up_response = frontend_session.post(
        f"{auth_session.frontend_url()}/signUp", json=sign_up_json
    )
    assert sign_up_response
    assert "errors" not in sign_up_response

    # Creating the same user again fails
    same_user_sign_up_response = frontend_session.post(
        f"{auth_session.frontend_url()}/signUp", json=sign_up_json
    )
    assert not same_user_sign_up_response

    # Test that a bad invite token leads to failed sign up
    bad_sign_up_json = {
        "fullName": "Test2 User",
        "email": "test2@email.com",
        "password": "password",
        "title": "Date Engineer",
        "inviteToken": "invite_token",
    }
    bad_sign_up_response = frontend_session.post(
        f"{auth_session.frontend_url()}/signUp", json=bad_sign_up_json
    )
    assert not bad_sign_up_response

    frontend_session.cookies.clear()

    # Reset credentials tests

    # Log in as root again
    headers = {
        "Content-Type": "application/json",
    }
    username, password = get_admin_credentials()
    root_login_data = '{"username":"' + username + '", "password":"' + password + '"}'
    frontend_session.post(
        f"{auth_session.frontend_url()}/logIn", headers=headers, data=root_login_data
    )

    # Test creating the password reset token
    create_reset_token_json = {
        "query": """mutation createNativeUserResetToken($input: CreateNativeUserResetTokenInput!) {\n
            createNativeUserResetToken(input: $input) {\n
              resetToken\n
            }\n
        }""",
        "variables": {"input": {"userUrn": "urn:li:corpuser:test@email.com"}},
    }

    create_reset_token_response = frontend_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=create_reset_token_json
    )
    create_reset_token_response.raise_for_status()
    create_reset_token_res_data = create_reset_token_response.json()

    assert create_reset_token_res_data
    assert create_reset_token_res_data["data"]
    reset_token = create_reset_token_res_data["data"]["createNativeUserResetToken"][
        "resetToken"
    ]
    assert reset_token is not None
    assert "errors" not in create_reset_token_res_data

    # Pass the reset token when resetting credentials
    reset_credentials_json = {
        "email": "test@email.com",
        "password": "newpassword",
        "resetToken": reset_token,
    }

    reset_credentials_response = frontend_session.post(
        f"{auth_session.frontend_url()}/resetNativeUserCredentials", json=reset_credentials_json
    )
    assert reset_credentials_response
    assert "errors" not in reset_credentials_response

    # Test that a bad reset token leads to failed response
    bad_user_reset_credentials_json = {
        "email": "test@email.com",
        "password": "newerpassword",
        "resetToken": "reset_token",
    }
    bad_reset_credentials_response = frontend_session.post(
        f"{auth_session.frontend_url()}/resetNativeUserCredentials",
        json=bad_user_reset_credentials_json,
    )
    assert not bad_reset_credentials_response

    # Test that only a native user can reset their password
    jaas_user_reset_credentials_json = {
        "email": "datahub",
        "password": "password",
        "resetToken": reset_token,
    }
    jaas_user_reset_credentials_response = frontend_session.post(
        f"{auth_session.frontend_url()}/resetNativeUserCredentials",
        json=jaas_user_reset_credentials_json,
    )
    assert not jaas_user_reset_credentials_response

    # Tests that unauthenticated users can't invite users or send reset password links

    unauthenticated_session = requests.Session()

    unauthenticated_get_invite_token_response = unauthenticated_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=get_invite_token_json
    )
    assert (
        unauthenticated_get_invite_token_response.status_code == HTTPStatus.UNAUTHORIZED
    )

    unauthenticated_create_reset_token_json = {
        "query": """mutation createNativeUserResetToken($input: CreateNativeUserResetTokenInput!) {\n
            createNativeUserResetToken(input: $input) {\n
              resetToken\n
            }\n
        }""",
        "variables": {"input": {"userUrn": "urn:li:corpuser:test@email.com"}},
    }

    unauthenticated_create_reset_token_response = unauthenticated_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql",
        json=unauthenticated_create_reset_token_json,
    )
    assert (
        unauthenticated_create_reset_token_response.status_code
        == HTTPStatus.UNAUTHORIZED
    )

    # cleanup steps
    json = {
        "query": """mutation removeUser($urn: String!) {\n
            removeUser(urn: $urn) }""",
        "variables": {"urn": "urn:li:corpuser:test@email.com"},
    }

    frontend_session.post(
        f"{auth_session.frontend_url()}/logIn", headers=headers, data=root_login_data
    )

    remove_user_response = frontend_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    remove_user_response.raise_for_status()
    assert "errors" not in remove_user_response
