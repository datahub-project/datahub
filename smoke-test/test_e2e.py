import time

import pytest
import requests
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
def test_gms_list_data_platforms():
    response = requests.get(
        f"{GMS_ENDPOINT}/dataPlatforms",
        headers={
            **restli_default_headers,
            "X-RestLi-Method": "get_all",
        },
    )
    response.raise_for_status()
    data = response.json()

    assert len(data["elements"]) > 10


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_gms_get_all_users():
    response = requests.get(
        f"{GMS_ENDPOINT}/corpUsers",
        headers={
            **restli_default_headers,
            "X-RestLi-Method": "get_all",
        },
    )
    response.raise_for_status()
    data = response.json()

    assert len(data["elements"]) >= 3


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_gms_get_user():
    username = "jdoe"
    response = requests.get(
        f"{GMS_ENDPOINT}/corpUsers/($params:(),name:{username})",
        headers={
            **restli_default_headers,
        },
    )
    response.raise_for_status()
    data = response.json()

    assert data["username"] == username
    assert data["info"]["displayName"]
    assert data["info"]["email"]


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
        f"{GMS_ENDPOINT}/datasets/($params:(),name:{dataset_name},origin:{env},platform:{requests.utils.quote(platform)})",
        headers={
            **restli_default_headers,
            "X-RestLi-Method": "get",
        },
    )
    response.raise_for_status()
    data = response.json()

    assert data["urn"] == urn
    assert data["name"] == dataset_name
    assert data["platform"] == platform
    assert len(data["schemaMetadata"]["fields"]) >= 2


@pytest.mark.parametrize(
    "query,min_expected_results",
    [
        ("covid", 1),
        ("sample", 3),
    ],
)
@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_gms_search_dataset(query, min_expected_results):
    response = requests.get(
        f"{GMS_ENDPOINT}/datasets?q=search&input={query}",
        headers={
            **restli_default_headers,
            "X-RestLi-Method": "finder",
        },
    )
    response.raise_for_status()
    data = response.json()

    assert len(data["elements"]) >= min_expected_results
    assert data["paging"]["total"] >= min_expected_results
    assert data["elements"][0]["urn"]


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
        f"{FRONTEND_ENDPOINT}/authenticate", headers=headers, data=data
    )
    response.raise_for_status()

    yield session


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_frontend_auth(frontend_session):
    pass


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_frontend_browse_datasets(frontend_session):
    response = frontend_session.get(
        f"{FRONTEND_ENDPOINT}/api/v2/browse?type=dataset&path=/prod"
    )
    response.raise_for_status()
    data = response.json()

    assert data["metadata"]["totalNumEntities"] >= 4
    assert len(data["metadata"]["groups"]) >= 4
    assert len(data["metadata"]["groups"]) <= 8


@pytest.mark.parametrize(
    "query,min_expected_results",
    [
        ("covid", 1),
        ("sample", 3),
    ],
)
@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_frontend_browse_datasets(frontend_session, query, min_expected_results):
    response = frontend_session.get(
        f"{FRONTEND_ENDPOINT}/api/v2/search?type=dataset&input={query}"
    )
    response.raise_for_status()
    data = response.json()

    assert len(data["elements"]) >= min_expected_results


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_frontend_list_users(frontend_session):
    response = frontend_session.get(f"{FRONTEND_ENDPOINT}/api/v1/party/entities")
    response.raise_for_status()
    data = response.json()

    assert data["status"] == "ok"
    assert len(data["userEntities"]) >= 3


@pytest.mark.dependency(depends=["test_healthchecks", "test_run_ingestion"])
def test_frontend_user_info(frontend_session):
    response = frontend_session.get(f"{FRONTEND_ENDPOINT}/api/v1/user/me")
    response.raise_for_status()
    data = response.json()

    assert data["status"] == "ok"
    assert data["user"]["userName"] == "datahub"
    assert data["user"]["name"]
    assert data["user"]["email"]


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
def test_frontend_user_info(frontend_session, platform, dataset_name, env):
    urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    # Basic dataset info.
    response = frontend_session.get(f"{FRONTEND_ENDPOINT}/api/v2/datasets/{urn}")
    response.raise_for_status()
    data = response.json()

    assert data["nativeName"] == dataset_name
    assert data["fabric"] == env
    assert data["uri"] == urn

    # Schema info.
    response = frontend_session.get(f"{FRONTEND_ENDPOINT}/api/v2/datasets/{urn}/schema")
    response.raise_for_status()
    data = response.json()

    assert len(data["schema"]["columns"]) >= 2

    # Ownership info.
    response = frontend_session.get(f"{FRONTEND_ENDPOINT}/api/v2/datasets/{urn}/owners")
    response.raise_for_status()
    data = response.json()

    assert len(data["owners"]) >= 1

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
