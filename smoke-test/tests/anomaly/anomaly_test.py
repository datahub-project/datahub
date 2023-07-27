import pytest
import time

from tests.utils import delete_urns_from_file, get_frontend_url, get_gms_url, ingest_file_via_rest


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(request):
    print("ingesting anomalies test data")
    ingest_file_via_rest("tests/anomaly/data.json")
    yield
    print("removing anomalies test data")
    delete_urns_from_file("tests/anomaly/data.json")

@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass

TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,anomalies-sample-dataset,PROD)"
TEST_ANOMALY_URN = "urn:li:anomaly:test"

@pytest.mark.dependency(depends=["test_healthchecks"])
def test_list_dataset_anomalies(frontend_session):

    # Sleep for eventual consistency (not ideal)
    time.sleep(2)

    list_dataset_anomalies_json = {
        "query": """query dataset($urn: String!) {\n
            dataset(urn: $urn) {\n
              anomalies(state: ACTIVE, start: 0, count: 10) {\n
                start\n
                count\n
                total\n
                anomalies {\n
                  urn\n
                  type\n
                  anomalyType\n
                  severity\n
                  description\n
                  status {\n
                    state\n
                    lastUpdated {\n
                      time\n
                      actor\n
                    }\n
                  }\n
                  source {\n
                    type\n
                    source {\n
                      ... on Assertion {\n
                        urn\n
                        info {\n
                          type
                        }\n
                      }\n
                    }\n
                  }\n
                  review {\n
                    state\n
                    message\n
                    lastUpdated {\n
                      time\n
                      actor\n
                    }\n
                  }\n
                  entity {\n
                    urn\n
                  }\n
                  created {\n
                    time\n
                    actor\n
                  }\n
                }\n
              }\n
            }\n
        }""",
        "variables": {
          "urn": TEST_DATASET_URN
        }
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=list_dataset_anomalies_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]["anomalies"] == {
     'start': 0,
     'count': 10,
     'total': 1,
     'anomalies': [
        {
          "urn": TEST_ANOMALY_URN,
          "type": "ANOMALY",
          "anomalyType": "FRESHNESS",
          "severity": 0,
          "description": "test description",
          "status": {
            "state": "ACTIVE",
            "lastUpdated": {
              "time": 0,
              "actor": "urn:li:corpuser:admin"
            }
          },
          "source": {
            "type": "INFERRED_ASSERTION_FAILURE",
            "source": {
              "urn": "urn:li:assertion:assertion-test",
              "info": {
                "type": "DATASET"
              }
            }
          },
          "review": {
            "state": "CONFIRMED",
            "message": None,
            "lastUpdated": {
              "time": 0,
              "actor": "urn:li:corpuser:admin"
            }
          },
          "entity": { "urn": TEST_DATASET_URN },
          "created": {
            "time": 0,
            "actor": "urn:li:corpuser:admin"
          }
        }
     ]
    }
