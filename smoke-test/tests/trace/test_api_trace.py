import time

import pytest
from opensearchpy import OpenSearch

from tests.utils import delete_urns, wait_for_writes_to_sync

es = OpenSearch(["http://localhost:9200"])


generated_urns = {
    "apiTraceHappyPath": "urn:li:dataset:(urn:li:dataPlatform:test,apiTraceHappyPath,PROD)",
    "apiTraceMCPFail": "urn:li:dataset:(urn:li:dataPlatform:test,apiTraceMCPFail,PROD)",
    "apiTraceDroppedElasticsearch": "urn:li:dataset:(urn:li:dataPlatform:test,apiTraceDroppedElasticsearch,PROD)",
    "apiTraceOverwritten": "urn:li:dataset:(urn:li:dataPlatform:test,apiTraceOverwritten,PROD)",
    "apiTraceTimeseries": "urn:li:dataset:(urn:li:dataPlatform:test,apiTraceTimeseries,PROD)",
    "apiTraceNoop": "urn:li:dataset:(urn:li:dataPlatform:test,apiTraceNoop,PROD)",
    "apiTraceNoopWithFMCP": "urn:li:dataset:(urn:li:dataPlatform:test,apiTraceNoopWithFMCP,PROD)",
}


@pytest.fixture(scope="module", autouse=True)
def test_setup(graph_client):
    """Fixture to clean-up urns before and after a test is run"""
    print("removing previous test data")
    delete_urns(graph_client, list(generated_urns.values()))
    wait_for_writes_to_sync()
    yield
    print("removing generated test data")
    delete_urns(graph_client, list(generated_urns.values()))
    wait_for_writes_to_sync()


def test_successful_async_write(auth_session):
    urn = generated_urns["apiTraceHappyPath"]
    aspect_name = "status"

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v3/entity/dataset",
        params={"async": "true", "systemMetadata": "true"},
        json=[{"urn": urn, aspect_name: {"value": {"removed": False}}}],
    )

    trace_id = compare_trace_header_system_metadata(
        resp, resp.json()[0][aspect_name]["systemMetadata"]
    )
    wait_for_writes_to_sync()

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/trace/write/{trace_id}",
        params={"onlyIncludeErrors": "false", "detailed": "true"},
        json={urn: [aspect_name]},
    )
    assert resp.json() == {
        urn: {
            aspect_name: {
                "success": True,
                "primaryStorage": {"writeStatus": "ACTIVE_STATE"},
                "searchStorage": {"writeStatus": "ACTIVE_STATE"},
            }
        }
    }


def test_mcp_fail_aspect_async_write(auth_session):
    urn = generated_urns["apiTraceMCPFail"]
    aspect_name = "glossaryTerms"

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v3/entity/dataset/{urn}/{aspect_name}",
        params={"async": "true", "systemMetadata": "true"},
        json={
            "value": {
                "terms": [{"urn": "urn:li:glossaryTerm:someTerm"}],
                "auditStamp": {"time": 0, "actor": "urn:li:corpuser:datahub"},
            },
            "headers": {"If-Version-Match": "-10000"},
        },
    )

    trace_id = compare_trace_header_system_metadata(
        resp, resp.json()[aspect_name]["systemMetadata"]
    )
    wait_for_writes_to_sync()

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/trace/write/{trace_id}",
        params={"onlyIncludeErrors": "false", "detailed": "true", "skipCache": "true"},
        json={urn: [aspect_name]},
    )
    assert resp.json()[urn][aspect_name]["success"] is False
    assert resp.json()[urn][aspect_name]["primaryStorage"]["writeStatus"] == "ERROR"
    assert (
        resp.json()[urn][aspect_name]["primaryStorage"]["writeExceptions"][0]["message"]
        == "Expected version -10000, actual version -1"
    )
    assert resp.json()[urn][aspect_name]["searchStorage"] == {
        "writeStatus": "ERROR",
        "writeMessage": "Primary storage write failed.",
    }


def test_overwritten_async_write(auth_session):
    urn = generated_urns["apiTraceOverwritten"]
    aspect_name = "datasetProperties"

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v3/entity/dataset",
        params={"async": "true", "systemMetadata": "true"},
        json=[
            {
                "urn": urn,
                aspect_name: {
                    "value": {"name": "original", "customProperties": {}, "tags": []}
                },
            }
        ],
    )

    original_trace_id = compare_trace_header_system_metadata(
        resp, resp.json()[0][aspect_name]["systemMetadata"]
    )
    wait_for_writes_to_sync()

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/trace/write/{original_trace_id}",
        params={"onlyIncludeErrors": "false", "detailed": "true"},
        json={urn: [aspect_name]},
    )
    assert resp.json() == {
        urn: {
            aspect_name: {
                "success": True,
                "primaryStorage": {"writeStatus": "ACTIVE_STATE"},
                "searchStorage": {"writeStatus": "ACTIVE_STATE"},
            }
        }
    }

    # Perform 2nd write
    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v3/entity/dataset",
        params={"async": "true", "systemMetadata": "true"},
        json=[
            {
                "urn": urn,
                aspect_name: {
                    "value": {"name": "updated", "customProperties": {}, "tags": []}
                },
            }
        ],
    )

    second_trace_id = compare_trace_header_system_metadata(
        resp, resp.json()[0][aspect_name]["systemMetadata"]
    )
    wait_for_writes_to_sync()

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/trace/write/{second_trace_id}",
        params={"onlyIncludeErrors": "false", "detailed": "true", "skipCache": "true"},
        json={urn: [aspect_name]},
    )
    assert resp.json() == {
        urn: {
            aspect_name: {
                "success": True,
                "primaryStorage": {"writeStatus": "ACTIVE_STATE"},
                "searchStorage": {"writeStatus": "ACTIVE_STATE"},
            }
        }
    }

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/trace/write/{original_trace_id}",
        params={"onlyIncludeErrors": "false", "detailed": "true"},
        json={urn: [aspect_name]},
    )
    assert resp.json() == {
        urn: {
            aspect_name: {
                "success": True,
                "primaryStorage": {"writeStatus": "HISTORIC_STATE"},
                "searchStorage": {"writeStatus": "HISTORIC_STATE"},
            }
        }
    }


def test_missing_elasticsearch_async_write(auth_session):
    urn = generated_urns["apiTraceDroppedElasticsearch"]
    aspect_name = "status"

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v3/entity/dataset",
        params={"async": "true", "systemMetadata": "true"},
        json=[{"urn": urn, aspect_name: {"value": {"removed": False}}}],
    )

    trace_id = compare_trace_header_system_metadata(
        resp, resp.json()[0][aspect_name]["systemMetadata"]
    )
    wait_for_writes_to_sync()

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/trace/write/{trace_id}",
        params={"onlyIncludeErrors": "false", "detailed": "true"},
        json={urn: [aspect_name]},
    )
    assert resp.json() == {
        urn: {
            aspect_name: {
                "success": True,
                "primaryStorage": {"writeStatus": "ACTIVE_STATE"},
                "searchStorage": {"writeStatus": "ACTIVE_STATE"},
            }
        }
    }

    # Simulate overwrite
    delete_elasticsearch_trace(trace_id)

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/trace/write/{trace_id}",
        params={"onlyIncludeErrors": "false", "detailed": "true", "skipCache": "true"},
        json={urn: [aspect_name]},
    )
    assert resp.json() == {
        urn: {
            aspect_name: {
                "success": True,
                "primaryStorage": {"writeStatus": "ACTIVE_STATE"},
                "searchStorage": {"writeStatus": "HISTORIC_STATE"},
            }
        }
    }

    # Simulate dropped write
    delete_elasticsearch_system_metadata(urn)

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/trace/write/{trace_id}",
        params={"onlyIncludeErrors": "false", "detailed": "true", "skipCache": "true"},
        json={urn: [aspect_name]},
    )
    assert resp.json() == {
        urn: {
            aspect_name: {
                "success": False,
                "primaryStorage": {"writeStatus": "ACTIVE_STATE"},
                "searchStorage": {
                    "writeStatus": "ERROR",
                    "writeMessage": "Consumer has processed past the offset.",
                },
            }
        }
    }


def test_timeseries_async_write(auth_session):
    urn = generated_urns["apiTraceTimeseries"]
    aspect_name = "datasetProfile"

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v3/entity/dataset",
        params={"async": "true", "systemMetadata": "true"},
        json=[
            {
                "urn": urn,
                aspect_name: {
                    "value": {
                        "timestampMillis": time.time_ns() // 1_000_000,
                        "messageId": "test timeseries",
                        "rowCount": 1,
                    }
                },
            }
        ],
    )

    trace_id = compare_trace_header_system_metadata(
        resp, resp.json()[0][aspect_name]["systemMetadata"]
    )
    wait_for_writes_to_sync()

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/trace/write/{trace_id}",
        params={"onlyIncludeErrors": "false", "detailed": "true"},
        json={urn: [aspect_name]},
    )
    assert resp.json() == {
        urn: {
            aspect_name: {
                "success": True,
                "primaryStorage": {"writeStatus": "NO_OP"},
                "searchStorage": {"writeStatus": "TRACE_NOT_IMPLEMENTED"},
            }
        }
    }


def test_noop_async_write(auth_session):
    urn = generated_urns["apiTraceNoop"]
    aspect_name = "status"

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v3/entity/dataset",
        params={"async": "true", "systemMetadata": "true"},
        json=[{"urn": urn, aspect_name: {"value": {"removed": False}}}],
    )

    trace_id = compare_trace_header_system_metadata(
        resp, resp.json()[0][aspect_name]["systemMetadata"]
    )
    wait_for_writes_to_sync()

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/trace/write/{trace_id}",
        params={"onlyIncludeErrors": "false", "detailed": "true"},
        json={urn: [aspect_name]},
    )
    assert resp.json() == {
        urn: {
            aspect_name: {
                "success": True,
                "primaryStorage": {"writeStatus": "ACTIVE_STATE"},
                "searchStorage": {"writeStatus": "ACTIVE_STATE"},
            }
        }
    }

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v3/entity/dataset",
        params={"async": "true", "systemMetadata": "true"},
        json=[{"urn": urn, aspect_name: {"value": {"removed": False}}}],
    )

    trace_id = compare_trace_header_system_metadata(
        resp, resp.json()[0][aspect_name]["systemMetadata"]
    )
    wait_for_writes_to_sync()

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/trace/write/{trace_id}",
        params={"onlyIncludeErrors": "false", "detailed": "true", "skipCache": "true"},
        json={urn: [aspect_name]},
    )
    assert resp.json() == {
        urn: {
            aspect_name: {
                "success": True,
                "primaryStorage": {"writeStatus": "NO_OP"},
                "searchStorage": {"writeStatus": "NO_OP"},
            }
        }
    }


def test_noop_with_fmcp_async_write(auth_session):
    urn = generated_urns["apiTraceNoopWithFMCP"]
    aspect_name = "status"

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v3/entity/dataset",
        params={"async": "true", "systemMetadata": "true"},
        json=[{"urn": urn, aspect_name: {"value": {"removed": False}}}],
    )

    trace_id = compare_trace_header_system_metadata(
        resp, resp.json()[0][aspect_name]["systemMetadata"]
    )
    wait_for_writes_to_sync()

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/trace/write/{trace_id}",
        params={"onlyIncludeErrors": "false", "detailed": "true"},
        json={urn: [aspect_name]},
    )
    assert resp.json() == {
        urn: {
            aspect_name: {
                "success": True,
                "primaryStorage": {"writeStatus": "ACTIVE_STATE"},
                "searchStorage": {"writeStatus": "ACTIVE_STATE"},
            }
        }
    }

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v3/entity/dataset",
        params={"async": "true", "systemMetadata": "true"},
        json=[
            {
                "urn": urn,
                aspect_name: {
                    "value": {"removed": False},
                    "headers": {"If-Version-Match": "-10000"},
                },
            }
        ],
    )

    trace_id = compare_trace_header_system_metadata(
        resp, resp.json()[0][aspect_name]["systemMetadata"]
    )
    wait_for_writes_to_sync()

    resp = auth_session.post(
        f"{auth_session.gms_url()}/openapi/v1/trace/write/{trace_id}",
        params={"onlyIncludeErrors": "false", "detailed": "true", "skipCache": "true"},
        json={urn: [aspect_name]},
    )
    assert resp.json()[urn][aspect_name]["success"] is False
    assert resp.json()[urn][aspect_name]["primaryStorage"]["writeStatus"] == "ERROR"
    assert (
        resp.json()[urn][aspect_name]["primaryStorage"]["writeExceptions"][0]["message"]
        == "Expected version -10000, actual version 1"
    )
    assert resp.json()[urn][aspect_name]["searchStorage"] == {
        "writeStatus": "ERROR",
        "writeMessage": "Primary storage write failed.",
    }


def compare_trace_header_system_metadata(resp, system_metadata):
    header_trace_id = extract_trace_header(resp)
    system_metadata_trace_id = extract_trace_system_metadata(system_metadata)
    assert header_trace_id.startswith("00-" + system_metadata_trace_id)
    return system_metadata_trace_id


def extract_trace_header(resp):
    assert "traceparent" in resp.headers
    return resp.headers["traceparent"]


def extract_trace_system_metadata(system_metadata):
    assert "properties" in system_metadata
    assert "telemetryTraceId" in system_metadata["properties"]
    return system_metadata["properties"]["telemetryTraceId"]


def delete_elasticsearch_trace(trace_id, timeout=10, refresh_interval=1):
    field_name = "telemetryTraceId"
    index_name = "system_metadata_service_v1"

    update_body = {
        "query": {"term": {field_name: trace_id}},
        "script": {"source": f"ctx._source.remove('{field_name}')"},
    }

    response = es.update_by_query(
        index=index_name,
        body=update_body,
        conflicts="proceed",
        timeout=timeout,
        wait_for_completion=True,
    )

    if response.get("failures"):
        raise Exception(
            f"Update by query operation had failures: {response['failures']}"
        )

    time.sleep(refresh_interval)


def delete_elasticsearch_system_metadata(urn, timeout=10, refresh_interval=1):
    index_name = "system_metadata_service_v1"

    update_body = {"query": {"term": {"urn": urn}}}

    response = es.delete_by_query(
        index=index_name,
        body=update_body,
        conflicts="proceed",
        timeout=timeout,
        wait_for_completion=True,
    )

    if response.get("failures"):
        raise Exception(
            f"Update by query operation had failures: {response['failures']}"
        )

    time.sleep(refresh_interval)
