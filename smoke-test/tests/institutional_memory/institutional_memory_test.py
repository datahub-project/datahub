import pytest

from tests.utils import delete_urns_from_file, get_admin_username, ingest_file_via_rest


@pytest.fixture(scope="function", autouse=True)
def ingest_cleanup_data(auth_session, graph_client, request):
    print("ingesting institutional memory test data")
    ingest_file_via_rest(auth_session, "tests/institutional_memory/data.json")
    yield
    print("removing institutional memory test data")
    delete_urns_from_file(graph_client, "tests/institutional_memory/data.json")


TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,institutional-memory-sample-dataset,PROD)"

QUERY_LIST = """
query dataset($urn: String!) {\n
  dataset(urn: $urn) {\n
    institutionalMemory {\n
      elements {\n
        url\n
        label\n
        created {\n
          actor\n
          time\n
        }\n
        updated {\n
          actor\n
          time\n
        }\n
        settings {\n
          showInAssetPreview\n
        }\n
      }\n
    }\n
  }\n
}"""

MUTATION_ADD = """
mutation addLink($input: AddLinkInput!) {\n
  addLink(input: $input)\n
}"""

MUTATION_UPDATE = """
mutation updateLink($input: UpdateLinkInput!) {\n
  updateLink(input: $input)\n
}"""

MUTATION_REMOVE = """
mutation removeLink($input: RemoveLinkInput!) {\n
  removeLink(input: $input)\n
}"""

MUTATION_UPSERT = """
mutation upsertLink($input: UpsertLinkInput!) {\n
  upsertLink(input: $input)\n
}"""

ADMIN_USERNAME = get_admin_username()


def execute_query(auth_session, query, variables):
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql",
        json={"query": query, "variables": variables},
    )
    response.raise_for_status()
    res_data = response.json()

    return res_data


def test_get_institutional_memory(auth_session):
    res_data = execute_query(auth_session, QUERY_LIST, {"urn": TEST_DATASET_URN})

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]["institutionalMemory"] == {
        "elements": [
            {
                "url": "https://www.linkedin.com",
                "label": "Sample doc",
                "created": {
                    "actor": "urn:li:corpuser:jdoe",
                    "time": 1581407189000,
                },
                "updated": None,
                "settings": None,
            }
        ],
    }


def test_add_institutional_memory(auth_session):
    res_data = execute_query(
        auth_session,
        MUTATION_ADD,
        {
            "input": {
                "linkUrl": "http://example.com",
                "label": "example",
                "resourceUrn": TEST_DATASET_URN,
                "settings": {"showInAssetPreview": True},
            }
        },
    )

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["addLink"]

    res_data = execute_query(auth_session, QUERY_LIST, {"urn": TEST_DATASET_URN})

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    res_data["data"]["dataset"]["institutionalMemory"]["elements"][1]["created"][
        "time"
    ] = 0
    assert res_data["data"]["dataset"]["institutionalMemory"] == {
        "elements": [
            {
                "url": "https://www.linkedin.com",
                "label": "Sample doc",
                "created": {
                    "actor": "urn:li:corpuser:jdoe",
                    "time": 1581407189000,
                },
                "updated": None,
                "settings": None,
            },
            {
                "url": "http://example.com",
                "label": "example",
                "created": {
                    "actor": f"urn:li:corpuser:{ADMIN_USERNAME}",
                    "time": 0,
                },
                "updated": None,
                "settings": {"showInAssetPreview": True},
            },
        ],
    }


def test_update_institutional_memory(auth_session):
    res_data = execute_query(
        auth_session,
        MUTATION_UPDATE,
        {
            "input": {
                "currentUrl": "https://www.linkedin.com",
                "currentLabel": "Sample doc",
                "linkUrl": "http://example.com",
                "label": "example",
                "resourceUrn": TEST_DATASET_URN,
                "settings": {"showInAssetPreview": True},
            }
        },
    )

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["updateLink"]

    res_data = execute_query(auth_session, QUERY_LIST, {"urn": TEST_DATASET_URN})

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    res_data["data"]["dataset"]["institutionalMemory"]["elements"][0]["updated"][
        "time"
    ] = 0
    assert res_data["data"]["dataset"]["institutionalMemory"] == {
        "elements": [
            {
                "url": "http://example.com",
                "label": "example",
                "created": {
                    "actor": "urn:li:corpuser:jdoe",
                    "time": 1581407189000,
                },
                "updated": {
                    "actor": f"urn:li:corpuser:{ADMIN_USERNAME}",
                    "time": 0,
                },
                "settings": {"showInAssetPreview": True},
            }
        ],
    }


def test_remove_institutional_memory(auth_session):
    res_data = execute_query(
        auth_session,
        MUTATION_REMOVE,
        {
            "input": {
                "linkUrl": "https://www.linkedin.com",
                "label": "Sample doc",
                "resourceUrn": TEST_DATASET_URN,
            }
        },
    )

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["removeLink"]

    res_data = execute_query(auth_session, QUERY_LIST, {"urn": TEST_DATASET_URN})

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]["institutionalMemory"] == {"elements": []}


def test_upsert_institutional_memory(auth_session):
    res_data = execute_query(
        auth_session,
        MUTATION_UPSERT,
        {
            "input": {
                "linkUrl": "https://www.linkedin.com",
                "label": "Sample doc",
                "resourceUrn": TEST_DATASET_URN,
                "settings": {
                    "showInAssetPreview": True,
                },
            }
        },
    )

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["upsertLink"]

    res_data = execute_query(
        auth_session,
        MUTATION_UPSERT,
        {
            "input": {
                "linkUrl": "https://example.com",
                "label": "example",
                "resourceUrn": TEST_DATASET_URN,
                "settings": {
                    "showInAssetPreview": True,
                },
            }
        },
    )

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["upsertLink"]

    res_data = execute_query(auth_session, QUERY_LIST, {"urn": TEST_DATASET_URN})

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    res_data["data"]["dataset"]["institutionalMemory"]["elements"][0]["updated"][
        "time"
    ] = 0
    res_data["data"]["dataset"]["institutionalMemory"]["elements"][1]["created"][
        "time"
    ] = 0
    assert res_data["data"]["dataset"]["institutionalMemory"] == {
        "elements": [
            {
                "url": "https://www.linkedin.com",
                "label": "Sample doc",
                "created": {
                    "actor": "urn:li:corpuser:jdoe",
                    "time": 1581407189000,
                },
                "updated": {
                    "actor": f"urn:li:corpuser:{ADMIN_USERNAME}",
                    "time": 0,
                },
                "settings": {"showInAssetPreview": True},
            },
            {
                "url": "https://example.com",
                "label": "example",
                "created": {
                    "actor": f"urn:li:corpuser:{ADMIN_USERNAME}",
                    "time": 0,
                },
                "updated": None,
                "settings": {"showInAssetPreview": True},
            },
        ],
    }
