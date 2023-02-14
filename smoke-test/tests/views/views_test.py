import pytest
import time
import tenacity
from tests.utils import (
    delete_urns_from_file,
    get_frontend_url,
    get_gms_url,
    ingest_file_via_rest,
    get_sleep_info,
)

sleep_sec, sleep_times = get_sleep_info()

@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_more_views(frontend_session, list_views_json, query_name, before_count):

    # Get new count of Views
    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=list_views_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"][query_name]["total"] is not None
    assert "errors" not in res_data

    # Assert that there are more views now.
    after_count = res_data["data"][query_name]["total"]
    print(f"after_count is {after_count}")
    assert after_count == before_count + 1
    return after_count

@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_less_views(frontend_session, list_views_json, query_name, before_count):

    # Get new count of Views
    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=list_views_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"][query_name]["total"] is not None
    assert "errors" not in res_data

    # Assert that there are more views now.
    after_count = res_data["data"][query_name]["total"]
    print(f"after_count is {after_count}")
    assert after_count == before_count - 1


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_create_list_delete_global_view(frontend_session):

    # Get count of existing views
    list_global_views_json = {
        "query": """query listGlobalViews($input: ListGlobalViewsInput!) {\n
            listGlobalViews(input: $input) {\n
              start\n
              count\n
              total\n
              views {\n
                urn\n
                viewType\n
                name\n
                description\n
                definition {\n
                  entityTypes\n
                  filter {\n
                    operator\n
                    filters {\n
                      field\n
                      values\n
                      condition\n
                    }\n
                  }\n
                }\n
              }\n
            }\n
        }""",
        "variables": {"input": {"start": "0", "count": "20"}},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=list_global_views_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listGlobalViews"]["total"] is not None
    assert "errors" not in res_data

    before_count = res_data["data"]["listGlobalViews"]["total"]

    new_view_name = "Test View"
    new_view_description = "Test Description"
    new_view_definition = {
      "entityTypes": ["DATASET", "DASHBOARD"],
      "filter": {
        "operator": "AND",
        "filters": [
          {
            "field": "tags",
            "values": ["urn:li:tag:test"],
            "negated": False,
            "condition": "EQUAL"
          }
        ]
      }
    }

    # Create new View
    create_view_json = {
        "query": """mutation createView($input: CreateViewInput!) {\n
            createView(input: $input) {\n
              urn\n
            }\n
        }""",
        "variables": {
            "input": {
                "viewType": "GLOBAL",
                "name": new_view_name,
                "description": new_view_description,
                "definition": new_view_definition
            }
        },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=create_view_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createView"] is not None
    assert "errors" not in res_data

    view_urn = res_data["data"]["createView"]["urn"]

    new_count = _ensure_more_views(
        frontend_session=frontend_session,
        list_views_json=list_global_views_json,
        query_name="listGlobalViews",
        before_count=before_count,
    )

    delete_json = {"urn": view_urn}

    # Delete the View
    delete_view_json = {
        "query": """mutation deleteView($urn: String!) {\n
            deleteView(urn: $urn)
        }""",
        "variables": {
            "urn": view_urn
        },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=delete_view_json
    )
    response.raise_for_status()
    res_data = response.json()
    assert "errors" not in res_data

    _ensure_less_views(
        frontend_session=frontend_session,
        list_views_json=list_global_views_json,
        query_name="listGlobalViews",
        before_count=new_count,
    )


@pytest.mark.dependency(depends=["test_healthchecks", "test_create_list_delete_global_view"])
def test_create_list_delete_personal_view(frontend_session):

    # Get count of existing views
    list_my_views_json = {
        "query": """query listMyViews($input: ListMyViewsInput!) {\n
            listMyViews(input: $input) {\n
              start\n
              count\n
              total\n
              views {\n
                urn\n
                viewType\n
                name\n
                description\n
                definition {\n
                  entityTypes\n
                  filter {\n
                    operator\n
                    filters {\n
                      field\n
                      values\n
                      condition\n
                    }\n
                  }\n
                }\n
              }\n
            }\n
        }""",
        "variables": {"input": {"start": "0", "count": "20"}},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=list_my_views_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listMyViews"]["total"] is not None
    assert "errors" not in res_data

    before_count = res_data["data"]["listMyViews"]["total"]

    new_view_name = "Test View"
    new_view_description = "Test Description"
    new_view_definition = {
      "entityTypes": ["DATASET", "DASHBOARD"],
      "filter": {
        "operator": "AND",
        "filters": [
          {
            "field": "tags",
            "values": ["urn:li:tag:test"],
            "negated": False,
            "condition": "EQUAL"
          }
        ]
      }
    }

    # Create new View
    create_view_json = {
        "query": """mutation createView($input: CreateViewInput!) {\n
            createView(input: $input) {\n
              urn\n
            }\n
        }""",
        "variables": {
            "input": {
                "viewType": "PERSONAL",
                "name": new_view_name,
                "description": new_view_description,
                "definition": new_view_definition
            }
        },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=create_view_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createView"] is not None
    assert "errors" not in res_data

    view_urn = res_data["data"]["createView"]["urn"]

    new_count = _ensure_more_views(
        frontend_session=frontend_session,
        list_views_json=list_my_views_json,
        query_name="listMyViews",
        before_count=before_count,
    )

    # Delete the View
    delete_view_json = {
        "query": """mutation deleteView($urn: String!) {\n
            deleteView(urn: $urn)
        }""",
        "variables": {
            "urn": view_urn
        },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=delete_view_json
    )
    response.raise_for_status()
    res_data = response.json()
    assert "errors" not in res_data

    _ensure_less_views(
        frontend_session=frontend_session,
        list_views_json=list_my_views_json,
        query_name="listMyViews",
        before_count=new_count,
    )

@pytest.mark.dependency(depends=["test_healthchecks", "test_create_list_delete_personal_view"])
def test_update_global_view(frontend_session):

    # First create a view
    new_view_name = "Test View"
    new_view_description = "Test Description"
    new_view_definition = {
      "entityTypes": ["DATASET", "DASHBOARD"],
      "filter": {
        "operator": "AND",
        "filters": [
          {
            "field": "tags",
            "values": ["urn:li:tag:test"],
            "negated": False,
            "condition": "EQUAL"
          }
        ]
      }
    }

    # Create new View
    create_view_json = {
        "query": """mutation createView($input: CreateViewInput!) {\n
            createView(input: $input) {\n
              urn\n
            }\n
        }""",
        "variables": {
            "input": {
                "viewType": "PERSONAL",
                "name": new_view_name,
                "description": new_view_description,
                "definition": new_view_definition
            }
        },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=create_view_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createView"] is not None
    assert "errors" not in res_data

    view_urn = res_data["data"]["createView"]["urn"]

    new_view_name = "New Test View"
    new_view_description = "New Test Description"
    new_view_definition = {
      "entityTypes": ["DATASET", "DASHBOARD", "CHART", "DATA_FLOW"],
      "filter": {
        "operator": "OR",
        "filters": [
          {
            "field": "glossaryTerms",
            "values": ["urn:li:glossaryTerm:test"],
            "negated": True,
            "condition": "CONTAIN"
          }
        ]
      }
    }

    update_view_json = {
        "query": """mutation updateView($urn: String!, $input: UpdateViewInput!) {\n
            updateView(urn: $urn, input: $input) {\n
              urn\n
            }\n
        }""",
        "variables": {
            "urn": view_urn,
            "input": {
                "name": new_view_name,
                "description": new_view_description,
                "definition": new_view_definition
            }
        },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=update_view_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]["updateView"] is not None
    assert "errors" not in res_data

    # Delete the View
    delete_view_json = {
        "query": """mutation deleteView($urn: String!) {\n
            deleteView(urn: $urn)
        }""",
        "variables": {
            "urn": view_urn
        },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=delete_view_json
    )
    response.raise_for_status()
    res_data = response.json()
    assert "errors" not in res_data
