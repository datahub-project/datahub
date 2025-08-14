import pytest
import tenacity

from tests.utils import execute_graphql_mutation, execute_graphql_query, get_sleep_info

sleep_sec, sleep_times = get_sleep_info()


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_more_views(auth_session, query, variables, query_name, before_count):
    # Get new count of Views
    res_data = execute_graphql_query(
        auth_session,
        query,
        variables=variables,
        expected_data_key=query_name,
    )

    # Assert that there are more views now.
    after_count = res_data["data"][query_name]["total"]
    print(f"after_count is {after_count}")
    assert after_count == before_count + 1
    return after_count


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_less_views(auth_session, query, variables, query_name, before_count):
    # Get new count of Views
    res_data = execute_graphql_query(
        auth_session,
        query,
        variables=variables,
        expected_data_key=query_name,
    )

    # Assert that there are less views now.
    after_count = res_data["data"][query_name]["total"]
    print(f"after_count is {after_count}")
    assert after_count == before_count - 1


@pytest.mark.dependency()
def test_create_list_delete_global_view(auth_session):
    # Get count of existing views
    list_global_views_query = """query listGlobalViews($input: ListGlobalViewsInput!) {
        listGlobalViews(input: $input) {
          start
          count
          total
          views {
            urn
            viewType
            name
            description
            definition {
              entityTypes
              filter {
                operator
                filters {
                  field
                  values
                  condition
                }
              }
            }
          }
        }
    }"""

    list_global_views_variables = {"input": {"start": 0, "count": 20}}

    res_data = execute_graphql_query(
        auth_session,
        list_global_views_query,
        variables=list_global_views_variables,
        expected_data_key="listGlobalViews",
    )

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
                    "condition": "EQUAL",
                }
            ],
        },
    }

    # Create new View
    create_view_mutation = """mutation createView($input: CreateViewInput!) {
        createView(input: $input) {
          urn
        }
    }"""

    create_view_variables = {
        "input": {
            "viewType": "GLOBAL",
            "name": new_view_name,
            "description": new_view_description,
            "definition": new_view_definition,
        }
    }

    res_data = execute_graphql_mutation(
        auth_session, create_view_mutation, create_view_variables, "createView"
    )

    view_urn = res_data["data"]["createView"]["urn"]

    new_count = _ensure_more_views(
        auth_session=auth_session,
        query=list_global_views_query,
        variables=list_global_views_variables,
        query_name="listGlobalViews",
        before_count=before_count,
    )

    # Delete the View
    delete_view_mutation = """mutation deleteView($urn: String!) {
        deleteView(urn: $urn)
    }"""

    execute_graphql_mutation(
        auth_session, delete_view_mutation, {"urn": view_urn}, "deleteView"
    )

    _ensure_less_views(
        auth_session=auth_session,
        query=list_global_views_query,
        variables=list_global_views_variables,
        query_name="listGlobalViews",
        before_count=new_count,
    )


@pytest.mark.dependency(depends=["test_create_list_delete_global_view"])
def test_create_list_delete_personal_view(auth_session):
    # Get count of existing views
    list_my_views_query = """query listMyViews($input: ListMyViewsInput!) {
        listMyViews(input: $input) {
          start
          count
          total
          views {
            urn
            viewType
            name
            description
            definition {
              entityTypes
              filter {
                operator
                filters {
                  field
                  values
                  condition
                }
              }
            }
          }
        }
    }"""

    list_my_views_variables = {"input": {"start": 0, "count": 20}}

    res_data = execute_graphql_query(
        auth_session,
        list_my_views_query,
        variables=list_my_views_variables,
        expected_data_key="listMyViews",
    )

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
                    "condition": "EQUAL",
                }
            ],
        },
    }

    # Create new View
    create_view_mutation = """mutation createView($input: CreateViewInput!) {
        createView(input: $input) {
          urn
        }
    }"""

    create_view_variables = {
        "input": {
            "viewType": "PERSONAL",
            "name": new_view_name,
            "description": new_view_description,
            "definition": new_view_definition,
        }
    }

    res_data = execute_graphql_mutation(
        auth_session, create_view_mutation, create_view_variables, "createView"
    )

    view_urn = res_data["data"]["createView"]["urn"]

    new_count = _ensure_more_views(
        auth_session=auth_session,
        query=list_my_views_query,
        variables=list_my_views_variables,
        query_name="listMyViews",
        before_count=before_count,
    )

    # Delete the View
    delete_view_mutation = """mutation deleteView($urn: String!) {
            deleteView(urn: $urn)
        }"""

    execute_graphql_mutation(
        auth_session, delete_view_mutation, {"urn": view_urn}, "deleteView"
    )

    _ensure_less_views(
        auth_session=auth_session,
        query=list_my_views_query,
        variables=list_my_views_variables,
        query_name="listMyViews",
        before_count=new_count,
    )


@pytest.mark.dependency(depends=["test_create_list_delete_personal_view"])
def test_update_global_view(auth_session):
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
                    "condition": "EQUAL",
                }
            ],
        },
    }

    # Create new View
    create_view_mutation = """mutation createView($input: CreateViewInput!) {
            createView(input: $input) {
              urn
            }
        }"""

    create_view_variables = {
        "input": {
            "viewType": "PERSONAL",
            "name": new_view_name,
            "description": new_view_description,
            "definition": new_view_definition,
        }
    }

    res_data = execute_graphql_mutation(
        auth_session, create_view_mutation, create_view_variables, "createView"
    )

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
                    "condition": "CONTAIN",
                }
            ],
        },
    }

    update_view_mutation = """mutation updateView($urn: String!, $input: UpdateViewInput!) {
            updateView(urn: $urn, input: $input) {
              urn
            }
        }"""

    update_view_variables = {
        "urn": view_urn,
        "input": {
            "name": new_view_name,
            "description": new_view_description,
            "definition": new_view_definition,
        },
    }

    res_data = execute_graphql_mutation(
        auth_session, update_view_mutation, update_view_variables, "updateView"
    )

    assert res_data["data"]["updateView"] is not None

    # Delete the View
    delete_view_mutation = """mutation deleteView($urn: String!) {
            deleteView(urn: $urn)
        }"""

    execute_graphql_mutation(
        auth_session, delete_view_mutation, {"urn": view_urn}, "deleteView"
    )
