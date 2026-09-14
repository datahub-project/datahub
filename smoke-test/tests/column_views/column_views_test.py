"""GraphQL smoke tests for Column Views (saved column layouts for the dataset Schema tab).

Mirrors tests/views/views_test.py: create / list / update / delete for PERSONAL and GLOBAL
views, the per-user and organization default settings, the aspect validator's rejections, and
the server-side relationship-preview contract.
"""

import logging
from typing import Any, Dict, List, Optional

import pytest

from tests.utilities.domains import Domain
from tests.utils import execute_graphql, with_test_retry

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

TARGET = "DATASET_SCHEMA_FIELDS"

LIST_MY_QUERY = """query listMyColumnViews($input: ListMyColumnViewsInput!) {
  listMyColumnViews(input: $input) {
    start
    count
    total
    columnViews {
      urn
      name
      viewType
      target
    }
  }
}"""

LIST_GLOBAL_QUERY = """query listGlobalColumnViews($input: ListGlobalColumnViewsInput!) {
  listGlobalColumnViews(input: $input) {
    start
    count
    total
    columnViews {
      urn
      name
      viewType
      target
    }
  }
}"""

GET_QUERY = """query columnView($urn: String!) {
  columnView(urn: $urn) {
    urn
    name
    description
    viewType
    target
    definition {
      columns {
        type
        structuredPropertyParams { structuredProperty { urn } }
        labelParams { label { urn } }
        display { width maxItems overflow expand labelStyle }
      }
      sort { column { type } order }
      filter { operator filters { field values condition negated } }
    }
  }
}"""

CREATE_MUTATION = """mutation createColumnView($input: CreateColumnViewInput!) {
  createColumnView(input: $input) { urn }
}"""

UPDATE_MUTATION = """mutation updateColumnView($urn: String!, $input: UpdateColumnViewInput!) {
  updateColumnView(urn: $urn, input: $input) { urn }
}"""

DELETE_MUTATION = """mutation deleteColumnView($urn: String!) {
  deleteColumnView(urn: $urn)
}"""

USER_DEFAULT_MUTATION = """mutation updateCorpUserColumnViewsSettings(
  $input: UpdateCorpUserColumnViewsSettingsInput!
) {
  updateCorpUserColumnViewsSettings(input: $input)
}"""

GLOBAL_DEFAULT_MUTATION = """mutation updateGlobalColumnViewsSettings(
  $input: UpdateGlobalColumnViewsSettingsInput!
) {
  updateGlobalColumnViewsSettings(input: $input)
}"""

ME_DEFAULTS_QUERY = """query me {
  me {
    corpUser {
      settings {
        columnViews {
          defaults { target view { urn } }
        }
      }
    }
  }
}"""

RELATIONSHIPS_QUERY = """query columnViewRelationships($input: ColumnViewRelationshipsInput!) {
  columnViewRelationships(input: $input) {
    fieldUrn
    total
    totalIsCapped
    related { urn entityType fieldPath datasetUrn datasetName platformUrn }
  }
}"""

LIST_VARIABLES: Dict[str, Any] = {"input": {"start": 0, "count": 50, "target": TARGET}}


def _basic_definition() -> Dict[str, Any]:
    return {
        "columns": [
            {"type": "TYPE"},
            {"type": "DESCRIPTION", "display": {"width": 320}},
            {"type": "LABEL", "labelParams": {"urn": "urn:li:tag:test"}},
            {"type": "GLOSSARY_TERMS"},
            {"type": "LOGICAL_PARENT", "display": {"maxItems": 3, "overflow": "COUNT"}},
            # Field-attribute kinds: native type, its derived length / precision-scale, and the
            # three constraint flags, each an independent column.
            {"type": "NATIVE_TYPE"},
            {"type": "LENGTH"},
            {"type": "PRECISION_SCALE"},
            {"type": "NULLABLE"},
            {"type": "PRIMARY_KEY"},
            {"type": "PARTITION_KEY"},
        ],
        "sort": {"column": {"type": "TYPE"}, "order": "ASCENDING"},
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


def _create(auth_session, view_type: str, name: str, definition: Dict[str, Any]) -> str:
    res = execute_graphql(
        auth_session,
        CREATE_MUTATION,
        {
            "input": {
                "viewType": view_type,
                "name": name,
                "description": f"{name} description",
                "target": TARGET,
                "definition": definition,
            }
        },
    )
    assert res["data"]["createColumnView"] is not None
    return res["data"]["createColumnView"]["urn"]


def _delete(auth_session, urn: str, no_sync_wait: bool = False) -> None:
    execute_graphql(
        auth_session, DELETE_MUTATION, {"urn": urn}, no_sync_wait=no_sync_wait
    )


def _total(auth_session, query: str, query_name: str) -> int:
    res = execute_graphql(auth_session, query, LIST_VARIABLES)
    total = res["data"][query_name]["total"]
    assert total is not None
    return total


@with_test_retry()
def _ensure_total(auth_session, query: str, query_name: str, expected: int) -> None:
    assert _total(auth_session, query, query_name) == expected


def _errors(res: Dict[str, Any]) -> List[str]:
    return [e.get("message", "") for e in (res.get("errors") or [])]


def _expect_rejected(auth_session, definition: Dict[str, Any], why: str) -> None:
    res = execute_graphql(
        auth_session,
        CREATE_MUTATION,
        {
            "input": {
                "viewType": "PERSONAL",
                "name": f"should fail: {why}",
                "target": TARGET,
                "definition": definition,
            }
        },
        expect_errors=True,
        no_sync_wait=True,
    )
    assert _errors(res), f"expected the validator to reject: {why}"
    created: Optional[Dict[str, Any]] = (res.get("data") or {}).get("createColumnView")
    if created:  # belt and braces: never leave a bad view behind
        _delete(auth_session, created["urn"], no_sync_wait=True)
        pytest.fail(f"validator accepted an invalid definition: {why}")


def test_create_list_delete_personal_column_view(auth_session):
    before = _total(auth_session, LIST_MY_QUERY, "listMyColumnViews")

    urn = _create(
        auth_session, "PERSONAL", "Smoke personal column view", _basic_definition()
    )
    _ensure_total(auth_session, LIST_MY_QUERY, "listMyColumnViews", before + 1)

    # The definition round-trips: leaf kinds, label urn, display hints, sort and filter.
    res = execute_graphql(auth_session, GET_QUERY, {"urn": urn})
    view = res["data"]["columnView"]
    assert view["viewType"] == "PERSONAL"
    assert view["target"] == TARGET
    columns = view["definition"]["columns"]
    assert [c["type"] for c in columns] == [
        "TYPE",
        "DESCRIPTION",
        "LABEL",
        "GLOSSARY_TERMS",
        "LOGICAL_PARENT",
        "NATIVE_TYPE",
        "LENGTH",
        "PRECISION_SCALE",
        "NULLABLE",
        "PRIMARY_KEY",
        "PARTITION_KEY",
    ]
    assert columns[1]["display"]["width"] == 320
    assert columns[2]["labelParams"]["label"]["urn"] == "urn:li:tag:test"
    assert columns[4]["display"]["maxItems"] == 3
    assert view["definition"]["sort"] == {
        "column": {"type": "TYPE"},
        "order": "ASCENDING",
    }
    assert view["definition"]["filter"]["filters"][0]["field"] == "tags"

    _delete(auth_session, urn)
    _ensure_total(auth_session, LIST_MY_QUERY, "listMyColumnViews", before)


def test_create_list_delete_global_column_view(auth_session):
    before = _total(auth_session, LIST_GLOBAL_QUERY, "listGlobalColumnViews")

    # The admin session holds MANAGE_GLOBAL_VIEWS, which GLOBAL column views reuse.
    urn = _create(
        auth_session, "GLOBAL", "Smoke global column view", _basic_definition()
    )
    _ensure_total(auth_session, LIST_GLOBAL_QUERY, "listGlobalColumnViews", before + 1)

    _delete(auth_session, urn)
    _ensure_total(auth_session, LIST_GLOBAL_QUERY, "listGlobalColumnViews", before)


def test_update_column_view(auth_session):
    urn = _create(auth_session, "PERSONAL", "Smoke update me", _basic_definition())

    updated_definition = {
        "columns": [
            {"type": "DESCRIPTION"},
            {"type": "TYPE"},
            {"type": "UPSTREAM_COLUMNS"},
            {"type": "DOWNSTREAM_COLUMNS"},
        ],
        "sort": {"column": {"type": "DESCRIPTION"}, "order": "DESCENDING"},
    }
    res = execute_graphql(
        auth_session,
        UPDATE_MUTATION,
        {
            "urn": urn,
            "input": {
                "name": "Smoke updated",
                "description": "updated",
                "definition": updated_definition,
            },
        },
    )
    assert res["data"]["updateColumnView"]["urn"] == urn

    res = execute_graphql(auth_session, GET_QUERY, {"urn": urn})
    view = res["data"]["columnView"]
    assert view["name"] == "Smoke updated"
    assert [c["type"] for c in view["definition"]["columns"]] == [
        "DESCRIPTION",
        "TYPE",
        "UPSTREAM_COLUMNS",
        "DOWNSTREAM_COLUMNS",
    ]
    assert view["definition"]["sort"]["order"] == "DESCENDING"
    assert view["definition"]["filter"] is None

    _delete(auth_session, urn, no_sync_wait=True)


def test_validator_rejects_invalid_definitions(auth_session):
    _expect_rejected(
        auth_session,
        {"columns": [{"type": "TYPE"}, {"type": "TYPE"}]},
        "duplicate column",
    )
    _expect_rejected(
        auth_session,
        {
            "columns": [{"type": "TYPE"}, {"type": "PHYSICAL_CHILDREN"}],
            "sort": {"column": {"type": "PHYSICAL_CHILDREN"}, "order": "ASCENDING"},
        },
        "sort by a relationship column",
    )
    _expect_rejected(
        auth_session,
        {
            "columns": [
                {"type": "LOGICAL_PARENT"},
                {"type": "PHYSICAL_CHILDREN"},
                {"type": "UPSTREAM_COLUMNS"},
                {"type": "DOWNSTREAM_COLUMNS"},
            ]
        },
        "more than three relationship columns",
    )
    _expect_rejected(
        auth_session,
        {"columns": [{"type": "STRUCTURED_PROPERTY"}]},
        "STRUCTURED_PROPERTY without structuredPropertyParams",
    )
    _expect_rejected(
        auth_session,
        {
            "columns": [
                {
                    "type": "LABEL",
                    "labelParams": {
                        "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,not_a_label,PROD)"
                    },
                }
            ]
        },
        "LABEL pointing at a non-label urn",
    )
    _expect_rejected(
        auth_session,
        {"columns": [{"type": "TYPE", "display": {"maxItems": 0}}]},
        "display.maxItems out of bounds",
    )
    _expect_rejected(
        auth_session,
        {
            "columns": [{"type": "TYPE"}],
            "filter": {
                "operator": "AND",
                "filters": [
                    {
                        "field": "PHYSICAL_CHILDREN",
                        "values": ["x"],
                        "condition": "EQUAL",
                    }
                ],
            },
        },
        "filter on a relationship column",
    )


def test_default_column_view_settings(auth_session):
    personal_urn = _create(
        auth_session, "PERSONAL", "Smoke personal default", _basic_definition()
    )
    global_urn = _create(
        auth_session, "GLOBAL", "Smoke org default", _basic_definition()
    )
    try:
        # Personal default: set, read back through `me`, clear.
        res = execute_graphql(
            auth_session,
            USER_DEFAULT_MUTATION,
            {"input": {"target": TARGET, "defaultView": personal_urn}},
        )
        assert res["data"]["updateCorpUserColumnViewsSettings"] is True

        res = execute_graphql(auth_session, ME_DEFAULTS_QUERY)
        defaults = res["data"]["me"]["corpUser"]["settings"]["columnViews"]["defaults"]
        assert [d["view"]["urn"] for d in defaults if d["target"] == TARGET] == [
            personal_urn
        ]

        res = execute_graphql(
            auth_session, USER_DEFAULT_MUTATION, {"input": {"target": TARGET}}
        )
        assert res["data"]["updateCorpUserColumnViewsSettings"] is True
        res = execute_graphql(auth_session, ME_DEFAULTS_QUERY)
        settings = res["data"]["me"]["corpUser"]["settings"]["columnViews"]
        assert [d for d in settings["defaults"] if d["target"] == TARGET] == []

        # Organization default must be a GLOBAL view: a PERSONAL one is rejected...
        res = execute_graphql(
            auth_session,
            GLOBAL_DEFAULT_MUTATION,
            {"input": {"target": TARGET, "defaultView": personal_urn}},
            expect_errors=True,
            no_sync_wait=True,
        )
        assert _errors(res), "a PERSONAL view must not become the organization default"

        # ...a GLOBAL one is accepted, then cleared.
        res = execute_graphql(
            auth_session,
            GLOBAL_DEFAULT_MUTATION,
            {"input": {"target": TARGET, "defaultView": global_urn}},
        )
        assert res["data"]["updateGlobalColumnViewsSettings"] is True
        res = execute_graphql(
            auth_session, GLOBAL_DEFAULT_MUTATION, {"input": {"target": TARGET}}
        )
        assert res["data"]["updateGlobalColumnViewsSettings"] is True

        # A default pointing at a view that does not exist is rejected up front.
        res = execute_graphql(
            auth_session,
            USER_DEFAULT_MUTATION,
            {
                "input": {
                    "target": TARGET,
                    "defaultView": "urn:li:dataHubColumnView:does-not-exist",
                }
            },
            expect_errors=True,
            no_sync_wait=True,
        )
        assert _errors(res)
    finally:
        _delete(auth_session, personal_urn, no_sync_wait=True)
        _delete(auth_session, global_urn, no_sync_wait=True)


def test_relationship_preview_contract(auth_session):
    # Only schemaField urns are accepted: this endpoint is not a generic relationship probe.
    res = execute_graphql(
        auth_session,
        RELATIONSHIPS_QUERY,
        {
            "input": {
                "fieldUrns": [
                    "urn:li:dataset:(urn:li:dataPlatform:hive,anything,PROD)"
                ],
                "column": "PHYSICAL_CHILDREN",
            }
        },
        expect_errors=True,
        no_sync_wait=True,
    )
    assert _errors(res)

    # Non-relationship kinds are rejected too.
    res = execute_graphql(
        auth_session,
        RELATIONSHIPS_QUERY,
        {
            "input": {
                "fieldUrns": [
                    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,x,PROD),col)"
                ],
                "column": "TYPE",
            }
        },
        expect_errors=True,
        no_sync_wait=True,
    )
    assert _errors(res)

    # A field with no edges yields an empty, uncapped preview rather than an error.
    field_urn = "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,smoke_no_such_dataset,PROD),col)"
    res = execute_graphql(
        auth_session,
        RELATIONSHIPS_QUERY,
        {
            "input": {
                "fieldUrns": [field_urn],
                "column": "DOWNSTREAM_COLUMNS",
                "count": 500,
            }
        },
        no_sync_wait=True,
    )
    previews = res["data"]["columnViewRelationships"]
    assert len(previews) == 1
    assert previews[0]["fieldUrn"] == field_urn
    assert previews[0]["total"] == 0
    assert previews[0]["totalIsCapped"] is False
    assert previews[0]["related"] == []
