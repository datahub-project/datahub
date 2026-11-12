"""Strict field projection on get_entities.

`fields` is an allowlist, so the risk is that too little comes back: an entity type's
identity projected away, or something withheld without saying it exists. Both fail
quietly, so these cover the floor and the omitted-fields manifest.
"""

from copy import deepcopy
from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from datahub_core_mcp_tools.entity_cleaning import clean_get_entities_response
from datahub_core_mcp_tools.tools.entities import (
    _ACCEPTED_FIELDS,
    _ADVERTISED_FIELDS,
    _ALWAYS_PRESENT_FIELDS,
    EntityField,
    project_entity,
)

_FIELD_ADAPTER: TypeAdapter[str] = TypeAdapter(EntityField)


DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"


FULL_DATASET: dict[str, Any] = {
    "urn": DATASET_URN,
    "name": "orders",
    "platform": {"name": "snowflake"},
    "url": "https://example.com/dataset",
    "properties": {"description": "Order facts", "qualifiedName": "db.schema.orders"},
    "schemaMetadata": {
        "fields": [
            {"fieldPath": "user_id", "type": "STRING"},
            {"fieldPath": "user_email", "type": "STRING"},
            {"fieldPath": "total_amount", "type": "NUMBER"},
        ]
    },
    "ownership": {"owners": [{"owner": {"urn": "urn:li:corpuser:alice"}}]},
    "tags": {"tags": [{"tag": {"urn": "urn:li:tag:pii"}}]},
    "glossaryTerms": {"terms": [{"term": {"urn": "urn:li:glossaryTerm:revenue"}}]},
    "domain": {"domain": {"urn": "urn:li:domain:sales"}},
    "statsSummary": {"rowCount": 100},
}


def test_named_field_excludes_everything_else():
    out = project_entity(FULL_DATASET, ["ownership"])

    assert out["ownership"] == FULL_DATASET["ownership"]
    # The point of a strict allowlist: naming ownership must not also deliver the
    # schema, which is 41.9% of a payload and the reason the additive form saved little.
    assert "schemaMetadata" not in out
    assert "tags" not in out
    assert "glossaryTerms" not in out


def test_core_identity_always_survives():
    out = project_entity(FULL_DATASET, [])

    for field in ("urn", "name", "platform", "url"):
        assert field in out, f"{field} must survive an empty request"


def test_default_is_identity_plus_properties_without_the_description():
    out = project_entity(FULL_DATASET, None)

    # `properties` stays in the default for a measured reason, not a structural one:
    # removing it was a significant regression on the full suite (19 cases better, 40
    # worse, p=0.0086) because the agent made more calls to recover what it withheld.
    assert set(out) <= {"urn", "name", "platform", "url", "properties", "omittedFields"}
    assert "description" not in out["properties"]
    assert "schemaMetadata" not in out
    assert "ownership" not in out


def test_name_is_lifted_out_of_properties_when_that_is_its_only_home():
    entity = {"urn": DATASET_URN, "properties": {"name": "orders", "description": "x"}}

    # 18% of real entities carry their name only inside `properties`. The default happens
    # to include that block, but a caller passing fields=[] does not get it — and a
    # response that cannot be attributed to an entity is the one thing the core prevents.
    assert project_entity(entity, None)["name"] == "orders"
    assert project_entity(entity, [])["name"] == "orders"


def test_query_entity_keeps_properties_because_its_sql_lives_there():
    query = {
        "urn": "urn:li:query:abc",
        "properties": {"statement": {"value": "SELECT 1"}, "source": "SYSTEM"},
    }

    # For a query, `properties.statement` *is* the entity.
    out = project_entity(query, None)
    assert out["properties"]["statement"]["value"] == "SELECT 1"


def test_description_returned_at_top_level_when_named():
    out = project_entity(FULL_DATASET, ["description"])

    # Surfaced at the top level whichever shape the entity stores it in, so the caller
    # looks in one place regardless of what else was requested.
    assert out["description"] == "Order facts"
    assert "description" not in out.get("properties", {})


def test_omitted_manifest_reports_description_length_in_chars():
    out = project_entity(FULL_DATASET, None)

    # Characters, not tokens: it is what decides whether the description is worth a
    # request, and the caller cannot see it otherwise.
    assert "description (11 chars)" in out["omittedFields"]


def test_projection_does_not_mutate_the_input_entity():
    entity: dict[str, Any] = {
        "urn": DATASET_URN,
        "properties": {"description": "keep me", "qualifiedName": "db.t"},
    }

    project_entity(entity, None)

    # `projected` shares the nested properties dict with the caller, and in get_lineage
    # the same object is counted against the token budget — stripping in place would
    # corrupt both.
    assert entity["properties"]["description"] == "keep me"


def test_omitted_fields_advertises_what_was_withheld():
    out = project_entity(FULL_DATASET, None)
    omitted = out["omittedFields"]

    # A column count is the difference between "there is a schema" and "there is a
    # 133-column schema", which decides whether to name columns or take the whole thing.
    assert "schemaMetadata (3 columns)" in omitted
    assert "ownership" in omitted
    assert "statsSummary" in omitted


def test_empty_values_are_not_advertised():
    entity: dict[str, Any] = {
        "urn": DATASET_URN,
        "ownership": None,
        "tags": {},
        "glossaryTerms": [],
    }

    # Advertising an empty field would send the caller to spend a turn fetching nothing.
    assert "omittedFields" not in project_entity(entity, None)


def _project_columns(columns: list[str]) -> dict:
    # get_entities selects the columns while cleaning, before the token budget.
    cleaned = clean_get_entities_response(
        deepcopy(FULL_DATASET), selected_columns=columns
    )
    return project_entity(cleaned, None, schema_columns=columns)


def test_schema_columns_filters_and_implies_schema():
    out = _project_columns(["user"])

    # schema_columns alone must be enough; requiring fields=["schemaMetadata"] too
    # would make a reasonable-looking call a silent no-op.
    paths = [f["fieldPath"] for f in out["schemaMetadata"]["fields"]]
    assert paths == ["user_email", "user_id"]
    assert out["schemaMetadata"]["matchedColumns"] == 2
    assert out["schemaMetadata"]["totalColumns"] == 3


def test_schema_columns_no_match_is_distinguishable_from_no_schema():
    out = _project_columns(["nonexistent"])

    assert out["schemaMetadata"]["fields"] == []
    # Without this, an empty list is indistinguishable from a dataset that has no
    # schema at all, and the caller cannot tell it simply guessed the wrong name.
    assert "3 available" in out["schemaMetadata"]["note"]


def test_direct_schema_field_keeps_its_aspects():
    field = {
        "urn": (
            "urn:li:schemaField:"
            "(urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD),user_id)"
        ),
        "type": "SCHEMA_FIELD",
        "structuredProperties": [{"name": "Retention", "values": ["30"]}],
        "documentation": [{"text": "The user's identifier.", "origin": "propagated"}],
    }

    # When a schemaField URN is fetched directly its aspects are the substance of the
    # fetch, the same way `subjects` is for a query. The strict default must not
    # project them away — an empty request still returns them.
    out = project_entity(field, [])

    assert out["structuredProperties"] == field["structuredProperties"]
    assert out["documentation"] == field["documentation"]
    assert "omittedFields" not in out


def test_document_body_omission_points_to_grep_documents():
    document = {
        "urn": "urn:li:document:doc1",
        "info": {"title": "Runbook", "contents": {"text": "step one: ..."}},
    }
    out = project_entity(document, None)

    # The body lives in info.contents.text, which the allowlist never keeps. Left alone,
    # an omitted `info` reads as "this document has no body" — so point the caller at the
    # tool that actually returns document content instead of concluding it's missing.
    assert "info" not in out
    assert "info" in out["omittedFields"]
    assert "grep_documents" in out["contentHint"]


def test_naming_info_returns_the_document_body():
    document = {
        "urn": "urn:li:document:doc1",
        "info": {"title": "Runbook", "contents": {"text": "step one: ..."}},
    }
    out = project_entity(document, ["info"])

    # In-product skills tell agents to load their full text this way.
    assert out["info"]["contents"]["text"] == "step one: ..."
    assert "contentHint" not in out


DOCUMENT: dict[str, Any] = {
    "urn": "urn:li:document:doc1",
    "subType": "RUNBOOK",
    "info": {
        "title": "Orders runbook",
        "contents": {"text": "step one: ..."},
        "source": {"sourceType": "EXTERNAL", "externalUrl": "https://example.com/doc"},
        "relatedAssets": [
            {"asset": {"urn": DATASET_URN, "type": "DATASET"}},
            {"asset": {"urn": "urn:li:dashboard:(looker,sales)", "type": "DASHBOARD"}},
        ],
        "parentDocument": {"document": {"urn": "urn:li:document:parent"}},
    },
}


def test_document_identity_and_links_survive_the_projection():
    out = project_entity(DOCUMENT, None)

    # Before the lift, a document came back as urn + subType: no title, and no way to ask
    # for one, since `info` is not a nameable field.
    assert out["name"] == "Orders runbook"
    assert out["relatedAssets"] == [DATASET_URN, "urn:li:dashboard:(looker,sales)"]
    assert out["parentDocument"] == "urn:li:document:parent"
    assert out["source"]["externalUrl"] == "https://example.com/doc"

    # The body still is not lifted.
    assert "info" not in out
    assert "grep_documents" in out["contentHint"]


def test_fully_lifted_info_is_not_advertised_as_omitted():
    document = {
        "urn": "urn:li:document:doc1",
        "info": {
            "title": "Orders runbook",
            "relatedAssets": [{"asset": {"urn": DATASET_URN, "type": "DATASET"}}],
        },
    }
    out = project_entity(document, None)

    # Listing `info` as withheld would send the caller after data they already have.
    assert "omittedFields" not in out


def test_related_assets_are_capped_with_a_total():
    urns = [
        f"urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t{i},PROD)"
        for i in range(40)
    ]
    document = {
        "urn": "urn:li:document:doc1",
        "info": {
            "title": "Everything",
            "relatedAssets": [{"asset": {"urn": u, "type": "DATASET"}} for u in urns],
        },
    }
    out = project_entity(document, None)

    # A truncated list must not read as the whole set, or the caller concludes the rest
    # of the assets do not exist.
    assert len(out["relatedAssets"]) == 25
    assert out["relatedAssetsTotal"] == 40


def test_every_nameable_field_is_actually_honoured():
    entity = {
        "urn": DATASET_URN,
        "deprecation": {"deprecated": True, "note": "use orders_v2"},
        "info": {"type": "FRESHNESS"},
        "runEvents": {"total": 1, "runEvents": []},
        "editableProperties": {"name": "Orders", "description": "curated"},
        "viewProperties": {"logic": "SELECT 1"},
        "parentDomains": {"domains": [{"urn": "urn:li:domain:sales"}]},
        "dataProduct": {
            "relationships": [{"entity": {"urn": "urn:li:dataProduct:dp"}}]
        },
        "documentation": [
            {"text": "AI-inferred table description", "origin": "ai_generated"}
        ],
        "inputOutput": {"inputDatasets": [{"urn": DATASET_URN}]},
    }

    # A name in the enum that the projection then drops is worse than no name at all: the
    # caller asked correctly, got a clean response, and concludes the data does not exist.
    for field in entity:
        if field == "urn":
            continue
        assert field in _ACCEPTED_FIELDS, f"{field} must be nameable"
        assert field in project_entity(entity, [field]), f"{field} must survive"


def test_asking_for_description_also_returns_documentation():
    entity = {
        "urn": DATASET_URN,
        "properties": {"description": "Ingested blurb."},
        "documentation": [
            {"text": "Daily orders, one row per order.", "origin": "ai_generated"}
        ],
    }

    projected = project_entity(entity, ["description"])

    # One question, two aspects. Handing back only `description` would let an agent
    # conclude a propagated-only asset has no description at all.
    assert projected["description"] == "Ingested blurb."
    assert projected["documentation"] == [
        {"text": "Daily orders, one row per order.", "origin": "ai_generated"}
    ]


def test_always_returned_fields_are_accepted_but_not_advertised():
    # 86.7% of rejected requests named something the response already contained, so these
    # have to validate — but advertising them tells the caller to spend a field slot on
    # what it already has. Pydantic's default is that both cannot hold at once: the schema
    # enum is also the validator.
    advertised = _FIELD_ADAPTER.json_schema()["enum"]
    assert advertised == list(_ADVERTISED_FIELDS)

    for field in (
        "subTypes",
        "definition",
        "relatedAssets",
        "parentDocument",
        "siblings",
    ):
        assert _FIELD_ADAPTER.validate_python(field) == field
        assert field not in advertised


def test_invented_names_are_still_rejected():
    # Guesses must still fail, and since the schema no longer lists every accepted name,
    # the validator is the only thing left enforcing that.
    for field in ("subtype", "logicalParent", "relationships", "usage"):
        assert field not in _ACCEPTED_FIELDS
        with pytest.raises(ValidationError):
            _FIELD_ADAPTER.validate_python(field)


def test_always_present_set_matches_what_the_projection_actually_does():
    # Two entities that between them carry every nameable field: the document-only ones
    # are lifted out of `info`, the rest are top-level.
    dataset = {
        "urn": DATASET_URN,
        "description": "d",
        "properties": {"name": "orders"},
        "schemaMetadata": {"fields": []},
        "ownership": {"owners": []},
        "tags": {"tags": []},
        "glossaryTerms": {"terms": []},
        "domain": {"domain": {"urn": "urn:li:domain:sales"}},
        "structuredProperties": {"properties": []},
        "statsSummary": {"rowCount": 1},
        "relatedDocuments": {"total": 1},
        "health": [{"status": "PASS"}],
        "info": {"type": "FRESHNESS"},
        "runEvents": {"total": 1, "runEvents": []},
        "editableProperties": {"description": "curated"},
        "viewProperties": {"logic": "SELECT 1"},
        "parentDomains": {"domains": [{"urn": "urn:li:domain:parent"}]},
        "dataProduct": {"relationships": []},
        "deprecation": {"deprecated": True},
        "subTypes": {"typeNames": ["View"]},
        "definition": {"qualifiedName": "prop"},
        "siblings": ["urn:li:dataset:(urn:li:dataPlatform:dbt,orders,PROD)"],
        "documentation": [
            {"text": "propagated table description", "origin": "propagated"}
        ],
        "inputOutput": {"inputDatasets": [{"urn": DATASET_URN}]},
    }
    document = {
        "urn": "urn:li:document:doc1",
        "info": {
            "title": "Runbook",
            "relatedAssets": [{"asset": {"urn": DATASET_URN, "type": "DATASET"}}],
            "parentDocument": {"document": {"urn": "urn:li:document:parent"}},
        },
    }
    unasked = set(project_entity(dataset, [])) | set(project_entity(document, []))

    # Two kinds of accepted name: the levers the schema advertises and the no-ops it does
    # not. Deriving the split from behaviour rather than memory means promoting a field to
    # core, or lifting a new one, fails here rather than quietly leaving a dead lever.
    for field in _ACCEPTED_FIELDS:
        assert (field in unasked) == (field in _ALWAYS_PRESENT_FIELDS), (
            f"{field}: returned-unasked={field in unasked}, "
            f"listed-as-always-present={field in _ALWAYS_PRESENT_FIELDS}"
        )


GLOSSARY_TERM: dict[str, Any] = {
    "urn": "urn:li:glossaryTerm:revenue",
    "hierarchicalName": "finance.revenue",
    "properties": {"name": "Revenue"},
    "structuredProperties": {"properties": [{"values": [{"stringValue": "high"}]}]},
}


def test_named_fields_that_came_back_empty_are_listed():
    out = project_entity(GLOSSARY_TERM, ["structuredProperties", "ownership", "domain"])

    # Without this list an empty `ownership` reads as "no owner", which is how an agent
    # told a user a term with owners had none.
    assert out["structuredProperties"] == GLOSSARY_TERM["structuredProperties"]
    assert out["notReturnedFields"] == ["ownership", "domain"]


def test_description_found_only_in_documentation_is_not_reported_missing():
    entity = {
        "urn": DATASET_URN,
        "documentation": [{"text": "Daily orders.", "origin": "propagated"}],
    }

    assert "notReturnedFields" not in project_entity(entity, ["description"])
    assert project_entity({"urn": DATASET_URN}, ["description"])[
        "notReturnedFields"
    ] == ["description"]


METRIC_URN = "urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,net_revenue)"


SEMANTIC_MODEL_URN = (
    "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,revenue)"
)


METRIC: dict[str, Any] = {
    "urn": METRIC_URN,
    "type": "METRIC",
    "platform": {"name": "snowflake"},
    "info": {
        "name": "Net Revenue",
        "description": "Recognized revenue net of returns",
        "expression": {
            "dialects": [
                {
                    "dialect": "SNOWFLAKE",
                    "expression": "SUM(amount) - SUM(returns)",
                },
                {"dialect": "ANSI_SQL", "expression": "SUM(amount) - SUM(returns)"},
            ]
        },
    },
    "semanticModel": {
        "urn": SEMANTIC_MODEL_URN,
        "type": "SEMANTIC_MODEL",
        "info": {"name": "Revenue"},
    },
    "metricUpstreams": {
        "datasetUpstreams": [{"destination": {"urn": DATASET_URN, "type": "DATASET"}}]
    },
    "metricRelationships": {
        "derivedFrom": [
            {
                "destination": {
                    "urn": "urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,gross)",
                    "type": "METRIC",
                    "info": {"name": "Gross Revenue"},
                }
            }
        ]
    },
}


SEMANTIC_MODEL: dict[str, Any] = {
    "urn": SEMANTIC_MODEL_URN,
    "type": "SEMANTIC_MODEL",
    "platform": {"name": "snowflake"},
    "info": {
        "name": "Revenue",
        "description": "Orders joined to customers",
        "nativeDefinition": "SELECT amount FROM orders",
        "relationships": [
            {
                "name": "orders_to_customers",
                "from": "orders",
                "fromColumns": ["customer_id"],
                "to": "customers",
                "toColumns": ["id"],
                "cardinality": "N_ONE",
            }
        ],
    },
}


def test_metric_name_is_lifted_and_description_stays_opt_in():
    out = project_entity(METRIC, None)

    # The name's only home is info.name. A default response that cannot name the
    # metric is the bug this projection exists to close.
    assert out["name"] == "Net Revenue"
    assert out["type"] == "METRIC"
    assert "description" not in out
    description = METRIC["info"]["description"]
    assert f"description ({len(description):,} chars)" in out["omittedFields"]
    # Expression and the metric edges stay behind their field names. The edges are
    # unbounded lists, so a default read does not include them.
    assert "info" in out["omittedFields"]
    assert "expression" not in out
    assert "semanticModel" not in out
    assert "metricUpstreams" not in out
    assert "metricRelationships" not in out
    assert "semanticModel" in out["omittedFields"]
    assert "metricUpstreams" in out["omittedFields"]
    assert "metricRelationships" in out["omittedFields"]


def test_metric_info_request_moves_name_and_description_out():
    out = project_entity(METRIC, ["info", "description"])

    assert out["name"] == "Net Revenue"
    assert out["description"] == "Recognized revenue net of returns"
    assert set(out["info"]) == {"expression"}
    assert out["info"]["expression"]["dialects"][0]["dialect"] == "SNOWFLAKE"
    assert len(out["info"]["expression"]["dialects"]) == 2
    assert "notReturnedFields" not in out
    assert "description (" not in " ".join(out.get("omittedFields", []))


def test_assertion_info_description_is_left_in_place():
    entity = {
        "urn": "urn:li:assertion:freshness-orders",
        "info": {"type": "FRESHNESS", "description": "Rows arrive daily"},
    }

    out = project_entity(entity, ["info", "description"])

    # Assertions already store their description in info. The metric lift must
    # not move it, or a caller asking for info loses the text it came for.
    assert out["info"]["description"] == "Rows arrive daily"
    assert out["info"]["type"] == "FRESHNESS"
    assert "description" not in out
    assert out["notReturnedFields"] == ["description"]


def test_schema_columns_keeps_measure_logic_and_the_owning_model():
    entity = {
        "urn": DATASET_URN,
        "subTypes": {"typeNames": ["Semantic Model Dataset"]},
        "semanticModelProperties": {
            "alias": "ORDERS",
            "semanticModel": {
                "urn": SEMANTIC_MODEL_URN,
                "info": {"name": "Orders Model"},
            },
        },
        "schemaMetadata": {
            "fields": [
                {
                    "fieldPath": "discount_pct_applied",
                    "schemaFieldEntity": {
                        "semanticFieldAnnotation": {
                            "type": "MEASURE",
                            "aggregationFunction": "AVG",
                            "expression": {
                                "dialects": [
                                    {
                                        "dialect": "SNOWFLAKE",
                                        "expression": "AVG(ORDERS.discount_pct_applied)",
                                    }
                                ]
                            },
                        }
                    },
                },
                {
                    "fieldPath": "order_id",
                    "schemaFieldEntity": {
                        "semanticFieldAnnotation": {
                            "type": "DIMENSION",
                            "dimension": {"isTime": False},
                        }
                    },
                },
            ]
        },
    }

    cleaned = clean_get_entities_response(entity, selected_columns=["discount"])
    out = project_entity(cleaned, None, schema_columns=["discount"])

    # Filtering columns must not drop the measure expression, and the owning model
    # is identity, so it comes back without being named.
    fields = out["schemaMetadata"]["fields"]
    assert [field["fieldPath"] for field in fields] == ["discount_pct_applied"]
    assert (
        fields[0]["semanticFieldAnnotation"]["expression"]["dialects"][0]["expression"]
        == "AVG(ORDERS.discount_pct_applied)"
    )
    assert out["schemaMetadata"]["matchedColumns"] == 1
    assert out["schemaMetadata"]["totalColumns"] == 2
    assert out["semanticModelProperties"]["alias"] == "ORDERS"
    assert out["semanticModelProperties"]["semanticModel"]["info"]["name"] == (
        "Orders Model"
    )
    assert "semanticModelProperties" not in out.get("omittedFields", [])


def test_ui_description_wins_over_the_ingested_one():
    entity = {
        "urn": DATASET_URN,
        "properties": {"description": "Ingested blurb.", "name": "orders"},
        "editableProperties": {"description": "Curated blurb."},
    }

    out = project_entity(entity, ["description", "properties"])

    # The entity page shows the UI description. Returning the ingested one
    # instead would disagree with that page.
    assert out["description"] == "Curated blurb."
    assert "description" not in out["properties"]
    assert out["properties"]["name"] == "orders"
    assert "editableProperties" not in out


CONTRACT_URN = "urn:li:dataContract:orders-contract"


def test_dataset_contract_is_opt_in():
    dataset = {
        **FULL_DATASET,
        "contract": {"urn": CONTRACT_URN, "status": {"state": "ACTIVE"}},
    }

    assert _FIELD_ADAPTER.validate_python("contract") == "contract"
    default = project_entity(dataset, None)
    assert "contract" not in default
    assert "contract" in default["omittedFields"]
    assert project_entity(dataset, ["contract"])["contract"] == dataset["contract"]


def test_data_contract_returns_its_status_assertions_and_result_by_default():
    contract = {
        "urn": CONTRACT_URN,
        "type": "DATA_CONTRACT",
        "status": {"state": "PENDING"},
        "properties": {
            "entityUrn": DATASET_URN,
            "freshness": [{"assertion": {"urn": "urn:li:assertion:fresh"}}],
        },
        # Cloud's result reads PASSING while a member errors and the state is still
        # PENDING; the docstring tells agents to judge by state and member statuses.
        "result": {
            "type": "PASSING",
            "assertionResults": {"passingCount": 1, "errorCount": 1},
        },
    }

    # The contract's state and members are the whole entity, so a narrow request
    # must not project them away the way it does for other types.
    for fields in (None, [], ["ownership"]):
        out = project_entity(contract, fields)
        assert out["status"] == contract["status"]
        assert out["properties"] == contract["properties"]
        assert out["result"] == contract["result"]
        assert "omittedFields" not in out
