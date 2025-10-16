from typing import Any, Dict

import pytest

from conftest import _ingest_cleanup_data_impl
from tests.utilities.metadata_operations import (
    add_tag,
    add_term,
    remove_tag,
    remove_term,
    update_description,
)
from tests.utils import execute_graphql


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client, "tests/tags_and_terms/data.json", "tags_and_terms"
    )


def test_add_tag(auth_session):
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-tags-terms-sample-kafka"
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    dataset_query = """query getDataset($urn: String!) {
            dataset(urn: $urn) {
                globalTags {
                    tags {
                        tag {
                            urn
                            name
                            description
                        }
                    }
                }
            }
        }"""
    dataset_variables: Dict[str, Any] = {"urn": dataset_urn}

    # Fetch tags
    res_data = execute_graphql(auth_session, dataset_query, dataset_variables)
    assert res_data["data"]["dataset"]["globalTags"] is None

    assert add_tag(auth_session, dataset_urn, "urn:li:tag:Legacy")

    # Refetch the dataset
    res_data = execute_graphql(auth_session, dataset_query, dataset_variables)
    assert res_data["data"]["dataset"]["globalTags"] == {
        "tags": [
            {
                "tag": {
                    "description": "Indicates the dataset is no longer supported",
                    "name": "Legacy",
                    "urn": "urn:li:tag:Legacy",
                }
            }
        ]
    }

    assert remove_tag(auth_session, dataset_urn, "urn:li:tag:Legacy")

    # Refetch the dataset
    res_data = execute_graphql(auth_session, dataset_query, dataset_variables)
    assert res_data["data"]["dataset"]["globalTags"] == {"tags": []}


def test_add_tag_to_chart(auth_session):
    chart_urn = "urn:li:chart:(looker,test-tags-terms-sample-chart)"

    chart_query = """query getChart($urn: String!) {
        chart(urn: $urn) {
            globalTags {
                tags {
                    tag {
                        urn
                        name
                        description
                    }
                }
            }
        }
    }"""
    chart_variables: Dict[str, Any] = {"urn": chart_urn}

    # Fetch tags
    res_data = execute_graphql(auth_session, chart_query, chart_variables)
    assert res_data["data"]["chart"]["globalTags"] is None

    assert add_tag(auth_session, chart_urn, "urn:li:tag:Legacy")

    # Refetch the chart
    res_data = execute_graphql(auth_session, chart_query, chart_variables)
    assert res_data["data"]["chart"]["globalTags"] == {
        "tags": [
            {
                "tag": {
                    "description": "Indicates the dataset is no longer supported",
                    "name": "Legacy",
                    "urn": "urn:li:tag:Legacy",
                }
            }
        ]
    }

    assert remove_tag(auth_session, chart_urn, "urn:li:tag:Legacy")

    # Refetch the chart
    res_data = execute_graphql(auth_session, chart_query, chart_variables)
    assert res_data["data"]["chart"]["globalTags"] == {"tags": []}


def test_add_term(auth_session):
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-tags-terms-sample-kafka"
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    dataset_query = """query getDataset($urn: String!) {
        dataset(urn: $urn) {
            glossaryTerms {
                terms {
                    term {
                        urn
                        name
                    }
                }
            }
        }
    }"""
    dataset_variables: Dict[str, Any] = {"urn": dataset_urn}

    # Fetch the terms
    res_data = execute_graphql(auth_session, dataset_query, dataset_variables)
    assert res_data["data"]["dataset"]["glossaryTerms"] is None

    assert add_term(auth_session, dataset_urn, "urn:li:glossaryTerm:SavingAccount")

    # Refetch the dataset
    res_data = execute_graphql(auth_session, dataset_query, dataset_variables)
    assert res_data["data"]["dataset"]["glossaryTerms"] == {
        "terms": [
            {
                "term": {
                    "name": "SavingAccount",
                    "urn": "urn:li:glossaryTerm:SavingAccount",
                }
            }
        ]
    }

    assert remove_term(auth_session, dataset_urn, "urn:li:glossaryTerm:SavingAccount")

    # Refetch the dataset
    res_data = execute_graphql(auth_session, dataset_query, dataset_variables)
    assert res_data["data"]["dataset"]["glossaryTerms"] == {"terms": []}


def test_update_schemafield(auth_session):
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-tags-terms-sample-kafka"
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    dataset_schema_json_terms = {
        "query": """query getDataset($urn: String!) {\n
            dataset(urn: $urn) {\n
                urn\n
                name\n
                description\n
                platform {\n
                    urn\n
                }\n
                editableSchemaMetadata {\n
                    editableSchemaFieldInfo {\n
                        glossaryTerms {\n
                            terms {\n
                                term {\n
                                    urn\n
                                    name\n
                                }\n
                            }\n
                        }\n
                    }\n
                }\n
            }\n
        }""",
        "variables": {"urn": dataset_urn},
    }

    dataset_schema_json_tags = {
        "query": """query getDataset($urn: String!) {\n
            dataset(urn: $urn) {\n
                urn\n
                name\n
                description\n
                platform {\n
                    urn\n
                }\n
                editableSchemaMetadata {\n
                    editableSchemaFieldInfo {\n
                        globalTags {\n
                            tags {\n
                                tag {\n
                                    urn\n
                                    name\n
                                    description\n
                                }\n
                            }\n
                        }\n
                    }\n
                }\n
            }\n
        }""",
        "variables": {"urn": dataset_urn},
    }

    dataset_schema_json_description = {
        "query": """query getDataset($urn: String!) {\n
            dataset(urn: $urn) {\n
                urn\n
                name\n
                description\n
                platform {\n
                    urn\n
                }\n
                editableSchemaMetadata {\n
                    editableSchemaFieldInfo {\n
                      description\n
                    }\n
                }\n
            }\n
        }""",
        "variables": {"urn": dataset_urn},
    }

    # dataset schema tags
    query_tags: str = dataset_schema_json_tags["query"]  # type: ignore
    variables_tags: Dict[str, Any] = dataset_schema_json_tags["variables"]  # type: ignore
    res_data = execute_graphql(auth_session, query_tags, variables_tags)
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] is None

    assert add_tag(
        auth_session,
        dataset_urn,
        "urn:li:tag:Legacy",
        sub_resource="[version=2.0].[type=boolean].field_bar",
        sub_resource_type="DATASET_FIELD",
    )

    # Refetch the dataset schema
    res_data = execute_graphql(auth_session, query_tags, variables_tags)
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [
            {
                "globalTags": {
                    "tags": [
                        {
                            "tag": {
                                "description": "Indicates the dataset is no longer supported",
                                "name": "Legacy",
                                "urn": "urn:li:tag:Legacy",
                            }
                        }
                    ]
                }
            }
        ]
    }

    assert remove_tag(
        auth_session,
        dataset_urn,
        "urn:li:tag:Legacy",
        sub_resource="[version=2.0].[type=boolean].field_bar",
        sub_resource_type="DATASET_FIELD",
    )

    # Refetch the dataset
    res_data = execute_graphql(auth_session, query_tags, variables_tags)
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [{"globalTags": {"tags": []}}]
    }

    assert add_term(
        auth_session,
        dataset_urn,
        "urn:li:glossaryTerm:SavingAccount",
        sub_resource="[version=2.0].[type=boolean].field_bar",
        sub_resource_type="DATASET_FIELD",
    )

    # Refetch the dataset schema
    query_terms: str = dataset_schema_json_terms["query"]  # type: ignore
    variables_terms: Dict[str, Any] = dataset_schema_json_terms["variables"]  # type: ignore
    res_data = execute_graphql(auth_session, query_terms, variables_terms)
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [
            {
                "glossaryTerms": {
                    "terms": [
                        {
                            "term": {
                                "name": "SavingAccount",
                                "urn": "urn:li:glossaryTerm:SavingAccount",
                            }
                        }
                    ]
                }
            }
        ]
    }

    assert remove_term(
        auth_session,
        dataset_urn,
        "urn:li:glossaryTerm:SavingAccount",
        sub_resource="[version=2.0].[type=boolean].field_bar",
        sub_resource_type="DATASET_FIELD",
    )

    # Refetch the dataset
    res_data = execute_graphql(auth_session, query_terms, variables_terms)
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [{"glossaryTerms": {"terms": []}}]
    }

    # dataset schema tags
    res_data = execute_graphql(auth_session, query_tags, variables_tags)

    # fetch no description
    query_description: str = dataset_schema_json_description["query"]  # type: ignore
    variables_description: Dict[str, Any] = dataset_schema_json_description["variables"]  # type: ignore
    res_data = execute_graphql(auth_session, query_description, variables_description)
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [{"description": None}]
    }

    assert update_description(
        auth_session,
        dataset_urn,
        "new description",
        sub_resource="[version=2.0].[type=boolean].field_bar",
        sub_resource_type="DATASET_FIELD",
    )

    # Refetch the dataset
    res_data = execute_graphql(auth_session, query_description, variables_description)
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [{"description": "new description"}]
    }
