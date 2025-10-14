from typing import Any, Dict

import pytest

from tests.utils import delete_urns_from_file, execute_graphql, ingest_file_via_rest


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client, request):
    print("ingesting test data")
    ingest_file_via_rest(auth_session, "tests/tags_and_terms/data.json")
    yield
    print("removing test data")
    delete_urns_from_file(graph_client, "tests/tags_and_terms/data.json")


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

    add_query = """mutation addTag($input: TagAssociationInput!) {
            addTag(input: $input)
        }"""
    add_variables: Dict[str, Any] = {
        "input": {
            "tagUrn": "urn:li:tag:Legacy",
            "resourceUrn": dataset_urn,
        }
    }

    res_data = execute_graphql(auth_session, add_query, add_variables)
    assert res_data["data"]["addTag"] is True

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

    remove_query = """mutation removeTag($input: TagAssociationInput!) {
            removeTag(input: $input)
        }"""
    remove_variables: Dict[str, Any] = {
        "input": {
            "tagUrn": "urn:li:tag:Legacy",
            "resourceUrn": dataset_urn,
        }
    }

    res_data = execute_graphql(auth_session, remove_query, remove_variables)
    print(res_data)
    assert res_data["data"]["removeTag"] is True

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

    add_mutation = """mutation addTag($input: TagAssociationInput!) {
        addTag(input: $input)
    }"""
    add_variables: Dict[str, Any] = {
        "input": {
            "tagUrn": "urn:li:tag:Legacy",
            "resourceUrn": chart_urn,
        }
    }
    res_data = execute_graphql(auth_session, add_mutation, add_variables)
    assert res_data["data"]["addTag"] is True

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

    remove_mutation = """mutation removeTag($input: TagAssociationInput!) {
        removeTag(input: $input)
    }"""
    remove_variables: Dict[str, Any] = {
        "input": {
            "tagUrn": "urn:li:tag:Legacy",
            "resourceUrn": chart_urn,
        }
    }
    res_data = execute_graphql(auth_session, remove_mutation, remove_variables)
    assert res_data["data"]["removeTag"] is True

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

    add_mutation = """mutation addTerm($input: TermAssociationInput!) {
        addTerm(input: $input)
    }"""
    add_variables: Dict[str, Any] = {
        "input": {
            "termUrn": "urn:li:glossaryTerm:SavingAccount",
            "resourceUrn": dataset_urn,
        }
    }
    res_data = execute_graphql(auth_session, add_mutation, add_variables)
    print(res_data)
    assert res_data["data"]["addTerm"] is True

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

    remove_mutation = """mutation removeTerm($input: TermAssociationInput!) {
        removeTerm(input: $input)
    }"""
    remove_variables: Dict[str, Any] = {
        "input": {
            "termUrn": "urn:li:glossaryTerm:SavingAccount",
            "resourceUrn": dataset_urn,
        }
    }
    res_data = execute_graphql(auth_session, remove_mutation, remove_variables)
    print(res_data)
    assert res_data["data"]["removeTerm"] is True

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

    add_query = """mutation addTag($input: TagAssociationInput!) {\n
            addTag(input: $input)
        }"""
    add_variables: Dict[str, Any] = {
        "input": {
            "tagUrn": "urn:li:tag:Legacy",
            "resourceUrn": dataset_urn,
            "subResource": "[version=2.0].[type=boolean].field_bar",
            "subResourceType": "DATASET_FIELD",
        }
    }
    res_data = execute_graphql(auth_session, add_query, add_variables)
    assert res_data["data"]["addTag"] is True

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

    remove_query = """mutation removeTag($input: TagAssociationInput!) {\n
            removeTag(input: $input)
        }"""
    remove_variables: Dict[str, Any] = {
        "input": {
            "tagUrn": "urn:li:tag:Legacy",
            "resourceUrn": dataset_urn,
            "subResource": "[version=2.0].[type=boolean].field_bar",
            "subResourceType": "DATASET_FIELD",
        }
    }
    res_data = execute_graphql(auth_session, remove_query, remove_variables)
    print(res_data)
    assert res_data["data"]["removeTag"] is True

    # Refetch the dataset
    res_data = execute_graphql(auth_session, query_tags, variables_tags)
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [{"globalTags": {"tags": []}}]
    }

    add_query = """mutation addTerm($input: TermAssociationInput!) {\n
            addTerm(input: $input)
        }"""
    add_variables = {
        "input": {
            "termUrn": "urn:li:glossaryTerm:SavingAccount",
            "resourceUrn": dataset_urn,
            "subResource": "[version=2.0].[type=boolean].field_bar",
            "subResourceType": "DATASET_FIELD",
        }
    }
    res_data = execute_graphql(auth_session, add_query, add_variables)
    assert res_data["data"]["addTerm"] is True

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

    remove_query = """mutation removeTerm($input: TermAssociationInput!) {\n
            removeTerm(input: $input)
        }"""
    remove_variables = {
        "input": {
            "termUrn": "urn:li:glossaryTerm:SavingAccount",
            "resourceUrn": dataset_urn,
            "subResource": "[version=2.0].[type=boolean].field_bar",
            "subResourceType": "DATASET_FIELD",
        }
    }
    res_data = execute_graphql(auth_session, remove_query, remove_variables)
    assert res_data["data"]["removeTerm"] is True

    # Refetch the dataset
    res_data = execute_graphql(auth_session, query_terms, variables_terms)
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [{"glossaryTerms": {"terms": []}}]
    }

    # dataset schema tags
    res_data = execute_graphql(auth_session, query_tags, variables_tags)

    update_description_json = {
        "query": """mutation updateDescription($input: DescriptionUpdateInput!) {\n
            updateDescription(input: $input)
        }""",
        "variables": {
            "input": {
                "description": "new description",
                "resourceUrn": dataset_urn,
                "subResource": "[version=2.0].[type=boolean].field_bar",
                "subResourceType": "DATASET_FIELD",
            }
        },
    }

    # fetch no description
    query_description: str = dataset_schema_json_description["query"]  # type: ignore
    variables_description: Dict[str, Any] = dataset_schema_json_description["variables"]  # type: ignore
    res_data = execute_graphql(auth_session, query_description, variables_description)
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [{"description": None}]
    }

    update_query: str = update_description_json["query"]  # type: ignore
    update_variables: Dict[str, Any] = update_description_json["variables"]  # type: ignore
    res_data = execute_graphql(auth_session, update_query, update_variables)
    assert res_data["data"]["updateDescription"] is True

    # Refetch the dataset
    res_data = execute_graphql(auth_session, query_description, variables_description)
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [{"description": "new description"}]
    }
