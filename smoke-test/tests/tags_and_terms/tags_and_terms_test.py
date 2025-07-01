import pytest

from tests.utils import delete_urns_from_file, ingest_file_via_rest


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

    dataset_json = {
        "query": """query getDataset($urn: String!) {\n
            dataset(urn: $urn) {\n
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
        }""",
        "variables": {"urn": dataset_urn},
    }

    # Fetch tags
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["globalTags"] is None

    add_json = {
        "query": """mutation addTag($input: TagAssociationInput!) {\n
            addTag(input: $input)
        }""",
        "variables": {
            "input": {
                "tagUrn": "urn:li:tag:Legacy",
                "resourceUrn": dataset_urn,
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=add_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["addTag"] is True

    # Refetch the dataset
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
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

    remove_json = {
        "query": """mutation removeTag($input: TagAssociationInput!) {\n
            removeTag(input: $input)
        }""",
        "variables": {
            "input": {
                "tagUrn": "urn:li:tag:Legacy",
                "resourceUrn": dataset_urn,
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=remove_json
    )
    response.raise_for_status()
    res_data = response.json()

    print(res_data)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["removeTag"] is True

    # Refetch the dataset
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["globalTags"] == {"tags": []}


def test_add_tag_to_chart(auth_session):
    chart_urn = "urn:li:chart:(looker,test-tags-terms-sample-chart)"

    chart_json = {
        "query": """query getChart($urn: String!) {\n
            chart(urn: $urn) {\n
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
        }""",
        "variables": {"urn": chart_urn},
    }

    # Fetch tags
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=chart_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["chart"]
    assert res_data["data"]["chart"]["globalTags"] is None

    add_json = {
        "query": """mutation addTag($input: TagAssociationInput!) {\n
            addTag(input: $input)
        }""",
        "variables": {
            "input": {
                "tagUrn": "urn:li:tag:Legacy",
                "resourceUrn": chart_urn,
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=add_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["addTag"] is True

    # Refetch the dataset
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=chart_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["chart"]
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

    remove_json = {
        "query": """mutation removeTag($input: TagAssociationInput!) {\n
            removeTag(input: $input)
        }""",
        "variables": {
            "input": {
                "tagUrn": "urn:li:tag:Legacy",
                "resourceUrn": chart_urn,
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=remove_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["removeTag"] is True

    # Refetch the dataset
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=chart_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["chart"]
    assert res_data["data"]["chart"]["globalTags"] == {"tags": []}


def test_add_term(auth_session):
    platform = "urn:li:dataPlatform:kafka"
    dataset_name = "test-tags-terms-sample-kafka"
    env = "PROD"
    dataset_urn = f"urn:li:dataset:({platform},{dataset_name},{env})"

    dataset_json = {
        "query": """query getDataset($urn: String!) {\n
            dataset(urn: $urn) {\n
                glossaryTerms {\n
                    terms {\n
                        term {\n
                            urn\n
                            name\n
                        }\n
                    }\n
                }\n
            }\n
        }""",
        "variables": {"urn": dataset_urn},
    }

    # Fetch the terms
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["glossaryTerms"] is None

    add_json = {
        "query": """mutation addTerm($input: TermAssociationInput!) {\n
            addTerm(input: $input)
        }""",
        "variables": {
            "input": {
                "termUrn": "urn:li:glossaryTerm:SavingAccount",
                "resourceUrn": dataset_urn,
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=add_json
    )
    response.raise_for_status()
    res_data = response.json()

    print(res_data)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["addTerm"] is True

    # Refetch the dataset
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
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

    remove_json = {
        "query": """mutation removeTerm($input: TermAssociationInput!) {\n
            removeTerm(input: $input)
        }""",
        "variables": {
            "input": {
                "termUrn": "urn:li:glossaryTerm:SavingAccount",
                "resourceUrn": dataset_urn,
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=remove_json
    )
    response.raise_for_status()
    res_data = response.json()

    print(res_data)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["removeTerm"] is True
    # Refetch the dataset
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
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
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_schema_json_tags
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] is None

    add_json = {
        "query": """mutation addTag($input: TagAssociationInput!) {\n
            addTag(input: $input)
        }""",
        "variables": {
            "input": {
                "tagUrn": "urn:li:tag:Legacy",
                "resourceUrn": dataset_urn,
                "subResource": "[version=2.0].[type=boolean].field_bar",
                "subResourceType": "DATASET_FIELD",
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=add_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["addTag"] is True

    # Refetch the dataset schema
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_schema_json_tags
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
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

    remove_json = {
        "query": """mutation removeTag($input: TagAssociationInput!) {\n
            removeTag(input: $input)
        }""",
        "variables": {
            "input": {
                "tagUrn": "urn:li:tag:Legacy",
                "resourceUrn": dataset_urn,
                "subResource": "[version=2.0].[type=boolean].field_bar",
                "subResourceType": "DATASET_FIELD",
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=remove_json
    )
    response.raise_for_status()
    res_data = response.json()

    print(res_data)

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["removeTag"] is True

    # Refetch the dataset
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_schema_json_tags
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [{"globalTags": {"tags": []}}]
    }

    add_json = {
        "query": """mutation addTerm($input: TermAssociationInput!) {\n
            addTerm(input: $input)
        }""",
        "variables": {
            "input": {
                "termUrn": "urn:li:glossaryTerm:SavingAccount",
                "resourceUrn": dataset_urn,
                "subResource": "[version=2.0].[type=boolean].field_bar",
                "subResourceType": "DATASET_FIELD",
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=add_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["addTerm"] is True

    # Refetch the dataset schema
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_schema_json_terms
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
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

    remove_json = {
        "query": """mutation removeTerm($input: TermAssociationInput!) {\n
            removeTerm(input: $input)
        }""",
        "variables": {
            "input": {
                "termUrn": "urn:li:glossaryTerm:SavingAccount",
                "resourceUrn": dataset_urn,
                "subResource": "[version=2.0].[type=boolean].field_bar",
                "subResourceType": "DATASET_FIELD",
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=remove_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["removeTerm"] is True

    # Refetch the dataset
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_schema_json_terms
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [{"glossaryTerms": {"terms": []}}]
    }

    # dataset schema tags
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_schema_json_tags
    )
    response.raise_for_status()
    res_data = response.json()

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
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql",
        json=dataset_schema_json_description,
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [{"description": None}]
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=update_description_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["updateDescription"] is True

    # Refetch the dataset
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql",
        json=dataset_schema_json_description,
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["editableSchemaMetadata"] == {
        "editableSchemaFieldInfo": [{"description": "new description"}]
    }
