import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urns_from_file, ingest_file_via_rest

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-proposal-properties,PROD)"
property_urn = "urn:li:structuredProperty:test-property-to-apply"
already_applied_property_urn = "urn:li:structuredProperty:test-property-already-applied"


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client, request):
    print("ingesting properties test data")
    ingest_file_via_rest(
        auth_session, "tests/proposals/structured_property_proposal_data.json"
    )
    ingest_file_via_rest(
        auth_session, "tests/proposals/structured_property_proposal_data_2.json"
    )
    yield
    print("removing properties test data")
    delete_urns_from_file(
        graph_client, "tests/proposals/structured_property_proposal_data.json"
    )
    delete_urns_from_file(
        graph_client, "tests/proposals/structured_property_proposal_data_2.json"
    )


def execute_gql(auth_session, query, variables=None):
    """
    Helper for sending GraphQL requests via the auth_session's post method.
    Raises an HTTP error on bad status, returns the parsed JSON response on success.
    """
    payload = {"query": query}
    if variables is not None:
        payload["variables"] = variables

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=payload
    )
    response.raise_for_status()

    return response.json()


@pytest.mark.dependency()
def test_complete_entity_structured_property_proposal_accept(
    auth_session, ingest_cleanup_data
):
    """
    1) Propose a structured property on the dataset entity
    2) Accept the newly created proposal
    3) Verify the proposal is now COMPLETED and ACCEPTED in listActionRequests
    4) Verify the property was applied to the dataset
    5) Remove the property to reset for future tests
    """

    # 1) Propose a property
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{"stringValue": "test1"}],
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeStructuredProperties"]
    assert proposal_urn, "Expected a proposal URN"

    # 2) Accept the proposal
    accept_proposals_mutation = """
        mutation acceptProposals($urns: [String!]!, $note: String) {
            acceptProposals(urns: $urns, note: $note)
        }
    """
    variables_accept = {
        "urns": [proposal_urn],
        "note": "Accepting the proposal via test",
    }
    accept_resp = execute_gql(
        auth_session, query=accept_proposals_mutation, variables=variables_accept
    )
    assert "errors" not in accept_resp, f"Errors found: {accept_resp.get('errors')}"
    assert (
        accept_resp["data"]["acceptProposals"] is True
    ), "Expected acceptProposals to return true"

    # Wait again before continuing.
    wait_for_writes_to_sync()

    # 3) Verify listActionRequests -> COMPLETED & ACCEPTED
    list_requests_query = """
        query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            total
            actionRequests {
                urn
                status
                result
            }
        }
        }
    """
    variables_list = {
        "input": {
            "resourceUrn": dataset_urn,
            "type": "STRUCTURED_PROPERTY_ASSOCIATION",
            "status": "COMPLETED",
            "start": 0,
            "count": 1000,
        }
    }
    list_response = execute_gql(
        auth_session, query=list_requests_query, variables=variables_list
    )
    assert "errors" not in list_response, f"Errors found: {list_response.get('errors')}"
    requests = list_response["data"]["listActionRequests"]["actionRequests"]

    # We should find our request with COMPLETED & ACCEPTED
    matching = [r for r in requests if r["urn"] == proposal_urn]
    assert (
        len(matching) == 1
    ), f"Expected to find exactly one request matching {proposal_urn}"
    assert matching[0]["status"] == "COMPLETED"
    assert matching[0]["result"] == "ACCEPTED"

    # 4) Verify the property was applied by fetching the dataset
    dataset_properties_query = """
        query getDatasetProperties($urn: String!) {
            dataset(urn: $urn) {
                urn
                structuredProperties {
                    properties {
                        structuredProperty {
                            urn
                        }
                        values {
                            ... on StringValue {
                                stringValue
                            }
                        }
                    }
                }
            }
        }
    """
    variables_dataset_properties = {"urn": dataset_urn}
    dataset_properties_resp = execute_gql(
        auth_session,
        query=dataset_properties_query,
        variables=variables_dataset_properties,
    )
    assert (
        "errors" not in dataset_properties_resp
    ), f"Errors found: {dataset_properties_resp.get('errors')}"

    # Extract the list of properties from the dataset
    actual_properties = dataset_properties_resp["data"]["dataset"][
        "structuredProperties"
    ]["properties"]

    matching_property = next(
        (
            prop
            for prop in actual_properties
            if prop["structuredProperty"]["urn"] == property_urn
        ),
        None,
    )

    assert (
        matching_property is not None
    ), f"Did not find property urn {property_urn} in entity property set"
    assert len(matching_property["values"]) == 1
    assert matching_property["values"][0]["stringValue"] == "test1"

    # 5) Remove the property to reset for subsequent tests (cleanup step)
    remove_property_mutation = """
        mutation removeStructuredProperties($input: RemoveStructuredPropertiesInput!) {
            removeStructuredProperties(input: $input) {
                properties {
                    associatedUrn
                }
            }
        }
    """
    variables_remove_property = {
        "input": {"assetUrn": dataset_urn, "structuredPropertyUrns": [property_urn]}
    }
    remove_resp = execute_gql(
        auth_session,
        query=remove_property_mutation,
        variables=variables_remove_property,
    )
    assert (
        "errors" not in remove_resp
    ), f"Errors found removing property: {remove_resp.get('errors')}"

    # Optional: verify the property was removed
    dataset_properties_resp_after_removal = execute_gql(
        auth_session,
        query=dataset_properties_query,
        variables=variables_dataset_properties,
    )
    assert (
        "errors" not in dataset_properties_resp_after_removal
    ), f"Errors found: {dataset_properties_resp_after_removal.get('errors')}"
    updated_properties = dataset_properties_resp_after_removal["data"]["dataset"][
        "structuredProperties"
    ]["properties"]
    updated_property_urns = [t["structuredProperty"]["urn"] for t in updated_properties]
    assert (
        property_urn not in updated_property_urns
    ), f"Expected {property_urn} to be removed, but it still appears: {updated_property_urns}"


@pytest.mark.dependency()
def test_complete_entity_structured_property_proposal_overwrite_existing_property(
    auth_session, ingest_cleanup_data
):
    """
    1) Propose a structured property on the dataset entity for a property that already exists
    2) Accept the newly created proposal
    3) Verify the property was applied to the dataset, overwriting existing value
    4) Remove the property to reset for future tests
    """

    # 1) Propose a property
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": already_applied_property_urn,
                    "values": [
                        {"stringValue": "newValue1"},
                        {"stringValue": "newValue2"},
                    ],
                }
            ],
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeStructuredProperties"]
    assert proposal_urn, "Expected a proposal URN"

    # 2) Accept the proposal
    accept_proposals_mutation = """
        mutation acceptProposals($urns: [String!]!, $note: String) {
            acceptProposals(urns: $urns, note: $note)
        }
    """
    variables_accept = {
        "urns": [proposal_urn],
        "note": "Accepting the proposal via test",
    }
    accept_resp = execute_gql(
        auth_session, query=accept_proposals_mutation, variables=variables_accept
    )
    assert "errors" not in accept_resp, f"Errors found: {accept_resp.get('errors')}"
    assert (
        accept_resp["data"]["acceptProposals"] is True
    ), "Expected acceptProposals to return true"

    # 3) Verify the property was applied by fetching the dataset properties
    dataset_properties_query = """
        query getDatasetProperties($urn: String!) {
            dataset(urn: $urn) {
                urn
                structuredProperties {
                    properties {
                        structuredProperty {
                            urn
                        }
                        values {
                            ... on StringValue {
                                stringValue
                            }
                        }
                    }
                }
            }
        }
    """
    variables_dataset_properties = {"urn": dataset_urn}
    dataset_properties_resp = execute_gql(
        auth_session,
        query=dataset_properties_query,
        variables=variables_dataset_properties,
    )
    assert (
        "errors" not in dataset_properties_resp
    ), f"Errors found: {dataset_properties_resp.get('errors')}"

    # Extract the list of properties from the dataset
    actual_properties = dataset_properties_resp["data"]["dataset"][
        "structuredProperties"
    ]["properties"]

    matching_property = next(
        (
            prop
            for prop in actual_properties
            if prop["structuredProperty"]["urn"] == already_applied_property_urn
        ),
        None,
    )

    assert (
        matching_property is not None
    ), "Did not find actual applied property in entity property set"
    assert matching_property["values"] == [
        {"stringValue": "newValue1"},
        {"stringValue": "newValue2"},
    ]

    # 5) Reset the properties to the before values
    reset_property_mutation = """
        mutation upsertStructuredProperties($input: UpsertStructuredPropertiesInput!) {
            upsertStructuredProperties(input: $input) {
                properties {
                    associatedUrn
                }
            }
        }
    """
    variables_reset_property = {
        "input": {
            "assetUrn": dataset_urn,
            "structuredPropertyInputParams": [
                {
                    "structuredPropertyUrn": already_applied_property_urn,
                    "values": [{"stringValue": "test1"}, {"stringValue": "test2"}],
                }
            ],
        }
    }
    remove_resp = execute_gql(
        auth_session,
        query=reset_property_mutation,
        variables=variables_reset_property,
    )
    assert (
        "errors" not in remove_resp
    ), f"Errors found resetting property: {remove_resp.get('errors')}"


@pytest.mark.dependency()
def test_complete_entity_structured_property_proposal_reject(
    auth_session, ingest_cleanup_data
):
    """
    1) Propose a property on the dataset entity
    2) Reject the newly created proposal
    3) Verify the proposal is now COMPLETED and REJECTED in listActionRequests
    4) Confirm that the property was never applied to the Dataset
    """

    # 1) Propose a property
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{"stringValue": "test1"}],
                }
            ],
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeStructuredProperties"]
    assert proposal_urn, "Expected a proposal URN"

    # 2) Reject the proposal
    reject_proposals_mutation = """
    mutation rejectProposals($urns: [String!]!, $note: String) {
        rejectProposals(urns: $urns, note: $note)
    }
    """
    variables_reject = {
        "urns": [proposal_urn],
        "note": "Rejecting the proposal via test",
    }
    reject_resp = execute_gql(
        auth_session, query=reject_proposals_mutation, variables=variables_reject
    )
    assert "errors" not in reject_resp, f"Errors found: {reject_resp.get('errors')}"
    assert (
        reject_resp["data"]["rejectProposals"] is True
    ), "Expected rejectProposals to return true"

    wait_for_writes_to_sync()

    # 3) Verify listActionRequests => COMPLETED & REJECTED
    list_requests_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            actionRequests {
                urn
                status
                result
            }
        }
    }
    """
    variables_list = {
        "input": {
            "resourceUrn": dataset_urn,
            "type": "STRUCTURED_PROPERTY_ASSOCIATION",
            "status": "COMPLETED",
            "start": 0,
            "count": 1000,
        }
    }
    list_response = execute_gql(
        auth_session, query=list_requests_query, variables=variables_list
    )
    assert "errors" not in list_response, f"Errors found: {list_response.get('errors')}"
    requests = list_response["data"]["listActionRequests"]["actionRequests"]
    matching = [r for r in requests if r["urn"] == proposal_urn]
    assert len(matching) == 1, f"Expected exactly one request matching {proposal_urn}"
    assert matching[0]["status"] == "COMPLETED"
    assert matching[0]["result"] == "REJECTED"

    # 4) Confirm that the property was NOT applied to the dataset
    dataset_properties_query = """
        query getDatasetProperties($urn: String!) {
            dataset(urn: $urn) {
                urn
                structuredProperties {
                    properties {
                        structuredProperty {
                            urn
                        }
                        values {
                            ... on StringValue {
                                stringValue
                            }
                        }
                    }
                }
            }
        }
    """
    variables_dataset_properties = {"urn": dataset_urn}
    dataset_properties_resp = execute_gql(
        auth_session,
        query=dataset_properties_query,
        variables=variables_dataset_properties,
    )
    assert (
        "errors" not in dataset_properties_resp
    ), f"Errors found: {dataset_properties_resp.get('errors')}"
    applied_properties = dataset_properties_resp["data"]["dataset"][
        "structuredProperties"
    ]["properties"]
    applied_property_urns = [t["structuredProperty"]["urn"] for t in applied_properties]

    # Since proposal was REJECTED, we expect the property NOT to be applied
    assert property_urn not in applied_property_urns, (
        f"Property {property_urn} should NOT have been applied to Dataset {dataset_urn}, "
        f"but we found: {applied_property_urns}"
    )


@pytest.mark.dependency()
def test_list_action_requests_property_params(auth_session, ingest_cleanup_data):
    """
    1) Propose a property on the dataset entity
    2) List the action requests
    3) Validate that property proposal parameters are correctly resolved
    """

    # 1) Propose a property
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{"stringValue": "test1"}],
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeStructuredProperties"]
    assert proposal_urn, "Expected a proposal URN"

    wait_for_writes_to_sync()

    # 2) Verify listActionRequests => COMPLETED & PENDING
    list_requests_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            actionRequests {
                urn
                status
                result
                params {
                    structuredPropertyProposal {
                        structuredProperties {
                            structuredProperty {
                                urn
                                definition {
                                    qualifiedName
                                }
                            }
                            values {
                                ... on StringValue {
                                    stringValue
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """
    variables_list = {
        "input": {
            "resourceUrn": dataset_urn,
            "type": "STRUCTURED_PROPERTY_ASSOCIATION",
            "status": "PENDING",
            "start": 0,
            "count": 1000,
        }
    }
    list_response = execute_gql(
        auth_session, query=list_requests_query, variables=variables_list
    )
    assert "errors" not in list_response, f"Errors found: {list_response.get('errors')}"
    requests = list_response["data"]["listActionRequests"]["actionRequests"]
    matching = [r for r in requests if r["urn"] == proposal_urn]
    assert len(matching) == 1, f"Expected exactly one request matching {proposal_urn}"
    assert (
        matching[0]["params"]["structuredPropertyProposal"]["structuredProperties"][0][
            "structuredProperty"
        ]["urn"]
        == property_urn
    )
    assert (
        matching[0]["params"]["structuredPropertyProposal"]["structuredProperties"][0][
            "structuredProperty"
        ]["definition"]["qualifiedName"]
        == "test-property-name"
    )
    assert matching[0]["params"]["structuredPropertyProposal"]["structuredProperties"][
        0
    ]["values"] == [{"stringValue": "test1"}]

    # 3) Reject the proposal
    reject_proposals_mutation = """
    mutation rejectProposals($urns: [String!]!, $note: String) {
        rejectProposals(urns: $urns, note: $note)
    }
    """
    variables_reject = {
        "urns": [proposal_urn],
        "note": "Rejecting the proposal via test",
    }
    reject_resp = execute_gql(
        auth_session, query=reject_proposals_mutation, variables=variables_reject
    )
    assert "errors" not in reject_resp, f"Errors found: {reject_resp.get('errors')}"
    assert (
        reject_resp["data"]["rejectProposals"] is True
    ), "Expected rejectProposals to return true"


@pytest.mark.dependency()
def test_complete_schema_field_property_proposal_accept(
    auth_session, ingest_cleanup_data
):
    """
    1) Propose a property on a sub-resource (schema field)
    2) Accept the newly created proposal
    3) Verify the proposal is now COMPLETED & ACCEPTED
    4) Confirm the property was applied to the schema field
    5) Remove the property to reset for subsequent tests
    """
    field_name = "field_bar"

    # 1) Propose a property
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{"stringValue": "test1"}],
                }
            ],
            "description": "Testing accept sub-resource property",
            "subResourceType": "DATASET_FIELD",
            "subResource": field_name,
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeStructuredProperties"]
    assert proposal_urn, "Expected a proposal URN"

    # 2) Accept the proposal
    accept_proposals_mutation = """
    mutation acceptProposals($urns: [String!]!, $note: String) {
      acceptProposals(urns: $urns, note: $note)
    }
    """
    variables_accept = {
        "urns": [proposal_urn],
        "note": "Accepting sub-resource proposal",
    }
    accept_resp = execute_gql(
        auth_session, query=accept_proposals_mutation, variables=variables_accept
    )
    assert "errors" not in accept_resp, f"Errors found: {accept_resp.get('errors')}"
    assert (
        accept_resp["data"]["acceptProposals"] is True
    ), "Expected acceptProposals to return True"

    wait_for_writes_to_sync()

    # 3) Verify listActionRequests => COMPLETED & ACCEPTED
    list_requests_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
      listActionRequests(input: $input) {
        actionRequests {
          urn
          status
          result
        }
      }
    }
    """
    variables_list = {
        "input": {
            "resourceUrn": dataset_urn,
            "type": "STRUCTURED_PROPERTY_ASSOCIATION",
            "status": "COMPLETED",
            "start": 0,
            "count": 1000,
        }
    }
    list_resp = execute_gql(
        auth_session, query=list_requests_query, variables=variables_list
    )
    assert "errors" not in list_resp, f"Errors found: {list_resp.get('errors')}"
    requests = list_resp["data"]["listActionRequests"]["actionRequests"]
    matching = [r for r in requests if r["urn"] == proposal_urn]
    assert len(matching) == 1, f"Expected exactly one proposal matching {proposal_urn}"
    assert matching[0]["status"] == "COMPLETED"
    assert matching[0]["result"] == "ACCEPTED"

    # 4) Confirm the property was applied to the schema field
    dataset_fields_query = """
    query getSchemaFields($urn: String!) {
      dataset(urn: $urn) {
          schemaMetadata {
            fields {
                fieldPath
                schemaFieldEntity {
                    structuredProperties {
                        properties {
                            structuredProperty {
                                urn
                                definition {
                                    qualifiedName
                                }
                            }
                            values {
                                ... on StringValue {
                                    stringValue
                                }
                            }
                        }
                    }
                }
            }
        }
      }
    }
    """
    fields_resp = execute_gql(
        auth_session, query=dataset_fields_query, variables={"urn": dataset_urn}
    )
    assert "errors" not in fields_resp, f"Errors found: {fields_resp.get('errors')}"

    # Find the field matching field_name
    fields = fields_resp["data"]["dataset"]["schemaMetadata"]["fields"]
    field_bar = next(f for f in fields if f["fieldPath"] == field_name)
    applied_properties = field_bar["schemaFieldEntity"]["structuredProperties"][
        "properties"
    ]

    matching_property = next(
        (
            prop
            for prop in applied_properties
            if prop["structuredProperty"]["urn"] == property_urn
        ),
        None,
    )

    assert (
        matching_property is not None
    ), "Did not find actual applied property in entity property set"
    assert matching_property["values"] == [{"stringValue": "test1"}]

    # 5) Remove the property to reset for subsequent tests (cleanup step)
    remove_property_mutation = """
        mutation removeStructuredProperties($input: RemoveStructuredPropertiesInput!) {
            removeStructuredProperties(input: $input) {
                properties {
                    associatedUrn
                }
            }
        }
    """

    schema_field_urn = f"urn:li:schemaField:({dataset_urn},{field_name})"
    variables_remove_property = {
        "input": {
            "assetUrn": schema_field_urn,
            "structuredPropertyUrns": [property_urn],
        }
    }
    remove_resp = execute_gql(
        auth_session,
        query=remove_property_mutation,
        variables=variables_remove_property,
    )
    assert (
        "errors" not in remove_resp
    ), f"Errors found removing property: {remove_resp.get('errors')}"

    # Optional: verify the property is gone
    fields_resp_after_removal = execute_gql(
        auth_session, query=dataset_fields_query, variables={"urn": dataset_urn}
    )
    assert (
        "errors" not in fields_resp_after_removal
    ), f"Errors found: {fields_resp_after_removal.get('errors')}"

    updated_fields = fields_resp_after_removal["data"]["dataset"]["schemaMetadata"][
        "fields"
    ]
    updated_field_bar = next(f for f in updated_fields if f["fieldPath"] == field_name)
    updated_properties = [
        t["structuredProperty"]["urn"]
        for t in updated_field_bar["schemaFieldEntity"]["structuredProperties"][
            "properties"
        ]
    ]
    assert (
        property_urn not in updated_properties
    ), f"Property {property_urn} should have been removed from field {field_name}, but is still there: {updated_properties}"


@pytest.mark.dependency()
def test_complete_schema_field_property_proposal_reject(
    auth_session, ingest_cleanup_data
):
    """
    1) Propose a property on a sub-resource (schema field)
    2) Reject the newly created proposal
    3) Verify the proposal is now COMPLETED & REJECTED
    4) Confirm the property is NOT applied to the schema field
    """
    field_name = "field_bar"

    # 1) Propose a property
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{"stringValue": "test1"}],
                }
            ],
            "description": "Testing accept sub-resource property",
            "subResourceType": "DATASET_FIELD",
            "subResource": field_name,
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeStructuredProperties"]
    assert proposal_urn, "Expected a proposal URN"

    # 2) Reject
    reject_proposals_mutation = """
    mutation rejectProposals($urns: [String!]!, $note: String) {
      rejectProposals(urns: $urns, note: $note)
    }
    """
    variables_reject = {
        "urns": [proposal_urn],
        "note": "Rejecting sub-resource proposal",
    }
    reject_resp = execute_gql(
        auth_session, query=reject_proposals_mutation, variables=variables_reject
    )
    assert "errors" not in reject_resp, f"Errors found: {reject_resp.get('errors')}"
    assert (
        reject_resp["data"]["rejectProposals"] is True
    ), "Expected rejectProposals to return True"

    wait_for_writes_to_sync()

    # 3) Verify listActionRequests => COMPLETED & REJECTED
    list_requests_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
      listActionRequests(input: $input) {
        actionRequests {
          urn
          status
          result
        }
      }
    }
    """
    variables_list = {
        "input": {
            "resourceUrn": dataset_urn,
            "type": "STRUCTURED_PROPERTY_ASSOCIATION",
            "status": "COMPLETED",
            "start": 0,
            "count": 1000,
        }
    }
    list_resp = execute_gql(
        auth_session, query=list_requests_query, variables=variables_list
    )
    assert "errors" not in list_resp, f"Errors found: {list_resp.get('errors')}"
    requests = list_resp["data"]["listActionRequests"]["actionRequests"]
    matching = [r for r in requests if r["urn"] == proposal_urn]
    assert len(matching) == 1, f"Expected exactly one proposal matching {proposal_urn}"
    assert matching[0]["status"] == "COMPLETED"
    assert matching[0]["result"] == "REJECTED"

    # 4) Confirm the property was NOT applied to the schema field
    dataset_fields_query = """
    query getSchemaFields($urn: String!) {
      dataset(urn: $urn) {
          schemaMetadata {
            fields {
                fieldPath
                schemaFieldEntity {
                    structuredProperties {
                        properties {
                            structuredProperty {
                                urn
                                definition {
                                    qualifiedName
                                }
                            }
                            values {
                                ... on StringValue {
                                    stringValue
                                }
                            }
                        }
                    }
                }
            }
        }
      }
    }
    """
    fields_resp = execute_gql(
        auth_session, query=dataset_fields_query, variables={"urn": dataset_urn}
    )
    assert "errors" not in fields_resp, f"Errors found: {fields_resp.get('errors')}"

    fields = fields_resp["data"]["dataset"]["schemaMetadata"]["fields"]
    field_bar = next(f for f in fields if f["fieldPath"] == field_name)
    schema_field_entity = field_bar["schemaFieldEntity"]
    if (
        schema_field_entity is not None
        and schema_field_entity["structuredProperties"] is not None
    ):
        applied_properties = [
            t["structuredProperty"]["urn"]
            for t in schema_field_entity["structuredProperties"]["properties"]
        ]

        # Since it was rejected, we expect not to see the property
        assert (
            property_urn not in applied_properties
        ), f"Property {property_urn} should NOT have been applied, but is present: {applied_properties}"


@pytest.mark.dependency()
def test_propose_property_property_does_not_exist(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a property that doesn't exist
    2) Verify an error is returned containing code and message
    """

    non_existent_property_urn = "urn:li:structuredProperty:non-existent-property-12345"

    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": non_existent_property_urn,
                    "values": [{"stringValue": "test1"}],
                }
            ],
        }
    }
    resp = execute_gql(
        auth_session=auth_session,
        query=propose_properties_mutation,
        variables=variables,
    )
    # We expect an error here
    assert "errors" in resp, "Expected an error for non-existent property"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")
    # Adjust these as needed if your actual server returns a different code / message
    assert error_code == 404, f"Expected 404 status code, got {error_code}"
    assert "does not exist" in error_msg


@pytest.mark.dependency()
def test_propose_property_entity_does_not_exist(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a property for an entity that doesn't exist
    2) Verify an error is returned
    """

    non_existent_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,not-real-entity,PROD)"
    )

    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": non_existent_dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{"stringValue": "test1"}],
                }
            ],
        }
    }
    resp = execute_gql(
        auth_session=auth_session,
        query=propose_properties_mutation,
        variables=variables,
    )
    assert "errors" in resp, "Expected an error for non-existent entity"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")
    # Adjust these as needed
    assert error_code == 404, f"Expected 404, got {error_code}"
    assert "does not exist" in error_msg


@pytest.mark.dependency()
def test_propose_property_property_values_already_applied(
    auth_session, ingest_cleanup_data
):
    """
    1) Propose a property that is already applied on the entity
    2) Expect an error or graceful handling
    """
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": already_applied_property_urn,
                    "values": [{"stringValue": "test1"}, {"stringValue": "test2"}],
                }
            ],
        }
    }
    resp = execute_gql(
        auth_session=auth_session,
        query=propose_properties_mutation,
        variables=variables,
    )

    assert "errors" in resp, "Expected an error for already-applied property"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")
    assert error_code == 400, f"Expected 400, got {error_code}"
    assert "already applied" in error_msg.lower()

    # Wait for kafka
    wait_for_writes_to_sync()


@pytest.mark.dependency()
def test_propose_property_property_values_already_proposed(
    auth_session, ingest_cleanup_data
):
    """
    1) Propose a property
    2) Propose it again while the first proposal is still PENDING
    3) Expect an error or separate pending proposals
    """

    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{"stringValue": "test1"}],
                }
            ],
        }
    }

    # 1) First propose
    first_resp = execute_gql(
        auth_session=auth_session,
        query=propose_properties_mutation,
        variables=variables,
    )
    assert (
        "errors" not in first_resp
    ), f"Unexpected error in first proposal: {first_resp.get('errors')}"
    first_proposal_urn = first_resp["data"]["proposeStructuredProperties"]
    assert first_proposal_urn, "Expected a proposal URN for the first proposal"

    # Wait for kafka
    wait_for_writes_to_sync()

    # 2) Propose again
    second_resp = execute_gql(
        auth_session=auth_session,
        query=propose_properties_mutation,
        variables=variables,
    )

    assert "errors" in second_resp, "Expected an error for already-proposed property"
    error_msg = second_resp["errors"][0]["message"]
    error_code = second_resp["errors"][0].get("extensions", {}).get("code")
    assert error_code == 400, f"Expected 400, got {error_code}"
    assert "already been proposed" in error_msg.lower()

    # 3) Reject the original proposal to clean up
    reject_proposals_mutation = """
    mutation rejectProposals($urns: [String!]!, $note: String) {
        rejectProposals(urns: $urns, note: $note)
    }
    """
    variables_reject = {
        "urns": [first_proposal_urn],
        "note": "Rejecting the proposal via test",
    }
    reject_resp = execute_gql(
        auth_session, query=reject_proposals_mutation, variables=variables_reject
    )
    assert "errors" not in reject_resp, f"Errors found: {reject_resp.get('errors')}"
    assert (
        reject_resp["data"]["rejectProposals"] is True
    ), "Expected rejectProposals to return true"

    # Wait for kafka
    wait_for_writes_to_sync()


@pytest.mark.dependency()
def test_propose_property_malformed_resource_urn(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a property using a malformed resource URN
    2) Expect an error (code=400), with mention of "malformed" or similar in the message
    """
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": "urn:li:invalid-urn",
            "structuredProperties": [
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{"stringValue": "test1"}],
                }
            ],
        }
    }
    resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables
    )
    assert "errors" in resp, "Expected an error for malformed resource URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "invalid resource urn" in error_msg.lower() or "invalid" in error_msg.lower()
    ), f"Expected error message about a malformed URN, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_property_schema_field_no_subresource(
    auth_session, ingest_cleanup_data
):
    """
    1) Provide subResourceType = SCHEMA_FIELD but do NOT supply subResource
    2) Expect an error (code=400), with mention of "subResource" missing
    """
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "subResourceType": "DATASET_FIELD",
            # "subResource": not provided intentionally
            "structuredProperties": [
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{"stringValue": "test1"}],
                }
            ],
        }
    }
    resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables
    )
    assert (
        "errors" in resp
    ), "Expected an error when subResource is missing for subResourceType=DATASET_FIELD"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "subresource field must be provided" in error_msg.lower()
    ), f"Expected error message about missing subResource, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_property_malformed_property_urn(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a malformed property URN
    2) Expect an error (code=400), with mention of "malformed" or "invalid" in the message
    """
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": "urn:li:invalid-urn",
                    "values": [{"stringValue": "test1"}],
                }
            ],
        }
    }
    resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables
    )
    assert "errors" in resp, "Expected an error for malformed property URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "invalid structured property urns" in error_msg.lower()
        or "invalid" in error_msg.lower()
    ), f"Expected error message about malformed or invalid URN, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_property_empty_property_urns(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose an empty property array
    2) Expect an error (code=400)
    """
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [],
        }
    }
    resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables
    )
    assert "errors" in resp, "Expected an error for malformed property URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "no structured properties provided" in error_msg.lower()
        or "invalid" in error_msg.lower()
    ), f"Expected error message about malformed or invalid URN, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_property_empty_property_value_keys(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose property values
    2) Expect an error (code=400)
    """
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{}, {}],
                }
            ],
        }
    }
    resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables
    )
    assert "errors" in resp, "Expected an error for malformed property URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "some properties are missing" in error_msg.lower()
    ), f"Expected error message about malformed or invalid URN, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_property_invalid_property_value_type(
    auth_session, ingest_cleanup_data
):
    """
    1) Attempt to propose an invalid property value type (numeric for string property)
    2) Expect an error (code=400)
    """
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{"numberValue": 0}],
                }
            ],
        }
    }
    resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables
    )
    assert "errors" in resp, "Expected an error for malformed property URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "invalid values" in error_msg.lower()
    ), f"Expected error message about malformed or invalid URN, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_property_same_property_proposed_twice(
    auth_session, ingest_cleanup_data
):
    """
    1) Attempt to propose the same property twice
    2) Expect an error (code=400)
    """
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{"stringValue": "test1"}],
                },
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{"stringValue": "test2"}],
                },
            ],
        }
    }
    resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables
    )
    assert "errors" in resp, "Expected an error for malformed property URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "same structured property" in error_msg.lower()
    ), f"Expected error message about same structured property proposed twice, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_property_single_cardinality_multiple_values(
    auth_session, ingest_cleanup_data
):
    """
    1) Attempt to propose multiple values for property with cardinality SINGLE
    2) Expect an error (code=400)
    """
    propose_properties_mutation = """
        mutation proposeStructuredProperties($input: ProposeStructuredPropertiesInput!) {
            proposeStructuredProperties(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "structuredProperties": [
                {
                    "structuredPropertyUrn": property_urn,
                    "values": [{"stringValue": "test1"}, {"stringValue": "test2"}],
                },
            ],
        }
    }
    resp = execute_gql(
        auth_session, query=propose_properties_mutation, variables=variables
    )
    assert "errors" in resp, "Expected an error for malformed property URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "invalid values" in error_msg.lower()
    ), f"Expected error message about invalid cardinality, but got: {error_msg}"
