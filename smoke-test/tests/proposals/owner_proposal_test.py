import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.proposals.test_utils import execute_gql
from tests.utils import delete_urns_from_file, ingest_file_via_rest

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-proposal-owners,PROD)"
user_urn = "urn:li:corpuser:test-user"
already_applied_user_urn = "urn:li:corpuser:test-user-already-applied"
group_urn = "urn:li:corpuser:test-group"


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client, request):
    print("ingesting owners test data")
    ingest_file_via_rest(auth_session, "tests/proposals/owner_proposal_data.json")
    yield
    print("removing owners test data")
    delete_urns_from_file(graph_client, "tests/proposals/owner_proposal_data.json")


@pytest.mark.dependency()
def test_complete_entity_owner_proposal_accept(auth_session, ingest_cleanup_data):
    """
    1) Propose an owner on the dataset entity
    2) Accept the newly created proposal
    3) Verify the proposal is now COMPLETED and ACCEPTED in listActionRequests
    4) Verify the owner was applied to the dataset
    5) Remove the owner to reset for future tests
    """

    # 1) Propose an owner
    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "owners": [
                {
                    "ownerUrn": user_urn,
                    "ownerEntityType": "CORP_USER",
                    "type": "TECHNICAL_OWNER",
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_owners_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeOwners"]
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
            "type": "OWNER_ASSOCIATION",
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

    # 4) Verify the owner was applied by fetching the dataset
    dataset_owners_query = """
        query getDatasetOwners($urn: String!) {
            dataset(urn: $urn) {
                urn
                ownership {
                    owners {
                        owner {
                            ... on CorpUser {
                                urn
                                properties {
                                    email
                                }
                            }
                        }
                        type
                        ownershipType {
                            urn
                            info {
                                name
                            }
                        }
                    }
                }
            }
        }
    """
    variables_dataset_owners = {"urn": dataset_urn}
    dataset_properties_resp = execute_gql(
        auth_session,
        query=dataset_owners_query,
        variables=variables_dataset_owners,
    )
    assert (
        "errors" not in dataset_properties_resp
    ), f"Errors found: {dataset_properties_resp.get('errors')}"

    # Extract the list of owners from the dataset
    actual_owners = dataset_properties_resp["data"]["dataset"]["ownership"]["owners"]

    matching_owner = next(
        (prop for prop in actual_owners if prop["owner"]["urn"] == user_urn),
        None,
    )

    assert (
        matching_owner is not None
    ), f"Did not find owner with urn {user_urn} in entity owners set"
    assert matching_owner["type"] == "TECHNICAL_OWNER"

    # 5) Remove the owner to reset for subsequent tests (cleanup step)
    remove_owner_mutation = """
        mutation removeOwner($input: RemoveOwnerInput!) {
            removeOwner(input: $input)
        }
    """
    variables_remove_owner = {
        "input": {"ownerUrn": user_urn, "resourceUrn": dataset_urn}
    }
    remove_resp = execute_gql(
        auth_session,
        query=remove_owner_mutation,
        variables=variables_remove_owner,
    )
    assert (
        "errors" not in remove_resp
    ), f"Errors found removing owner: {remove_resp.get('errors')}"


@pytest.mark.dependency()
def test_complete_entity_owner_propose_new_ownership_type(
    auth_session, ingest_cleanup_data
):
    """
    1) Propose an owner on the dataset entity for a owner that already exists with a different ownership type
    2) Accept the newly created proposal
    3) Verify the owner was applied to the dataset, keeping the previus value as well
    4) Remove the owner to reset for future tests
    """

    # 1) Propose an owner with a new type
    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "owners": [
                {
                    "ownerUrn": already_applied_user_urn,
                    "ownerEntityType": "CORP_USER",
                    "type": "CUSTOM",
                    "ownershipTypeUrn": "urn:li:ownershipType:test-custom",
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_owners_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeOwners"]
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

    # 3) Verify the owner was applied by fetching the dataset
    dataset_owners_query = """
        query getDatasetOwners($urn: String!) {
            dataset(urn: $urn) {
                urn
                ownership {
                    owners {
                        owner {
                            ... on CorpUser {
                                urn
                                properties {
                                    email
                                }
                            }
                        }
                        type
                        ownershipType {
                            urn
                            info {
                                name
                            }
                        }
                    }
                }
            }
        }
    """
    variables_dataset_owners = {"urn": dataset_urn}
    dataset_properties_resp = execute_gql(
        auth_session,
        query=dataset_owners_query,
        variables=variables_dataset_owners,
    )
    assert (
        "errors" not in dataset_properties_resp
    ), f"Errors found: {dataset_properties_resp.get('errors')}"

    # Extract the list of owners from the dataset
    actual_owners = dataset_properties_resp["data"]["dataset"]["ownership"]["owners"]

    matching_owners = [
        owner
        for owner in actual_owners
        if owner["owner"]["urn"] == already_applied_user_urn
    ]

    assert len(matching_owners) == 2, "Expected to find 2 owners, but did not"
    assert matching_owners[0]["type"] == "BUSINESS_OWNER"
    assert matching_owners[1]["type"] == "CUSTOM"
    assert (
        matching_owners[1]["ownershipType"]["urn"] == "urn:li:ownershipType:test-custom"
    )
    assert matching_owners[1]["ownershipType"]["info"]["name"] == "Custom"

    # 5) Reset the owners to the before values
    remove_owner_mutation = """
        mutation removeOwner($input: RemoveOwnerInput!) {
            removeOwner(input: $input)
        }
    """
    variables_remove_owner = {
        "input": {"ownerUrn": user_urn, "resourceUrn": dataset_urn}
    }
    remove_resp = execute_gql(
        auth_session,
        query=remove_owner_mutation,
        variables=variables_remove_owner,
    )
    assert (
        "errors" not in remove_resp
    ), f"Errors found removing owner: {remove_resp.get('errors')}"


@pytest.mark.dependency()
def test_complete_entity_owner_proposal_reject(auth_session, ingest_cleanup_data):
    """
    1) Propose an owner on the dataset entity
    2) Reject the newly created proposal
    3) Verify the proposal is now COMPLETED and REJECTED in listActionRequests
    4) Confirm that the owner was never applied to the Dataset
    """

    # 1) Propose an owner
    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "owners": [
                {
                    "ownerUrn": user_urn,
                    "ownerEntityType": "CORP_USER",
                    "type": "TECHNICAL_OWNER",
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_owners_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeOwners"]
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
            "type": "OWNER_ASSOCIATION",
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

    # 4) Confirm that the owner was NOT applied to the dataset
    dataset_owners_query = """
        query getDatasetOwners($urn: String!) {
            dataset(urn: $urn) {
                urn
                ownership {
                    owners {
                        owner {
                            ... on CorpUser {
                                urn
                                properties {
                                    email
                                }
                            }
                        }
                        type
                        ownershipType {
                            urn
                            info {
                                name
                            }
                        }
                    }
                }
            }
        }
    """
    variables_dataset_properties = {"urn": dataset_urn}
    dataset_owners_resp = execute_gql(
        auth_session,
        query=dataset_owners_query,
        variables=variables_dataset_properties,
    )
    assert (
        "errors" not in dataset_owners_resp
    ), f"Errors found: {dataset_owners_resp.get('errors')}"
    applied_owners = dataset_owners_resp["data"]["dataset"]["ownership"]["owners"]
    applied_owner_urns = [t["owner"]["urn"] for t in applied_owners]

    # Since proposal was REJECTED, we expect the owner NOT to be applied
    assert user_urn not in applied_owner_urns, (
        f"User with urn {user_urn} should NOT have been applied to Dataset {dataset_urn}, "
        f"but we found: {applied_owner_urns}"
    )


@pytest.mark.dependency()
def test_list_action_requests_owner_params(auth_session, ingest_cleanup_data):
    """
    1) Propose an owner on the dataset entity
    2) List the action requests
    3) Validate that owner proposal parameters are correctly resolved
    """

    # 1) Propose an owner
    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "owners": [
                {
                    "ownerUrn": user_urn,
                    "ownerEntityType": "CORP_USER",
                    "type": "TECHNICAL_OWNER",
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_owners_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeOwners"]
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
                    ownerProposal {
                        owners {
                            owner {
                                ... on CorpUser {
                                    urn
                                    properties {
                                        email
                                    }
                                }
                            }
                            type
                            ownershipType {
                                urn
                                info {
                                    name
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
            "type": "OWNER_ASSOCIATION",
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

    proposal_owner = matching[0]["params"]["ownerProposal"]["owners"][0]

    assert proposal_owner["owner"]["urn"] == user_urn
    assert proposal_owner["owner"]["properties"]["email"] == "test@test.com"
    assert proposal_owner["type"] == "TECHNICAL_OWNER"
    assert proposal_owner["ownershipType"]["info"]["name"] == "Technical Owner"

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
def test_propose_owner_owner_does_not_exist(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose an owner that doesn't exist
    2) Verify an error is returned containing code and message
    """

    non_existent_owner_urn = "urn:li:corpuser:non-existent-user"

    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "owners": [
                {
                    "ownerUrn": non_existent_owner_urn,
                    "ownerEntityType": "CORP_USER",
                    "type": "TECHNICAL_OWNER",
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    resp = execute_gql(
        auth_session=auth_session,
        query=propose_owners_mutation,
        variables=variables,
    )
    # We expect an error here
    assert "errors" in resp, "Expected an error for non-existent owner"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")
    # Adjust these as needed if your actual server returns a different code / message
    assert error_code == 404, f"Expected 404 status code, got {error_code}"
    assert "does not exist" in error_msg


@pytest.mark.dependency()
def test_propose_owner_entity_does_not_exist(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose an owner for an entity that doesn't exist
    2) Verify an error is returned
    """

    non_existent_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,not-real-entity,PROD)"
    )

    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": non_existent_dataset_urn,
            "owners": [
                {
                    "ownerUrn": user_urn,
                    "ownerEntityType": "CORP_USER",
                    "type": "TECHNICAL_OWNER",
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    resp = execute_gql(
        auth_session=auth_session,
        query=propose_owners_mutation,
        variables=variables,
    )
    assert "errors" in resp, "Expected an error for non-existent entity"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")
    # Adjust these as needed
    assert error_code == 404, f"Expected 404, got {error_code}"
    assert "does not exist" in error_msg


@pytest.mark.dependency()
def test_propose_owner_owner_and_type_already_applied(
    auth_session, ingest_cleanup_data
):
    """
    1) Propose an owner + type that is already applied on the entity
    2) Expect an error or graceful handling
    """
    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "owners": [
                {
                    "ownerUrn": already_applied_user_urn,
                    "ownerEntityType": "CORP_USER",
                    "type": "BUSINESS_OWNER",
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    resp = execute_gql(
        auth_session=auth_session,
        query=propose_owners_mutation,
        variables=variables,
    )

    assert "errors" in resp, "Expected an error for already-applied owner"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")
    assert error_code == 400, f"Expected 400, got {error_code}"
    assert "already applied" in error_msg.lower()

    # Wait for kafka
    wait_for_writes_to_sync()


@pytest.mark.dependency()
def test_propose_owner_owner_and_type_already_proposed(
    auth_session, ingest_cleanup_data
):
    """
    1) Propose an owner + type
    2) Propose it again while the first proposal is still PENDING
    3) Expect an error or separate pending proposals
    """
    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "owners": [
                {
                    "ownerUrn": user_urn,
                    "ownerEntityType": "CORP_USER",
                    "type": "TECHNICAL_OWNER",
                }
            ],
            "description": "Proposing for accept test",
        }
    }

    # 1) First propose
    first_resp = execute_gql(
        auth_session=auth_session,
        query=propose_owners_mutation,
        variables=variables,
    )
    assert (
        "errors" not in first_resp
    ), f"Unexpected error in first proposal: {first_resp.get('errors')}"
    first_proposal_urn = first_resp["data"]["proposeOwners"]
    assert first_proposal_urn, "Expected a proposal URN for the first proposal"

    # Wait for kafka
    wait_for_writes_to_sync()

    # 2) Propose again
    second_resp = execute_gql(
        auth_session=auth_session,
        query=propose_owners_mutation,
        variables=variables,
    )

    assert "errors" in second_resp, "Expected an error for already-proposed owner"
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
def test_propose_owner_malformed_resource_urn(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose an owner using a malformed resource URN
    2) Expect an error (code=400), with mention of "malformed" or similar in the message
    """
    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": "urn:li:invalid-urn",
            "owners": [
                {
                    "ownerUrn": user_urn,
                    "ownerEntityType": "CORP_USER",
                    "type": "TECHNICAL_OWNER",
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    resp = execute_gql(auth_session, query=propose_owners_mutation, variables=variables)
    assert "errors" in resp, "Expected an error for malformed resource URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "invalid resource urn" in error_msg.lower() or "invalid" in error_msg.lower()
    ), f"Expected error message about a malformed URN, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_owner_ownership_type_does_not_exist(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose an owner with an ownership type that doesn't exist
    2) Verify an error is returned
    """

    non_existent_ownership_type = "urn:li:ownershipType:my-invalid-ownership-type"

    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "owners": [
                {
                    "ownerUrn": user_urn,
                    "ownerEntityType": "CORP_USER",
                    "type": "CUSTOM",
                    "ownershipTypeUrn": non_existent_ownership_type,
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    resp = execute_gql(
        auth_session=auth_session,
        query=propose_owners_mutation,
        variables=variables,
    )
    assert "errors" in resp, "Expected an error for non-existent entity"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")
    # Adjust these as needed
    assert error_code == 404, f"Expected 404, got {error_code}"
    assert "does not exist" in error_msg


@pytest.mark.dependency()
def test_propose_owner_malformed_ownership_type_urn(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a malformed ownership type URN
    2) Expect an error (code=400), with mention of "malformed" or "invalid" in the message
    """

    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "owners": [
                {
                    "ownerUrn": user_urn,
                    "ownerEntityType": "CORP_USER",
                    "type": "CUSTOM",
                    "ownershipTypeUrn": "urn:li:invalid-urn",
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    resp = execute_gql(auth_session, query=propose_owners_mutation, variables=variables)
    assert "errors" in resp, "Expected an error for malformed ownership type URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "invalid ownership type urn" in error_msg.lower()
        or "invalid" in error_msg.lower()
    ), f"Expected error message about malformed or invalid URN, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_owner_malformed_owner_urn(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a malformed owner URN
    2) Expect an error (code=400), with mention of "malformed" or "invalid" in the message
    """

    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "owners": [
                {
                    "ownerUrn": "urn:li:invalid-urn",
                    "ownerEntityType": "CORP_USER",
                    "type": "TECHNICAL_OWNER",
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    resp = execute_gql(auth_session, query=propose_owners_mutation, variables=variables)
    assert "errors" in resp, "Expected an error for malformed owner URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "invalid owner urn" in error_msg.lower() or "invalid" in error_msg.lower()
    ), f"Expected error message about malformed or invalid URN, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_owner_empty_owners(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose an empty owner array
    2) Expect an error (code=400)
    """
    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "owners": [],
            "description": "Proposing for accept test",
        }
    }
    resp = execute_gql(auth_session, query=propose_owners_mutation, variables=variables)
    assert "errors" in resp, "Expected an error for invalid empty owners "
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "at least one owner must be provided" in error_msg.lower()
        or "invalid" in error_msg.lower()
    ), f"Expected error message about missing owners, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_owner_invalid_owner_entity_type(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose owner values with invalid owner entity type
    2) Expect an error (code=400)
    """
    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "owners": [
                {
                    "ownerUrn": dataset_urn,  # Should be user or group urn.
                    "ownerEntityType": "CORP_USER",
                    "type": "TECHNICAL_OWNER",
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    resp = execute_gql(auth_session, query=propose_owners_mutation, variables=variables)
    assert "errors" in resp, "Expected an error for invalid owner type"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "all provided owners must be of type 'corpuser' or 'corpgroup'"
        in error_msg.lower()
    ), f"Expected error message about bad owner entity type, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_owner_custom_type_without_type_urn(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose an owner with custom ownership type without an ownership type urn
    2) Expect an error (code=400)
    """
    propose_owners_mutation = """
        mutation proposeOwners($input: ProposeOwnersInput!) {
            proposeOwners(input: $input)
        }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "owners": [
                {
                    "ownerUrn": user_urn,
                    "ownerEntityType": "CORP_USER",
                    "type": "CUSTOM",
                }
            ],
            "description": "Proposing for accept test",
        }
    }
    resp = execute_gql(auth_session, query=propose_owners_mutation, variables=variables)
    assert "errors" in resp, "Expected an error"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "ownership type urn must be provided for custom owner type" in error_msg.lower()
    ), f"Expected error message about ownership type being required, but got: {error_msg}"
