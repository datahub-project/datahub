import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.proposals.test_utils import execute_gql, validate_assignee_is_correct
from tests.utils import delete_urns_from_file, ingest_file_via_rest

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-proposal-domain,PROD)"
domain_urn = "urn:li:domain:test-domain-to-apply"
already_applied_domain_urn = "urn:li:domain:test-domain-already-applied"


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client, request):
    print("ingesting domains test data")
    ingest_file_via_rest(auth_session, "tests/proposals/domain_proposal_data.json")
    yield
    print("removing domains test data")
    delete_urns_from_file(graph_client, "tests/proposals/domain_proposal_data.json")


@pytest.mark.dependency()
def test_complete_entity_domain_proposal_accept(auth_session, ingest_cleanup_data):
    """
    1) Propose a domain on the dataset entity
    2) Accept the newly created proposal
    3) Verify the proposal is now COMPLETED and ACCEPTED in listActionRequests
    4) Verify the domain was applied to the dataset
    5) Remove the domain to reset for future tests
    """

    # 1) Propose a domain
    propose_domain_mutation = """
        mutation proposeDomain($input: ProposeDomainInput!) {
            proposeDomain(input: $input)
        }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "domainUrn": domain_urn,
            "description": "Proposing for accept test",
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_domain_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeDomain"]
    assert proposal_urn, "Expected a proposal URN"

    validate_assignee_is_correct(
        auth_session=auth_session,
        dataset_urn=dataset_urn,
        request_type="DOMAIN_ASSOCIATION",
    )

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
    assert accept_resp["data"]["acceptProposals"] is True, (
        "Expected acceptProposals to return true"
    )

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
            "type": "DOMAIN_ASSOCIATION",
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
    assert len(matching) == 1, (
        f"Expected to find exactly one request matching {proposal_urn}"
    )
    assert matching[0]["status"] == "COMPLETED"
    assert matching[0]["result"] == "ACCEPTED"

    # 4) Verify the domain was applied by fetching the dataset
    dataset_domain_query = """
        query getDatasetDomain($urn: String!) {
            dataset(urn: $urn) {
                urn
                domain {
                    domain {
                        urn
                    }
                }
            }
        }
    """
    variables_dataset_domain = {"urn": dataset_urn}
    dataset_domain_resp = execute_gql(
        auth_session, query=dataset_domain_query, variables=variables_dataset_domain
    )
    assert "errors" not in dataset_domain_resp, (
        f"Errors found: {dataset_domain_resp.get('errors')}"
    )

    # Extract the domain URN from the dataset
    actual_domain_urn = dataset_domain_resp["data"]["dataset"]["domain"]["domain"][
        "urn"
    ]
    assert domain_urn == actual_domain_urn, (
        f"Expected {domain_urn}, found {actual_domain_urn}"
    )

    # 5) Reset the domain to reset for subsequent tests (cleanup step)
    unset_domain_mutation = """
        mutation setDomain($entityUrn: String!, $domainUrn: String!) {
            setDomain(entityUrn: $entityUrn, domainUrn: $domainUrn)
        }
    """
    variables_unset_domain = {
        "entityUrn": dataset_urn,
        "domainUrn": already_applied_domain_urn,
    }
    remove_resp = execute_gql(
        auth_session, query=unset_domain_mutation, variables=variables_unset_domain
    )
    assert "errors" not in remove_resp, (
        f"Errors found resetting domain: {remove_resp.get('errors')}"
    )
    assert remove_resp["data"]["setDomain"] is True, "Expected setDomain to return True"


@pytest.mark.dependency()
def test_complete_entity_domain_proposal_reject(auth_session, ingest_cleanup_data):
    """
    1) Propose a domain on the dataset entity
    2) Reject the newly created proposal
    3) Verify the proposal is now COMPLETED and REJECTED in listActionRequests
    4) Confirm that the domain was never applied to the Dataset
    """

    # 1) Propose a domain
    propose_domain_mutation = """
    mutation proposeDomain($input: ProposeDomainInput!) {
        proposeDomain(input: $input)
    }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "domainUrn": domain_urn,
            "description": "Proposing for reject test",
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_domain_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeDomain"]
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
    assert reject_resp["data"]["rejectProposals"] is True, (
        "Expected rejectProposals to return true"
    )

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
            "type": "DOMAIN_ASSOCIATION",
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

    # 4) Confirm that the domain was NOT applied to the dataset
    dataset_domain_query = """
        query getDatasetDomain($urn: String!) {
            dataset(urn: $urn) {
                urn
                domain {
                    domain {
                        urn
                    }
                }
            }
        }
    """
    variables_dataset_domain = {"urn": dataset_urn}
    dataset_domain_resp = execute_gql(
        auth_session, query=dataset_domain_query, variables=variables_dataset_domain
    )
    assert "errors" not in dataset_domain_resp, (
        f"Errors found: {dataset_domain_resp.get('errors')}"
    )

    # Extract the domain URN from the dataset
    actual_domain_urn = dataset_domain_resp["data"]["dataset"]["domain"]["domain"][
        "urn"
    ]
    assert actual_domain_urn == already_applied_domain_urn, (
        "Expected the domain to be set to applied domain, not the newly proposed domain"
    )


@pytest.mark.dependency()
def test_list_action_requests_domain_params(auth_session, ingest_cleanup_data):
    """
    1) Propose a domain on the dataset entity
    2) List the action requests
    3) Validate that domain proposal parameters are correctly resolved
    """

    # 1) Propose a domain
    propose_domain_mutation = """
    mutation proposeDomain($input: ProposeDomainInput!) {
        proposeDomain(input: $input)
    }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "domainUrn": domain_urn,
            "description": "Proposing for reject test",
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_domain_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeDomain"]
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
                    domainProposal {
                        domain {
                            urn
                            properties {
                                name
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
            "type": "DOMAIN_ASSOCIATION",
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
    assert matching[0]["params"]["domainProposal"]["domain"]["urn"] == domain_urn
    assert (
        matching[0]["params"]["domainProposal"]["domain"]["properties"]["name"]
        == "Test Domain"
    )

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
    assert reject_resp["data"]["rejectProposals"] is True, (
        "Expected rejectProposals to return true"
    )


@pytest.mark.dependency()
def test_propose_domain_domain_does_not_exist(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a domain that doesn't exist
    2) Verify an error is returned containing code and message
    """

    non_existent_domain_urn = "urn:li:domain:non-existent-domain-12345"

    propose_domain_mutation = """
    mutation proposeDomain($input: ProposeDomainInput!) {
        proposeDomain(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "domainUrn": non_existent_domain_urn,
            "description": "Testing non-existent domain error",
        }
    }
    resp = execute_gql(
        auth_session=auth_session, query=propose_domain_mutation, variables=variables
    )
    # We expect an error here
    assert "errors" in resp, "Expected an error for non-existent domain"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")
    # Adjust these as needed if your actual server returns a different code / message
    assert error_code == 404, f"Expected 404 status code, got {error_code}"
    assert "does not exist" in error_msg


@pytest.mark.dependency()
def test_propose_domain_entity_does_not_exist(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a domain for an entity that doesn't exist
    2) Verify an error is returned
    """

    non_existent_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,not-real-entity,PROD)"
    )

    propose_domain_mutation = """
    mutation proposeDomain($input: ProposeDomainInput!) {
        proposeDomain(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": non_existent_dataset_urn,
            "domainUrn": domain_urn,
            "description": "Testing propose domain on non-existent entity",
        }
    }
    resp = execute_gql(
        auth_session=auth_session, query=propose_domain_mutation, variables=variables
    )
    assert "errors" in resp, "Expected an error for non-existent entity"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")
    # Adjust these as needed
    assert error_code == 404, f"Expected 404, got {error_code}"
    assert "does not exist" in error_msg


@pytest.mark.dependency()
def test_propose_domain_domain_already_applied(auth_session, ingest_cleanup_data):
    """
    1) Propose a domain that is already applied on the entity
    2) Expect an error or graceful handling
    """
    propose_domain_mutation = """
    mutation proposeDomain($input: ProposeDomainInput!) {
        proposeDomain(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "domainUrn": already_applied_domain_urn,
            "description": "Testing already-applied domain",
        }
    }
    resp = execute_gql(
        auth_session=auth_session, query=propose_domain_mutation, variables=variables
    )

    assert "errors" in resp, "Expected an error for already-applied domain"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")
    assert error_code == 400, f"Expected 400, got {error_code}"
    assert "already applied" in error_msg.lower()


@pytest.mark.dependency()
def test_propose_domain_domain_already_proposed(auth_session, ingest_cleanup_data):
    """
    1) Propose a domain
    2) Propose it again while the first proposal is still PENDING
    3) Expect an error or separate pending proposals
    """

    propose_domain_mutation = """
    mutation proposeDomain($input: ProposeDomainInput!) {
        proposeDomain(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "domainUrn": domain_urn,
            "description": "Testing already proposed domain scenario",
        }
    }

    # 1) First propose
    first_resp = execute_gql(
        auth_session=auth_session, query=propose_domain_mutation, variables=variables
    )
    assert "errors" not in first_resp, (
        f"Unexpected error in first proposal: {first_resp.get('errors')}"
    )
    first_proposal_urn = first_resp["data"]["proposeDomain"]
    assert first_proposal_urn, "Expected a proposal URN for the first proposal"

    # Wait for kafka
    wait_for_writes_to_sync()

    # 2) Propose again
    second_resp = execute_gql(
        auth_session=auth_session, query=propose_domain_mutation, variables=variables
    )

    assert "errors" in second_resp, "Expected an error for already-proposed domain"
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
    assert reject_resp["data"]["rejectProposals"] is True, (
        "Expected rejectProposals to return true"
    )


@pytest.mark.dependency()
def test_propose_domain_malformed_resource_urn(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a domain using a malformed resource URN
    2) Expect an error (code=400), with mention of "malformed" or similar in the message
    """
    propose_domain_mutation = """
    mutation proposeDomain($input: ProposeDomainInput!) {
        proposeDomain(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": "urn:lisdfd",  # malformed URN
            "domainUrn": domain_urn,
            "description": "Testing malformed resource URN",
        }
    }
    resp = execute_gql(auth_session, query=propose_domain_mutation, variables=variables)
    assert "errors" in resp, "Expected an error for malformed resource URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "invalid resource urn" in error_msg.lower() or "invalid" in error_msg.lower()
    ), f"Expected error message about a malformed URN, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_domain_malformed_domain_urn(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a malformed domain URN
    2) Expect an error (code=400), with mention of "malformed" or "invalid" in the message
    """
    propose_domain_mutation = """
    mutation proposeDomain($input: ProposeDomainInput!) {
        proposeDomain(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "domainUrn": "urn:lisdfdf",  # malformed domain URN
            "description": "Testing malformed domain URN",
        }
    }
    resp = execute_gql(auth_session, query=propose_domain_mutation, variables=variables)
    assert "errors" in resp, "Expected an error for malformed domain URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "invalid domain urn" in error_msg.lower() or "invalid" in error_msg.lower()
    ), f"Expected error message about malformed or invalid URN, but got: {error_msg}"
