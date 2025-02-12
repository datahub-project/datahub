import pytest

from tests.consistency_utils import wait_for_writes_to_sync
from tests.utils import delete_urns_from_file, ingest_file_via_rest

dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,test-proposal-tags,PROD)"
tag_urn = "urn:li:tag:test-tag-to-apply"
already_applied_tag_urn = "urn:li:tag:test-tag-already-applied"


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client, request):
    print("ingesting tags test data")
    ingest_file_via_rest(auth_session, "tests/proposals/tag_proposal_data.json")
    yield
    print("removing tags test data")
    delete_urns_from_file(graph_client, "tests/proposals/tag_proposal_data.json")


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
def test_complete_entity_tag_proposal_accept(auth_session, ingest_cleanup_data):
    """
    1) Propose a tag on the dataset entity
    2) Accept the newly created proposal
    3) Verify the proposal is now COMPLETED and ACCEPTED in listActionRequests
    4) Verify the tag was applied to the dataset
    5) Remove the tag to reset for future tests
    """

    # 1) Propose a tag
    propose_tags_mutation = """
        mutation proposeTags($input: ProposeTagsInput!) {
            proposeTags(input: $input)
        }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "tagUrns": [tag_urn],
            "description": "Proposing for accept test",
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_tags_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeTags"]
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
            "type": "TAG_ASSOCIATION",
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

    # 4) Verify the tag was applied by fetching the dataset
    dataset_tags_query = """
        query getDatasetTags($urn: String!) {
            dataset(urn: $urn) {
                urn
                tags {
                    tags {
                        tag {
                            urn
                        }
                    }
                }
            }
        }
    """
    variables_dataset_tags = {"urn": dataset_urn}
    dataset_tags_resp = execute_gql(
        auth_session, query=dataset_tags_query, variables=variables_dataset_tags
    )
    assert "errors" not in dataset_tags_resp, (
        f"Errors found: {dataset_tags_resp.get('errors')}"
    )

    # Extract the list of tag URNs from the dataset
    actual_tags = dataset_tags_resp["data"]["dataset"]["tags"]["tags"]
    tag_urns = [t["tag"]["urn"] for t in actual_tags]
    assert tag_urn in tag_urns, f"Expected {tag_urn} to be present in {tag_urns}"

    # 5) Remove the tag to reset for subsequent tests (cleanup step)
    remove_tag_mutation = """
        mutation removeTag($input: TagAssociationInput!) {
            removeTag(input: $input)
        }
    """
    variables_remove_tag = {"input": {"resourceUrn": dataset_urn, "tagUrn": tag_urn}}
    remove_resp = execute_gql(
        auth_session, query=remove_tag_mutation, variables=variables_remove_tag
    )
    assert "errors" not in remove_resp, (
        f"Errors found removing tag: {remove_resp.get('errors')}"
    )
    assert remove_resp["data"]["removeTag"] is True, "Expected removeTag to return True"

    # Optional: verify the tag was removed
    dataset_tags_resp_after_removal = execute_gql(
        auth_session, query=dataset_tags_query, variables=variables_dataset_tags
    )
    assert "errors" not in dataset_tags_resp_after_removal, (
        f"Errors found: {dataset_tags_resp_after_removal.get('errors')}"
    )
    updated_tags = dataset_tags_resp_after_removal["data"]["dataset"]["tags"]["tags"]
    updated_tag_urns = [t["tag"]["urn"] for t in updated_tags]
    assert tag_urn not in updated_tag_urns, (
        f"Expected {tag_urn} to be removed, but it still appears: {updated_tag_urns}"
    )


@pytest.mark.dependency()
def test_complete_entity_tag_proposal_reject(auth_session, ingest_cleanup_data):
    """
    1) Propose a tag on the dataset entity
    2) Reject the newly created proposal
    3) Verify the proposal is now COMPLETED and REJECTED in listActionRequests
    4) Confirm that the tag was never applied to the Dataset
    """

    # 1) Propose a tag
    propose_tags_mutation = """
    mutation proposeTags($input: ProposeTagsInput!) {
        proposeTags(input: $input)
    }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "tagUrns": [tag_urn],
            "description": "Proposing for reject test",
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_tags_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeTags"]
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
            "type": "TAG_ASSOCIATION",
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

    # 4) Confirm that the tag was NOT applied to the dataset
    dataset_tags_query = """
    query getDatasetTags($urn: String!) {
        dataset(urn: $urn) {
            urn
            tags {
                tags {
                    tag {
                        urn
                    }
                }
            }
        }
    }
    """
    variables_dataset_tags = {"urn": dataset_urn}
    dataset_tags_resp = execute_gql(
        auth_session, query=dataset_tags_query, variables=variables_dataset_tags
    )
    assert "errors" not in dataset_tags_resp, (
        f"Errors found: {dataset_tags_resp.get('errors')}"
    )
    applied_tags = dataset_tags_resp["data"]["dataset"]["tags"]["tags"]
    applied_tag_urns = [t["tag"]["urn"] for t in applied_tags]

    # Since proposal was REJECTED, we expect the tag NOT to be applied
    assert tag_urn not in applied_tag_urns, (
        f"Tag {tag_urn} should NOT have been applied to Dataset {dataset_urn}, "
        f"but we found: {applied_tag_urns}"
    )


@pytest.mark.dependency()
def test_list_action_requests_tag_params(auth_session, ingest_cleanup_data):
    """
    1) Propose a tag on the dataset entity
    2) List the action requests
    3) Validate that tag proposal parameters are correctly resolved
    """

    # 1) Propose a tag
    propose_tags_mutation = """
    mutation proposeTags($input: ProposeTagsInput!) {
        proposeTags(input: $input)
    }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "tagUrns": [tag_urn],
            "description": "Proposing for reject test",
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_tags_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeTags"]
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
                    tagProposal {
                        tags {
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
            "type": "TAG_ASSOCIATION",
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
    assert matching[0]["params"]["tagProposal"]["tags"][0]["urn"] == tag_urn
    assert (
        matching[0]["params"]["tagProposal"]["tags"][0]["properties"]["name"]
        == "Test Tag"
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
def test_complete_schema_field_tag_proposal_accept(auth_session, ingest_cleanup_data):
    """
    1) Propose a tag on a sub-resource (schema field)
    2) Accept the newly created proposal
    3) Verify the proposal is now COMPLETED & ACCEPTED
    4) Confirm the tag was applied to the schema field
    5) Remove the tag to reset for subsequent tests
    """
    field_name = "field_bar"

    # 1) Propose the tag
    propose_tags_mutation = """
    mutation proposeTags($input: ProposeTagsInput!) {
        proposeTags(input: $input)
    }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "tagUrns": [tag_urn],
            "description": "Testing accept sub-resource tag",
            "subResourceType": "DATASET_FIELD",
            "subResource": field_name,
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_tags_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeTags"]
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
    assert accept_resp["data"]["acceptProposals"] is True, (
        "Expected acceptProposals to return True"
    )

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
            "type": "TAG_ASSOCIATION",
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

    # 4) Confirm the tag was applied to the schema field
    dataset_fields_query = """
    query getSchemaFields($urn: String!) {
      dataset(urn: $urn) {
        editableSchemaMetadata {
          editableSchemaFieldInfo {
            fieldPath
            tags {
              tags {
                tag {
                  urn
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
    fields = fields_resp["data"]["dataset"]["editableSchemaMetadata"][
        "editableSchemaFieldInfo"
    ]
    field_bar = next(f for f in fields if f["fieldPath"] == field_name)
    print(fields)
    print(field_bar)
    applied_tags = [t["tag"]["urn"] for t in field_bar["tags"]["tags"]]
    assert tag_urn in applied_tags, (
        f"Expected {tag_urn} on {field_name}, found {applied_tags}"
    )

    # 5) Remove the tag to reset for subsequent tests
    remove_tag_mutation = """
    mutation removeTag($input: TagAssociationInput!) {
        removeTag(input: $input)
    }
    """
    variables_remove_tag = {
        "input": {
            "resourceUrn": dataset_urn,
            "tagUrn": tag_urn,
            "subResourceType": "DATASET_FIELD",
            "subResource": field_name,
        }
    }
    remove_resp = execute_gql(
        auth_session, query=remove_tag_mutation, variables=variables_remove_tag
    )
    assert "errors" not in remove_resp, (
        f"Errors found removing tag: {remove_resp.get('errors')}"
    )
    assert remove_resp["data"]["removeTag"] is True, "Expected removeTag to return True"

    # Optional: verify the tag is gone
    fields_resp_after_removal = execute_gql(
        auth_session, query=dataset_fields_query, variables={"urn": dataset_urn}
    )
    assert "errors" not in fields_resp_after_removal, (
        f"Errors found: {fields_resp_after_removal.get('errors')}"
    )
    updated_fields = fields_resp_after_removal["data"]["dataset"][
        "editableSchemaMetadata"
    ]["editableSchemaFieldInfo"]
    updated_field_bar = next(f for f in updated_fields if f["fieldPath"] == field_name)
    updated_tags = [t["tag"]["urn"] for t in updated_field_bar["tags"]["tags"]]
    assert tag_urn not in updated_tags, (
        f"Tag {tag_urn} should have been removed from field {field_name}, but is still there: {updated_tags}"
    )


@pytest.mark.dependency()
def test_complete_schema_field_tag_proposal_reject(auth_session, ingest_cleanup_data):
    """
    1) Propose a tag on a sub-resource (schema field)
    2) Reject the newly created proposal
    3) Verify the proposal is now COMPLETED & REJECTED
    4) Confirm the tag is NOT applied to the schema field
    """
    field_name = "field_bar"

    # 1) Propose
    propose_tags_mutation = """
    mutation proposeTags($input: ProposeTagsInput!) {
        proposeTags(input: $input)
    }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "tagUrns": [tag_urn],
            "description": "Testing reject sub-resource tag",
            "subResourceType": "DATASET_FIELD",
            "subResource": field_name,
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_tags_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    proposal_urn = propose_resp["data"]["proposeTags"]
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
    assert reject_resp["data"]["rejectProposals"] is True, (
        "Expected rejectProposals to return True"
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
            "type": "TAG_ASSOCIATION",
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

    # 4) Confirm the tag was NOT applied to the schema field
    dataset_fields_query = """
    query getSchemaFields($urn: String!) {
      dataset(urn: $urn) {
        editableSchemaMetadata {
          editableSchemaFieldInfo {
            fieldPath
            tags {
              tags {
                tag {
                  urn
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

    if fields_resp["data"]["dataset"]["editableSchemaMetadata"] is not None:
        fields = fields_resp["data"]["dataset"]["editableSchemaMetadata"][
            "editableSchemaFieldInfo"
        ]
        field_bar = next(f for f in fields if f["fieldPath"] == field_name)

        if field_bar is not None:
            applied_tags = [t["tag"]["urn"] for t in field_bar["tags"]["tags"]]

            # Since it was rejected, we expect not to see the tag on field_bar
            assert tag_urn not in applied_tags, (
                f"Tag {tag_urn} should NOT have been applied, but is present: {applied_tags}"
            )


@pytest.mark.dependency()
def test_propose_tag_tag_does_not_exist(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a tag that doesn't exist
    2) Verify an error is returned containing code and message
    """

    non_existent_tag_urn = "urn:li:tag:non-existent-tag-12345"

    propose_tags_mutation = """
    mutation proposeTags($input: ProposeTagsInput!) {
        proposeTags(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "tagUrns": [non_existent_tag_urn],
            "description": "Testing non-existent tag error",
        }
    }
    resp = execute_gql(
        auth_session=auth_session, query=propose_tags_mutation, variables=variables
    )
    # We expect an error here
    assert "errors" in resp, "Expected an error for non-existent tag"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")
    # Adjust these as needed if your actual server returns a different code / message
    assert error_code == 404, f"Expected 404 status code, got {error_code}"
    assert "does not exist" in error_msg


@pytest.mark.dependency()
def test_propose_tag_entity_does_not_exist(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a tag for an entity that doesn't exist
    2) Verify an error is returned
    """

    non_existent_dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,not-real-entity,PROD)"
    )

    propose_tags_mutation = """
    mutation proposeTags($input: ProposeTagsInput!) {
        proposeTags(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": non_existent_dataset_urn,
            "tagUrns": [tag_urn],
            "description": "Testing propose tag on non-existent entity",
        }
    }
    resp = execute_gql(
        auth_session=auth_session, query=propose_tags_mutation, variables=variables
    )
    assert "errors" in resp, "Expected an error for non-existent entity"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")
    # Adjust these as needed
    assert error_code == 404, f"Expected 404, got {error_code}"
    assert "does not exist" in error_msg


@pytest.mark.dependency()
def test_propose_tag_tag_already_applied(auth_session, ingest_cleanup_data):
    """
    1) Propose a tag that is already applied on the entity
    2) Expect an error or graceful handling
    """
    propose_tags_mutation = """
    mutation proposeTags($input: ProposeTagsInput!) {
        proposeTags(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "tagUrns": [already_applied_tag_urn],
            "description": "Testing already-applied tag",
        }
    }
    resp = execute_gql(
        auth_session=auth_session, query=propose_tags_mutation, variables=variables
    )

    assert "errors" in resp, "Expected an error for already-applied tag"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")
    assert error_code == 400, f"Expected 400, got {error_code}"
    assert "already applied" in error_msg.lower()


@pytest.mark.dependency()
def test_propose_tag_tag_already_proposed(auth_session, ingest_cleanup_data):
    """
    1) Propose a tag
    2) Propose it again while the first proposal is still PENDING
    3) Expect an error or separate pending proposals
    """

    propose_tags_mutation = """
    mutation proposeTags($input: ProposeTagsInput!) {
        proposeTags(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": dataset_urn,
            "tagUrns": [tag_urn],
            "description": "Testing already proposed tag scenario",
        }
    }

    # 1) First propose
    first_resp = execute_gql(
        auth_session=auth_session, query=propose_tags_mutation, variables=variables
    )
    assert "errors" not in first_resp, (
        f"Unexpected error in first proposal: {first_resp.get('errors')}"
    )
    first_proposal_urn = first_resp["data"]["proposeTags"]
    assert first_proposal_urn, "Expected a proposal URN for the first proposal"

    # Wait for kafka
    wait_for_writes_to_sync()

    # 2) Propose again
    second_resp = execute_gql(
        auth_session=auth_session, query=propose_tags_mutation, variables=variables
    )

    assert "errors" in second_resp, "Expected an error for already-proposed tag"
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
def test_propose_tag_malformed_resource_urn(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a tag using a malformed resource URN
    2) Expect an error (code=400), with mention of "malformed" or similar in the message
    """
    propose_tags_mutation = """
    mutation proposeTags($input: ProposeTagsInput!) {
        proposeTags(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": "urn:lisdfd",  # malformed URN
            "tagUrns": ["urn:li:tag:test-tag-to-apply"],
            "description": "Testing malformed resource URN",
        }
    }
    resp = execute_gql(auth_session, query=propose_tags_mutation, variables=variables)
    assert "errors" in resp, "Expected an error for malformed resource URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert (
        "invalid resource urn" in error_msg.lower() or "invalid" in error_msg.lower()
    ), f"Expected error message about a malformed URN, but got: {error_msg}"


@pytest.mark.dependency()
def test_propose_tag_schema_field_no_subresource(auth_session, ingest_cleanup_data):
    """
    1) Provide subResourceType = SCHEMA_FIELD but do NOT supply subResource
    2) Expect an error (code=400), with mention of "subResource" missing
    """
    propose_tags_mutation = """
    mutation proposeTags($input: ProposeTagsInput!) {
        proposeTags(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": "urn:li:dataset:(urn:li:dataPlatform:kafka,test-proposal-tags,PROD)",
            "tagUrns": ["urn:li:tag:test-tag-to-apply"],
            "description": "Testing SCHEMA_FIELD without specifying subResource",
            "subResourceType": "DATASET_FIELD",
            # "subResource": not provided intentionally
        }
    }
    resp = execute_gql(auth_session, query=propose_tags_mutation, variables=variables)
    assert "errors" in resp, (
        "Expected an error when subResource is missing for subResourceType=DATASET_FIELD"
    )
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert "subresource field must be provided" in error_msg.lower(), (
        f"Expected error message about missing subResource, but got: {error_msg}"
    )


@pytest.mark.dependency()
def test_propose_tag_malformed_tag_urn(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose a malformed tag URN
    2) Expect an error (code=400), with mention of "malformed" or "invalid" in the message
    """
    propose_tags_mutation = """
    mutation proposeTags($input: ProposeTagsInput!) {
        proposeTags(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": "urn:li:dataset:(urn:li:dataPlatform:kafka,test-proposal-tags,PROD)",
            "tagUrns": ["urn:lisdfdf"],  # malformed Tag URN
            "description": "Testing malformed tag URN",
        }
    }
    resp = execute_gql(auth_session, query=propose_tags_mutation, variables=variables)
    assert "errors" in resp, "Expected an error for malformed tag URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert "invalid tag urns" in error_msg.lower() or "invalid" in error_msg.lower(), (
        f"Expected error message about malformed or invalid URN, but got: {error_msg}"
    )


@pytest.mark.dependency()
def test_propose_tag_empty_tag_urns(auth_session, ingest_cleanup_data):
    """
    1) Attempt to propose an empty tags array
    2) Expect an error (code=400)
    """
    propose_tags_mutation = """
    mutation proposeTags($input: ProposeTagsInput!) {
        proposeTags(input: $input)
    }
    """
    variables = {
        "input": {
            "resourceUrn": "urn:li:dataset:(urn:li:dataPlatform:kafka,test-proposal-tags,PROD)",
            "tagUrns": [],
            "description": "Testing malformed tag URN",
        }
    }
    resp = execute_gql(auth_session, query=propose_tags_mutation, variables=variables)
    assert "errors" in resp, "Expected an error for malformed tag URN"
    error_msg = resp["errors"][0]["message"]
    error_code = resp["errors"][0].get("extensions", {}).get("code")

    assert error_code == 400, f"Expected 400, got {error_code}"
    assert "no tags provided" in error_msg.lower() or "invalid" in error_msg.lower(), (
        f"Expected error message about malformed or invalid URN, but got: {error_msg}"
    )


# TESTS FOR THE LEGACY (deprecated) proposeTag API


@pytest.mark.dependency()
def test_complete_legacy_entity_tag_proposal_accept(auth_session, ingest_cleanup_data):
    """
    1) Propose a tag on the dataset entity using the legacy proposeTag API
    2) Accept the newly created proposal
    3) Verify the proposal is now COMPLETED and ACCEPTED in listActionRequests
    4) Verify the tag was applied to the dataset
    5) Remove the tag to reset for future tests
    """

    # 1) Propose a tag
    propose_tags_mutation = """
        mutation proposeTag($input: TagAssociationInput!) {
            proposeTag(input: $input)
        }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "tagUrn": tag_urn,
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_tags_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    succeeded = propose_resp["data"]["proposeTag"]
    assert succeeded is True, "Expected operation to succeed"

    # Wait again before continuing.
    wait_for_writes_to_sync()

    # 2) Verify listActionRequests -> COMPLETED & PENDING
    list_requests_query = """
        query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            total
            actionRequests {
                urn
                status
                result
                params {
                    tagProposal {
                        tag {
                            urn
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
            "type": "TAG_ASSOCIATION",
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

    # We should find our request with pending
    matching = [
        r for r in requests if r["params"]["tagProposal"]["tag"]["urn"] == tag_urn
    ]
    assert len(matching) == 1, (
        f"Expected to find exactly one request matching {tag_urn}"
    )
    assert matching[0]["status"] == "PENDING"
    proposal_urn = matching[0]["urn"]

    # 3) Accept the proposal
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

    # 4) Verify the tag was applied by fetching the dataset
    dataset_tags_query = """
        query getDatasetTags($urn: String!) {
            dataset(urn: $urn) {
                urn
                tags {
                    tags {
                        tag {
                            urn
                        }
                    }
                }
            }
        }
    """
    variables_dataset_tags = {"urn": dataset_urn}
    dataset_tags_resp = execute_gql(
        auth_session, query=dataset_tags_query, variables=variables_dataset_tags
    )
    assert "errors" not in dataset_tags_resp, (
        f"Errors found: {dataset_tags_resp.get('errors')}"
    )

    # Extract the list of tag URNs from the dataset
    actual_tags = dataset_tags_resp["data"]["dataset"]["tags"]["tags"]
    tag_urns = [t["tag"]["urn"] for t in actual_tags]
    assert tag_urn in tag_urns, f"Expected {tag_urn} to be present in {tag_urns}"

    # 5) Remove the tag to reset for subsequent tests (cleanup step)
    remove_tag_mutation = """
        mutation removeTag($input: TagAssociationInput!) {
            removeTag(input: $input)
        }
    """
    variables_remove_tag = {"input": {"resourceUrn": dataset_urn, "tagUrn": tag_urn}}
    remove_resp = execute_gql(
        auth_session, query=remove_tag_mutation, variables=variables_remove_tag
    )
    assert "errors" not in remove_resp, (
        f"Errors found removing tag: {remove_resp.get('errors')}"
    )
    assert remove_resp["data"]["removeTag"] is True, "Expected removeTag to return True"

    # Optional: verify the tag was removed
    dataset_tags_resp_after_removal = execute_gql(
        auth_session, query=dataset_tags_query, variables=variables_dataset_tags
    )
    assert "errors" not in dataset_tags_resp_after_removal, (
        f"Errors found: {dataset_tags_resp_after_removal.get('errors')}"
    )
    updated_tags = dataset_tags_resp_after_removal["data"]["dataset"]["tags"]["tags"]
    updated_tag_urns = [t["tag"]["urn"] for t in updated_tags]
    assert tag_urn not in updated_tag_urns, (
        f"Expected {tag_urn} to be removed, but it still appears: {updated_tag_urns}"
    )


@pytest.mark.dependency()
def test_complete_legacy_entity_tag_proposal_reject(auth_session, ingest_cleanup_data):
    """
    1) Propose a tag on the dataset entity using legacy proposeTag API
    2) Reject the newly created proposal
    3) Verify the proposal is now COMPLETED and REJECTED in listActionRequests
    4) Confirm that the tag was never applied to the Dataset
    """

    # 1) Propose a tag
    propose_tags_mutation = """
    mutation proposeTag($input: TagAssociationInput!) {
        proposeTag(input: $input)
    }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "tagUrn": tag_urn,
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_tags_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    succeeded = propose_resp["data"]["proposeTag"]
    assert succeeded is True, "Expected operation to succeed"

    wait_for_writes_to_sync()

    # 2) Verify listActionRequests => pending
    list_requests_query = """
    query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            actionRequests {
                urn
                status
                result
                params {
                    tagProposal {
                        tag {
                            urn
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
            "type": "TAG_ASSOCIATION",
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
    matching = [
        r for r in requests if r["params"]["tagProposal"]["tag"]["urn"] == tag_urn
    ]
    assert len(matching) == 1, f"Expected exactly one request proposing {tag_urn}"
    assert matching[0]["status"] == "PENDING"
    proposal_urn = matching[0]["urn"]

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

    # 4) Confirm that the tag was NOT applied to the dataset
    dataset_tags_query = """
    query getDatasetTags($urn: String!) {
        dataset(urn: $urn) {
            urn
            tags {
                tags {
                    tag {
                        urn
                    }
                }
            }
        }
    }
    """
    variables_dataset_tags = {"urn": dataset_urn}
    dataset_tags_resp = execute_gql(
        auth_session, query=dataset_tags_query, variables=variables_dataset_tags
    )
    assert "errors" not in dataset_tags_resp, (
        f"Errors found: {dataset_tags_resp.get('errors')}"
    )
    applied_tags = dataset_tags_resp["data"]["dataset"]["tags"]["tags"]
    applied_tag_urns = [t["tag"]["urn"] for t in applied_tags]

    # Since proposal was REJECTED, we expect the tag NOT to be applied
    assert tag_urn not in applied_tag_urns, (
        f"Tag {tag_urn} should NOT have been applied to Dataset {dataset_urn}, "
        f"but we found: {applied_tag_urns}"
    )


@pytest.mark.dependency()
def test_list_action_requests_legacy_tag_params(auth_session, ingest_cleanup_data):
    """
    1) Propose a tag on the dataset entity using legacy proposeTag API
    2) List the action requests
    3) Validate that tag proposal parameters are correctly resolved
    """

    # 1) Propose a tag
    propose_tags_mutation = """
    mutation proposeTag($input: TagAssociationInput!) {
        proposeTag(input: $input)
    }
    """
    variables_propose = {
        "input": {
            "resourceUrn": dataset_urn,
            "tagUrn": tag_urn,
        }
    }
    propose_resp = execute_gql(
        auth_session, query=propose_tags_mutation, variables=variables_propose
    )
    assert "errors" not in propose_resp, f"Errors found: {propose_resp.get('errors')}"
    succeeded = propose_resp["data"]["proposeTag"]
    assert succeeded is True, "Expected operation to succeed"

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
                    tagProposal {
                        tag {
                            urn
                            properties {
                                name
                            }
                        }
                        tags {
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
            "type": "TAG_ASSOCIATION",
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
    matching = [
        r for r in requests if r["params"]["tagProposal"]["tag"]["urn"] == tag_urn
    ]
    assert len(matching) == 1, f"Expected exactly one request matching {tag_urn}"
    proposal_urn = matching[0]["urn"]

    # Confirm that both tags and tag field are populated for backwards compatibility on the read side.
    assert matching[0]["params"]["tagProposal"]["tags"][0]["urn"] == tag_urn
    assert (
        matching[0]["params"]["tagProposal"]["tags"][0]["properties"]["name"]
        == "Test Tag"
    )
    assert matching[0]["params"]["tagProposal"]["tag"]["urn"] == tag_urn
    assert (
        matching[0]["params"]["tagProposal"]["tag"]["properties"]["name"] == "Test Tag"
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
