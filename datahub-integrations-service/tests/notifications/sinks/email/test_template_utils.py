from urllib.parse import quote

import pytest
from datahub.metadata.schema_classes import (
    NotificationContextClass,
    NotificationMessageClass,
    NotificationRequestClass,
    NotificationTemplateTypeClass,
)

from datahub_integrations.notifications.constants import NON_INGESTION_RUN_ID
from datahub_integrations.notifications.sinks.email.template_utils import (
    build_assertion_status_change_parameters,
    build_entity_change_parameters,
    build_incident_status_change_parameters,
    build_ingestion_run_change_parameters,
    build_new_incident_parameters,
    build_new_proposal_parameters,
    build_proposal_status_change_parameters,
    build_proposer_proposal_status_change_parameters,
    timestamp_to_date,
)


@pytest.fixture
def base_url_fix() -> str:
    return "https://example.acryl.io"


def get_notification_request(
    template: str, parameters: dict
) -> NotificationRequestClass:
    message = NotificationMessageClass(template=template, parameters=parameters)
    return NotificationRequestClass(message=message, recipients=[], sinks=[])


def test_build_new_incident_parameters(base_url_fix: str) -> None:
    parameters = {
        "actorName": "John Doe",
        "entityPath": "/datasets/dataset123",
        "entityName": "Dataset123",
        "entityType": "Table",
        "entityPlatform": "Hive",
        "incidentTitle": "Data Quality Issue",
        "incidentDescription": "Null values found in critical field.",
    }
    notification_request = get_notification_request(
        NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT, parameters
    )
    expected = {
        "subject": "A new incident has been raised on Hive Table Dataset123 by John Doe.",
        "message": "A new incident has been raised on <b>Hive Table Dataset123</b> by John Doe.<br><br><b>Incident Name</b>: Data Quality Issue<br><b>Incident Description</b>: Null values found in critical field.<br><br>",
        "entityName": "Dataset123",
        "detailsUrl": "https://example.acryl.io/datasets/dataset123/Incidents",
        "entityUrl": "https://example.acryl.io/datasets/dataset123",
        "baseUrl": base_url_fix,
    }
    assert build_new_incident_parameters(notification_request, base_url_fix) == expected


def test_build_incident_status_change_parameters(base_url_fix: str) -> None:
    parameters = {
        "actorName": "John Doe",
        "entityPath": "/datasets/dataset123",
        "entityName": "Dataset123",
        "entityType": "Table",
        "entityPlatform": "Hive",
        "incidentTitle": "Data Quality Issue",
        "incidentDescription": "Null values found in critical field.",
        "prevStatus": "Open",
        "newStatus": "Resolved",
    }
    notification_request = get_notification_request(
        NotificationTemplateTypeClass.BROADCAST_INCIDENT_STATUS_CHANGE, parameters
    )
    expected = {
        "subject": "The status of incident on Hive Table Dataset123 has been changed from Open to Resolved by John Doe.",
        "message": "The status of incident on <b>Hive Table Dataset123</b> has been changed from Open to Resolved by John Doe.<br><br><b>Incident Name</b>: Data Quality Issue<br><b>Incident Description</b>: Null values found in critical field.<br><br>",
        "entityName": "Dataset123",
        "detailsUrl": "https://example.acryl.io/datasets/dataset123/Incidents",
        "entityUrl": "https://example.acryl.io/datasets/dataset123",
        "baseUrl": base_url_fix,
    }
    assert (
        build_incident_status_change_parameters(notification_request, base_url_fix)
        == expected
    )


#
# Tests for build_new_proposal_parameters(...)
#


def test_build_new_proposal_parameters_for_entity(base_url_fix: str) -> None:
    """
    Tests a "new proposal" for a normal (non-glossary) modifier, no sub-resource,
    using JSON array for modifierNames.
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_PROPOSAL,
            parameters={
                "actorName": "John Joyce",
                "entityName": "SampleKafkaDataset",
                "entityType": "Table",
                "entityPath": "/datasets/samplekafkadataset",
                "operation": "add",
                "modifierType": "Tag",
                "modifierNames": '["PII"]',  # JSON string => ["PII"]
                "modifierPaths": '["/glossary/urn:li:glossaryTerm:pii"]',
            },
        ),
        recipients=[],
    )

    # According to your updated code:
    # subject => "John Joyce proposed to add PII for Table SampleKafkaDataset"
    # message => "<b>John Joyce</b> has proposed to <b>add</b> PII for <b>Table SampleKafkaDataset</b>."
    expected = {
        "subject": "John Joyce proposed to add tag PII for table SampleKafkaDataset",
        "message": (
            '<b>John Joyce</b> has proposed to add tag <b><a href="https://example.acryl.io/glossary/urn:li:glossaryTerm:pii">PII</a></b> '
            "for table <b>SampleKafkaDataset</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_new_proposal_parameters(request, base_url_fix)
    assert actual == expected


def test_build_new_proposal_parameters_for_sub_resource_no_modifier_paths(
    base_url_fix: str,
) -> None:
    """
    Tests a "new proposal" for a normal (non-glossary) modifier WITH a sub-resource,
    with multiple modifierNames in the array.
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_PROPOSAL,
            parameters={
                "actorName": "John Joyce",
                "entityName": "SampleKafkaDataset",
                "entityType": "Table",
                "entityPath": "/datasets/samplekafkadataset",
                "operation": "add",
                "modifierType": "Tag(s)",
                "modifierNames": '["Sensitive","Confidential"]',
                "modifierPaths": "[]",
                "subResourceType": "DATASET_FIELD",
                "subResource": "bar",
            },
        ),
        recipients=[],
    )

    # The code will join "Sensitive" and "Confidential" as "Sensitive and Confidential"
    expected = {
        "subject": "John Joyce proposed to add tag(s) Sensitive and Confidential for column bar of table SampleKafkaDataset",
        "message": (
            "<b>John Joyce</b> has proposed to add tag(s) <b>Sensitive</b> and <b>Confidential</b> "
            "for column <b>bar</b> of table <b>SampleKafkaDataset</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_new_proposal_parameters(request, base_url_fix)
    assert actual == expected


def test_build_new_proposal_parameters_for_glossary_term(base_url_fix: str) -> None:
    """
    Tests a "new proposal" for a Glossary Term, ignoring modifierNames array
    in favor of entityName for the "named ...".
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_PROPOSAL,
            parameters={
                "actorName": "Alice",
                "entityName": "Email Address",
                "entityPath": "/glossary/term/123",
                "operation": "create",
                "modifierType": "Glossary Term",  # triggers special-case
                "modifierNames": "[]",  # not used in the special block
                "modifierPaths": "[]",
                # optional
                "parentTermGroupName": "PII",
            },
        ),
        recipients=[],
    )

    # No "entityUrl" returned for glossary scenario, per your snippet
    expected = {
        "subject": "Alice has proposed creating Glossary Term named Email Address in Term Group PII.",
        "message": (
            "<b>Alice</b> has proposed creating Glossary Term named <b>Email Address</b>"
            " in Term Group <b>PII</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_new_proposal_parameters(request, base_url_fix)
    assert actual == expected


def test_build_new_proposal_parameters_for_glossary_node(base_url_fix: str) -> None:
    """
    Tests a "new proposal" for a Glossary Term Group, ignoring modifierNames array
    in favor of entityName for the "named ...".
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_PROPOSAL,
            parameters={
                "actorName": "Alice",
                "entityName": "PII",
                "entityPath": "/glossary/term/123",
                "operation": "create",
                "modifierType": "Glossary Term Group",  # triggers special-case
                "modifierNames": "[]",  # not used in the special block
                "modifierPaths": "[]",
                # optional
                "parentTermGroupName": "Root",
            },
        ),
        recipients=[],
    )

    # No "entityUrl" returned for glossary scenario, per your snippet
    expected = {
        "subject": "Alice has proposed creating Glossary Term Group named PII in Term Group Root.",
        "message": (
            "<b>Alice</b> has proposed creating Glossary Term Group named <b>PII</b>"
            " in Term Group <b>Root</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_new_proposal_parameters(request, base_url_fix)
    assert actual == expected


def test_build_new_proposal_parameters_for_description(base_url_fix: str) -> None:
    """
    Tests a "new proposal" for a description
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_PROPOSAL,
            parameters={
                "actorName": "Alice",
                "entityName": "SampleKafkaDataset",
                "entityType": "Table",
                "entityPath": "/glossary/term/123",
                "operation": "update",
                "modifierType": "Description",
                "modifierNames": "[]",
                "modifierPaths": "[]",
            },
        ),
        recipients=[],
    )

    # No "entityUrl" returned for glossary scenario, per your snippet
    expected = {
        "subject": "Alice proposed to update description for table SampleKafkaDataset",
        "message": (
            "<b>Alice</b> has proposed to update description for table "
            "<b>SampleKafkaDataset</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_new_proposal_parameters(request, base_url_fix)
    assert actual == expected


def test_build_new_proposal_parameters_missing_parameters(base_url_fix: str) -> None:
    """
    Validates that passing None for 'parameters' raises ValueError.
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_PROPOSAL,
            parameters=None,
        ),
        recipients=[],
    )
    with pytest.raises(ValueError):
        build_new_proposal_parameters(request, base_url_fix)


#
# Tests for build_proposal_status_change_parameters(...)
#


def test_build_proposal_status_change_parameters_for_entity(base_url_fix: str) -> None:
    """
    "Proposal status change" for a normal (non-glossary) scenario, no sub-resource,
    with single item in modifierNames array.
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters={
                "actorName": "John Joyce",
                "entityName": "SampleKafkaDataset",
                "entityPath": "/datasets/samplekafkadataset",
                "entityType": "Table",
                "modifierType": "Tag(s)",
                "modifierNames": '["PII"]',  # single item array
                "modifierPaths": '["/glossary/urn:li:glossaryTerm:pii"]',
                "operation": "add",
                "action": "approved",
            },
        ),
        recipients=[],
    )

    expected = {
        "subject": "John Joyce has approved the proposal to add tag(s) PII for table SampleKafkaDataset.",
        "message": (
            '<b>John Joyce</b> has <b>approved</b> the proposal to add tag(s) <b><a href="https://example.acryl.io/glossary/urn:li:glossaryTerm:pii">PII</a></b> '
            "for table <b>SampleKafkaDataset</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_proposal_status_change_parameters(request, base_url_fix)
    assert actual == expected


def test_build_proposal_status_change_parameters_for_sub_resource_no_paths(
    base_url_fix: str,
) -> None:
    """
    "Proposal status change" for a normal scenario WITH sub-resource,
    with multiple items in modifierNames array.
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters={
                "actorName": "John Joyce",
                "entityName": "SampleKafkaDataset",
                "entityPath": "/datasets/samplekafkadataset",
                "entityType": "Table",
                "modifierType": "Tag(s)",
                "modifierNames": '["PII","Sensitive"]',
                "modifierPaths": "[]",
                "operation": "remove",
                "action": "denied",
                "subResourceType": "DATASET_FIELD",
                "subResource": "foo",
            },
        ),
        recipients=[],
    )

    # "PII" and "Sensitive" => "PII and Sensitive"
    expected = {
        "subject": "John Joyce has denied the proposal to remove tag(s) PII and Sensitive for column foo of table SampleKafkaDataset.",
        "message": (
            "<b>John Joyce</b> has <b>denied</b> the proposal to remove tag(s) <b>PII</b> and <b>Sensitive</b> "
            "for column <b>foo</b> of table <b>SampleKafkaDataset</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_proposal_status_change_parameters(request, base_url_fix)
    assert actual == expected


def test_build_proposal_status_change_parameters_for_glossary_term(
    base_url_fix: str,
) -> None:
    """
    "Proposal status change" for a Glossary Term or Term Group scenario,
    ignoring modifierNames in favor of entityName as "named X".
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters={
                "actorName": "Bob",
                "entityName": "SSN",
                "entityPath": "/glossary/term/456",
                "modifierType": "Glossary Term",
                "modifierNames": '["SomeTerm"]',  # not used in the special-case block
                "modifierPaths": "[]",
                "operation": "create",
                "action": "accepted",
                "parentTermGroupName": "PII",
            },
        ),
        recipients=[],
    )

    # No entityUrl in the returned dict for glossary scenario
    expected = {
        "subject": "Bob has accepted the proposal to create Glossary Term named SSN in Term Group PII.",
        "message": (
            "<b>Bob</b> has <b>accepted</b> the proposal to create Glossary Term "
            "named <b>SSN</b> in Term Group <b>PII</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_proposal_status_change_parameters(request, base_url_fix)
    assert actual == expected


def test_build_proposal_status_change_parameters_for_glossary_node(
    base_url_fix: str,
) -> None:
    """
    "Proposal status change" for a Glossary Term or Term Group scenario,
    ignoring modifierNames in favor of entityName as "named X".
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters={
                "actorName": "Bob",
                "entityName": "PII",
                "entityPath": "/glossary/term/456",
                "modifierType": "Glossary Term Group",
                "modifierNames": '["SomeTerm"]',  # not used in the special-case block
                "modifierPaths": "[]",
                "operation": "create",
                "action": "accepted",
                "parentTermGroupName": "Root",
            },
        ),
        recipients=[],
    )

    # No entityUrl in the returned dict for glossary scenario
    expected = {
        "subject": "Bob has accepted the proposal to create Glossary Term Group named PII in Term Group Root.",
        "message": (
            "<b>Bob</b> has <b>accepted</b> the proposal to create Glossary Term Group "
            "named <b>PII</b> in Term Group <b>Root</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_proposal_status_change_parameters(request, base_url_fix)
    assert actual == expected


def test_build_proposal_status_change_parameters_for_description(
    base_url_fix: str,
) -> None:
    """
    Tests a "new proposal" for a description
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters={
                "actorName": "Alice",
                "entityName": "SampleKafkaDataset",
                "entityType": "Table",
                "entityPath": "/glossary/term/123",
                "operation": "update",
                "modifierType": "Description",
                "modifierNames": "[]",
                "modifierPaths": "[]",
                "action": "accepted",
            },
        ),
        recipients=[],
    )

    # No "entityUrl" returned for glossary scenario, per your snippet
    expected = {
        "subject": "Alice has accepted the proposal to update description for table SampleKafkaDataset.",
        "message": (
            "<b>Alice</b> has <b>accepted</b> the proposal to update description "
            "for table <b>SampleKafkaDataset</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_proposal_status_change_parameters(request, base_url_fix)
    assert actual == expected


def test_build_proposal_status_change_parameters_missing_parameters(
    base_url_fix: str,
) -> None:
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters=None,
        ),
        recipients=[],
    )
    with pytest.raises(ValueError):
        build_proposal_status_change_parameters(request, base_url_fix)


#
# Tests for build_proposer_proposal_status_change_parameters(...)
#


def test_build_proposer_proposal_status_change_parameters_normal(
    base_url_fix: str,
) -> None:
    """
    "Personalized" scenario for a normal, non-glossary proposal,
    with multiple items in modifierNames.
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters={
                "action": "approved",
                "operation": "add",
                "modifierType": "Tag(s)",
                "modifierNames": '["PII","Sensitive"]',  # => "PII and Sensitive"
                "modifierPaths": '["/glossary/urn:li:glossaryTerm:pii", "/glossary/urn:li:glossaryTerm:sensitive"]',
                "entityName": "SampleKafkaDataset",
                "entityPath": "/datasets/samplekafkadataset",
                "entityType": "Table",
            },
        ),
        recipients=[],
    )

    # => "Your proposal to add PII and Sensitive for Table SampleKafkaDataset has been approved."
    expected = {
        "subject": "Your proposal has been approved.",
        "message": (
            'Your proposal to add tag(s) <b><a href="https://example.acryl.io/glossary/urn:li:glossaryTerm:pii">PII</a></b> and <b><a href="https://example.acryl.io/glossary/urn:li:glossaryTerm:sensitive">Sensitive</a></b> '
            "for table <b>SampleKafkaDataset</b> has been <b>approved</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_proposer_proposal_status_change_parameters(request, base_url_fix)
    assert actual == expected


def test_build_proposer_proposal_status_change_parameters_for_sub_resource_no_modifier_paths(
    base_url_fix: str,
) -> None:
    """
    "Proposal status change" for a normal scenario WITH sub-resource,
    with multiple items in modifierNames array.
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters={
                "actorName": "John Joyce",
                "entityName": "SampleKafkaDataset",
                "entityPath": "/datasets/samplekafkadataset",
                "entityType": "Table",
                "modifierType": "Tag(s)",
                "modifierNames": '["PII","Sensitive"]',
                "modifierPaths": "[]",
                "operation": "remove",
                "action": "approved",
                "subResourceType": "DATASET_FIELD",
                "subResource": "foo",
            },
        ),
        recipients=[],
    )

    # => "Your proposal to add PII and Sensitive for schema field foo fo Table SampleKafkaDataset has been approved."
    expected = {
        "subject": "Your proposal has been approved.",
        "message": (
            "Your proposal to remove tag(s) <b>PII</b> and <b>Sensitive</b> "
            "for column <b>foo</b> of table <b>SampleKafkaDataset</b> has been <b>approved</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_proposer_proposal_status_change_parameters(request, base_url_fix)
    assert actual == expected


def test_build_proposer_proposal_status_change_parameters_glossary_term(
    base_url_fix: str,
) -> None:
    """
    "Personalized" scenario for a Glossary Term proposal,
    ignoring the modifierNames array.
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters={
                "action": "accepted",
                "operation": "create",
                "modifierType": "Glossary Term",
                "modifierNames": '["Ignored"]',
                "modifierPaths": "[]",
                "entityName": "Email Address",
                "entityPath": "/glossary/term/123",
                "parentTermGroupName": "PII",
            },
        ),
        recipients=[],
    )

    expected = {
        "subject": "Your proposal has been accepted.",
        "message": (
            "Your proposal to create Glossary Term named <b>Email Address</b>"
            " in Term Group <b>PII</b> has been <b>accepted</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_proposer_proposal_status_change_parameters(request, base_url_fix)
    assert actual == expected


def test_build_proposer_proposal_status_change_parameters_glossary_node(
    base_url_fix: str,
) -> None:
    """
    Tests the "personalized" scenario for a Glossary Term Group proposal.
    E.g. "Your proposal to create Glossary Term Group named PII has been accepted."
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters={
                "action": "rejected",
                "operation": "create",
                "modifierType": "Glossary Term Group",
                "modifierNames": '["Ignored"]',
                "modifierPaths": "[]",
                "entityName": "PII",
                "entityPath": "/glossary/term/123",
                "parentTermGroupName": "Root",
            },
        ),
        recipients=[],
    )

    expected = {
        "subject": "Your proposal has been rejected.",
        "message": (
            "Your proposal to create Glossary Term Group named <b>PII</b>"
            " in Term Group <b>Root</b> has been <b>rejected</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_proposer_proposal_status_change_parameters(request, base_url_fix)
    assert actual == expected


def test_build_proposer_proposal_status_change_parameters_description(
    base_url_fix: str,
) -> None:
    """
    Tests the "personalized" scenario for a description proposal.
    """
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters={
                "action": "rejected",
                "operation": "update",
                "modifierType": "Description",
                "modifierNames": "[]",
                "modifierPaths": "[]",
                "entityName": "SampleHiveDataset",
                "entityType": "Table",
                "entityPath": "/glossary/term/123",
            },
        ),
        recipients=[],
    )

    expected = {
        "subject": "Your proposal has been rejected.",
        "message": (
            "Your proposal to update description"
            " for table <b>SampleHiveDataset</b> has been <b>rejected</b>."
        ),
        "baseUrl": base_url_fix,
        "detailsUrl": "https://example.acryl.io/proposals",
    }

    actual = build_proposer_proposal_status_change_parameters(request, base_url_fix)
    assert actual == expected


def test_build_assertion_status_change_parameters_success(base_url_fix: str) -> None:
    # Case 1: Normal assertion.
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ASSERTION_STATUS_CHANGE,
            parameters={
                "assertionUrn": "assertion123",
                "assertionType": "FRESHNESS",
                "entityName": "SampleHiveDataset",
                "entityType": "Table",
                "entityPlatform": "Hive",
                "entityPath": "/datasets/samplehivedataset",
                "result": "SUCCESS",
                "description": "column x must not be null",
                "externalUrl": "https://external.com/results123",
            },
        ),
        recipients=[],
    )

    expected = {
        "subject": "Freshness Assertion has passed for Hive Table SampleHiveDataset!",
        "message": "Freshness Assertion 'column x must not be null' has <b>passed</b> for <b>Hive Table SampleHiveDataset</b>!",
        "entityName": "SampleHiveDataset",
        "detailsUrl": "https://external.com/results123",
        "entityUrl": "https://example.acryl.io/datasets/samplehivedataset",
        "baseUrl": base_url_fix,
    }

    assert build_assertion_status_change_parameters(request, base_url_fix) == expected

    # Case 2: Smart Assertion
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ASSERTION_STATUS_CHANGE,
            parameters={
                "assertionUrn": "assertion123",
                "assertionType": "FRESHNESS",
                "sourceType": "INFERRED",
                "entityName": "SampleHiveDataset",
                "entityType": "Table",
                "entityPlatform": "Hive",
                "entityPath": "/datasets/samplehivedataset",
                "result": "SUCCESS",
                "description": "column x must not be null",
                "externalUrl": "https://external.com/results123",
            },
        ),
        recipients=[],
    )

    expected = {
        "subject": "Smart Assertion has passed for Hive Table SampleHiveDataset!",
        "message": "Smart Assertion 'column x must not be null' has <b>passed</b> for <b>Hive Table SampleHiveDataset</b>!",
        "entityName": "SampleHiveDataset",
        "detailsUrl": "https://external.com/results123",
        "entityUrl": "https://example.acryl.io/datasets/samplehivedataset",
        "baseUrl": base_url_fix,
    }

    assert build_assertion_status_change_parameters(request, base_url_fix) == expected


def test_build_assertion_status_change_parameters_failure_with_default_url(
    base_url_fix: str,
) -> None:
    assertion_urn = "assertion123"
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ASSERTION_STATUS_CHANGE,
            parameters={
                "assertionUrn": assertion_urn,
                "assertionType": "VOLUME",
                "entityName": "SampleHiveDataset",
                "entityType": "Table",
                "entityPlatform": "Hive",
                "entityPath": "/datasets/samplehivedataset",
                "result": "FAILURE",
                "description": "Data volume decreased significantly",
            },
        ),
        recipients=[],
    )

    expected = {
        "subject": "Volume Assertion has failed for Hive Table SampleHiveDataset!",
        "message": "Volume Assertion 'Data volume decreased significantly' has <b>failed</b> for <b>Hive Table SampleHiveDataset</b>!",
        "entityName": "SampleHiveDataset",
        "detailsUrl": f"https://example.acryl.io/datasets/samplehivedataset/Validation/Assertions?assertion_urn={quote(assertion_urn)}",
        "entityUrl": "https://example.acryl.io/datasets/samplehivedataset",
        "baseUrl": base_url_fix,
    }

    assert build_assertion_status_change_parameters(request, base_url_fix) == expected


def test_build_assertion_status_change_parameters_missing_parameters(
    base_url_fix: str,
) -> None:
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ASSERTION_STATUS_CHANGE,
            parameters=None,
        ),
        recipients=[],
    )
    with pytest.raises(ValueError):
        build_assertion_status_change_parameters(request, base_url_fix)


def test_build_entity_change_parameters_with_single_modifier() -> None:
    base_url = "https://example.acryl.io"
    modifiers = {
        "actorName": "John Doe",
        "entityName": "SampleDataset",
        "entityPath": "/datasets/sampledataset",
        "entityType": "Table",
        "entityPlatform": "Hive",
        "operation": "added",
        "modifierType": "Tag(s)",
        "modifierCount": "1",
        "modifier0Name": "Test Tag",
    }

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters=modifiers,
        ),
        context=NotificationContextClass(runId=NON_INGESTION_RUN_ID),
        recipients=[],
    )

    expected_subject = (
        "Tag(s) Test Tag has been added for Hive Table SampleDataset by John Doe"
    )
    expected_message = "Tag(s) <b>Test Tag</b> has been added for <b>Hive Table SampleDataset</b> by John Doe."
    expected = {
        "subject": expected_subject,
        "message": expected_message,
        "entityName": "SampleDataset",
        "detailsUrl": f"{base_url}/datasets/sampledataset",
        "entityUrl": f"{base_url}/datasets/sampledataset",
        "baseUrl": base_url,
    }

    assert build_entity_change_parameters(request, base_url) == expected


@pytest.mark.parametrize(
    "modifier_count,expected_modifiers_string",
    [(3, "Tag1, Tag2, Tag3"), (5, "Tag1, Tag2, Tag3, + 2 more")],
)
def test_build_entity_change_parameters_with_multiple_modifiers(
    modifier_count: int, expected_modifiers_string: str
) -> None:
    base_url = "https://example.acryl.io"
    modifiers = {
        "actorName": "John Doe",
        "entityName": "SampleDataset",
        "entityPath": "/datasets/sampledataset",
        "entityType": "Table",
        "entityPlatform": "Hive",
        "operation": "added",
        "modifierType": "tags",
        "modifierCount": str(modifier_count),
    }
    for i in range(modifier_count):
        modifiers[f"modifier{i}Name"] = f"Tag{i + 1}"

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters=modifiers,
        ),
        context=NotificationContextClass(runId=NON_INGESTION_RUN_ID),
        recipients=[],
    )

    expected_subject = f"{modifier_count} tags have been added for Hive Table SampleDataset by John Doe"
    expected_message = f"Tags <b>{expected_modifiers_string}</b> have been added for <b>Hive Table SampleDataset</b> by John Doe."
    expected = {
        "subject": expected_subject,
        "message": expected_message,
        "entityName": "SampleDataset",
        "detailsUrl": f"{base_url}/datasets/sampledataset",
        "entityUrl": f"{base_url}/datasets/sampledataset",
        "baseUrl": base_url,
    }

    assert build_entity_change_parameters(request, base_url) == expected


@pytest.mark.parametrize(
    "modifier_count,expected_modifiers_string",
    [(3, "Tag1, Tag2, Tag3"), (5, "Tag1, Tag2, Tag3, + 2 more")],
)
def test_build_entity_change_parameters_with_ingestion_source_context(
    modifier_count: int, expected_modifiers_string: str
) -> None:
    base_url = "https://example.acryl.io"
    modifiers = {
        "actorName": "__datahub_system",
        "entityName": "SampleDataset",
        "entityPath": "/datasets/sampledataset",
        "entityType": "Table",
        "entityPlatform": "Hive",
        "operation": "added",
        "modifierType": "tags",
        "modifierCount": str(modifier_count),
    }
    for i in range(modifier_count):
        modifiers[f"modifier{i}Name"] = f"Tag{i + 1}"

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters=modifiers,
        ),
        context=NotificationContextClass(runId="ingestion-run-id"),
        recipients=[],
    )

    expected_subject = f"{modifier_count} tags have been added for Hive Table SampleDataset during sync with Hive"
    expected_message = f"Tags <b>{expected_modifiers_string}</b> have been added for <b>Hive Table SampleDataset</b> during sync with Hive."
    expected = {
        "subject": expected_subject,
        "message": expected_message,
        "entityName": "SampleDataset",
        "detailsUrl": f"{base_url}/datasets/sampledataset",
        "entityUrl": f"{base_url}/datasets/sampledataset",
        "baseUrl": base_url,
    }

    assert build_entity_change_parameters(request, base_url) == expected


def test_build_entity_change_parameters_for_deprecation() -> None:
    base_url = "https://example.acryl.io"
    timestamp = "0"
    modifiers = {
        "actorName": "John Doe",
        "entityName": "SampleDataset",
        "entityPath": "/datasets/sampledataset",
        "entityType": "Table",
        "entityPlatform": "Hive",
        "operation": "marked as deprecated",
        "modifierType": "deprecation",
        "timestamp": timestamp,
        "note": "Test Note",
    }

    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters=modifiers,
        ),
        context=NotificationContextClass(runId=NON_INGESTION_RUN_ID),
        recipients=[],
    )

    expected_subject = (
        "Hive Table SampleDataset has been marked as deprecated by John Doe"
    )
    expected_message = "<b>Hive Table SampleDataset</b> has been <b>marked as deprecated</b> by John Doe."
    expected_timestamp = timestamp_to_date(timestamp)
    expected_message_context = f"<br><br><b>Note</b>: Test Note<br><b>Deprecation Date</b>: {expected_timestamp}<br><br>"
    expected_message_final = expected_message + expected_message_context
    expected = {
        "subject": expected_subject,
        "message": expected_message_final,
        "entityName": "SampleDataset",
        "detailsUrl": f"{base_url}/datasets/sampledataset",
        "entityUrl": f"{base_url}/datasets/sampledataset",
        "baseUrl": base_url,
    }

    assert build_entity_change_parameters(request, base_url) == expected


def test_build_entity_change_parameters_missing_parameters(base_url_fix: str) -> None:
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters=None,
        ),
        recipients=[],
    )
    with pytest.raises(ValueError):
        build_entity_change_parameters(request, base_url_fix)


@pytest.mark.parametrize(
    "status_text,expected_subject,expected_message",
    [
        (
            "failed",
            "Ingestion source my-ingestion-source of type kafka has failed.",
            "Ingestion source <b>my-ingestion-source</b> of type kafka has <b>failed</b>.",
        ),
        (
            "completed",
            "Ingestion source my-ingestion-source of type kafka has completed.",
            "Ingestion source <b>my-ingestion-source</b> of type kafka has <b>completed</b>.",
        ),
        (
            "cancelled",
            "Ingestion source my-ingestion-source of type kafka has cancelled.",
            "Ingestion source <b>my-ingestion-source</b> of type kafka has <b>cancelled</b>.",
        ),
        (
            "timed out",
            "Ingestion source my-ingestion-source of type kafka has timed out.",
            "Ingestion source <b>my-ingestion-source</b> of type kafka has <b>timed out</b>.",
        ),
        (
            "started",
            "Ingestion source my-ingestion-source of type kafka has started.",
            "Ingestion source <b>my-ingestion-source</b> of type kafka has <b>started</b>.",
        ),
    ],
)
def test_build_ingestion_run_change_parameters_various_statuses(
    status_text: str, expected_subject: str, expected_message: str
) -> None:
    base_url = "https://example.acryl.io"
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_INGESTION_RUN_CHANGE,
            parameters={
                "sourceName": "my-ingestion-source",
                "sourceType": "kafka",  # Keeping the source type constant for simplicity
                "statusText": status_text,
            },
        ),
        recipients=[],
    )

    expected = {
        "subject": expected_subject,
        "message": expected_message,
        "detailsUrl": f"{base_url}/ingestion",
        "baseUrl": base_url,
    }

    assert build_ingestion_run_change_parameters(request, base_url) == expected


def test_build_ingestion_run_change_parameters_missing_parameters() -> None:
    base_url = "https://example.acryl.io"
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_INGESTION_RUN_CHANGE,
            parameters=None,
        ),
        recipients=[],
    )
    with pytest.raises(ValueError):
        build_ingestion_run_change_parameters(request, base_url)
