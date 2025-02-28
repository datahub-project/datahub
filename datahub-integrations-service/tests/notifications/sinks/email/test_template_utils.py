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
    timestamp_to_date,
)


@pytest.fixture
def base_url() -> str:
    return "https://example.acryl.io"


def get_notification_request(
    template: str, parameters: dict
) -> NotificationRequestClass:
    message = NotificationMessageClass(template=template, parameters=parameters)
    return NotificationRequestClass(message=message, recipients=[], sinks=[])


def test_build_new_incident_parameters(base_url: str) -> None:
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
        "baseUrl": base_url,
    }
    assert build_new_incident_parameters(notification_request, base_url) == expected


def test_build_incident_status_change_parameters(base_url: str) -> None:
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
        "baseUrl": base_url,
    }
    assert (
        build_incident_status_change_parameters(notification_request, base_url)
        == expected
    )


def test_build_new_proposal_parameters_for_entity(base_url: str) -> None:
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_PROPOSAL,
            parameters={
                "actorName": "John Joyce",
                "entityName": "SampleKafkaDataset",
                "entityType": "Table",
                "entityPlatform": "Hive",
                "entityPath": "/datasets/samplekafkadataset",
                "modifierType": "Tag",
                "modifierName": "PII",
            },
        ),
        recipients=[],
    )

    expected = {
        "subject": "Tag PII has been proposed for Hive Table SampleKafkaDataset by John Joyce.",
        "message": "Tag <b>PII</b> has been proposed for <b>Hive Table SampleKafkaDataset</b> by John Joyce.",
        "entityName": "SampleKafkaDataset",
        "detailsUrl": "https://example.acryl.io/datasets/samplekafkadataset",
        "entityUrl": "https://example.acryl.io/datasets/samplekafkadataset",
        "baseUrl": base_url,
    }

    assert build_new_proposal_parameters(request, base_url) == expected


def test_build_new_proposal_parameters_for_sub_resource(base_url: str) -> None:
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_PROPOSAL,
            parameters={
                "actorName": "John Joyce",
                "entityName": "SampleKafkaDataset",
                "entityType": "Table",
                "entityPlatform": "Hive",
                "entityPath": "/datasets/samplekafkadataset",
                "modifierType": "glossary term",
                "modifierName": "FOOBAR",
                "subResourceType": "column",
                "subResource": "bar",
            },
        ),
        recipients=[],
    )

    expected = {
        "subject": "Glossary term FOOBAR has been proposed for column bar of Hive Table SampleKafkaDataset by John Joyce.",
        "message": "Glossary term <b>FOOBAR</b> has been proposed for column <b>bar</b> of <b>Hive Table SampleKafkaDataset</b> by John Joyce.",
        "entityName": "SampleKafkaDataset",
        "detailsUrl": "https://example.acryl.io/datasets/samplekafkadataset",
        "entityUrl": "https://example.acryl.io/datasets/samplekafkadataset",
        "baseUrl": base_url,
    }

    assert build_new_proposal_parameters(request, base_url) == expected


def test_build_new_proposal_parameters_missing_parameters(base_url: str) -> None:
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_NEW_PROPOSAL,
            parameters=None,
        ),
        recipients=[],
    )
    with pytest.raises(ValueError):
        build_new_proposal_parameters(request, base_url)


def test_build_proposal_status_change_parameters_for_entity(base_url: str) -> None:
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters={
                "actorName": "John Joyce",
                "entityName": "SampleKafkaDataset",
                "entityPath": "/datasets/samplekafkadataset",
                "entityType": "Table",
                "entityPlatform": "Hive",
                "modifierType": "tag",
                "modifierName": "PII",
                "operation": "add",
                "action": "approved",
            },
        ),
        recipients=[],
    )

    expected = {
        "subject": "Proposal to add tag PII for Hive Table SampleKafkaDataset has been approved by John Joyce.",
        "message": "Proposal to add tag <b>PII</b> for <b>Hive Table SampleKafkaDataset</b> has been approved by John Joyce.",
        "entityName": "SampleKafkaDataset",
        "detailsUrl": "https://example.acryl.io/datasets/samplekafkadataset",
        "entityUrl": "https://example.acryl.io/datasets/samplekafkadataset",
        "baseUrl": base_url,
    }

    assert build_proposal_status_change_parameters(request, base_url) == expected


def test_build_proposal_status_change_parameters_for_sub_resource(
    base_url: str,
) -> None:
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters={
                "actorName": "John Joyce",
                "entityName": "SampleKafkaDataset",
                "entityType": "Table",
                "entityPlatform": "Hive",
                "entityPath": "/datasets/samplekafkadataset",
                "modifierType": "glossary term",
                "modifierName": "FOOBAR",
                "operation": "remove",
                "action": "denied",
                "subResourceType": "column",
                "subResource": "foo",
            },
        ),
        recipients=[],
    )

    expected = {
        "subject": "Proposal to remove glossary term FOOBAR for column foo of Hive Table SampleKafkaDataset has been denied by John Joyce.",
        "message": "Proposal to remove glossary term <b>FOOBAR</b> for column <b>foo</b> of <b>Hive Table SampleKafkaDataset</b> has been denied by John Joyce.",
        "entityName": "SampleKafkaDataset",
        "detailsUrl": "https://example.acryl.io/datasets/samplekafkadataset",
        "entityUrl": "https://example.acryl.io/datasets/samplekafkadataset",
        "baseUrl": base_url,
    }

    assert build_proposal_status_change_parameters(request, base_url) == expected


def test_build_proposal_status_change_parameters_missing_parameters(
    base_url: str,
) -> None:
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_PROPOSAL_STATUS_CHANGE,
            parameters=None,
        ),
        recipients=[],
    )
    with pytest.raises(ValueError):
        build_proposal_status_change_parameters(request, base_url)


def test_build_assertion_status_change_parameters_success(base_url: str) -> None:
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
        "baseUrl": base_url,
    }

    assert build_assertion_status_change_parameters(request, base_url) == expected

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
        "baseUrl": base_url,
    }

    assert build_assertion_status_change_parameters(request, base_url) == expected


def test_build_assertion_status_change_parameters_failure_with_default_url(
    base_url: str,
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
        "baseUrl": base_url,
    }

    assert build_assertion_status_change_parameters(request, base_url) == expected


def test_build_assertion_status_change_parameters_missing_parameters(
    base_url: str,
) -> None:
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ASSERTION_STATUS_CHANGE,
            parameters=None,
        ),
        recipients=[],
    )
    with pytest.raises(ValueError):
        build_assertion_status_change_parameters(request, base_url)


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


def test_build_entity_change_parameters_missing_parameters(base_url: str) -> None:
    request = NotificationRequestClass(
        message=NotificationMessageClass(
            template=NotificationTemplateTypeClass.BROADCAST_ENTITY_CHANGE,
            parameters=None,
        ),
        recipients=[],
    )
    with pytest.raises(ValueError):
        build_entity_change_parameters(request, base_url)


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
