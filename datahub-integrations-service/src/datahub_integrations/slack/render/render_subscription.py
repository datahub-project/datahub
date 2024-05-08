import json
from enum import Enum

from datahub_integrations.slack.utils.entity_extract import ExtractedEntity


class EntityChangeType(Enum):
    AssertionError = "ASSERTION_ERROR"
    AssertionFailed = "ASSERTION_FAILED"
    # Assertion status changes.
    AssertionPassed = "ASSERTION_PASSED"
    # Deprecation status changes.
    Deprecated = "DEPRECATED"
    # Documentation changes. Added removed or proposed.
    DocumentationChange = "DOCUMENTATION_CHANGE"
    # Glossary term changes. Added removed or proposed.
    GlossaryTermAdded = "GLOSSARY_TERM_ADDED"
    GlossaryTermProposed = "GLOSSARY_TERM_PROPOSED"
    GlossaryTermRemoved = "GLOSSARY_TERM_REMOVED"
    # Incident status changes.
    IncidentRaised = "INCIDENT_RAISED"
    IncidentResolved = "INCIDENT_RESOLVED"
    IngestionFailed = "INGESTION_FAILED"
    # Ingestion status changes.
    IngestionSucceeded = "INGESTION_SUCCEEDED"
    # Schema changes.
    OperationColumnAdded = "OPERATION_COLUMN_ADDED"
    OperationColumnModified = "OPERATION_COLUMN_MODIFIED"
    OperationColumnRemoved = "OPERATION_COLUMN_REMOVED"
    # Operational metadata changes.
    OperationRowsInserted = "OPERATION_ROWS_INSERTED"
    OperationRowsRemoved = "OPERATION_ROWS_REMOVED"
    OperationRowsUpdated = "OPERATION_ROWS_UPDATED"
    # Ownership changes. Added or removed.
    OwnerAdded = "OWNER_ADDED"
    OwnerRemoved = "OWNER_REMOVED"
    # Tag changes. Added removed or proposed.
    TagAdded = "TAG_ADDED"
    TagProposed = "TAG_PROPOSED"
    TagRemoved = "TAG_REMOVED"
    TestFailed = "TEST_FAILED"
    # Test status changes.
    TestPassed = "TEST_PASSED"
    Undeprecated = "UNDEPRECATED"


class EntityChangeTypeGroup(Enum):
    Assertion = "ASSERTION"
    Deprecated = "DEPRECATED"
    Incident = "INCIDENT"
    Schema = "SCHEMA"
    Owner = "OWNER"
    Glossary = "GLOSSARY"
    Tag = "TAG"

    def get_change_types(self):
        if self == EntityChangeTypeGroup.Assertion:
            return [
                EntityChangeType.AssertionFailed,
                EntityChangeType.AssertionPassed,
            ]
        if self == EntityChangeTypeGroup.Deprecated:
            return [
                EntityChangeType.Deprecated,
                EntityChangeType.Undeprecated,
            ]
        if self == EntityChangeTypeGroup.Incident:
            return [
                EntityChangeType.IncidentRaised,
                EntityChangeType.IncidentResolved,
            ]
        if self == EntityChangeTypeGroup.Schema:
            return [
                EntityChangeType.OperationColumnAdded,
                EntityChangeType.OperationColumnModified,
                EntityChangeType.OperationColumnRemoved,
            ]
        if self == EntityChangeTypeGroup.Owner:
            return [
                EntityChangeType.OwnerAdded,
                EntityChangeType.OwnerRemoved,
            ]
        if self == EntityChangeTypeGroup.Glossary:
            return [
                EntityChangeType.GlossaryTermAdded,
                EntityChangeType.GlossaryTermProposed,
                EntityChangeType.GlossaryTermRemoved,
            ]
        if self == EntityChangeTypeGroup.Tag:
            return [
                EntityChangeType.TagAdded,
                EntityChangeType.TagProposed,
                EntityChangeType.TagRemoved,
            ]
        return []


def render_subscription_modal(entity: ExtractedEntity, response_url: str) -> dict:
    return {
        "type": "modal",
        "callback_id": "subscribe",
        "private_metadata": json.dumps(
            {"urn": entity.urn, "response_url": response_url}
        ),
        "title": {
            "type": "plain_text",
            "text": "Create Subscription",
        },
        "submit": {
            "type": "plain_text",
            "text": "Subscribe",
        },
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{entity.name}",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "image",
                        "alt_text": entity.platform,
                        "image_url": f"{entity.platform_url}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f" {' | '.join([entity.type])}",
                    },
                ],
            },
            {"type": "divider"},
            {
                "type": "input",
                "block_id": "ect_block",
                "label": {
                    "type": "plain_text",
                    "text": "Send notifications for...",
                },
                "element": {
                    "type": "checkboxes",
                    "action_id": "ect_action",
                    "options": [
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Entity has been deprecated",
                            },
                            "value": EntityChangeTypeGroup.Deprecated.value,
                        },
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Assertion status changes",
                            },
                            "value": EntityChangeTypeGroup.Assertion.value,
                        },
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Incident status changes",
                            },
                            "value": EntityChangeTypeGroup.Incident.value,
                        },
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Schema change events",
                            },
                            "value": EntityChangeTypeGroup.Schema.value,
                        },
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Ownership changes",
                            },
                            "value": EntityChangeTypeGroup.Owner.value,
                        },
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Glossary term changes",
                            },
                            "value": EntityChangeTypeGroup.Glossary.value,
                        },
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Tag changes",
                            },
                            "value": EntityChangeTypeGroup.Tag.value,
                        },
                    ],
                    "initial_options": [
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Entity has been deprecated",
                            },
                            "value": EntityChangeTypeGroup.Deprecated.value,
                        },
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Assertion status changes",
                            },
                            "value": EntityChangeTypeGroup.Assertion.value,
                        },
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Incident status changes",
                            },
                            "value": EntityChangeTypeGroup.Incident.value,
                        },
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Schema change events",
                            },
                            "value": EntityChangeTypeGroup.Schema.value,
                        },
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Ownership changes",
                            },
                            "value": EntityChangeTypeGroup.Owner.value,
                        },
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Glossary term changes",
                            },
                            "value": EntityChangeTypeGroup.Glossary.value,
                        },
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Tag changes",
                            },
                            "value": EntityChangeTypeGroup.Tag.value,
                        },
                    ],
                },
            },
        ],
    }
