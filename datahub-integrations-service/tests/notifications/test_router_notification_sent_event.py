from unittest.mock import patch

from datahub.metadata.schema_classes import (
    NotificationRecipientTypeClass,
    NotificationTemplateTypeClass,
)
from fastapi.testclient import TestClient

from datahub_integrations.notifications.notification_tracking import NotificationChannel
from datahub_integrations.server import app


def test_notifications_send_emits_notification_sent_event_for_assertion_status_change() -> (
    None
):
    client = TestClient(app)

    payload = {
        "message": {
            "template": NotificationTemplateTypeClass.BROADCAST_ASSERTION_STATUS_CHANGE,
            "parameters": {
                "assertionUrn": "urn:li:assertion:test",
                "assertionRunId": "run-1",
                "assertionRunTimestampMillis": "1700000000000",
                "entityName": "Sample",
                "entityPath": "/datasets/test",
                "result": "FAILURE",
                "resultReason": "bad",
                "description": "my assertion",
            },
        },
        "recipients": [{"type": NotificationRecipientTypeClass.SLACK_DM, "id": "U123"}],
    }

    with patch(
        "datahub_integrations.notifications.router.track_saas_event"
    ) as mock_track:
        resp = client.post("/private/notifications/send", json=payload)
        assert resp.status_code == 200
        assert mock_track.call_count == 1
        event = mock_track.call_args[0][0]

        assert event.type == "NotificationSentEvent"
        assert event.notificationType == "assertion"
        assert event.notificationChannel == NotificationChannel.SLACK
        assert event.notificationId == "urn:li:assertion:test|1700000000000|run-1"
        assert event.recipientCount == 1


def test_notifications_send_emits_notification_sent_event_for_incident() -> None:
    client = TestClient(app)

    payload = {
        "message": {
            "template": NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
            "parameters": {
                "incidentUrn": "urn:li:incident:test",
                "entityName": "Sample",
                "entityPath": "/datasets/test",
            },
        },
        "recipients": [
            {"type": NotificationRecipientTypeClass.SLACK_CHANNEL, "id": "C123"}
        ],
    }

    with patch(
        "datahub_integrations.notifications.router.track_saas_event"
    ) as mock_track:
        resp = client.post("/private/notifications/send", json=payload)
        assert resp.status_code == 200
        assert mock_track.call_count == 1
        event = mock_track.call_args[0][0]

        assert event.type == "NotificationSentEvent"
        assert event.notificationType == "incident"
        assert event.notificationChannel == NotificationChannel.SLACK
        assert event.notificationId == "urn:li:incident:test"
        assert event.recipientCount == 1
