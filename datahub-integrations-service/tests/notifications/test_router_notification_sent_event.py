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
        assert event.externalPlatform is None
        assert event.hasExternalUrl is False


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
        assert event.externalPlatform is None
        assert event.hasExternalUrl is False


def test_notifications_send_emits_notification_sent_event_for_multiple_channels() -> (
    None
):
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
            {"type": NotificationRecipientTypeClass.SLACK_CHANNEL, "id": "C123"},
            {"type": NotificationRecipientTypeClass.EMAIL, "id": "test@example.com"},
            {"type": NotificationRecipientTypeClass.TEAMS_DM, "id": "teams-user"},
        ],
    }

    with patch(
        "datahub_integrations.notifications.router.track_saas_event"
    ) as mock_track:
        resp = client.post("/private/notifications/send", json=payload)
        assert resp.status_code == 200
        assert mock_track.call_count == 3

        events = [call_args[0][0] for call_args in mock_track.call_args_list]
        channels = {event.notificationChannel for event in events}

        assert channels == {
            NotificationChannel.SLACK,
            NotificationChannel.EMAIL,
            NotificationChannel.TEAMS,
        }

        for event in events:
            assert event.type == "NotificationSentEvent"
            assert event.notificationType == "incident"
            assert event.notificationId == "urn:li:incident:test"
            assert event.recipientCount == 1
            assert event.externalPlatform is None
            assert event.hasExternalUrl is False


def test_notifications_send_emits_notification_sent_event_for_assertion_multiple_channels() -> (
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
        "recipients": [
            {"type": NotificationRecipientTypeClass.SLACK_DM, "id": "U123"},
            {"type": NotificationRecipientTypeClass.EMAIL, "id": "test@example.com"},
            {"type": NotificationRecipientTypeClass.TEAMS_CHANNEL, "id": "teams-ch"},
        ],
    }

    with patch(
        "datahub_integrations.notifications.router.track_saas_event"
    ) as mock_track:
        resp = client.post("/private/notifications/send", json=payload)
        assert resp.status_code == 200
        assert mock_track.call_count == 3

        events = [call_args[0][0] for call_args in mock_track.call_args_list]
        channels = {event.notificationChannel for event in events}

        assert channels == {
            NotificationChannel.SLACK,
            NotificationChannel.EMAIL,
            NotificationChannel.TEAMS,
        }

        for event in events:
            assert event.type == "NotificationSentEvent"
            assert event.notificationType == "assertion"
            assert event.notificationId == "urn:li:assertion:test|1700000000000|run-1"
            assert event.recipientCount == 1
            assert event.externalPlatform is None
            assert event.hasExternalUrl is False


def test_notifications_send_emits_notification_sent_event_external_destination() -> (
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
                "externalUrl": "https://example.com/result",
                "externalPlatform": "dbt",
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
        assert event.externalPlatform == "dbt"
        assert event.hasExternalUrl is True


def test_notifications_send_emits_notification_sent_event_external_url_without_platform() -> (
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
                "externalUrl": "https://example.com/result",
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
        assert event.externalPlatform is None
        assert event.hasExternalUrl is True


def test_notifications_send_emits_notification_sent_event_email_external_destination() -> (
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
                "externalUrl": "https://example.com/result",
                "externalPlatform": "dbt",
            },
        },
        "recipients": [
            {"type": NotificationRecipientTypeClass.EMAIL, "id": "test@example.com"}
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
        assert event.notificationType == "assertion"
        assert event.notificationChannel == NotificationChannel.EMAIL
        assert event.notificationId == "urn:li:assertion:test|1700000000000|run-1"
        assert event.recipientCount == 1
        assert event.externalPlatform == "dbt"
        assert event.hasExternalUrl is True


def test_notifications_send_emits_notification_sent_event_teams_external_destination() -> (
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
                "externalUrl": "https://example.com/result",
                "externalPlatform": "dbt",
            },
        },
        "recipients": [
            {"type": NotificationRecipientTypeClass.TEAMS_DM, "id": "teams-user"}
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
        assert event.notificationType == "assertion"
        assert event.notificationChannel == NotificationChannel.TEAMS
        assert event.notificationId == "urn:li:assertion:test|1700000000000|run-1"
        assert event.recipientCount == 1
        assert event.externalPlatform == "dbt"
        assert event.hasExternalUrl is True


def test_notifications_send_emits_subtype_for_inferred_source_type() -> None:
    """Assertion source type is tracked via notificationSubtype field."""
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
                "sourceType": "INFERRED",
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
        assert event.notificationSubtype == "INFERRED"
        assert event.notificationChannel == NotificationChannel.SLACK
        assert event.notificationId == "urn:li:assertion:test|1700000000000|run-1"
