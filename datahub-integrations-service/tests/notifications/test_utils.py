from datahub.metadata.schema_classes import (
    NotificationMessageClass,
    NotificationRecipientClass,
    NotificationRequestClass,
    NotificationTemplateTypeClass,
)

from datahub_integrations.notifications.notification_tracking import NotificationType
from datahub_integrations.notifications.utils import get_notification_tracking_info


class TestGetNotificationTrackingInfoAssertionSourceType:
    """Tests for assertion source type tracking via notification_subtype."""

    def _make_assertion_request(
        self, source_type: str | None = None
    ) -> NotificationRequestClass:
        params = {
            "assertionUrn": "urn:li:assertion:test",
            "assertionRunId": "run-1",
            "assertionRunTimestampMillis": "1700000000000",
            "result": "FAILURE",
        }
        if source_type is not None:
            params["sourceType"] = source_type

        return NotificationRequestClass(
            recipients=[
                NotificationRecipientClass(id="test@example.com", type="EMAIL")
            ],
            message=NotificationMessageClass(
                template=NotificationTemplateTypeClass.BROADCAST_ASSERTION_STATUS_CHANGE,
                parameters=params,
            ),
        )

    def test_source_type_tracked_as_subtype(self) -> None:
        request = self._make_assertion_request(source_type="INFERRED")
        tracking_info = get_notification_tracking_info(request)

        assert tracking_info is not None
        assert tracking_info.notification_type == NotificationType.ASSERTION
        assert tracking_info.notification_subtype == "INFERRED"

    def test_native_source_type_tracked_as_subtype(self) -> None:
        request = self._make_assertion_request(source_type="NATIVE")
        tracking_info = get_notification_tracking_info(request)

        assert tracking_info is not None
        assert tracking_info.notification_type == NotificationType.ASSERTION
        assert tracking_info.notification_subtype == "NATIVE"

    def test_external_source_type_tracked_as_subtype(self) -> None:
        request = self._make_assertion_request(source_type="EXTERNAL")
        tracking_info = get_notification_tracking_info(request)

        assert tracking_info is not None
        assert tracking_info.notification_type == NotificationType.ASSERTION
        assert tracking_info.notification_subtype == "EXTERNAL"

    def test_missing_source_type_has_none_subtype(self) -> None:
        request = self._make_assertion_request(source_type=None)
        tracking_info = get_notification_tracking_info(request)

        assert tracking_info is not None
        assert tracking_info.notification_type == NotificationType.ASSERTION
        assert tracking_info.notification_subtype is None
