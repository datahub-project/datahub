from dataclasses import dataclass
from typing import Any

from datahub.metadata.schema_classes import (
    AssertionResultTypeClass,
    NotificationRequestClass,
    NotificationTemplateTypeClass,
)
from loguru import logger

from datahub_integrations.notifications.notification_tracking import (
    NotificationChannel,
    NotificationType,
)
from datahub_integrations.telemetry.notification_events import (
    NotificationDeliveredEvent,
)
from datahub_integrations.telemetry.telemetry import (
    report_notification_delivery_failure,
    track_saas_event,
)


@dataclass(frozen=True)
class NotificationTrackingInfo:
    notification_type: NotificationType
    notification_id: str
    external_platform: str | None
    has_external_url: bool
    notification_subtype: str | None = None


def track_notification_delivery_success(
    tracking_info: NotificationTrackingInfo | None,
    *,
    notification_channel: NotificationChannel,
    recipient_count: int,
) -> None:
    if not tracking_info:
        return
    try:
        track_saas_event(
            NotificationDeliveredEvent(
                notificationType=tracking_info.notification_type,
                notificationChannel=notification_channel,
                notificationId=tracking_info.notification_id,
                recipientCount=recipient_count,
                externalPlatform=tracking_info.external_platform,
                hasExternalUrl=tracking_info.has_external_url,
                notificationSubtype=tracking_info.notification_subtype,
            )
        )
    except Exception as exc:
        logger.warning(f"Failed to track notification delivery success: {exc}")


def track_notification_delivery_failure(
    tracking_info: NotificationTrackingInfo | None,
    *,
    notification_channel: NotificationChannel,
    recipient_count: int,
    error_type: str | None,
    error_message: str | None,
) -> None:
    if not tracking_info:
        return
    try:
        report_notification_delivery_failure(
            notification_type=tracking_info.notification_type.value,
            notification_channel=notification_channel.value,
            notification_id=tracking_info.notification_id,
            recipient_count=recipient_count,
            error_type=error_type,
            error_message=error_message,
        )
    except Exception as exc:
        logger.warning(f"Failed to report notification delivery failure: {exc}")


def build_assertion_notification_id(
    *,
    assertion_urn: str,
    assertion_run_timestamp_millis: Any | None,
    assertion_run_id: Any | None,
) -> str:
    if assertion_run_timestamp_millis is None or assertion_run_id is None:
        return assertion_urn

    ts_millis = str(assertion_run_timestamp_millis)
    run_id = str(assertion_run_id)
    if not ts_millis or not run_id:
        return assertion_urn

    return f"{assertion_urn}|{ts_millis}|{run_id}"


def get_notification_tracking_info(
    notification_request: NotificationRequestClass,
) -> NotificationTrackingInfo | None:
    template = notification_request.message.template
    params = notification_request.message.parameters or {}

    if template == NotificationTemplateTypeClass.BROADCAST_ASSERTION_STATUS_CHANGE:
        assertion_urn = params.get("assertionUrn")
        if not assertion_urn:
            return None
        run_id = params.get("assertionRunId")
        ts_millis = params.get("assertionRunTimestampMillis")
        result_value = params.get("result")
        result_str = "" if result_value is None else str(result_value)
        # Failure notifications are the only ones we track.
        # Other events are too noisy and/or not useful, so we don't track them.
        if result_str != AssertionResultTypeClass.FAILURE:
            return None
        notification_id = build_assertion_notification_id(
            assertion_urn=assertion_urn,
            assertion_run_timestamp_millis=ts_millis,
            assertion_run_id=run_id,
        )
        # Track assertion source type as subtype for analytics filtering/grouping
        notification_subtype = params.get("sourceType")
        return NotificationTrackingInfo(
            notification_type=NotificationType.ASSERTION,
            notification_id=notification_id,
            external_platform=params.get("externalPlatform"),
            has_external_url=bool(params.get("externalUrl")),
            notification_subtype=notification_subtype,
        )

    if template in (
        NotificationTemplateTypeClass.BROADCAST_NEW_INCIDENT,
        NotificationTemplateTypeClass.BROADCAST_INCIDENT_STATUS_CHANGE,
    ):
        incident_urn = params.get("incidentUrn")
        if not incident_urn:
            return None
        return NotificationTrackingInfo(
            notification_type=NotificationType.INCIDENT,
            notification_id=incident_urn,
            external_platform=params.get("externalPlatform"),
            has_external_url=bool(params.get("externalUrl")),
        )

    return None
