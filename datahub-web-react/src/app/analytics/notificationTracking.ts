export const NOTIFICATION_QUERY_PARAM_TYPE = 'notification_type';
export const NOTIFICATION_QUERY_PARAM_ID = 'notification_id';
export const NOTIFICATION_QUERY_PARAM_CHANNEL = 'notification_channel';

export enum NotificationChannel {
    Slack = 'slack',
}

export enum NotificationType {
    Assertion = 'assertion',
    Incident = 'incident',
}

export const NOTIFICATION_CONTEXT_STORAGE_KEY = 'datahub.notificationContext.v1';
export const NOTIFICATION_CONTEXT_EMIT_PREFIX = 'datahub.notificationContext.emitted.v1';
export const NOTIFICATION_CONTEXT_TTL_MILLIS = 60 * 60 * 1000; // 1 hour
