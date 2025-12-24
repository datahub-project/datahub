import { useEffect } from 'react';
import { useLocation } from 'react-router-dom';

import analytics, { EventType } from '@app/analytics';
import {
    NOTIFICATION_CONTEXT_EMIT_PREFIX,
    NOTIFICATION_CONTEXT_STORAGE_KEY,
    NOTIFICATION_CONTEXT_TTL_MILLIS,
    NOTIFICATION_QUERY_PARAM_CHANNEL,
    NOTIFICATION_QUERY_PARAM_ID,
    NOTIFICATION_QUERY_PARAM_TYPE,
    NotificationChannel,
    NotificationType,
} from '@app/analytics/notificationTracking';

type NotificationContext = {
    notificationType: NotificationType;
    notificationId: string;
    notificationChannel: NotificationChannel;
    createdAtMillis: number;
    expiresAtMillis: number;
};

function readNotificationContextFromUrl(
    search: string,
): Pick<NotificationContext, 'notificationType' | 'notificationId' | 'notificationChannel'> | null {
    const params = new URLSearchParams(search);
    const rawType = params.get(NOTIFICATION_QUERY_PARAM_TYPE);
    const rawId = params.get(NOTIFICATION_QUERY_PARAM_ID);
    const rawChannel = params.get(NOTIFICATION_QUERY_PARAM_CHANNEL);

    if (!rawType || !rawId || !rawChannel) return null;

    const notificationType = Object.values(NotificationType).includes(rawType as NotificationType)
        ? (rawType as NotificationType)
        : null;
    const notificationChannel = Object.values(NotificationChannel).includes(rawChannel as NotificationChannel)
        ? (rawChannel as NotificationChannel)
        : null;

    if (!notificationType || !notificationChannel) return null;

    return { notificationType, notificationId: rawId, notificationChannel };
}

function buildEmitKey(
    ctx: Pick<NotificationContext, 'notificationType' | 'notificationId' | 'notificationChannel'>,
): string {
    return `${NOTIFICATION_CONTEXT_EMIT_PREFIX}.${ctx.notificationChannel}.${ctx.notificationType}.${ctx.notificationId}`;
}

export const useCaptureNotificationContext = () => {
    const location = useLocation();

    useEffect(() => {
        const urlCtx = readNotificationContextFromUrl(location.search);
        if (!urlCtx) return;

        const now = Date.now();
        const ctx: NotificationContext = {
            ...urlCtx,
            createdAtMillis: now,
            expiresAtMillis: now + NOTIFICATION_CONTEXT_TTL_MILLIS,
        };

        sessionStorage.setItem(
            NOTIFICATION_CONTEXT_STORAGE_KEY,
            JSON.stringify({
                type: 'notification',
                notification: {
                    type: urlCtx.notificationType,
                    notificationId: urlCtx.notificationId,
                    notificationChannel: urlCtx.notificationChannel,
                },
                createdAtMillis: ctx.createdAtMillis,
                expiresAtMillis: ctx.expiresAtMillis,
            }),
        );

        const emitKey = buildEmitKey(urlCtx);
        if (!sessionStorage.getItem(emitKey)) {
            sessionStorage.setItem(emitKey, 'true');
            analytics.event({
                type: EventType.NotificationOpenEvent,
                notificationType: urlCtx.notificationType,
                notificationId: urlCtx.notificationId,
                notificationChannel: urlCtx.notificationChannel,
                pathname: location.pathname,
            });
        }
    }, [location.pathname, location.search]);
};
