import Analytics, { PageData } from 'analytics';
import Cookies from 'js-cookie';

import { Event, EventType } from '@app/analytics/event';
import { fetchTrackingSettings } from '@app/analytics/fetchTrackingSettings';
import {
    NOTIFICATION_CONTEXT_STORAGE_KEY,
    NotificationChannel,
    NotificationType,
} from '@app/analytics/notificationTracking';
import plugins from '@app/analytics/plugin';
import { createPluginsFromSettings } from '@app/analytics/plugin/utils';
import { getBrowserId } from '@app/browserId';
import { loadUserPersonaFromLocalStorage } from '@app/homeV2/persona/useUserPersona';
import { loadUserTitleFromLocalStorage } from '@app/identity/user/useUserTitle';
import { loadThemeV2FromLocalStorage } from '@app/useIsThemeV2';
import { CLIENT_AUTH_COOKIE } from '@conf/Global';

const appName = 'datahub-react';

export const THIRD_PARTY_LOGGING_KEY = 'enableThirdPartyLogging';
export const SERVER_VERSION_KEY = 'dataHubServerVersion';
const { NODE_ENV } = import.meta.env;

type PersistedSessionContext = {
    // Future: support other entry points beyond alerts by adding more types here.
    type: 'notification';
    notification?: {
        type: NotificationType;
        notificationId: string;
        notificationChannel: NotificationChannel;
    };
    createdAtMillis: number;
    expiresAtMillis: number;
};

type NotificationContextFields = {
    notificationType: NotificationType;
    notificationId: string;
    notificationChannel: NotificationChannel;
};

function getPersistedSessionContext(): NotificationContextFields | null {
    try {
        const raw = sessionStorage.getItem(NOTIFICATION_CONTEXT_STORAGE_KEY);
        if (!raw) return null;
        const parsed = JSON.parse(raw) as PersistedSessionContext;
        const notification = parsed?.notification;
        if (!notification?.type || !notification?.notificationId || !notification?.notificationChannel) return null;
        if (parsed.expiresAtMillis && Date.now() > parsed.expiresAtMillis) {
            sessionStorage.removeItem(NOTIFICATION_CONTEXT_STORAGE_KEY);
            return null;
        }
        return {
            notificationType: notification.type,
            notificationId: notification.notificationId,
            notificationChannel: notification.notificationChannel,
        };
    } catch (e) {
        // Best-effort only. sessionStorage can be unavailable or contain bad JSON (e.g. user/devtools edits).
        if (NODE_ENV !== 'production') {
            // eslint-disable-next-line no-console
            console.debug('Failed to parse persisted session context from sessionStorage', e);
        }
        return null;
    }
}

let analytics: any = null; // Global variable for analytics. Uninitialized at the start

/**
 * Fetch the tracking settings from the backend, dynamically instantiate the plugins,
 * and initialize the Analytics instance.
 */
async function initializeAnalytics() {
    const settings = await fetchTrackingSettings(); // Hypothetical GraphQL call to fetch settings

    const dynamicPlugins = settings ? createPluginsFromSettings(settings) : [];

    // Here we merge analytics plugins, with dynamic plugins taking precedence.
    const allPlugins: any[] = [...plugins, ...dynamicPlugins];

    const enabledPlugins = allPlugins.filter((plugin) => plugin.isEnabled).map((plugin) => plugin.plugin);

    return Analytics({
        app: appName,
        plugins: enabledPlugins,
    });
}

/**
 * Ensure that the Analytics instance is initialized before using it.
 */
async function getAnalyticsInstance() {
    if (!analytics) {
        analytics = await initializeAnalytics(); // Initialize if not already done
    }
    return analytics;
}

export function getMergedTrackingOptions(options?: any) {
    const isThirdPartyLoggingEnabled = JSON.parse(localStorage.getItem(THIRD_PARTY_LOGGING_KEY) || 'false');
    return {
        ...options,
        plugins: {
            mixpanel: isThirdPartyLoggingEnabled,
            amplitude: isThirdPartyLoggingEnabled,
            googleAnalytics: isThirdPartyLoggingEnabled,
        },
    };
}

export default {
    /**
     * Track a page view event
     */
    page: async (data?: PageData, options?: any, callback?: (...params: any[]) => any) => {
        const isThemeV2Enabled = loadThemeV2FromLocalStorage();
        const userPersona = loadUserPersonaFromLocalStorage();
        const userTitle = loadUserTitleFromLocalStorage();
        const serverVersion = localStorage.getItem(SERVER_VERSION_KEY);
        const actorUrn = Cookies.get(CLIENT_AUTH_COOKIE) || undefined;
        const persistedSessionContext = getPersistedSessionContext();
        const modifiedData = {
            ...data,
            type: EventType[EventType.PageViewEvent],
            actorUrn,
            timestamp: Date.now(),
            date: new Date().toString(),
            userAgent: navigator.userAgent,
            browserId: getBrowserId(),
            origin: window.location.origin,
            isThemeV2Enabled,
            userPersona: userPersona || undefined,
            userTitle: userTitle || undefined,
            serverVersion,
            ...(persistedSessionContext || {}),
        };

        if (NODE_ENV === 'test' || !actorUrn) {
            return null;
        }

        const trackingOptions = getMergedTrackingOptions(options);
        const analyticsInstance = await getAnalyticsInstance();
        return analyticsInstance.page(modifiedData, trackingOptions, callback);
    },

    /**
     * Track a custom event
     */
    event: async (event: Event, options?: any, callback?: (...params: any[]) => any): Promise<any> => {
        const isThemeV2Enabled = loadThemeV2FromLocalStorage();
        const userPersona = loadUserPersonaFromLocalStorage();
        const userTitle = loadUserTitleFromLocalStorage();
        const serverVersion = localStorage.getItem(SERVER_VERSION_KEY);
        const eventTypeName = EventType[event.type];
        const persistedSessionContext = getPersistedSessionContext();
        const modifiedEvent = {
            ...event,
            type: eventTypeName,
            actorUrn: Cookies.get(CLIENT_AUTH_COOKIE) || undefined,
            timestamp: Date.now(),
            date: new Date().toString(),
            userAgent: navigator.userAgent,
            browserId: getBrowserId(),
            origin: window.location.origin,
            isThemeV2Enabled,
            userPersona: userPersona || undefined,
            userTitle: userTitle || undefined,
            serverVersion,
            ...(persistedSessionContext || {}),
        };

        if (NODE_ENV === 'test') {
            return Promise.resolve();
        }

        const trackingOptions = getMergedTrackingOptions(options);
        const analyticsInstance = await getAnalyticsInstance();
        return analyticsInstance.track(eventTypeName, modifiedEvent, trackingOptions, callback);
    },

    /**
     * Identify a user
     */
    identify: async (
        userId: string,
        traits?: any,
        options?: any,
        callback?: ((...params: any[]) => any) | undefined,
    ) => {
        const trackingOptions = getMergedTrackingOptions(options);
        const analyticsInstance = await getAnalyticsInstance();
        return analyticsInstance.identify(userId, traits, trackingOptions, callback);
    },
};
