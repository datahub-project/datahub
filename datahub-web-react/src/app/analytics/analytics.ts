import Analytics, { PageData } from 'analytics';
import Cookies from 'js-cookie';

import { Event, EventType } from '@app/analytics/event';
import { fetchTrackingSettings } from '@app/analytics/fetchTrackingSettings';
import plugins from '@app/analytics/plugin';
import { createPluginsFromSettings } from '@app/analytics/plugin/utils';
import { getBrowserId } from '@app/browserId';
import { loadUserPersonaFromLocalStorage } from '@app/homeV2/persona/useUserPersona';
import { loadUserTitleFromLocalStorage } from '@app/identity/user/useUserTitle';
import { loadThemeV2FromLocalStorage } from '@app/useIsThemeV2';
import { CLIENT_AUTH_COOKIE } from '@conf/Global';

const appName = 'datahub-react';

export const THIRD_PARTY_LOGGING_KEY = 'enableThirdPartyLogging';
const { NODE_ENV } = import.meta.env;

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
        const actorUrn = Cookies.get(CLIENT_AUTH_COOKIE) || undefined;
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
        const eventTypeName = EventType[event.type];
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
