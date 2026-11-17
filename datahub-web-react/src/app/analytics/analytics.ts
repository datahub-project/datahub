import Analytics, { PageData } from 'analytics';
import Cookies from 'js-cookie';

import { Event, EventType } from '@app/analytics/event';
import plugins from '@app/analytics/plugin';
import { getBrowserId } from '@app/browserId';
import { loadUserPersonaFromLocalStorage } from '@app/homeV2/persona/useUserPersona';
import { getServedLanguage } from '@app/i18n/utils';
import { loadUserTitleFromLocalStorage } from '@app/identity/user/useUserTitle';
import { CLIENT_AUTH_COOKIE } from '@conf/Global';

const appName = 'datahub-react';

export const THIRD_PARTY_LOGGING_KEY = 'enableThirdPartyLogging';

const analytics = Analytics({
    app: appName,
    plugins: plugins.filter((plugin) => plugin.isEnabled).map((plugin) => plugin.plugin),
});

export const SERVER_VERSION_KEY = 'dataHubServerVersion';
const { NODE_ENV } = import.meta.env;

/**
 * Context attached to every tracked page view and event.
 * `language` is the raw browser/OS locale (`navigator.language`).
 * `appLanguage` is the locale whose JSON the UI is actually serving.
 */
export function buildEventContext() {
    const userPersona = loadUserPersonaFromLocalStorage();
    const userTitle = loadUserTitleFromLocalStorage();
    return {
        actorUrn: Cookies.get(CLIENT_AUTH_COOKIE) || undefined,
        timestamp: Date.now(),
        date: new Date().toString(),
        userAgent: navigator.userAgent,
        language: navigator.language,
        appLanguage: getServedLanguage(),
        browserId: getBrowserId(),
        origin: window.location.origin,
        isThemeV2Enabled: true,
        userPersona: userPersona || undefined,
        userTitle: userTitle || undefined,
        serverVersion: localStorage.getItem(SERVER_VERSION_KEY),
    };
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
    page: (data?: PageData, options?: any, callback?: (...params: any[]) => any) => {
        const context = buildEventContext();
        const modifiedData = {
            ...data,
            type: EventType[EventType.PageViewEvent],
            ...context,
        };
        if (NODE_ENV === 'test' || !context.actorUrn) {
            return null;
        }
        const trackingOptions = getMergedTrackingOptions(options);
        return analytics.page(modifiedData, trackingOptions, callback);
    },
    event: (event: Event, options?: any, callback?: (...params: any[]) => any): Promise<any> => {
        const eventTypeName = EventType[event.type];
        const modifiedEvent = {
            ...event,
            type: eventTypeName,
            ...buildEventContext(),
        };
        if (NODE_ENV === 'test') {
            return Promise.resolve();
        }
        const trackingOptions = getMergedTrackingOptions(options);
        return analytics.track(eventTypeName, modifiedEvent, trackingOptions, callback);
    },
    identify: (userId: string, traits?: any, options?: any, callback?: ((...params: any[]) => any) | undefined) => {
        const trackingOptions = getMergedTrackingOptions(options);
        return analytics.identify(userId, traits, trackingOptions, callback);
    },
};
