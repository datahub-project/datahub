import Analytics, { PageData } from 'analytics';
import Cookies from 'js-cookie';

import { Event, EventType } from '@app/analytics/event';
import plugins from '@app/analytics/plugin';
import { getBrowserId } from '@app/browserId';
import { loadUserPersonaFromLocalStorage } from '@app/homeV2/persona/useUserPersona';
import { loadUserTitleFromLocalStorage } from '@app/identity/user/useUserTitle';
import { loadThemeV2FromLocalStorage } from '@app/useIsThemeV2';
import { CLIENT_AUTH_COOKIE } from '@conf/Global';

const appName = 'datahub-react';

export const THIRD_PARTY_LOGGING_KEY = 'enableThirdPartyLogging';

const analytics = Analytics({
    app: appName,
    plugins: plugins.filter((plugin) => plugin.isEnabled).map((plugin) => plugin.plugin),
});

export const SERVER_VERSION_KEY = 'dataHubServerVersion';
const { NODE_ENV } = import.meta.env;

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
        const isThemeV2Enabled = loadThemeV2FromLocalStorage();
        const userPersona = loadUserPersonaFromLocalStorage();
        const userTitle = loadUserTitleFromLocalStorage();
        const serverVersion = localStorage.getItem(SERVER_VERSION_KEY);
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
            serverVersion,
        };
        if (NODE_ENV === 'test' || !actorUrn) {
            return null;
        }
        const trackingOptions = getMergedTrackingOptions(options);
        return analytics.page(modifiedData, trackingOptions, callback);
    },
    event: (event: Event, options?: any, callback?: (...params: any[]) => any): Promise<any> => {
        const isThemeV2Enabled = loadThemeV2FromLocalStorage();
        const userPersona = loadUserPersonaFromLocalStorage();
        const userTitle = loadUserTitleFromLocalStorage();
        const serverVersion = localStorage.getItem(SERVER_VERSION_KEY);
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
            serverVersion,
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
