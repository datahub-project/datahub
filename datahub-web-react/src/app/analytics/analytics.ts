import Analytics, { PageData } from 'analytics';
import Cookies from 'js-cookie';
import plugins from './plugin';
import { Event, EventType } from './event';
import { CLIENT_AUTH_COOKIE } from '../../conf/Global';
import { getBrowserId } from '../browserId';

const appName = 'datahub-react';

export const THIRD_PARTY_LOGGING_KEY = 'enableThirdPartyLogging';

const analytics = Analytics({
    app: appName,
    plugins: plugins.filter((plugin) => plugin.isEnabled).map((plugin) => plugin.plugin),
});

const { NODE_ENV } = process.env;

export function getMergedTrackingOptions(options?: any) {
    const isThirdPartyLoggingEnabled = JSON.parse(localStorage.getItem(THIRD_PARTY_LOGGING_KEY) || 'false');
    /** If you trace back the code, this 'isThirdPartyLoggingEnabled' config option is ultimately
     *  configured from the gms-backend (the frontend makes a GraphQL call to get the value, and
     *  saves it in local storage).
     *
     *  Since I'd rather not have to build the entire gms-backend in our fork _just_ to change
     *  this single value, I'm hard-coding it here for amplitude. If this is particularly
     * offensive, we can do it the 'proper' way :D
     */
    return {
        ...options,
        plugins: {
            mixpanel: isThirdPartyLoggingEnabled,
            amplitude: true,
            googleAnalytics: isThirdPartyLoggingEnabled,
        },
    };
}

export default {
    page: (data?: PageData, options?: any, callback?: (...params: any[]) => any) => {
        const modifiedData = {
            ...data,
            type: EventType[EventType.PageViewEvent],
            actorUrn: Cookies.get(CLIENT_AUTH_COOKIE) || undefined,
            timestamp: Date.now(),
            date: new Date().toString(),
            userAgent: navigator.userAgent,
            browserId: getBrowserId(),
        };
        if (NODE_ENV === 'test') {
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
            actorUrn: Cookies.get(CLIENT_AUTH_COOKIE) || undefined,
            timestamp: Date.now(),
            date: new Date().toString(),
            userAgent: navigator.userAgent,
            browserId: getBrowserId(),
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
