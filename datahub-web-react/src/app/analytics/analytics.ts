import Analytics, { PageData } from 'analytics';
import Cookies from 'js-cookie';
import plugins from './plugin';
import { Event, EventType } from './event';
import { CLIENT_AUTH_COOKIE } from '../../conf/Global';
import { getBrowserId } from '../browserId';

const appName = 'datahub-react';

const analytics = Analytics({
    app: appName,
    plugins: plugins.filter((plugin) => plugin.isEnabled).map((plugin) => plugin.plugin),
});

const { NODE_ENV } = process.env;

export default {
    ...analytics,
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
        return analytics.page(modifiedData, options, callback);
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
        return analytics.track(eventTypeName, modifiedEvent, options, callback);
    },
};
