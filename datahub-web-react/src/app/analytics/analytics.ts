import Analytics from 'analytics';
import Cookies from 'js-cookie';
import plugins from './plugin';
import { Event, EventType } from './event';
import { CLIENT_AUTH_COOKIE } from '../../conf/Global';

const appName = 'datahub-react';

const analytics = Analytics({
    app: appName,
    plugins: plugins.filter((plugin) => plugin.isEnabled).map((plugin) => plugin.plugin),
});

export default {
    ...analytics,
    event: (event: Event, options?: any, callback?: (...params: any[]) => any): Promise<any> => {
        const eventTypeName = EventType[event.type];
        const modifiedEvent = {
            ...event,
            type: eventTypeName,
            actor: Cookies.get(CLIENT_AUTH_COOKIE) || undefined,
            timestamp: Date.now(),
            date: new Date().toString(),
            userAgent: navigator.userAgent,
        };
        return analytics.track(eventTypeName, modifiedEvent, options, callback);
    },
};
