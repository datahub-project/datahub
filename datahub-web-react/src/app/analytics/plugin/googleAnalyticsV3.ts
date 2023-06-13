import googleAnalyticsV3 from '@analytics/google-analytics-v3';
import { Event, EventType } from '../event';
import analyticsConfig from '../../../conf/analytics';

const ga3Configs = analyticsConfig.googleAnalytics;
const isEnabled: boolean = ga3Configs || false;
const trackingId = isEnabled ? ga3Configs.trackingId : undefined;

const getLabelFromEvent = (event: Event) => {
    switch (event.type) {
        case EventType.BrowseResultClickEvent:
            return event.browsePath;
        case EventType.SearchEvent:
            return event.query;
        case EventType.EntitySectionViewEvent:
            return event.section;
        default:
            return event.actorUrn;
    }
};

let wrappedGoogleAnalyticsV3Plugin;
if (isEnabled) {
    /**
     * Init default GA3 plugin
     */
    const googleAnalyticsPlugin = googleAnalyticsV3({ trackingId });

    /**
     * Lightweight wrapper on top of the default google analytics plugin
     * to transform DataHub Analytics Events into the Google Analytics event
     * format.
     */
    wrappedGoogleAnalyticsV3Plugin = {
        ...googleAnalyticsPlugin,
        track: ({ payload, config, instance }) => {
            const modifiedProperties = {
                label: getLabelFromEvent(payload.properties as Event),
                category: 'UserActions',
            };
            const modifiedPayload = {
                ...payload,
                properties: modifiedProperties,
            };
            return googleAnalyticsPlugin.track({
                payload: modifiedPayload,
                config,
                instance,
            });
        },
    };
}
export default {
    isEnabled,
    plugin: isEnabled && wrappedGoogleAnalyticsV3Plugin,
};
