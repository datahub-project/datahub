import googleAnalytics from '@analytics/google-analytics';
import { Event, EventType } from '../event';
import analyticsConfig from '../../../conf/analytics';

const ga4Configs = analyticsConfig.googleAnalytics;
const isEnabled: boolean = ga4Configs || false;
const measurementIds = isEnabled ? ga4Configs.measurementIds : undefined;

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

let wrappedGoogleAnalyticsPlugin;
if (isEnabled) {
    const googleAnalyticsPlugin = googleAnalytics({ measurementIds });
    /**
     * Lightweight wrapper on top of the default google analytics plugin
     * to transform DataHub Analytics Events into the Google Analytics event
     * format.
     */
    wrappedGoogleAnalyticsPlugin = {
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
    plugin: isEnabled && wrappedGoogleAnalyticsPlugin,
};
