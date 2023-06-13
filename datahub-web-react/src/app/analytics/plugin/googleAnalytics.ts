import googleAnalytics from '@analytics/google-analytics';
import googleAnalyticsV3Plugin from '@analytics/google-analytics-v3'
import {Event, EventType} from '../event';
import analyticsConfig from '../../../conf/analytics';

const ga3Configs = analyticsConfig.googleAnalytics;
const ga4Configs = analyticsConfig.googleAnalyticsV4;
const isEnabled: boolean = ga3Configs || ga4Configs || false;
const isGA4: boolean = ga4Configs || false;
const trackingId = isEnabled ? ga3Configs.trackingId : undefined;
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
    const googleAnalyticsPlugin = isGA4 ? googleAnalytics(
        {measurementIds: measurementIds}) : googleAnalyticsV3Plugin({trackingId: trackingId});
    /**
     * Lightweight wrapper on top of the default google analytics plugin
     * to transform DataHub Analytics Events into the Google Analytics event
     * format.
     */
    wrappedGoogleAnalyticsPlugin = {
        ...googleAnalyticsPlugin,
        track: ({payload, config, instance}) => {
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
