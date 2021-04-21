import googleAnalytics from '@analytics/google-analytics';

const isEnabled = process.env.ANALYTICS_GA_ENABLED || false;
const trackingId = process.env.ANALYTICS_GA_TRACKING_ID || undefined;

export default {
    isEnabled,
    plugin: isEnabled && googleAnalytics({ trackingId }),
};
