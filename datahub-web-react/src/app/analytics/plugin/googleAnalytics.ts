import googleAnalytics from '@analytics/google-analytics';

const isEnabled = process.env.ANALYTICS_GA_ENABLED || true;
const trackingId = process.env.ANALYTICS_GA_TRACKING_ID || 'UA-191801420-2';

export default {
    isEnabled,
    plugin: isEnabled && googleAnalytics({ trackingId }),
};
