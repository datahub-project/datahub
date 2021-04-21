import mixpanel from '@analytics/mixpanel';

const isEnabled = process.env.ANALYTICS_MIXPANEL_ENABLED || false;
const token = process.env.ANALYTICS_MIXPANEL_TOKEN || undefined;

export default {
    isEnabled,
    plugin: isEnabled && mixpanel({ token }),
};
