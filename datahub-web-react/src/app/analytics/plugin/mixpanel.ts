import mixpanel from '@analytics/mixpanel';

const isEnabled = process.env.ANALYTICS_MIXPANEL_ENABLED || true;
const token = process.env.ANALYTICS_MIXPANEL_TOKEN || 'fad1285da4e618b618973cacf6565e61';

export default {
    isEnabled,
    plugin: isEnabled && mixpanel({ token }),
};
