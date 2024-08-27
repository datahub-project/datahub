import mixpanel from '@analytics/mixpanel';
import analyticsConfig from '../../../conf/analytics';

const mixpanelConfigs = analyticsConfig.mixpanel;
const isEnabled: boolean = mixpanelConfigs || false;
const token = isEnabled ? mixpanelConfigs.token : undefined;

export default {
    isEnabled,
    plugin: isEnabled && mixpanel({ token }),
};
