import mixpanel from '@analytics/mixpanel';
import analyticsConfig from '../../../conf/analytics';

const mixpanelConfigs = analyticsConfig.mixpanel;
const isEnabled: boolean = mixpanelConfigs || false;
const token = isEnabled ? mixpanelConfigs.token : undefined;

export const getMixpanelPlugin = (t: string) => {
    return mixpanel({ token: t });
};

export default {
    isEnabled,
    plugin: isEnabled && getMixpanelPlugin(token),
};
