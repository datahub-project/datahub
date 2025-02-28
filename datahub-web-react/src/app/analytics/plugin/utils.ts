import { getWrappedGAPlugin } from './googleAnalytics';
import { getMixpanelPlugin } from './mixpanel';

export const createPluginsFromSettings = (settings: any) => {
    const plugins: any[] = [];

    if (!settings || !settings.enableThirdPartyLogging) {
        // Tracking is disabled. Short circuit.
        return plugins;
    }

    if (settings.mixpanel && settings.mixpanel.enabled && settings.mixpanel.token) {
        // Create mixpanel plugin.
        plugins.push({
            isEnabled: true,
            plugin: getMixpanelPlugin(settings.mixpanel.token),
        });
    }

    if (settings.googleAnalytics && settings.googleAnalytics.enabled && settings.googleAnalytics.measurementId) {
        // Create google analytics plugin.
        plugins.push({
            isEnabled: true,
            plugin: getWrappedGAPlugin(settings.googleAnalytics.measurementId),
        });
    }

    return plugins;
};
