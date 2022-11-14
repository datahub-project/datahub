import googleTagManager from '@analytics/google-tag-manager';
import analyticsConfig from '../../../conf/analytics';

const gaConfigs = analyticsConfig.googleTagManager;
const isEnabled: boolean = gaConfigs || false;
const containerId = isEnabled ? gaConfigs.containerId : undefined;

let wrappedGoogleTagManagerPlugin;
if (isEnabled) {
    /**
     * Init default GA plugin
     */
    const googleTagManagerPlugin = googleTagManager({
        containerId,
    });

    /**
     * Lightweight wrapper on top of the default google analytics plugin
     * to transform DataHub Analytics Events into the Google Analytics event
     * format.
     */
    wrappedGoogleTagManagerPlugin = {
        ...googleTagManagerPlugin,
        track: () => {},
    };
}

export default {
    isEnabled,
    plugin: isEnabled && wrappedGoogleTagManagerPlugin,
};
