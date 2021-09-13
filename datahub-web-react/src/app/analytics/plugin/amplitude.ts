import amplitude from '@analytics/amplitude';
import analyticsConfig from '../../../conf/analytics';

const amplitudeConfigs = analyticsConfig.amplitude;
const isEnabled: boolean = amplitudeConfigs || false;
const apiKey = isEnabled ? amplitudeConfigs.apiKey : undefined;

export default {
    isEnabled,
    plugin: apiKey && amplitude({ apiKey, options: {} }),
};
