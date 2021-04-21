/**
 * Change to true to enable event logging in the console.
 */
const isEnabled = false;

/**
 * Analytics plugin that logs tracking events + page views to console.s
 */
const loggingPlugin = () => {
    return {
        name: 'logging',
        initialize: () => {
            console.log('Initializing logging plugin');
        },
        page: ({ payload }) => {
            console.log(`Page view event: ${JSON.stringify(payload)}`);
        },
        track: ({ payload }) => {
            console.log(`Tracking event: ${JSON.stringify(payload)}`);
        },
        identify: ({ payload }) => {
            console.log(`Identify event: ${JSON.stringify(payload)}`);
        },
        loaded: () => {
            return true;
        },
    };
};

export default {
    isEnabled,
    plugin: isEnabled && loggingPlugin(),
};
