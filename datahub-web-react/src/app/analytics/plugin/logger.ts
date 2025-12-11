/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import analyticsConfig from '@conf/analytics';

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

/**
 * Change to true to enable event logging in the console.
 */
const isEnabled = analyticsConfig.logging || false;

export default {
    isEnabled,
    plugin: isEnabled && loggingPlugin(),
};
