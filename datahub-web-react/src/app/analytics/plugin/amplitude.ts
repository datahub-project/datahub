/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import amplitude from '@analytics/amplitude';

import analyticsConfig from '@conf/analytics';

const amplitudeConfigs = analyticsConfig.amplitude;
const isEnabled: boolean = amplitudeConfigs || false;
const apiKey = isEnabled ? amplitudeConfigs.apiKey : undefined;

export default {
    isEnabled,
    plugin: apiKey && amplitude({ apiKey, options: {} }),
};
