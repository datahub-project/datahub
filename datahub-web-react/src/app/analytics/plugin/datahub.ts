/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import analyticsConfig from '@conf/analytics';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

const { datahub } = analyticsConfig;
const isEnabled: boolean = (datahub && datahub.enabled) || false;

const track = (payload) => {
    fetch(resolveRuntimePath('/openapi/v1/tracking/track'), {
        method: 'POST',
        cache: 'no-cache',
        credentials: 'same-origin',
        headers: {
            'Content-Type': 'application/json',
        },
        referrerPolicy: 'no-referrer',
        body: JSON.stringify(payload),
    });
};

const datahubPlugin = () => {
    return {
        /* All plugins require a name */
        name: 'datahub',
        initialize: () => {},
        identify: () => {},
        loaded: () => {
            return true;
        },
        page: ({ payload }) => {
            track(payload.properties);
        },
        track: ({ payload }) => {
            track(payload.properties);
        },
    };
};

export default {
    isEnabled,
    plugin: isEnabled && datahubPlugin(),
};
