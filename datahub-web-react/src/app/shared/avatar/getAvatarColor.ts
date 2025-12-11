/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { lineColors } from '@app/analyticsDashboard/components/lineColors';
import { ANTD_GRAY } from '@app/entity/shared/constants';

export function hashString(str: string) {
    let hash = 0;
    if (str.length === 0) {
        return hash;
    }
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        // eslint-disable-next-line
        hash = (hash << 5) - hash + char;
        // eslint-disable-next-line
        hash = hash & hash; // Convert to 32bit integer
    }
    return Math.abs(hash);
}

export default function getAvatarColor(name?: string) {
    if (!name) {
        return ANTD_GRAY[7];
    }
    return lineColors[hashString(name) % lineColors.length];
}
