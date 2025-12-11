/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ANTD_GRAY } from '@app/entityV2/shared/constants';

/**
 * Normalizes a percentile integer to a 3-tier
 * label system of 'High', 'Med', 'Low' for usability.
 */
export const percentileToLabel = (pct: number) => {
    /* eslint-disable no-else-return */
    if (pct <= 30) {
        return 'Low';
    } else if (pct > 30 && pct <= 80) {
        return 'Med';
    }
    return 'High';
};

/**
 * Normalizes a percentile to a color.
 */
export const percentileToColor = (pct: number) => {
    /* eslint-disable no-else-return */
    if (pct <= 30) {
        return ANTD_GRAY[3];
    } else if (pct > 30 && pct <= 80) {
        return '#EBF3F2';
    }
    return '#cef5f0';
};
