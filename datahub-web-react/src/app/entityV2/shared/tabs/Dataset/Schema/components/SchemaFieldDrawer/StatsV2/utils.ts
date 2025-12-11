/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import dayjs from 'dayjs';
import advancedFormat from 'dayjs/plugin/advancedFormat';
import timezone from 'dayjs/plugin/timezone';
import utc from 'dayjs/plugin/utc';

import { isValuePresent } from '@src/app/entityV2/shared/containers/profile/sidebar/shared/utils';

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(advancedFormat);

// Example: 9/9/2024, 07:36 AM PDT
export const DATE_COMMA_TIME_TZ = 'L hh:mm A z';

export function formatTimestamp(
    timestamp: number | undefined | null,
    format: string,
    useTz = true,
): string | undefined | null {
    if (!isValuePresent(timestamp)) return undefined;
    const dayjsObj = dayjs(timestamp);

    if (useTz) return dayjsObj.tz(dayjs.tz.guess()).format(format);
    return dayjsObj.format(format);
}

export function formatNumber(value: number | null | undefined): string | undefined {
    if (!isValuePresent(value)) return undefined;

    return Number((value as number).toFixed(2)).toLocaleString();
}
