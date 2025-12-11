/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DateInterval } from '@types';

/**
 * Change this to add or modify the lookback windows that are selectable via the UI.
 */
export const LOOKBACK_WINDOWS = {
    HOUR: { text: '1 hour', windowSize: { interval: DateInterval.Hour, count: 1 } },
    DAY: { text: '1 day', windowSize: { interval: DateInterval.Day, count: 1 } },
    WEEK: { text: '1 week', windowSize: { interval: DateInterval.Week, count: 1 } },
    MONTH: { text: '1 month', windowSize: { interval: DateInterval.Month, count: 1 } },
    QUARTER: { text: '3 months', windowSize: { interval: DateInterval.Month, count: 3 } },
    YEAR: { text: '1 year', windowSize: { interval: DateInterval.Year, count: 1 } },
};

export type LookbackWindow = {
    text: string;
    windowSize: {
        interval: DateInterval;
        count: number;
    };
};
