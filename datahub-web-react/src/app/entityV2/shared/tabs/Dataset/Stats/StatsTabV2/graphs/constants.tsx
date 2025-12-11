/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LookbackWindow } from '@app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows';
import { DateInterval, TimeRange } from '@src/types.generated';

export enum LookbackWindowType {
    Week = 'WEEK',
    Month = 'MONTH',
    Quarter = 'QUARTER',
    HalfOfYear = 'HALF_OF_YEAR',
    Year = 'YEAR',
}

export const GRAPH_LOOKBACK_WINDOWS: { [key in LookbackWindowType]: LookbackWindow } = {
    [LookbackWindowType.Week]: { text: 'Last Week', windowSize: { interval: DateInterval.Week, count: 1 } },
    [LookbackWindowType.Month]: { text: 'Last 30 Days', windowSize: { interval: DateInterval.Day, count: 30 } },
    [LookbackWindowType.Quarter]: { text: 'Last 3 Months', windowSize: { interval: DateInterval.Month, count: 3 } },
    [LookbackWindowType.HalfOfYear]: { text: 'Last 6 Months', windowSize: { interval: DateInterval.Month, count: 6 } },
    [LookbackWindowType.Year]: { text: 'Last Year', windowSize: { interval: DateInterval.Year, count: 1 } },
};

export const GRAPH_LOOKBACK_WINDOWS_OPTIONS = [
    ...Object.entries(GRAPH_LOOKBACK_WINDOWS).map(([key, value]) => ({
        label: value.text,
        value: key,
    })),
];

const getTimeRangeLabel = (value: TimeRange) => {
    switch (value) {
        case TimeRange.Week:
            return 'Last Week';
        case TimeRange.Month:
            return 'Last 30 days';
        case TimeRange.Quarter:
            return 'Last 3 months';
        case TimeRange.HalfYear:
            return 'Last 6 months';
        case TimeRange.Year:
            return 'Last Year';
        default:
            return value;
    }
};

const QUERY_COUNT_TIME_RANGE_OPTIONS = [
    TimeRange.Week,
    TimeRange.Month,
    TimeRange.Quarter,
    TimeRange.HalfYear,
    TimeRange.Year,
];

export const AGGRAGATION_TIME_RANGE_OPTIONS = Object.values(QUERY_COUNT_TIME_RANGE_OPTIONS).map((value) => ({
    label: getTimeRangeLabel(value),
    value,
}));
