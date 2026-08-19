import i18next from 'i18next';

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
    [LookbackWindowType.Week]: {
        get text() {
            return i18next.t('entity.profile.stats:lookbackWindowOptions.lastWeek');
        },
        windowSize: { interval: DateInterval.Week, count: 1 },
    },
    [LookbackWindowType.Month]: {
        get text() {
            return i18next.t('entity.profile.stats:lookbackWindowOptions.last30Days');
        },
        windowSize: { interval: DateInterval.Day, count: 30 },
    },
    [LookbackWindowType.Quarter]: {
        get text() {
            return i18next.t('entity.profile.stats:lookbackWindowOptions.last3Months');
        },
        windowSize: { interval: DateInterval.Month, count: 3 },
    },
    [LookbackWindowType.HalfOfYear]: {
        get text() {
            return i18next.t('entity.profile.stats:lookbackWindowOptions.last6Months');
        },
        windowSize: { interval: DateInterval.Month, count: 6 },
    },
    [LookbackWindowType.Year]: {
        get text() {
            return i18next.t('entity.profile.stats:lookbackWindowOptions.lastYear');
        },
        windowSize: { interval: DateInterval.Year, count: 1 },
    },
};

export const getGraphLookbackWindowsOptions = () =>
    Object.entries(GRAPH_LOOKBACK_WINDOWS).map(([key, value]) => ({
        label: value.text,
        value: key,
    }));

const getTimeRangeLabel = (value: TimeRange) => {
    switch (value) {
        case TimeRange.Week:
            return i18next.t('entity.profile.stats:lookbackWindowOptions.lastWeek');
        case TimeRange.Month:
            return i18next.t('entity.profile.stats:lookbackWindowOptions.last30Days');
        case TimeRange.Quarter:
            return i18next.t('entity.profile.stats:lookbackWindowOptions.last3Months');
        case TimeRange.HalfYear:
            return i18next.t('entity.profile.stats:lookbackWindowOptions.last6Months');
        case TimeRange.Year:
            return i18next.t('entity.profile.stats:lookbackWindowOptions.lastYear');
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

export const getAggregationTimeRangeOptions = () =>
    QUERY_COUNT_TIME_RANGE_OPTIONS.map((value) => ({
        label: getTimeRangeLabel(value),
        value,
    }));
