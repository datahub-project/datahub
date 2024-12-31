import { DateInterval, TimeRange } from '@src/types.generated';

export const GRAPH_LOOPBACK_WINDOWS = {
    WEEK: { text: 'Last Week', windowSize: { interval: DateInterval.Week, count: 1 } },
    MONTH: { text: 'Last 30 Days', windowSize: { interval: DateInterval.Day, count: 30 } },
    QUATER: { text: 'Last 3 Months', windowSize: { interval: DateInterval.Month, count: 3 } },
    HALF_OF_YEAR: { text: 'Last 6 Months', windowSize: { interval: DateInterval.Month, count: 6 } },
    YEAR: { text: 'Last Year', windowSize: { interval: DateInterval.Year, count: 1 } },
};

export const GRAPH_LOOPBACK_WINDOWS_OPTIONS = [
    ...Object.entries(GRAPH_LOOPBACK_WINDOWS).map(([key, value]) => ({
        label: value.text,
        value: key,
    })),
];

const getTimeRangeLabel = (value: TimeRange) => {
    // Todo: add the 6 month time range when it's implemented
    switch (value) {
        case TimeRange.Day:
            return 'Last day';
        case TimeRange.Week:
            return 'Last Week';
        case TimeRange.Month:
            return 'Last 30 days';
        case TimeRange.Quarter:
            return 'Last 3 months';
        case TimeRange.Year:
            return 'Last Year';
        case TimeRange.All:
            return 'All the time';
        default:
            return value;
    }
};

// Todo: add the 6 month time range when it's implemented
const QUERY_COUNT_TIME_RANGE_OPTIONS = [TimeRange.Week, TimeRange.Month, TimeRange.Quarter, TimeRange.Year];

export const AGGRAGATION_TIME_RANGE_OPTIONS = Object.values(QUERY_COUNT_TIME_RANGE_OPTIONS).map((value) => ({
    label: getTimeRangeLabel(value),
    value,
}));
