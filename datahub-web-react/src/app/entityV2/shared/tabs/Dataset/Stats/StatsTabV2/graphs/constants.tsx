import { DateInterval } from '@src/types.generated';

export const GRAPH_LOOPBACK_WINDOWS = {
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
