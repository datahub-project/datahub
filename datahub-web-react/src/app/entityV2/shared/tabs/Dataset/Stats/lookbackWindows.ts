import { DateInterval } from '../../../../../../types.generated';

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
