import { getFixedLookbackWindow } from '@app/shared/time/timeUtils';

import { DateInterval } from '@types';

export type TimeRangeOption = {
    label: string;
    start: number;
    end: number;
};

export const TIME_RANGE_OPTIONS: TimeRangeOption[] = [
    {
        label: 'Last 24 hours',
        start: getFixedLookbackWindow({ interval: DateInterval.Day, count: 1 }).startTime,
        end: getFixedLookbackWindow({ interval: DateInterval.Day, count: 1 }).endTime,
    },
    {
        label: 'Last 7 days',
        start: getFixedLookbackWindow({ interval: DateInterval.Week, count: 1 }).startTime,
        end: getFixedLookbackWindow({ interval: DateInterval.Week, count: 1 }).endTime,
    },
    {
        label: 'Last 30 days',
        start: getFixedLookbackWindow({ interval: DateInterval.Month, count: 1 }).startTime,
        end: getFixedLookbackWindow({ interval: DateInterval.Month, count: 1 }).endTime,
    },
    {
        label: 'Last 90 days',
        start: getFixedLookbackWindow({ interval: DateInterval.Month, count: 3 }).startTime,
        end: getFixedLookbackWindow({ interval: DateInterval.Month, count: 3 }).endTime,
    },
];
