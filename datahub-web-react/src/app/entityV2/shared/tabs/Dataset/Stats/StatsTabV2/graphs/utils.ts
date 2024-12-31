import { getTimeWindowStart } from '@src/app/shared/time/timeUtils';
import { DateInterval, TimeRange } from '@src/types.generated';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import { LookbackWindow } from '../../lookbackWindows';

dayjs.extend(utc);

/**
 * Groups time data by interval (day, week, etc)
 * Only `day` interval is supported for now
 */
export function groupTimeData<T>(
    data: T[],
    interval: 'day',
    timeAccessor: (datum: T) => string | number,
    valueAccessor: (datum: T) => number,
    aggregationFuncttion: (values: number[]) => number,
) {
    const aggregated: Record<number, number[]> = {};

    data.forEach((datum) => {
        const time = timeAccessor(datum);
        const value = valueAccessor(datum);

        const date = dayjs(time).startOf(interval).toDate().getTime();
        aggregated[date] = [...(aggregated[date] || []), value];
    });

    return Object.entries(aggregated).map(([date, values]) => ({
        time: parseInt(date, 10),
        value: aggregationFuncttion(values),
    }));
}

/**
 * Adds month over month value to data
 *
 * FYI: the month over month functionality is temporarily not used
 * it is necessary to assess how to calculate the value correctly
 */
export function addMonthOverMonthValue<T>(
    data: T[],
    timeAccessor: (d: T) => number | string,
    valueAccessor: (d: T) => number,
) {
    return data.map((datum, index) => {
        const time = timeAccessor(datum);
        const value = valueAccessor(datum);

        const day = dayjs(time).startOf('day');
        const dayMonthAgo = day.subtract(1, 'month');
        const numberOfDaysBetween = day.diff(dayMonthAgo, 'days');
        const lastMonthDatum = data?.[index - numberOfDaysBetween];
        const lastMonthValue = lastMonthDatum && valueAccessor(lastMonthDatum);

        let mom: number | null = null;
        if (lastMonthValue === undefined) {
            mom = null;
        } else if (lastMonthValue === 0) {
            mom = 100; // if the last month's value is 0, mom is 100 (like 100% increase)
        } else {
            mom = Math.floor((value / lastMonthValue) * 100) - 100;
        }

        return {
            ...datum,
            mom,
        };
    });
}

export function getStartTimeByTimeRange(range: TimeRange): number | undefined {
    const endTimemillis = dayjs().utc(true).startOf('day').toDate().getTime();

    switch (range) {
        case TimeRange.Day:
            return getTimeWindowStart(endTimemillis, DateInterval.Day, 1);
        case TimeRange.Week:
            return getTimeWindowStart(endTimemillis, DateInterval.Week, 1);
        case TimeRange.Month:
            return getTimeWindowStart(endTimemillis, DateInterval.Month, 1);
        case TimeRange.Quarter:
            return getTimeWindowStart(endTimemillis, DateInterval.Month, 3);
        case TimeRange.Year:
            return getTimeWindowStart(endTimemillis, DateInterval.Year, 1);
        case TimeRange.All:
            return getTimeWindowStart(endTimemillis, DateInterval.Year, 1);
        default:
            return undefined;
    }
}

export function getStartTimeByWindowSize(window?: LookbackWindow): number | undefined {
    if (window === undefined) return undefined;

    const endTimemillis = dayjs().utc(true).startOf('day').toDate().getTime();

    return getTimeWindowStart(endTimemillis, window.windowSize.interval, window.windowSize.count);
}
