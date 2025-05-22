import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';

import { LookbackWindow } from '@app/entityV2/shared/tabs/Dataset/Stats/lookbackWindows';
import { getTimeWindowStart } from '@src/app/shared/time/timeUtils';
import { DateInterval, TimeRange } from '@src/types.generated';

dayjs.extend(utc);

export type TimeSeriesDatum = {
    time: number;
    value: number;
};

export type AggregationFunction = (values: number[]) => number | undefined;

export const MAX_VALUE_AGGREGATION: AggregationFunction = (values) => Math.max(...values);
export const MIN_VALUE_AGGREGATION: AggregationFunction = (values) => Math.min(...values);
export const LATEST_VALUE_AGGREGATION: AggregationFunction = (values) => values.at(-1);
export const SUM_VALUES_AGGREGATION: AggregationFunction = (values) => values.reduce((sum, val) => sum + val, 0);

/**
 * Groups time data by interval (day, week, etc)
 * Only `day` interval is supported for now
 */
export function groupTimeData<T>(
    data: T[],
    interval: TimeInterval,
    timeAccessor: (datum: T) => string | number,
    valueAccessor: (datum: T) => number,
    aggregationFuncttion: AggregationFunction = LATEST_VALUE_AGGREGATION,
): TimeSeriesDatum[] {
    const aggregated: Record<number, number[]> = {};

    data.forEach((datum) => {
        const time = timeAccessor(datum);
        const value = valueAccessor(datum);

        const date = dayjs(time).startOf(interval).toDate().getTime();
        aggregated[date] = [...(aggregated[date] || []), value];
    });

    const isTimeSeriesDatum = (item: Partial<TimeSeriesDatum>): item is TimeSeriesDatum => {
        return item.value !== undefined && item.time !== undefined;
    };

    return Object.entries(aggregated)
        .map(([date, values]) => ({
            time: parseInt(date, 10),
            value: aggregationFuncttion(values),
        }))
        .filter(isTimeSeriesDatum);
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
    valueAccessor: (d: T) => number | undefined,
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
        if (lastMonthValue === undefined || value === undefined) {
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
        case TimeRange.HalfYear:
            return getTimeWindowStart(endTimemillis, DateInterval.Month, 6);
        case TimeRange.Year:
            return getTimeWindowStart(endTimemillis, DateInterval.Year, 1);
        case TimeRange.All:
            return getTimeWindowStart(endTimemillis, DateInterval.Year, 1);
        default:
            return undefined;
    }
}

export enum TimeInterval {
    DAY = 'day',
    WEEK = 'week',
    MONTH = 'month',
}

export const getXAxisTickFormat = (interval: TimeInterval, time: number) => {
    if (interval === TimeInterval.WEEK) {
        const endOfWeekTime = dayjs(dayjs(time).endOf('week').toDate().getTime());
        if (endOfWeekTime.month() !== dayjs(time).month()) {
            return `${dayjs(time).format('D')}-${endOfWeekTime.format('D')} ${dayjs(time).format(
                'MMM',
            )}-${endOfWeekTime.format('MMM')}`;
        }
        return `${dayjs(time).format('D')}-${dayjs(dayjs(time).endOf('week').toDate().getTime()).format('D MMM')}`;
    }
    if (interval === TimeInterval.MONTH) {
        return dayjs(time).format('MMM YY');
    }
    return dayjs(time).format('D MMM');
};

export const getPopoverTimeFormat = (interval: TimeInterval, time: number | string) => {
    if (interval === TimeInterval.WEEK) {
        return `Week of ${dayjs(time).format('MMM. D ’YY')} - ${dayjs(
            dayjs(time).endOf('week').toDate().getTime(),
        ).format('MMM. D ’YY')}`;
    }
    if (interval === TimeInterval.MONTH) {
        return dayjs(time).format('MMMM ’YYYY');
    }
    return dayjs(time).format('dddd. MMM. D ’YY');
};

export function getStartTimeByWindowSize(window?: LookbackWindow): number | undefined {
    if (window === undefined) return undefined;

    const endTimemillis = dayjs().utc(true).startOf('day').toDate().getTime();

    return getTimeWindowStart(endTimemillis, window.windowSize.interval, window.windowSize.count);
}

export function getCalendarStartTimeByTimeRange(time: number, range: TimeRange): number | undefined {
    const dayjsTime = dayjs(time);

    switch (range) {
        case TimeRange.Day:
            return dayjsTime.subtract(1, 'day').valueOf();
        case TimeRange.Week:
            return dayjsTime.subtract(1, 'week').valueOf();
        case TimeRange.Month:
            return dayjsTime.subtract(1, 'month').valueOf();
        case TimeRange.Quarter:
            return dayjsTime.subtract(3, 'months').valueOf();
        case TimeRange.HalfYear:
            return dayjsTime.subtract(6, 'months').valueOf();
        case TimeRange.Year:
            return dayjsTime.subtract(1, 'year').valueOf();
        default:
            return undefined;
    }
}

export function roundTimeByTimeRange(time: number | undefined, range: TimeRange) {
    if (time === undefined) return time;

    const dayjsTime = dayjs(time);

    switch (range) {
        case TimeRange.Day:
            return dayjsTime.startOf('day').valueOf();
        case TimeRange.Week:
            return dayjsTime.startOf('day').valueOf();
        case TimeRange.Month:
            return dayjsTime.startOf('day').valueOf();
        case TimeRange.Quarter:
            return dayjsTime.startOf('week').valueOf();
        case TimeRange.HalfYear:
            return dayjsTime.startOf('month').valueOf();
        case TimeRange.Year:
            return dayjsTime.startOf('month').valueOf();
        default:
            return dayjsTime.valueOf();
    }
}
