import dayjs from 'dayjs';

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
 * Adds MoM value to data
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
            time,
            value,
            mom,
        };
    });
}
