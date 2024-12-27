import { scaleLinear } from '@visx/scale';
import * as d3interpolate from '@visx/vendor/d3-interpolate';
import dayjs from 'dayjs';
import isoWeek from 'dayjs/plugin/isoWeek';
import utc from 'dayjs/plugin/utc';
import advancedFormat from 'dayjs/plugin/advancedFormat';
import { CalendarChartProps, CalendarData, ColorAccessor, DayData, MonthData, WeekData } from './types';
import { DAYS_IN_WEEK, MIN_DAYS_IN_WEEK } from './private/constants';
import { CALENDAR_DATE_FORMAT } from './constants';

dayjs.extend(isoWeek);
dayjs.extend(utc);
dayjs.extend(advancedFormat);

export function prepareCalendarData<ValueType>(
    data: CalendarData<ValueType>[],
    start: string | Date,
    end: string | Date,
    labelFormat = 'MMM ’YY',
): MonthData<ValueType>[] {
    // Round start/end date to the start/end of the week
    const startDate = dayjs(start).utc(true).startOf('isoWeek').startOf('day');
    const endDate = dayjs(end).utc(true).endOf('isoWeek').startOf('day');

    // Helper functions to create day, week, and month objects
    const createDay = (keyDay, value?: ValueType): DayData<ValueType> => ({
        day: keyDay.format(CALENDAR_DATE_FORMAT),
        key: keyDay.format(CALENDAR_DATE_FORMAT),
        value,
    });

    const createWeek = (keyDay, days?: DayData<ValueType>[]): WeekData<ValueType> => ({
        days: days ?? [],
        key: keyDay.format('YYYY-MM-WW'),
    });

    const createMonth = (keyDay, weeks?: WeekData<ValueType>[]): MonthData<ValueType> => ({
        weeks: weeks ?? [],
        key: keyDay.format('YYYY-MM'),
        label: keyDay.format(labelFormat),
    });

    const months: MonthData<ValueType>[] = [];
    let month: MonthData<ValueType> = createMonth(startDate.endOf('isoWeek'));
    let week: WeekData<ValueType> | null = createWeek(startDate);

    // Iterate through each day from startDate to endDate
    for (let day = dayjs(startDate); !day.isAfter(endDate); day = day.add(1, 'day')) {
        // Find a value corresponding to the current day
        const value = data.filter((datum) => dayjs(datum.day).utc(true).isSame(day, 'date'))?.[0]?.value;

        const dayItem: DayData<ValueType> = createDay(day, value);

        if (week === null) week = createWeek(day);
        week.days.push(dayItem); // Add the day item to the current week

        // Check if it's the last day of the week
        if (day.isoWeekday() === DAYS_IN_WEEK) {
            const startOfWeek = day.startOf('isoWeek');
            const endOfWeek = day;
            const firstDayOfNextWeek = day.add(1, 'day');

            if (startOfWeek.month() !== endOfWeek.month()) {
                // Handle month transition in current week

                const lastWeekdayOfMonth = startOfWeek.endOf('month').isoWeekday();

                if (lastWeekdayOfMonth >= MIN_DAYS_IN_WEEK) {
                    // This week belongs to the current month
                    month.weeks.push(week);
                    months.push(month);
                    week = null;
                    month = createMonth(firstDayOfNextWeek);
                } else {
                    // This week belongs to the next month
                    if (month.weeks.length) months.push(month); // add current month if it has any weeks
                    month = createMonth(firstDayOfNextWeek, [week]);
                    week = null;
                }
            } else if (firstDayOfNextWeek.month() !== endOfWeek.month()) {
                // Handle if month changed on next week
                month.weeks.push(week);
                months.push(month);
                week = null;
                month = createMonth(firstDayOfNextWeek);
            } else {
                month.weeks.push(week);
                week = null;
            }

            if (endOfWeek.isSame(endDate)) {
                // Handle the end day
                if (week && month) month.weeks.push(week);
                if (month.weeks.length) months.push(month);
            }
        }
    }

    return months;
}

export function getColorAccessor<ValueType = any>(
    data: CalendarData<ValueType>[],
    colorAccessors: { [key: string]: ColorAccessor<ValueType> },
    defaultColor: string,
    correctiveMaxValue = 0,
) {
    if (Object.keys(colorAccessors).length === 0) return () => defaultColor;

    const scales = Object.entries(colorAccessors).reduce((acc, [key, accessor]) => {
        return {
            ...acc,
            ...{
                [key]: scaleLinear({
                    domain: [
                        0,
                        Math.max(...data.map((datum) => accessor.valueAccessor(datum.value)), correctiveMaxValue),
                    ],
                    range: [0, 1],
                    clamp: true,
                }),
            },
        };
    }, {});

    const colorInterpolators = Object.entries(colorAccessors).reduce((acc, [key, accessor]) => {
        return {
            ...acc,
            ...{
                [key]: d3interpolate.interpolateRgbBasis(accessor.colors),
            },
        };
    }, {});

    return function (datumValue?: ValueType) {
        if (datumValue === undefined) return defaultColor;

        // Get key and value of item with max value
        const [key, value] = Object.entries(colorAccessors)
            .map(([accessorKey, colorAccessor]) => [accessorKey, colorAccessor.valueAccessor(datumValue)])
            .reduce((max, current) => (current[1] > max[1] ? current : max));

        if ((value as number) <= 0) return defaultColor;

        const scaledValue = scales[key](value);
        return colorInterpolators[key](scaledValue);
    };
}

export type MockCalendarValue = {
    inserts: number;
    updates: number;
    deletes: number;
};

export function generateMockData(
    length: number,
    startDate: string | Date = '2024-11-30',
    maxValue = 3_000,
    minValue = 0,
): CalendarData<MockCalendarValue>[] {
    return Array(length)
        .fill(0)
        .map((_, index) => {
            const day = dayjs(startDate)
                .startOf('day')
                .add(index - length + 1, 'days')
                .format(CALENDAR_DATE_FORMAT);

            return {
                day,
                value: {
                    inserts: Math.max(Math.random() * maxValue, minValue),
                    updates: Math.max(Math.random() * maxValue, minValue),
                    deletes: Math.max(Math.random() * maxValue, minValue),
                },
            };
        });
}

export function getMockedProps(
    startDate = '2024-01-01',
    endDate = '2024-12-31',
): CalendarChartProps<MockCalendarValue> {
    const data = generateMockData(150, '2024-11-30');

    const colorAccessor = getColorAccessor<MockCalendarValue>(
        data,
        {
            insertsAndUpdates: {
                valueAccessor: (datum) => datum.inserts + datum.updates,
                colors: ['#CAC3F1', '#705EE4', '#3E2F9D'],
            },
            deletes: {
                valueAccessor: (datum) => datum.deletes,
                colors: ['#f1c3ca', '#CF6D6D', '#ab4242'],
            },
        },
        '#EBECF0',
    );

    return {
        data,
        startDate,
        endDate,
        colorAccessor,
    };
}
