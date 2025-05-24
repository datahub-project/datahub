import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';

import { CALENDAR_DATE_FORMAT } from '@src/alchemy-components';
import { TimeRange } from '@src/types.generated';

dayjs.extend(utc);

export default function useGetCalendarRangeByTimeRange(range: TimeRange | undefined) {
    const currentDay = dayjs().utc(true).startOf('day');
    const endDay = currentDay.format(CALENDAR_DATE_FORMAT);

    switch (range) {
        case TimeRange.Day:
            return {
                startDay: currentDay.subtract(3, 'months').format(CALENDAR_DATE_FORMAT),
                endDay,
            };
        case TimeRange.Week:
            return {
                startDay: currentDay.subtract(3, 'months').format(CALENDAR_DATE_FORMAT),
                endDay,
            };
        case TimeRange.Month:
            return {
                startDay: currentDay.subtract(3, 'months').format(CALENDAR_DATE_FORMAT),
                endDay,
            };
        case TimeRange.Quarter:
            return {
                startDay: currentDay.subtract(3, 'months').format(CALENDAR_DATE_FORMAT),
                endDay,
            };
        case TimeRange.Year:
            return {
                startDay: currentDay.subtract(1, 'year').format(CALENDAR_DATE_FORMAT),
                endDay,
            };
        default:
            return {
                startDay: currentDay.subtract(1, 'year').format(CALENDAR_DATE_FORMAT),
                endDay,
            };
    }
}
