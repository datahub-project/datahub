import dayjs from 'dayjs';
import { useMemo } from 'react';
import { CalendarData } from '@src/alchemy-components/components/CalendarChart/types';
import utc from 'dayjs/plugin/utc';
import { CALENDAR_DATE_FORMAT } from '@src/alchemy-components';
import { ValueType } from './useChangeHistoryData';

dayjs.extend(utc);

export default function useDataRange(data: CalendarData<ValueType>[], startTimeOfData: number | undefined | null) {
    const startDay = useMemo(() => {
        if (!startTimeOfData || data.length === 0) return undefined;

        const maxTimeMillis = Math.max(...[startTimeOfData, dayjs(data[0].day).utc(true).toDate().getTime()]);
        return dayjs(maxTimeMillis).format(CALENDAR_DATE_FORMAT);
    }, [startTimeOfData, data]);

    const endDay = useMemo(() => dayjs(new Date()).utc(true).format(CALENDAR_DATE_FORMAT), []);

    return {
        startDay,
        endDay,
    };
}
