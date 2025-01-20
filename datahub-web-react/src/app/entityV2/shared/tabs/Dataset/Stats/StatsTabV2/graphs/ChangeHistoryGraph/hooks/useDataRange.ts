import dayjs from 'dayjs';
import { useMemo } from 'react';
import { CalendarData } from '@src/alchemy-components/components/CalendarChart/types';
import utc from 'dayjs/plugin/utc';
import { CALENDAR_DATE_FORMAT } from '@src/alchemy-components';
import { OperationsData } from '../types';

dayjs.extend(utc);

export default function useDataRange(data: CalendarData<OperationsData>[], startTimeOfData: number | undefined | null) {
    const startDay = useMemo(() => {
        if (!startTimeOfData || data.length === 0) return undefined;

        const maxTimeMillis = Math.min(...[startTimeOfData, dayjs(data[0].day).toDate().getTime()]);
        return dayjs(maxTimeMillis).format(CALENDAR_DATE_FORMAT);
    }, [startTimeOfData, data]);

    const endDay = useMemo(() => dayjs(new Date()).utc(true).format(CALENDAR_DATE_FORMAT), []);

    return {
        startDay,
        endDay,
    };
}
