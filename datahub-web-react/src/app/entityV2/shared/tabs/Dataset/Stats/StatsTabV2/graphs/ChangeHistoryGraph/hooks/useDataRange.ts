import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import { useMemo } from 'react';

import { OperationsData } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { CALENDAR_DATE_FORMAT } from '@src/alchemy-components';
import { CalendarData } from '@src/alchemy-components/components/CalendarChart/types';

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
