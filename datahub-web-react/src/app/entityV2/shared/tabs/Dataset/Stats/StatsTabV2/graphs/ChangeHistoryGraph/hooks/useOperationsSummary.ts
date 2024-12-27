import { CalendarData } from '@src/alchemy-components/components/CalendarChart/types';
import { useMemo } from 'react';
import { ValueType } from './useChangeHistoryData';

export default function useOperationsSummary(data: CalendarData<ValueType>[]) {
    return useMemo(() => {
        return data.reduce(
            (summary, datum) => ({
                inserts: summary.inserts + datum.value.inserts,
                updates: summary.updates + datum.value.updates,
                deletes: summary.deletes + datum.value.deletes,
            }),
            {
                inserts: 0,
                updates: 0,
                deletes: 0,
            },
        );
    }, [data]);
}
