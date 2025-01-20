import { CalendarData } from '@src/alchemy-components/components/CalendarChart/types';
import { useMemo } from 'react';
import { AnyOperationType, OperationsData } from '../types';
import { createColorAccessors } from '../utils';

export default function useColorAccessors(
    summary: OperationsData | undefined,
    data: CalendarData<OperationsData>[],
    types: AnyOperationType[],
) {
    return useMemo(() => createColorAccessors(summary, data, types), [summary, data, types]);
}
