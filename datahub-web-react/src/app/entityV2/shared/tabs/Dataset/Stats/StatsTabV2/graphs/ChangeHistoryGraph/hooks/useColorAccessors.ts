import { useMemo } from 'react';

import {
    AnyOperationType,
    OperationsData,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/types';
import { createColorAccessors } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/ChangeHistoryGraph/utils';
import { CalendarData } from '@src/alchemy-components/components/CalendarChart/types';

export default function useColorAccessors(
    summary: OperationsData | undefined,
    data: CalendarData<OperationsData>[],
    types: AnyOperationType[],
) {
    return useMemo(() => createColorAccessors(summary, data, types), [summary, data, types]);
}
