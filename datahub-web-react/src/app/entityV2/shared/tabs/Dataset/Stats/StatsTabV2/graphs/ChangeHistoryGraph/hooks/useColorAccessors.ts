import { useMemo } from 'react';
import { useTheme } from 'styled-components';

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
    const theme = useTheme();
    return useMemo(() => createColorAccessors(summary, data, types, theme), [summary, data, types, theme]);
}
