import { useCallback } from 'react';

import useGetTimeRangeOptions from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useGetTimeRangeOptions';
import { getStartTimeByTimeRange } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { SelectOption } from '@src/alchemy-components';
import { TimeRange } from '@src/types.generated';

export default function useGetTimeRangeOptionsByTimeRange(
    timeRangeOptions: SelectOption[],
    timeOfOldestData: number | null | undefined,
) {
    const getTime = useCallback((value) => getStartTimeByTimeRange(value as TimeRange), []);

    return useGetTimeRangeOptions(timeRangeOptions, timeOfOldestData, getTime);
}
