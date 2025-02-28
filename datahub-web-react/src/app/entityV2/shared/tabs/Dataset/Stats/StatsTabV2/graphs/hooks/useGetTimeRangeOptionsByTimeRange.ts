import { SelectOption } from '@src/alchemy-components';
import { TimeRange } from '@src/types.generated';
import { useCallback } from 'react';
import { getStartTimeByTimeRange } from '../utils';
import useGetTimeRangeOptions from './useGetTimeRangeOptions';

export default function useGetTimeRangeOptionsByTimeRange(
    timeRangeOptions: SelectOption[],
    timeOfOldestData: number | null | undefined,
) {
    const getTime = useCallback((value) => getStartTimeByTimeRange(value as TimeRange), []);

    return useGetTimeRangeOptions(timeRangeOptions, timeOfOldestData, getTime);
}
