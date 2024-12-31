import { SelectOption } from '@src/alchemy-components';
import { useCallback } from 'react';
import { GRAPH_LOOPBACK_WINDOWS } from '../constants';
import { getStartTimeByWindowSize } from '../utils';
import useGetTimeRangeOptions from './useGetTimeRangeOptions';

export default function useGetTimeRangeOptionsByLookbackWindow(
    timeRangeOptions: SelectOption[],
    timeOfOldestData: number | null | undefined,
) {
    const getTime = useCallback((value) => getStartTimeByWindowSize(GRAPH_LOOPBACK_WINDOWS[value]), []);

    return useGetTimeRangeOptions(timeRangeOptions, timeOfOldestData, getTime);
}
