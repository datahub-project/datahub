import { SelectOption } from '@src/alchemy-components';
import { TimeRange } from '@src/types.generated';
import { useMemo } from 'react';
import { getStartTimeByTimeRange } from '../utils';

export default function useGetTimeRangeOptions(
    timeRangeOptions: SelectOption[],
    timeOfOldestData: number | null | undefined,
) {
    return useMemo(() => {
        if (timeOfOldestData === undefined || timeOfOldestData === null) return [];

        return timeRangeOptions.filter((value, index, options) => {
            const currentOption = value;
            const previousOption = options?.[index - 1];

            const currentOptionStart = getStartTimeByTimeRange(currentOption.value as TimeRange);
            const previousOptionStart = getStartTimeByTimeRange(previousOption?.value as TimeRange);

            if (!currentOptionStart) return false;

            if (currentOptionStart >= timeOfOldestData) return true;

            // if the oldest data time is older than the previous option, we should show this option (to allow them to show all possible data)
            return previousOptionStart && previousOptionStart >= timeOfOldestData;
        });
    }, [timeOfOldestData, timeRangeOptions]);
}
