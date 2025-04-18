import { useMemo } from 'react';

import { SelectOption } from '@src/alchemy-components';

export default function useGetTimeRangeOptions(
    timeRangeOptions: SelectOption[],
    timeOfOldestData: number | null | undefined,
    getStartTime: (value: string) => number | undefined,
) {
    return useMemo(() => {
        if (timeOfOldestData === undefined || timeOfOldestData === null) return [];

        return timeRangeOptions.filter((value, index, options) => {
            const currentOption = value;
            const previousOption = options?.[index - 1];

            const currentOptionStart = getStartTime(currentOption.value);
            const previousOptionStart = getStartTime(previousOption?.value);

            if (!currentOptionStart) return false;

            if (!previousOptionStart) return true;

            if (currentOptionStart >= timeOfOldestData) return true;

            // if the oldest data time is older than the previous option, we should show this option (to allow them to show all possible data)
            return previousOptionStart && previousOptionStart >= timeOfOldestData;
        });
    }, [timeOfOldestData, timeRangeOptions, getStartTime]);
}
