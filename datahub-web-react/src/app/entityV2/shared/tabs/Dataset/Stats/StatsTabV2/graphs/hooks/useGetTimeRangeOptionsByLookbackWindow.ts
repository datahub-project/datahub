/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback } from 'react';

import { GRAPH_LOOKBACK_WINDOWS } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import useGetTimeRangeOptions from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/hooks/useGetTimeRangeOptions';
import { getStartTimeByWindowSize } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { SelectOption } from '@src/alchemy-components';

export default function useGetTimeRangeOptionsByLookbackWindow(
    timeRangeOptions: SelectOption[],
    timeOfOldestData: number | null | undefined,
) {
    const getTime = useCallback((value) => getStartTimeByWindowSize(GRAPH_LOOKBACK_WINDOWS[value]), []);

    return useGetTimeRangeOptions(timeRangeOptions, timeOfOldestData, getTime);
}
