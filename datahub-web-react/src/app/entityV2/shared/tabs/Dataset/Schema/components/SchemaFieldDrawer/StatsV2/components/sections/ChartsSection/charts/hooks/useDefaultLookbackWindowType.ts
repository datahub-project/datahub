/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import { Datum } from '@src/alchemy-components/components/LineChart/types';
import { GRAPH_LOOKBACK_WINDOWS } from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import { getStartTimeByWindowSize } from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';

/**
 * Returns the minimum available lookback window containing the data
 */
export default function useDefaultLookbackWindowType(data: Datum[], availableLookbackWindowTypes: string[]) {
    return useMemo(() => {
        if (data.length === 0) return null;
        const latestDataTime = Math.max(...data.map((datum) => datum.x));

        const lookbackWindowType =
            Object.entries(GRAPH_LOOKBACK_WINDOWS)
                .filter(([key, _]) => availableLookbackWindowTypes.includes(key))
                .find(([_, value]) => {
                    const startTime = getStartTimeByWindowSize(value);
                    return startTime && startTime <= latestDataTime;
                })?.[0] ?? null;

        return lookbackWindowType;
    }, [data, availableLookbackWindowTypes]);
}
