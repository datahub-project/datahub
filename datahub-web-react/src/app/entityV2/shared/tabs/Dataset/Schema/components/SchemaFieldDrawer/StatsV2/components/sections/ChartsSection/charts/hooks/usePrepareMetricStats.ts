import { useMemo } from 'react';

import { Datum } from '@src/alchemy-components/components/LineChart/types';
import {
    GRAPH_LOOKBACK_WINDOWS,
    LookbackWindowType,
} from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import {
    AggregationFunction,
    MAX_VALUE_AGGREGATION,
    TimeInterval,
    groupTimeData,
} from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { getFixedLookbackWindow } from '@src/app/shared/time/timeUtils';

export const usePrepareMetricStats = (
    data: Datum[],
    windowType: LookbackWindowType | string | null,
    aggregationFunction: AggregationFunction = MAX_VALUE_AGGREGATION,
): Datum[] => {
    return useMemo(() => {
        const windowSize = windowType ? GRAPH_LOOKBACK_WINDOWS[windowType]?.windowSize : undefined;
        if (windowSize === undefined) return [];

        const { startTime } = getFixedLookbackWindow(windowSize);

        const dataFilteredByLookbackWindow = data.filter((datum) => datum.x > startTime);

        const dataGroupedByDays = groupTimeData(
            dataFilteredByLookbackWindow,
            TimeInterval.DAY,
            (datum) => datum.x,
            (datum) => datum.y,
            aggregationFunction,
        )
            .filter((datum) => datum.value !== undefined)
            .map((datum) => ({
                x: datum.time,
                y: datum.value ?? 0, // the value can't be undefined here as it was filtered above
            }));

        return dataGroupedByDays;
    }, [data, windowType, aggregationFunction]);
};
