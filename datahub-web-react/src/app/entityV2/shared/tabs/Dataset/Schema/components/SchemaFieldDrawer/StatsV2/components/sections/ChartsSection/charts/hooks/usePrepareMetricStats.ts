import { Datum } from '@src/alchemy-components/components/LineChart/types';
import {
    GRAPH_LOOKBACK_WINDOWS,
    LookbackWindowType,
} from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/constants';
import { groupTimeData, TimeInterval } from '@src/app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { getFixedLookbackWindow } from '@src/app/shared/time/timeUtils';
import { useMemo } from 'react';

const DEFAULT_AGGREGATION_FUNCTION = (values: number[]) => Math.max(...values);

export const usePrepareMetricStats = (
    data: Datum[],
    windowType: LookbackWindowType | string | null,
    aggregationFunction: ((values: number[]) => number) | undefined,
) => {
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
            (values) => (aggregationFunction ?? DEFAULT_AGGREGATION_FUNCTION)(values),
        ).map((datum) => ({
            x: datum.time,
            y: datum.value,
        }));

        return dataGroupedByDays;
    }, [data, windowType, aggregationFunction]);
};
