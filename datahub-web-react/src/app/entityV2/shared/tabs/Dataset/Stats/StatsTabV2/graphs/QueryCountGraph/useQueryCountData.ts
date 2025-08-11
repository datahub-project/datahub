import moment from 'moment';
import { useEffect, useState } from 'react';

import { useStatsSectionsContext } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/StatsSectionsContext';
import {
    MAX_VALUE_AGGREGATION,
    SUM_VALUES_AGGREGATION,
    TimeInterval,
    addMonthOverMonthValue,
    getCalendarStartTimeByTimeRange,
    groupTimeData,
    roundTimeByTimeRange,
} from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/graphs/utils';
import { useGetTimeRangeUsageAggregationsLazyQuery } from '@src/graphql/dataset.generated';
import { Maybe, TimeRange, UsageAggregation, UsageQueryResult } from '@src/types.generated';

const MAX_NUM_DAYS_PER_MONTH = 31;
const MAX_NUM_DAYS_PER_QUARTER = 95;

export interface ChartData {
    x: number;
    y: number;
    mom?: number | null;
}

// Filter out the buckets items or values with null or undefined values
const normalizeData = (data: Maybe<UsageAggregation>[]) => {
    const validBuckets: UsageAggregation[] =
        data?.filter((item): item is UsageAggregation => item !== null && item !== undefined) || [];
    return validBuckets.filter(
        (item) =>
            item.bucket !== null &&
            item.bucket !== undefined &&
            item.metrics?.totalSqlQueries !== null &&
            item.metrics?.totalSqlQueries !== undefined,
    );
};

const getChartData = (rawData: UsageAggregation[], interval: TimeInterval) => {
    const groupedData = groupTimeData(
        rawData,
        interval,
        (d) => d.bucket || 0,
        (d) => d.metrics?.totalSqlQueries || 0,
        interval === TimeInterval.DAY ? MAX_VALUE_AGGREGATION : SUM_VALUES_AGGREGATION,
    );

    return addMonthOverMonthValue(
        groupedData,
        (d) => d.time,
        (d) => d.value,
    );
};

export default function useQueryCountData(urn: string | undefined, timeRange?: TimeRange) {
    const [chartData, setChartData] = useState<ChartData[]>([]);
    const [groupInterval, setGroupInterval] = useState<TimeInterval>(TimeInterval.DAY);

    const {
        permissions: { canViewDatasetUsage },
    } = useStatsSectionsContext();

    // Required for the loading state to track if the lazy query has been called
    const [queryCalled, setQueryCalled] = useState(false);
    const [getTimeRangeUsageAggregations, { data: aggregationData, loading }] =
        useGetTimeRangeUsageAggregationsLazyQuery({
            onCompleted: () => setQueryCalled(true),
        });

    const handleSetGroupInterval = (length) => {
        if (length <= MAX_NUM_DAYS_PER_MONTH) setGroupInterval(TimeInterval.DAY);
        else if (length > MAX_NUM_DAYS_PER_MONTH && length <= MAX_NUM_DAYS_PER_QUARTER) {
            setGroupInterval(TimeInterval.WEEK);
        } else if (length > MAX_NUM_DAYS_PER_QUARTER) setGroupInterval(TimeInterval.MONTH);
    };

    useEffect(() => {
        if (aggregationData)
            handleSetGroupInterval((aggregationData.dataset?.usageStats as UsageQueryResult)?.buckets?.length);
    }, [aggregationData]);

    useEffect(() => {
        if (timeRange && urn !== undefined && canViewDatasetUsage) {
            const startTime = roundTimeByTimeRange(getCalendarStartTimeByTimeRange(Date.now(), timeRange), timeRange);

            getTimeRangeUsageAggregations({
                variables: {
                    urn,
                    timeRange,
                    startTime,
                    timeZone: moment.tz.guess(),
                },
            });
        }
    }, [timeRange, getTimeRangeUsageAggregations, urn, canViewDatasetUsage]);

    useEffect(() => {
        if (aggregationData && canViewDatasetUsage) {
            const updatedBuckets = (aggregationData.dataset?.usageStats as UsageQueryResult)?.buckets;
            const normalizedData: UsageAggregation[] = normalizeData(updatedBuckets || []);
            const processedData = getChartData(normalizedData, groupInterval);
            const convertedData = processedData.map((datum) => ({
                x: datum.time,
                y: datum.value,
                mom: datum.mom,
            }));
            setChartData(convertedData);
        }
    }, [aggregationData, groupInterval, canViewDatasetUsage]);

    if (!canViewDatasetUsage) {
        return {
            chartData: [],
            loading: false,
            groupInterval: TimeInterval.DAY,
        };
    }

    return {
        chartData,
        loading: queryCalled ? loading : true,
        groupInterval,
    };
}
