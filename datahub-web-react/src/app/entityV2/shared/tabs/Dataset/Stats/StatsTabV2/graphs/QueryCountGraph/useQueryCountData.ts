import { useGetTimeRangeUsageAggregationsLazyQuery } from '@src/graphql/dataset.generated';
import { Maybe, TimeRange, UsageAggregation, UsageQueryResult } from '@src/types.generated';
import { useEffect, useState } from 'react';
import { useStatsSectionsContext } from '../../StatsSectionsContext';
import { addMonthOverMonthValue, groupTimeData, TimeInterval } from '../utils';

const MAX_NUM_DAYS_PER_MONTH = 31;
const MAX_NUM_DAYS_PER_QUARTER = 95;

interface ChartData {
    time: string | number;
    value: number;
    mom: number | null;
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
        (values) => (interval === TimeInterval.DAY ? Math.max(...values) : values.reduce((sum, val) => sum + val, 0)),
    );

    return addMonthOverMonthValue(
        groupedData,
        (d) => d.time,
        (d) => d.value,
    );
};

export default function useQueryCountData(
    urn: string | undefined,
    timeRange?: TimeRange,
    initialData?: Array<Maybe<UsageAggregation>>,
) {
    const [chartData, setChartData] = useState<ChartData[]>([]);
    const [groupInterval, setGroupInterval] = useState<TimeInterval>(TimeInterval.DAY);

    const {
        permissions: { canViewDatasetUsage },
    } = useStatsSectionsContext();

    const [getTimeRangeUsageAggregations, { data: aggregationData, loading }] =
        useGetTimeRangeUsageAggregationsLazyQuery();

    const handleSetGroupInterval = (data) => {
        if (data.length <= MAX_NUM_DAYS_PER_MONTH) setGroupInterval(TimeInterval.DAY);
        else if (data.length > MAX_NUM_DAYS_PER_MONTH && data.length <= MAX_NUM_DAYS_PER_QUARTER) {
            setGroupInterval(TimeInterval.WEEK);
        } else if (data.length > MAX_NUM_DAYS_PER_QUARTER) setGroupInterval(TimeInterval.MONTH);
    };

    useEffect(() => {
        if (initialData?.length) handleSetGroupInterval(initialData);
        else if (aggregationData)
            handleSetGroupInterval((aggregationData.dataset?.usageStats as UsageQueryResult)?.buckets);
    }, [initialData, aggregationData]);

    useEffect(() => {
        if (timeRange && !initialData && urn !== undefined && canViewDatasetUsage) {
            getTimeRangeUsageAggregations({
                variables: { urn, timeRange },
            });
        }
    }, [timeRange, initialData, getTimeRangeUsageAggregations, urn, canViewDatasetUsage]);

    useEffect(() => {
        if (initialData?.length) {
            const normalizedData: UsageAggregation[] = normalizeData(initialData);
            const processedData = getChartData(normalizedData, groupInterval);
            setChartData(processedData);
        }
    }, [initialData, groupInterval]);

    useEffect(() => {
        if (!initialData?.length && aggregationData) {
            const updatedBuckets = (aggregationData.dataset?.usageStats as UsageQueryResult)?.buckets;
            const normalizedData: UsageAggregation[] = normalizeData(updatedBuckets || []);
            const processedData = getChartData(normalizedData, groupInterval);
            setChartData(processedData);
        }
    }, [aggregationData, groupInterval, initialData?.length]);

    if (!canViewDatasetUsage) {
        return {
            chartData: [],
            loading: false,
            groupInterval: TimeInterval.DAY,
        };
    }

    return {
        chartData,
        loading,
        groupInterval,
    };
}
