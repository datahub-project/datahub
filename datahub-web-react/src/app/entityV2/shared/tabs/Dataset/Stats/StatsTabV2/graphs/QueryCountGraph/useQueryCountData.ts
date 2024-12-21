import { useGetTimeRangeUsageAggregationsQuery } from '@src/graphql/dataset.generated';
import { Maybe, TimeRange, UsageAggregation, UsageQueryResult } from '@src/types.generated';
import { useEffect, useState } from 'react';
import { addMonthOverMonthValue, groupTimeData } from '../utils';

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

const getChartData = (rawData: UsageAggregation[]) => {
    const groupedData = groupTimeData(
        rawData,
        'day',
        (d) => d.bucket || 0,
        (d) => d.metrics?.totalSqlQueries || 0,
        (values) => Math.max(...values),
    );

    return addMonthOverMonthValue(
        groupedData,
        (d) => d.time,
        (d) => d.value,
    );
};

export default function useQueryCountData(
    urn: string,
    timeRange: TimeRange,
    initialData?: Array<Maybe<UsageAggregation>>,
) {
    const [chartData, setChartData] = useState<ChartData[]>([]);

    const { data: aggregationData, loading } = useGetTimeRangeUsageAggregationsQuery({
        variables: { urn, timeRange },
        skip: !!initialData,
    });

    useEffect(() => {
        if (initialData) {
            const normalizedData: UsageAggregation[] = normalizeData(initialData);
            const processedData = getChartData(normalizedData);
            setChartData(processedData);
        }
    }, [initialData]);

    useEffect(() => {
        if (aggregationData) {
            const updatedBuckets = (aggregationData.dataset?.usageStats as UsageQueryResult)?.buckets;
            const normalizedData: UsageAggregation[] = normalizeData(updatedBuckets || []);
            const processedData = getChartData(normalizedData);
            setChartData(processedData);
        }
    }, [aggregationData]);

    return {
        chartData,
        loading,
    };
}
