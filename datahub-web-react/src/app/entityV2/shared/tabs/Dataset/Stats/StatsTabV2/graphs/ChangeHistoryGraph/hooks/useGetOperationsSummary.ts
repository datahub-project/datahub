import { useGetOperationsStatsQuery } from '@src/graphql/dataset.generated';
import { TimeRange } from '@src/types.generated';
import { useMemo } from 'react';
import { convertAggregationsToValue, getCustomOperationsFromAggregations } from '../utils';

export default function useOperationsStatsSummary(urn: string | undefined, range: TimeRange) {
    const { data, loading } = useGetOperationsStatsQuery({
        variables: { urn: urn as string, range },
        skip: !urn,
    });

    const convertedAggregations = useMemo(
        () => convertAggregationsToValue(data?.dataset?.operationsStats?.aggregations),
        [data],
    );

    const customOperationTypes = useMemo(
        () => getCustomOperationsFromAggregations(data?.dataset?.operationsStats?.aggregations),
        [data],
    );

    return { data: convertedAggregations, loading, customOperationTypes };
}
