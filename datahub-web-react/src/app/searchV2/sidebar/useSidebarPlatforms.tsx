import { useMemo } from 'react';
import { PLATFORM_FILTER_NAME } from '../utils/constants';
import useAggregationsQuery from './useAggregationsQuery';

type Props = {
    skip: boolean;
};

const useSidebarPlatforms = ({ skip }: Props) => {
    const {
        error: filteredError,
        platformAggregations: filteredAggs,
        retry: retryFilteredAggs,
    } = useAggregationsQuery({
        skip,
        facets: [PLATFORM_FILTER_NAME],
    });

    const { error: baseError, platformAggregations: baseAggs } = useAggregationsQuery({
        skip,
        facets: [PLATFORM_FILTER_NAME],
        excludeFilters: true,
    });

    const result = useMemo(() => {
        if (filteredError || baseError) return filteredAggs; // Fallback to filtered aggs on any error
        if (!filteredAggs || !baseAggs) return null; // If we're loading one of the queries, wait to render
        return filteredAggs.filter((agg) => baseAggs.some((base) => base.value === agg.value && !!base.count));
    }, [baseAggs, baseError, filteredAggs, filteredError]);

    return { error: filteredError, platformAggregations: result, retry: retryFilteredAggs } as const;
};

export default useSidebarPlatforms;
