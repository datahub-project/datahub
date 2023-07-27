import { useMemo } from 'react';
import { ENTITY_FILTER_NAME } from '../utils/constants';
import useAggregationsQuery from './useAggregationsQuery';

type Props = {
    skip: boolean;
};

const useSidebarEntities = ({ skip }: Props) => {
    const {
        error: filteredError,
        entityAggregations: filteredAggs,
        retry: retryFilteredAggs,
    } = useAggregationsQuery({
        skip,
        facets: [ENTITY_FILTER_NAME],
    });

    const { error: baseError, entityAggregations: baseAggs } = useAggregationsQuery({
        skip,
        facets: [ENTITY_FILTER_NAME],
        excludeFilters: true,
    });

    const result = useMemo(() => {
        if (filteredError || baseError) return filteredAggs; // Fallback to filtered aggs on any error
        if (!filteredAggs || !baseAggs) return null; // If we're loading one of the queries, wait to render
        return filteredAggs.filter((agg) => baseAggs.some((base) => base.value === agg.value && !!base.count));
    }, [baseAggs, baseError, filteredAggs, filteredError]);

    return { error: filteredError, entityAggregations: result, retry: retryFilteredAggs } as const;
};

export default useSidebarEntities;
