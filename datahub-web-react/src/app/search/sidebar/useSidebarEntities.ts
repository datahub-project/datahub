import { useMemo } from 'react';
import { ENTITY_FILTER_NAME } from '../utils/constants';
import useAggregationsQuery from './useAggregationsQuery';

type Props = {
    skip: boolean;
};

const useSidebarEntities = ({ skip }: Props) => {
    const {
        error: filteredError,
        entityAggregations: filteredAggregations,
        retry,
    } = useAggregationsQuery({
        skip,
        facets: [ENTITY_FILTER_NAME],
    });

    const { error: baseError, entityAggregations: baseEntityAggregations } = useAggregationsQuery({
        skip,
        query: '*',
        orFilters: [],
        facets: [ENTITY_FILTER_NAME],
    });

    const result = useMemo(() => {
        if (filteredError || baseError) return filteredAggregations;
        if (!filteredAggregations) return filteredAggregations;
        if (!baseEntityAggregations) return baseEntityAggregations;
        return filteredAggregations.filter((agg) =>
            baseEntityAggregations.some((base) => base.value === agg.value && !!base.count),
        );
    }, [baseEntityAggregations, baseError, filteredAggregations, filteredError]);

    return { error: filteredError, entityAggregations: result, retry } as const;
};

export default useSidebarEntities;
