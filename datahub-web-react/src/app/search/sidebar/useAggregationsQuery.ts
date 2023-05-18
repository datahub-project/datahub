import { useCallback } from 'react';
import { useAggregateAcrossEntitiesLazyQuery } from '../../../graphql/search.generated';
import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import { EntityType } from '../../../types.generated';
import useGetSearchQueryInputs from '../useGetSearchQueryInputs';

const useAggregationsQuery = () => {
    const { query, orFilters, viewUrn } = useGetSearchQueryInputs();
    const [fetchAggregations, { data, loading, called, error }] = useAggregateAcrossEntitiesLazyQuery();

    const fetchAggregationsApi = useCallback(
        (entityType: EntityType, facets: Array<string>) => {
            fetchAggregations({
                variables: {
                    input: {
                        types: [entityType],
                        query,
                        orFilters,
                        viewUrn,
                        facets,
                    },
                },
            });
        },
        [fetchAggregations, query, orFilters, viewUrn],
    );

    const environmentAggregations =
        data?.aggregateAcrossEntities?.facets
            ?.find((facet) => facet.field === ORIGIN_FILTER_NAME)
            ?.aggregations.filter((aggregation) => aggregation.count > 0) ?? [];

    const platformAggregations =
        data?.aggregateAcrossEntities?.facets
            ?.find((facet) => facet.field === PLATFORM_FILTER_NAME)
            ?.aggregations.filter((aggregation) => aggregation.count > 0) ?? [];

    return [
        fetchAggregationsApi,
        { loading, loaded: !!data || !!error, error, called, environmentAggregations, platformAggregations } as const,
    ] as const;
};

export default useAggregationsQuery;
