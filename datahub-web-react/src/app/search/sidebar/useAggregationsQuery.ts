import { useEffect, useMemo } from 'react';
import { useAggregateAcrossEntitiesLazyQuery } from '../../../graphql/search.generated';
import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import { EntityType, QueryAggregateAcrossEntitiesArgs } from '../../../types.generated';
import useGetSearchQueryInputs from '../useGetSearchQueryInputs';

type Props = {
    entityType: EntityType;
    facets: string[];
    skip: boolean;
};

const useAggregationsQuery = ({ entityType, facets, skip }: Props) => {
    const { query, orFilters, viewUrn } = useGetSearchQueryInputs();

    const variables: QueryAggregateAcrossEntitiesArgs = useMemo(
        () => ({
            input: {
                types: [entityType],
                query,
                orFilters,
                viewUrn,
                facets,
            },
        }),
        [entityType, facets, orFilters, query, viewUrn],
    );

    const [fetchAggregations, { data, loading, called, error }] = useAggregateAcrossEntitiesLazyQuery({ variables });

    useEffect(() => {
        if (!skip) fetchAggregations();
    }, [fetchAggregations, skip]);

    const environmentAggregations =
        data?.aggregateAcrossEntities?.facets
            ?.find((facet) => facet.field === ORIGIN_FILTER_NAME)
            ?.aggregations.filter((aggregation) => aggregation.count > 0) ?? [];

    const platformAggregations =
        data?.aggregateAcrossEntities?.facets
            ?.find((facet) => facet.field === PLATFORM_FILTER_NAME)
            ?.aggregations.filter((aggregation) => aggregation.count > 0) ?? [];

    return {
        loading,
        loaded: !!data || !!error,
        error,
        called,
        environmentAggregations,
        platformAggregations,
    } as const;
};

export default useAggregationsQuery;
