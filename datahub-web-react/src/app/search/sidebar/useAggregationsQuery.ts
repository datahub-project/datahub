import { useEffect, useMemo } from 'react';
import { useAggregateAcrossEntitiesLazyQuery } from '../../../graphql/search.generated';
import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import { EntityType, QueryAggregateAcrossEntitiesArgs } from '../../../types.generated';
import useGetSearchQueryInputs from '../useGetSearchQueryInputs';
import applyOrFilterOverrides from '../utils/applyOrFilterOverrides';

type Props = {
    entityType: EntityType;
    environment?: string | null;
    facets: string[];
    skip: boolean;
};

const useAggregationsQuery = ({ entityType, environment, facets, skip }: Props) => {
    const filterOverrides = useMemo(
        () => [...(environment ? [{ field: ORIGIN_FILTER_NAME, value: environment }] : [])],
        [environment],
    );

    const excludedFilterFields = useMemo(() => filterOverrides.map((filter) => filter.field), [filterOverrides]);

    const { query, orFilters: orFiltersWithoutOverrides, viewUrn } = useGetSearchQueryInputs(excludedFilterFields);

    const orFilters = useMemo(
        () => applyOrFilterOverrides(orFiltersWithoutOverrides, filterOverrides),
        [filterOverrides, orFiltersWithoutOverrides],
    );

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

    const [fetchAggregations, { data, loading, error }] = useAggregateAcrossEntitiesLazyQuery({
        fetchPolicy: 'cache-first',
        variables,
    });

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
        environmentAggregations,
        platformAggregations,
    } as const;
};

export default useAggregationsQuery;
