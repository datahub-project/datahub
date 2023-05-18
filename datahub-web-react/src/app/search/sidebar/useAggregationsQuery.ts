import { useEffect, useMemo } from 'react';
import { useAggregateAcrossEntitiesLazyQuery } from '../../../graphql/search.generated';
import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import { EntityType, QueryAggregateAcrossEntitiesArgs } from '../../../types.generated';
import useGetSearchQueryInputs from '../useGetSearchQueryInputs';
import { generateOrFilters } from '../utils/generateOrFilters';

type Props = {
    entityType: EntityType;
    environment?: string | null;
    facets: string[];
    skip: boolean;
};

const useAggregationsQuery = ({ entityType, environment, facets, skip }: Props) => {
    const filterOverrides = useMemo(
        () => [...(typeof environment !== 'undefined' ? [{ field: ORIGIN_FILTER_NAME, value: environment }] : [])],
        [environment],
    );
    const filterFieldsToOverride = useMemo(() => filterOverrides.map((f) => f.field), [filterOverrides]);

    const { query, orFilters, viewUrn } = useGetSearchQueryInputs(filterFieldsToOverride);

    // todo - clean up this is bananas
    const orFiltersWithOverrides: ReturnType<typeof generateOrFilters> = useMemo(
        () =>
            orFilters.map((orFilter) =>
                orFilter.and
                    ? {
                          and: [...orFilter.and, ...filterOverrides],
                      }
                    : orFilter,
            ),
        [filterOverrides, orFilters],
    );

    const variables: QueryAggregateAcrossEntitiesArgs = useMemo(
        () => ({
            input: {
                types: [entityType],
                query,
                orFilters: orFiltersWithOverrides,
                viewUrn,
                facets,
            },
        }),
        [entityType, facets, orFiltersWithOverrides, query, viewUrn],
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
