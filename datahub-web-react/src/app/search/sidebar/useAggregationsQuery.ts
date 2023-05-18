import { useEffect, useMemo } from 'react';
import { useAggregateAcrossEntitiesLazyQuery } from '../../../graphql/search.generated';
import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME, UnionType } from '../utils/constants';
import {
    EntityType,
    FacetFilterInput,
    QueryAggregateAcrossEntitiesArgs,
    AndFilterInput,
} from '../../../types.generated';
import useGetSearchQueryInputs from '../useGetSearchQueryInputs';
import { generateOrFilters } from '../utils/generateOrFilters';

type Props = {
    entityType: EntityType;
    environment?: string | null;
    facets: string[];
    skip: boolean;
};

const applyFilterOverrides = (overrides: Array<FacetFilterInput>, orFilters: Array<AndFilterInput>) => {
    if (!orFilters.length) return generateOrFilters(UnionType.AND, overrides);

    return orFilters.map((orFilter) =>
        orFilter.and
            ? {
                  and: [...orFilter.and, ...overrides],
              }
            : orFilter,
    );
};

const useAggregationsQuery = ({ entityType, environment, facets, skip }: Props) => {
    const filterOverrides = useMemo(
        () => [...(environment ? [{ field: ORIGIN_FILTER_NAME, value: environment }] : [])],
        [environment],
    );

    const excludedFields = useMemo(() => filterOverrides.map((filter) => filter.field), [filterOverrides]);

    const { query, orFilters: orFiltersWithoutOverrides, viewUrn } = useGetSearchQueryInputs(excludedFields);

    const orFiltersWithOverrides: ReturnType<typeof generateOrFilters> = useMemo(
        () => applyFilterOverrides(filterOverrides, orFiltersWithoutOverrides),
        [filterOverrides, orFiltersWithoutOverrides],
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
