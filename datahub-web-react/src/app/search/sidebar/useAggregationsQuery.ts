import { useMemo } from 'react';
import { useAggregateAcrossEntitiesQuery } from '../../../graphql/search.generated';
import { ORIGIN_FILTER_NAME, PLATFORM_FILTER_NAME } from '../utils/constants';
import { EntityType } from '../../../types.generated';
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

    const { query, orFilters, viewUrn } = useGetSearchQueryInputs(excludedFilterFields);

    const {
        data: newData,
        previousData,
        loading,
        error,
    } = useAggregateAcrossEntitiesQuery({
        skip,
        fetchPolicy: 'cache-first',
        variables: {
            input: {
                types: [entityType],
                query,
                orFilters: applyOrFilterOverrides(orFilters, filterOverrides),
                viewUrn,
                facets,
            },
        },
    });

    const data = newData ?? previousData;

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
