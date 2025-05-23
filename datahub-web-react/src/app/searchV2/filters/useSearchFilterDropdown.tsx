import { useEffect, useMemo } from 'react';

import {
    deduplicateAggregations,
    filterEmptyAggregations,
    getNewFilters,
    getNumActiveFiltersForFilter,
} from '@app/searchV2/filters/utils';
import useGetSearchQueryInputs from '@app/searchV2/useGetSearchQueryInputs';
import { ENTITY_FILTER_NAME } from '@app/searchV2/utils/constants';
import usePrevious from '@app/shared/usePrevious';
import { useAggregateAcrossEntitiesLazyQuery } from '@src/graphql/search.generated';

import { EntityType, FacetFilterInput, FacetMetadata } from '@types';

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    aggregationsEntityTypes?: Array<EntityType>;
    shouldUseAggregationsFromFilter?: boolean;
    shouldApplyView?: boolean;
}

export default function useSearchFilterDropdown({
    filter,
    activeFilters,
    onChangeFilters,
    aggregationsEntityTypes,
    shouldUseAggregationsFromFilter,
    shouldApplyView = true,
}: Props) {
    const numActiveFilters = getNumActiveFiltersForFilter(activeFilters, filter);
    const shouldFetchAggregations: boolean = !!filter.field && numActiveFilters > 0 && !shouldUseAggregationsFromFilter;

    const { entityFilters, query, orFilters, viewUrn } = useGetSearchQueryInputs(
        useMemo(() => [filter.field], [filter.field]),
    );

    const [aggregateAcrossEntities, { data, loading }] = useAggregateAcrossEntitiesLazyQuery();

    useEffect(() => {
        // Fetch the aggregates of the current facet only if there are active filters
        // Otherwise, the aggregates are already fetched by the search query
        if (shouldFetchAggregations) {
            aggregateAcrossEntities({
                variables: {
                    input: {
                        types: aggregationsEntityTypes || (filter.field === ENTITY_FILTER_NAME ? null : entityFilters),
                        query,
                        orFilters,
                        ...(shouldApplyView ? { viewUrn } : {}),
                        facets: [filter.field],
                    },
                },
            });
        }
    }, [
        aggregateAcrossEntities,
        entityFilters,
        filter.field,
        orFilters,
        query,
        viewUrn,
        shouldFetchAggregations,
        aggregationsEntityTypes,
        shouldApplyView,
    ]);

    const newAggregations =
        data?.aggregateAcrossEntities?.facets?.find((f) => f.field === filter.field)?.aggregations || [];
    const searchAggregations = filter.aggregations;
    const activeAggregations = searchAggregations.filter((agg) =>
        activeFilters.find((f) => f.values?.includes(agg.value) || f.value === agg.value),
    );

    const prevNewAggregations = usePrevious(newAggregations);
    // Avoid flicker while we fetch aggregations
    const fetchedAggregations = newAggregations.length > 0 ? newAggregations : prevNewAggregations || [];

    const aggregations = shouldFetchAggregations
        ? [...fetchedAggregations, ...deduplicateAggregations(fetchedAggregations, activeAggregations)]
        : searchAggregations;

    const finalAggregations = filterEmptyAggregations(aggregations || [], activeFilters);

    function updateFilters(newFilters) {
        onChangeFilters(
            getNewFilters(
                filter.field,
                activeFilters,
                newFilters.map((f) => f.value),
            ),
        );
    }

    function manuallyUpdateFilters(newFilters: FacetFilterInput[]) {
        // remove any filters that are in newFilters to overwrite them
        const filtersNotInNewFilters = activeFilters.filter(
            (f) => !newFilters.find((newFilter) => newFilter.field === f.field),
        );
        onChangeFilters([...filtersNotInNewFilters, ...newFilters]);
    }

    return {
        updateFilters,
        finalAggregations,
        numActiveFilters,
        areFiltersLoading: loading,
        manuallyUpdateFilters,
    };
}
