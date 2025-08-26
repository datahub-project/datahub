import { useEffect, useMemo } from 'react';

import {
    deduplicateAggregations,
    filterEmptyAggregations,
    getNewFilters,
    getNumActiveFiltersForFilter,
} from '@app/searchV2/filters/utils';
import useGetSearchQueryInputs from '@app/searchV2/useGetSearchQueryInputs';
import usePrevious from '@app/shared/usePrevious';

import { useAggregateActionRequestsLazyQuery } from '@graphql/actionRequest.generated';
import { FacetFilterInput, FacetMetadata } from '@types';

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    shouldUseAggregationsFromFilter?: boolean;
    shouldApplyView?: boolean;
    getAllActionRequests?: boolean;
}

// This is a copy of useSearchFilterDropdown, to customize it for proposals
export default function useProposalsFilterDropdown({
    filter,
    activeFilters,
    onChangeFilters,
    shouldUseAggregationsFromFilter,
    shouldApplyView = true,
    getAllActionRequests = false,
}: Props) {
    const numActiveFilters = getNumActiveFiltersForFilter(activeFilters, filter);
    const shouldFetchAggregations: boolean = !!filter.field && numActiveFilters > 0 && !shouldUseAggregationsFromFilter;

    const { entityFilters, orFilters, viewUrn } = useGetSearchQueryInputs(
        useMemo(() => [filter.field], [filter.field]),
    );

    const [aggregateAcrossEntities, { data, loading }] = useAggregateActionRequestsLazyQuery({
        fetchPolicy: 'cache-first',
    });

    useEffect(() => {
        // Fetch the aggregates of the current facet only if there are active filters
        // Otherwise, the aggregates are already fetched by the search query
        if (shouldFetchAggregations) {
            aggregateAcrossEntities({
                variables: {
                    input: {
                        orFilters,
                        ...(shouldApplyView ? { viewUrn } : {}),
                        facets: [filter.field],
                        allActionRequests: getAllActionRequests,
                    },
                },
            });
        }
    }, [
        aggregateAcrossEntities,
        entityFilters,
        filter.field,
        orFilters,
        viewUrn,
        shouldFetchAggregations,
        shouldApplyView,
        getAllActionRequests,
    ]);

    const newAggregations = data?.listActionRequests?.facets?.find((f) => f.field === filter.field)?.aggregations || [];
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
        shouldFetchAggregations,
    };
}
