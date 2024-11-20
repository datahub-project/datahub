import { useEffect, useMemo } from 'react';
import { useAggregateAcrossEntitiesLazyQuery } from '@src/graphql/search.generated';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { filterEmptyAggregations, getNewFilters, getNumActiveFiltersForFilter } from './utils';
import useGetSearchQueryInputs from '../useGetSearchQueryInputs';
import { ENTITY_FILTER_NAME } from '../utils/constants';

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function useSearchFilterDropdown({ filter, activeFilters, onChangeFilters }: Props) {
    const numActiveFilters = getNumActiveFiltersForFilter(activeFilters, filter);
    const shouldFetchAggregations: boolean = !!filter.field && numActiveFilters > 0;

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
                        types: filter.field === ENTITY_FILTER_NAME ? null : entityFilters,
                        query,
                        orFilters,
                        viewUrn,
                        facets: [filter.field],
                    },
                },
            });
        }
    }, [aggregateAcrossEntities, entityFilters, filter.field, orFilters, query, viewUrn, shouldFetchAggregations]);

    const aggregations = shouldFetchAggregations
        ? data?.aggregateAcrossEntities?.facets?.[0]?.aggregations
        : filter.aggregations;

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
