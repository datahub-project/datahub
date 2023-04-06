import React, { useState } from 'react';
import { useAggregateAcrossEntitiesLazyQuery } from '../../../graphql/search.generated';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import useGetSearchQueryInputs from '../useGetSearchQueryInputs';
import { ENTITY_FILTER_NAME } from '../utils/constants';
import FilterOption from './FilterOption';
import { FilterFields } from './types';
import { combineAggregations, filterEmptyAggregations, getNewFilters, getNumActiveFiltersForFilter } from './utils';

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function useSearchFilterDropdown({ filter, activeFilters, onChangeFilters }: Props) {
    const initialFilters = activeFilters.find((f) => f.field === filter.field)?.values;
    const [selectedFilterValues, setSelectedFilterValues] = useState<string[]>(initialFilters || []);
    const [isMenuOpen, setIsMenuOpen] = useState(false);
    const { entityFilters, query, orFilters, viewUrn } = useGetSearchQueryInputs(filter.field);
    const [aggregateAcrossEntities, { data, loading }] = useAggregateAcrossEntitiesLazyQuery();

    const numActiveFilters = getNumActiveFiltersForFilter(activeFilters, filter);

    function updateIsMenuOpen(isOpen: boolean) {
        setIsMenuOpen(isOpen);
        // set filters to default every time you open or close the menu without saving
        setSelectedFilterValues(initialFilters || []);

        if (isOpen && numActiveFilters > 0) {
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
    }

    function updateFilters() {
        onChangeFilters(getNewFilters(filter.field, activeFilters, selectedFilterValues));
        setIsMenuOpen(false);
    }

    // combine existing aggs from search response with new aggregateAcrossEntities response
    const combinedAggregations = combineAggregations(
        filter.field,
        filter.aggregations,
        data?.aggregateAcrossEntities?.facets,
    );

    // filter out any aggregations with a count of 0 unless it's already selected and in activeFilters
    const finalAggregations = filterEmptyAggregations(combinedAggregations, activeFilters);

    const filterOptions = finalAggregations.map((agg) => {
        const filterFields: FilterFields = { field: filter.field, ...agg };
        return {
            key: agg.value,
            label: (
                <FilterOption
                    filterFields={filterFields}
                    selectedFilterValues={selectedFilterValues}
                    setSelectedFilterValues={setSelectedFilterValues}
                />
            ),
            style: { padding: 0 },
        };
    });

    return { isMenuOpen, updateIsMenuOpen, updateFilters, filterOptions, numActiveFilters, areFiltersLoading: loading };
}
