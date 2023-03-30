import React, { useState } from 'react';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import FilterOption from './FilterOption';
import { getNewFilters, getNumActiveFiltersForFilter } from './utils';

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function useSearchFilterDropdown({ filter, activeFilters, onChangeFilters }: Props) {
    const initialFilters = activeFilters.find((f) => f.field === filter.field)?.values;
    const [selectedFilterValues, setSelectedFilterValues] = useState<string[]>(initialFilters || []);
    const [isMenuOpen, setIsMenuOpen] = useState(false);

    function updateIsMenuOpen(isOpen: boolean) {
        setIsMenuOpen(isOpen);
        // set filters to default every time you open or close the menu without saving
        setSelectedFilterValues(initialFilters || []);
    }

    function updateFilters() {
        onChangeFilters(getNewFilters(filter.field, activeFilters, selectedFilterValues));
        setIsMenuOpen(false);
    }

    const filterOptions = filter.aggregations.map((agg) => {
        return {
            key: agg.value,
            label: (
                <FilterOption
                    filterField={filter.field}
                    aggregation={agg}
                    selectedFilterValues={selectedFilterValues}
                    setSelectedFilterValues={setSelectedFilterValues}
                />
            ),
            style: { padding: 0 },
        };
    });

    const numActiveFilters = getNumActiveFiltersForFilter(activeFilters, filter);

    return { isMenuOpen, updateIsMenuOpen, updateFilters, filterOptions, numActiveFilters };
}
