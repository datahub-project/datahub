import React from 'react';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import useSearchFilterDropdown from './useSearchFilterDropdown';
import { getFilterDropdownIcon } from './utils';
import SearchFilterView from './SearchFilterView';
import { FilterPredicate } from './types';

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    filterPredicates: FilterPredicate[];
}

export default function SearchFilter({ filter, filterPredicates, activeFilters, onChangeFilters }: Props) {
    const {
        updateFilters,
        numActiveFilters,
    } = useSearchFilterDropdown({
        filter,
        activeFilters,
        onChangeFilters,
    });
    const filterIcon = getFilterDropdownIcon(filter.field);

    const currentFilterPredicate = filterPredicates?.find(obj => obj.field.field.includes(filter.field)) as FilterPredicate
    return (
        <SearchFilterView
            filterPredicate={currentFilterPredicate}
            numActiveFilters={numActiveFilters}
            filterIcon={filterIcon}
            displayName={filter.displayName || ''}
            onChangeValues={(newValues) => updateFilters(newValues)}
        />
    );
}
