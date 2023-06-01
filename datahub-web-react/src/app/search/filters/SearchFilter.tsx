import React from 'react';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import useSearchFilterDropdown from './useSearchFilterDropdown';
import { getFilterDropdownIcon } from './utils';
import SearchFilterView from './SearchFilterView';
import { ENTITY_FILTER_NAME } from '../utils/constants';
import EntityTypeFilter from './EntityTypeFilter/EntityTypeFilter';

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function SearchFilter({ filter, activeFilters, onChangeFilters }: Props) {
    const {
        isMenuOpen,
        updateIsMenuOpen,
        updateFilters,
        filterOptions,
        numActiveFilters,
        areFiltersLoading,
        searchQuery,
        updateSearchQuery,
    } = useSearchFilterDropdown({
        filter,
        activeFilters,
        onChangeFilters,
    });
    const filterIcon = getFilterDropdownIcon(filter.field);

    if (filter.field === ENTITY_FILTER_NAME) {
        return <EntityTypeFilter filter={filter} activeFilters={activeFilters} onChangeFilters={onChangeFilters} />;
    }

    return (
        <SearchFilterView
            filterOptions={filterOptions}
            isMenuOpen={isMenuOpen}
            numActiveFilters={numActiveFilters}
            filterIcon={filterIcon}
            displayName={filter.displayName || ''}
            searchQuery={searchQuery}
            loading={areFiltersLoading}
            updateIsMenuOpen={updateIsMenuOpen}
            setSearchQuery={updateSearchQuery}
            updateFilters={updateFilters}
        />
    );
}
