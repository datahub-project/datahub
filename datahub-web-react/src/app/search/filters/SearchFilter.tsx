/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import EntityTypeFilter from '@app/search/filters/EntityTypeFilter/EntityTypeFilter';
import SearchFilterView from '@app/search/filters/SearchFilterView';
import useSearchFilterDropdown from '@app/search/filters/useSearchFilterDropdown';
import { getFilterDropdownIcon, useFilterDisplayName } from '@app/search/filters/utils';
import { ENTITY_FILTER_NAME } from '@app/search/utils/constants';

import { FacetFilterInput, FacetMetadata } from '@types';

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
        manuallyUpdateFilters,
    } = useSearchFilterDropdown({
        filter,
        activeFilters,
        onChangeFilters,
    });
    const filterIcon = getFilterDropdownIcon(filter.field);
    const displayName = useFilterDisplayName(filter);

    if (filter.field === ENTITY_FILTER_NAME) {
        return <EntityTypeFilter filter={filter} activeFilters={activeFilters} onChangeFilters={onChangeFilters} />;
    }

    return (
        <SearchFilterView
            filterOptions={filterOptions}
            isMenuOpen={isMenuOpen}
            numActiveFilters={numActiveFilters}
            filterIcon={filterIcon}
            displayName={displayName || ''}
            searchQuery={searchQuery}
            loading={areFiltersLoading}
            updateIsMenuOpen={updateIsMenuOpen}
            setSearchQuery={updateSearchQuery}
            updateFilters={updateFilters}
            filter={filter}
            manuallyUpdateFilters={manuallyUpdateFilters}
        />
    );
}
