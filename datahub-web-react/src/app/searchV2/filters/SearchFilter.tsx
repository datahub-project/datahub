import React from 'react';
import { CSSProperties } from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import useSearchFilterDropdown from './useSearchFilterDropdown';
import { getFilterDropdownIcon, useFilterDisplayName } from './utils';
import SearchFilterView from './SearchFilterView';
import { FilterPredicate } from './types';

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    filterPredicates: FilterPredicate[];
    labelStyle?: CSSProperties;
}

export default function SearchFilter({ filter, filterPredicates, activeFilters, onChangeFilters, labelStyle }: Props) {
    const { finalAggregations, updateFilters, numActiveFilters, manuallyUpdateFilters } = useSearchFilterDropdown({
        filter,
        activeFilters,
        onChangeFilters,
    });
    const filterIcon = getFilterDropdownIcon(filter.field);

    const currentFilterPredicate = filterPredicates?.find((obj) =>
        obj.field.field.includes(filter.field),
    ) as FilterPredicate;

    const displayName = useFilterDisplayName(filter, currentFilterPredicate?.field?.displayName);

    return (
        <SearchFilterView
            filterPredicate={currentFilterPredicate}
            numActiveFilters={numActiveFilters}
            filterOptions={finalAggregations}
            filterIcon={filterIcon}
            displayName={displayName}
            onChangeValues={updateFilters}
            labelStyle={labelStyle}
            manuallyUpdateFilters={manuallyUpdateFilters}
        />
    );
}
