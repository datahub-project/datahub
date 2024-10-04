import React from 'react';
import { CSSProperties } from 'styled-components';
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
    labelStyle?: CSSProperties;
}

export default function SearchFilter({ filter, filterPredicates, activeFilters, onChangeFilters, labelStyle }: Props) {
    const { updateFilters, numActiveFilters } = useSearchFilterDropdown({
        filter,
        activeFilters,
        onChangeFilters,
    });
    const filterIcon = getFilterDropdownIcon(filter.field);

    const currentFilterPredicate = filterPredicates?.find((obj) =>
        obj.field.field.includes(filter.field),
    ) as FilterPredicate;

    return (
        <SearchFilterView
            filterPredicate={currentFilterPredicate}
            numActiveFilters={numActiveFilters}
            filterIcon={filterIcon}
            displayName={currentFilterPredicate?.field?.displayName || filter.displayName || filter.field}
            onChangeValues={updateFilters}
            labelStyle={labelStyle}
        />
    );
}
