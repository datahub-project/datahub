import React from 'react';
import { CSSProperties } from 'styled-components';

import SearchFilterView from '@app/searchV2/filters/SearchFilterView';
import { FilterPredicate } from '@app/searchV2/filters/types';
import useSearchFilterDropdown from '@app/searchV2/filters/useSearchFilterDropdown';
import { getFilterDropdownIcon, useFilterDisplayName } from '@app/searchV2/filters/utils';

import { FacetFilterInput, FacetMetadata } from '@types';

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    filterPredicates: FilterPredicate[];
    labelStyle?: CSSProperties;
    shouldUseAggregationsFromFilter?: boolean;
}

export default function SearchFilter({
    filter,
    filterPredicates,
    activeFilters,
    onChangeFilters,
    labelStyle,
    shouldUseAggregationsFromFilter,
}: Props) {
    const { finalAggregations, updateFilters, numActiveFilters, manuallyUpdateFilters } = useSearchFilterDropdown({
        filter,
        activeFilters,
        onChangeFilters,
        shouldUseAggregationsFromFilter,
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
