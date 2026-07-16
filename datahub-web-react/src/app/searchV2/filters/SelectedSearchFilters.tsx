import React from 'react';

import SearchFiltersBuilder from '@app/searchV2/filters/SearchFiltersBuilder';
import { EXCLUDED_ACTIVE_FILTERS } from '@app/searchV2/filters/constants';
import { convertFrontendToBackendOperatorType } from '@app/searchV2/filters/operator/operator';
import { FilterPredicate } from '@app/searchV2/filters/types';
import { convertToSelectedFilterPredictes } from '@app/searchV2/filters/utils';
import { UnionType } from '@app/searchV2/utils/constants';

import { FacetFilterInput, FacetMetadata } from '@types';

interface Props {
    availableFilters: FacetMetadata[];
    selectedFilters: FacetFilterInput[];
    unionType: UnionType;
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    onChangeUnionType?: (unionType: UnionType) => void;
    onClearFilters: () => void;
    disabled?: boolean;
    showUnionType?: boolean;
    showAddFilter?: boolean;
    showClearAll?: boolean;
    isCompact?: boolean;
    isOperatorDisabled?: boolean;
}

export default function SelectedSearchFilters({
    availableFilters,
    selectedFilters,
    unionType,
    onChangeFilters,
    onChangeUnionType,
    onClearFilters,
    disabled = false,
    showUnionType,
    showAddFilter,
    showClearAll,
    isCompact,
    isOperatorDisabled,
}: Props) {
    // Create the final filter predicates required to render a selected filter option.
    const finalSelectedFilters = selectedFilters.filter((filter) => !EXCLUDED_ACTIVE_FILTERS.includes(filter.field));

    const filterPredicates: FilterPredicate[] = convertToSelectedFilterPredictes(
        finalSelectedFilters,
        availableFilters || [],
    );

    const updateFilters = (newFilters: FilterPredicate[]) => {
        const newSelectedFilters = newFilters.map((filter) => {
            const condition = convertFrontendToBackendOperatorType(filter.operator);
            return {
                field: filter.field.field,
                values: filter.values.map((value) => value.value),
                condition: condition.operator,
                negated: condition.negated,
            };
        });
        onChangeFilters(newSelectedFilters);
    };

    return (
        <SearchFiltersBuilder
            filters={filterPredicates}
            onChangeFilters={updateFilters}
            onClearFilters={onClearFilters}
            unionType={unionType}
            onChangeUnionType={onChangeUnionType}
            showUnionType={showUnionType}
            disabled={disabled}
            showAddFilter={showAddFilter}
            showClearAll={showClearAll}
            isCompact={isCompact}
            isOperatorDisabled={isOperatorDisabled}
            includeCount={false}
        />
    );
}
