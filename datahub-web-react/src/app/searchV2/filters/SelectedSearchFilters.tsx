import React from 'react';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { FilterPredicate } from './types';
import { convertToSelectedFilterPredictes } from './utils';
import { convertFrontendToBackendOperatorType } from './operator/operator';
import SearchFiltersBuilder from './SearchFiltersBuilder';
import { UnionType } from '../utils/constants';
import { EXCLUDED_ACTIVE_FILTERS } from './constants';

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
