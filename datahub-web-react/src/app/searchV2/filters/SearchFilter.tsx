import React from 'react';
import { CSSProperties } from 'styled-components';

import EntityFilterSelect from '@app/searchV2/filters/EntityFilterSelect';
import EntityTypeFilterSelect from '@app/searchV2/filters/EntityTypeFilterSelect';
import SearchFilterView from '@app/searchV2/filters/SearchFilterView';
import { FieldType, FilterPredicate } from '@app/searchV2/filters/types';
import useSearchFilterDropdown from '@app/searchV2/filters/useSearchFilterDropdown';
import { useFilterDisplayName } from '@app/searchV2/filters/utils';

import { EntityType, FacetFilterInput, FacetMetadata } from '@types';

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

    const currentFilterPredicate = filterPredicates?.find((obj) =>
        obj.field.field.includes(filter.field),
    ) as FilterPredicate;

    const displayName = useFilterDisplayName(filter, currentFilterPredicate?.field?.displayName);

    if (currentFilterPredicate?.field.type === FieldType.ENTITY) {
        return (
            <EntityFilterSelect
                field={currentFilterPredicate?.field}
                values={currentFilterPredicate?.values || []}
                defaultOptions={finalAggregations}
                onChangeValues={updateFilters}
                includeCount
                displayName={displayName}
            />
        );
    }

    if (currentFilterPredicate?.field.type === FieldType.NESTED_ENTITY_TYPE) {
        return (
            <EntityTypeFilterSelect
                field={currentFilterPredicate?.field}
                values={currentFilterPredicate?.values || []}
                defaultOptions={finalAggregations}
                onChangeValues={updateFilters}
                includeCount
                aggregationsEntityTypes={finalAggregations
                    .map((agg) => agg.entity?.type)
                    .filter((entityType): entityType is EntityType => !!entityType)}
            />
        );
    }

    return (
        <SearchFilterView
            filterPredicate={currentFilterPredicate}
            numActiveFilters={numActiveFilters}
            filterOptions={finalAggregations}
            displayName={displayName}
            onChangeValues={updateFilters}
            labelStyle={labelStyle}
            manuallyUpdateFilters={manuallyUpdateFilters}
        />
    );
}
