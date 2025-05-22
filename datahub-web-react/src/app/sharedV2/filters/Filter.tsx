import React from 'react';
import { CSSProperties } from 'styled-components';

import FilterLabel from '@app/sharedV2/filters/FilterLabel';
import { FilterPredicate } from '@src/app/searchV2/filters/types';
import useFilterDropdown from '@src/app/searchV2/filters/useSearchFilterDropdown';
import { getFilterDropdownIcon, useFilterDisplayName } from '@src/app/searchV2/filters/utils';
import ValueSelector from '@src/app/searchV2/filters/value/ValueSelector';
import { EntityType, FacetFilterInput, FacetMetadata } from '@src/types.generated';

export type FilterLabels = {
    [key: string]: {
        displayName: string;
        icon?: React.ReactElement;
    };
};

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
    filterPredicates: FilterPredicate[];
    labelStyle?: CSSProperties;
    customFilterLabels?: FilterLabels;
    aggregationsEntityTypes?: Array<EntityType>;
    shouldApplyView?: boolean;
}

export default function Filter({
    filter,
    filterPredicates,
    activeFilters,
    onChangeFilters,
    labelStyle,
    customFilterLabels,
    aggregationsEntityTypes,
    shouldApplyView,
}: Props) {
    const { finalAggregations, updateFilters, numActiveFilters } = useFilterDropdown({
        filter,
        activeFilters,
        onChangeFilters,
        aggregationsEntityTypes,
        shouldApplyView,
    });

    const currentFilterPredicate = filterPredicates?.find((obj) => obj.field.field === filter.field) as FilterPredicate;

    // TODO: Have config for the value labels as well
    const labelConfig = customFilterLabels?.[filter.field];

    const filterIcon = labelConfig ? labelConfig.icon : getFilterDropdownIcon(filter.field);
    const entityFilterName = useFilterDisplayName(filter, currentFilterPredicate?.field?.displayName);
    const displayName = labelConfig ? labelConfig.displayName : entityFilterName;

    return (
        <ValueSelector
            field={currentFilterPredicate?.field}
            values={currentFilterPredicate?.values}
            defaultOptions={finalAggregations}
            onChangeValues={updateFilters}
            aggregationsEntityTypes={aggregationsEntityTypes}
        >
            <FilterLabel
                numActiveFilters={numActiveFilters}
                labelStyle={labelStyle}
                displayName={displayName}
                filterIcon={filterIcon}
                onClear={() => updateFilters([])}
            />
        </ValueSelector>
    );
}
