import React from 'react';
import { CSSProperties } from 'styled-components';

import FilterLabel from '@app/sharedV2/filters/FilterLabel';
import { FilterQueryOptions } from '@app/sharedV2/filters/FilterSection';
import useProposalsFilterDropdown from '@app/taskCenterV2/proposalsV2/filters/useProposalsFilterDropdown';
import { FilterPredicate } from '@src/app/searchV2/filters/types';
import useSearchFilterDropdown from '@src/app/searchV2/filters/useSearchFilterDropdown';
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
    queryOptions?: FilterQueryOptions;
}

export default function Filter({
    filter,
    filterPredicates,
    activeFilters,
    onChangeFilters,
    labelStyle,
    customFilterLabels,
    queryOptions = {
        aggregationsEntityTypes: [],
        shouldApplyView: false,
        fetchPolicy: 'cache-first',
    },
}: Props) {
    const { aggregationsEntityTypes, shouldApplyView, fetchPolicy } = queryOptions;

    const isProposalsFilter = queryOptions.aggregationsEntityTypes?.includes(EntityType.ActionRequest);
    const hook = isProposalsFilter ? useProposalsFilterDropdown : useSearchFilterDropdown;

    const { finalAggregations, updateFilters, numActiveFilters } = hook({
        filter,
        activeFilters,
        onChangeFilters,
        aggregationsEntityTypes,
        shouldApplyView,
        fetchPolicy,
        // specific to action requests
        getAllActionRequests: queryOptions?.includeAll,
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
            showDefaultOptions={!isProposalsFilter}
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
