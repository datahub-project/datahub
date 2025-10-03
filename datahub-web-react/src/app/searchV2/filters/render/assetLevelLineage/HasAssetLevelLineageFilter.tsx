import React from 'react';

import BooleanMoreFilter from '@app/searchV2/filters/render/shared/BooleanMoreFilter';
import BooleanSearchFilter from '@app/searchV2/filters/render/shared/BooleanSearchFilter';
import { BooleanSimpleSearchFilter } from '@app/searchV2/filters/render/shared/BooleanSimpleSearchFilter';
import { FilterScenarioType } from '@app/searchV2/filters/render/types';
import { FIELD_TO_LABEL } from '@app/searchV2/utils/constants';

import { FacetFilter, FacetFilterInput, FacetMetadata } from '@types';

export interface Props {
    scenario: FilterScenarioType;
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilter[]) => void;
    icon?: React.ReactNode;
}

export function HasAssetLevelLineageFilter({ scenario, filter, activeFilters, icon, onChangeFilters }: Props) {
    const isSelected = activeFilters?.find((f) => f.field === filter.field)?.values?.includes('true');

    const toggleFilter = () => {
        let newFilters;
        if (isSelected) {
            newFilters = activeFilters.filter((f) => f.field !== filter.field);
        } else {
            newFilters = [...activeFilters, { field: filter.field, values: ['true'] }];
        }
        onChangeFilters(newFilters);
    };

    const aggregateCount = filter.aggregations.find((agg) => agg.value === 'true')?.count;

    if (!aggregateCount) {
        return null;
    }
    const title = 'Asset-Level Lineage';
    const option = FIELD_TO_LABEL[filter.field];

    return (
        <>
            {scenario === FilterScenarioType.SEARCH_V1 && (
                <BooleanSimpleSearchFilter
                    title={title}
                    option={option}
                    isSelected={isSelected || false}
                    onSelect={toggleFilter}
                    defaultDisplayFilters
                    count={aggregateCount}
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_PRIMARY && (
                <BooleanSearchFilter
                    icon={icon}
                    title={title}
                    option={option}
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_SECONDARY && (
                <BooleanMoreFilter
                    icon={icon}
                    title={title}
                    option={option}
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                />
            )}
        </>
    );
}
