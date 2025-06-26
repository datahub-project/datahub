import React from 'react';

import BooleanMoreFilter from '@app/search/filters/render/shared/BooleanMoreFilter';
import BooleanSearchFilter from '@app/search/filters/render/shared/BooleanSearchFilter';
import { BooleanSimpleSearchFilter } from '@app/search/filters/render/shared/BooleanSimpleSearchFilter';
import { FilterScenarioType } from '@app/search/filters/render/types';

import { FacetFilter, FacetFilterInput, FacetMetadata } from '@types';

export interface Props {
    scenario: FilterScenarioType;
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilter[]) => void;
    icon?: React.ReactNode;
}

export function HasSiblingsFilter({ icon, scenario, filter, activeFilters, onChangeFilters }: Props) {
    const isSelected = activeFilters?.find((f) => f.field === 'hasSiblings')?.values?.includes('true');

    const toggleFilter = () => {
        let newFilters;
        if (isSelected) {
            newFilters = activeFilters.filter((f) => f.field !== 'hasSiblings');
        } else {
            newFilters = [...activeFilters, { field: 'hasSiblings', values: ['true'] }];
        }
        onChangeFilters(newFilters);
    };

    const aggregateCount = filter.aggregations.find((agg) => agg.value === 'true')?.count;

    if (!aggregateCount) {
        return null;
    }

    return (
        <>
            {scenario === FilterScenarioType.SEARCH_V1 && (
                <BooleanSimpleSearchFilter
                    title="Siblings"
                    option="Has siblings"
                    isSelected={isSelected || false}
                    onSelect={toggleFilter}
                    defaultDisplayFilters
                    count={aggregateCount}
                    key="hasSiblings"
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_PRIMARY && (
                <BooleanSearchFilter
                    icon={icon}
                    title="Siblings"
                    option="Has siblings"
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                    key="hasSiblings"
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_SECONDARY && (
                <BooleanMoreFilter
                    icon={icon}
                    title="Siblings"
                    option="Has siblings"
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                    key="hasSiblings"
                />
            )}
        </>
    );
}
