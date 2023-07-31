import React from 'react';
import { FilterScenarioType } from '../types';
import { BooleanSimpleSearchFilter } from '../shared/BooleanSimpleSearchFilter';
import BooleanMoreFilter from '../shared/BooleanMoreFilter';
import { FacetFilterInput, FacetMetadata, FacetFilter } from '../../../../../types.generated';
import BooleanSearchFilter from '../shared/BooleanSearchFilter';

export interface Props {
    scenario: FilterScenarioType;
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilter[]) => void;
    icon?: React.ReactNode;
}

export function HasFailingAssertionsFilter({ icon, scenario, filter, activeFilters, onChangeFilters }: Props) {
    const isSelected = activeFilters?.find((f) => f.field === 'hasFailingAssertions')?.values?.includes('true');

    const toggleFilter = () => {
        let newFilters;
        if (isSelected) {
            newFilters = activeFilters.filter((f) => f.field !== 'hasFailingAssertions');
        } else {
            newFilters = [...activeFilters, { field: 'hasFailingAssertions', values: ['true'] }];
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
                    title="Assertions"
                    option="Has failing assertions"
                    isSelected={isSelected || false}
                    onSelect={toggleFilter}
                    defaultDisplayFilters
                    count={aggregateCount}
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_PRIMARY && (
                <BooleanSearchFilter
                    icon={icon}
                    title="Assertions"
                    option="Has failing assertions"
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_SECONDARY && (
                <BooleanMoreFilter
                    icon={icon}
                    title="Assertions"
                    option="Has failing assertions"
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                />
            )}
        </>
    );
}
