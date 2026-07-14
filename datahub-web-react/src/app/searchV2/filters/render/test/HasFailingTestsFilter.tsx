import React from 'react';

import BooleanMoreFilter from '@app/searchV2/filters/render/shared/BooleanMoreFilter';
import BooleanSearchFilter from '@app/searchV2/filters/render/shared/BooleanSearchFilter';
import { FilterScenarioType } from '@app/searchV2/filters/render/types';

import { FacetFilter, FacetFilterInput, FacetMetadata } from '@types';

interface Props {
    scenario: FilterScenarioType;
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilter[]) => void;
    icon?: React.ReactNode;
}

export function HasFailingTestsFilter({ icon, scenario, filter, activeFilters, onChangeFilters }: Props) {
    const isSelected = activeFilters?.find((f) => f.field === 'hasFailingTests')?.values?.includes('true');

    const toggleFilter = () => {
        let newFilters;
        if (isSelected) {
            newFilters = activeFilters.filter((f) => f.field !== 'hasFailingTests');
        } else {
            newFilters = [...activeFilters, { field: 'hasFailingTests', values: ['true'] }];
        }
        onChangeFilters(newFilters);
    };

    const aggregateCount = filter.aggregations.find((agg) => agg.value === 'true')?.count;

    if (!aggregateCount) {
        return null;
    }

    return (
        <>
            {scenario === FilterScenarioType.SEARCH_V2_PRIMARY && (
                <BooleanSearchFilter
                    icon={icon}
                    title="Metadata Tests"
                    option="Has failing metadata tests"
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_SECONDARY && (
                <BooleanMoreFilter
                    icon={icon}
                    title="Metadata Tests"
                    option="Has failing metadata tests"
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                />
            )}
        </>
    );
}
