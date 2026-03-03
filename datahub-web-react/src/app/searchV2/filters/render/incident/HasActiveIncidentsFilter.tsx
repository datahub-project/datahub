import React from 'react';

import BooleanMoreFilter from '@app/searchV2/filters/render/shared/BooleanMoreFilter';
import BooleanSearchFilter from '@app/searchV2/filters/render/shared/BooleanSearchFilter';
import { BooleanSimpleSearchFilter } from '@app/searchV2/filters/render/shared/BooleanSimpleSearchFilter';
import { FilterScenarioType } from '@app/searchV2/filters/render/types';

import { FacetFilter, FacetFilterInput, FacetMetadata } from '@types';

export interface Props {
    scenario: FilterScenarioType;
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilter[]) => void;
    icon?: React.ReactNode;
}

export function HasActiveIncidentsFilter({ scenario, filter, activeFilters, icon, onChangeFilters }: Props) {
    const isSelected = activeFilters?.find((f) => f.field === 'hasActiveIncidents')?.values?.includes('true');

    const toggleFilter = () => {
        let newFilters;
        if (isSelected) {
            newFilters = activeFilters.filter((f) => f.field !== 'hasActiveIncidents');
        } else {
            newFilters = [...activeFilters, { field: 'hasActiveIncidents', values: ['true'] }];
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
                    title="Incidents"
                    option="Has active incidents"
                    isSelected={isSelected || false}
                    onSelect={toggleFilter}
                    defaultDisplayFilters
                    count={aggregateCount}
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_PRIMARY && (
                <BooleanSearchFilter
                    icon={icon}
                    title="Incidents"
                    option="Has active incidents"
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_SECONDARY && (
                <BooleanMoreFilter
                    icon={icon}
                    title="Incidents"
                    option="Has active incidents"
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                />
            )}
        </>
    );
}
