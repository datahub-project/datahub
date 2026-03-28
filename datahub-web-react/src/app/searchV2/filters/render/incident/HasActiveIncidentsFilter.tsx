import React from 'react';

import FilterOption from '@app/searchV2/filters/FilterOption';
import BooleanMoreFilter from '@app/searchV2/filters/render/shared/BooleanMoreFilter';
import BooleanSearchFilter from '@app/searchV2/filters/render/shared/BooleanSearchFilter';
import { BooleanSimpleSearchFilter } from '@app/searchV2/filters/render/shared/BooleanSimpleSearchFilter';
import { FilterScenarioType } from '@app/searchV2/filters/render/types';

import { FacetFilter, FacetFilterInput, FacetMetadata } from '@types';

const TITLE = 'Incidents';
const OPTION = 'Has active incidents';
const FIELD = 'hasActiveIncidents';

interface Props {
    scenario: FilterScenarioType;
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilter[]) => void;
}

export function HasActiveIncidentsFilter({ scenario, filter, activeFilters, onChangeFilters }: Props) {
    const isSelected = activeFilters?.find((f) => f.field === FIELD)?.values?.includes('true');

    const toggleFilter = () => {
        let newFilters;
        if (isSelected) {
            newFilters = activeFilters.filter((f) => f.field !== FIELD);
        } else {
            newFilters = [...activeFilters, { field: FIELD, values: ['true'] }];
        }
        onChangeFilters(newFilters);
    };

    const aggregateCount = filter.aggregations.find((agg) => agg.value === 'true')?.count;

    if (!aggregateCount) {
        return null;
    }

    const filterOption = { field: FIELD, value: OPTION, count: aggregateCount };

    return (
        <>
            {scenario === FilterScenarioType.SEARCH_V1 && (
                <BooleanSimpleSearchFilter
                    title={TITLE}
                    option={OPTION}
                    isSelected={isSelected || false}
                    onSelect={toggleFilter}
                    defaultDisplayFilters
                    count={aggregateCount}
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_PRIMARY && (
                <BooleanSearchFilter
                    title={TITLE}
                    option={OPTION}
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_SECONDARY && (
                <BooleanMoreFilter
                    title={TITLE}
                    option={OPTION}
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_DROPDOWN_ONLY && (
                <FilterOption
                    filterOption={filterOption}
                    selectedFilterOptions={isSelected ? [filterOption] : []}
                    setSelectedFilterOptions={() => toggleFilter()}
                />
            )}
        </>
    );
}
