import React from 'react';
import { useTranslation } from 'react-i18next';

import BooleanMoreFilter from '@app/search/filters/render/shared/BooleanMoreFilter';
import BooleanSearchFilter from '@app/search/filters/render/shared/BooleanSearchFilter';
import { BooleanSimpleSearchFilter } from '@app/search/filters/render/shared/BooleanSimpleSearchFilter';
import { FilterScenarioType } from '@app/search/filters/render/types';

import { FacetFilter, FacetFilterInput, FacetMetadata } from '@types';

interface Props {
    scenario: FilterScenarioType;
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilter[]) => void;
    icon?: React.ReactNode;
}

export function HasActiveIncidentsFilter({ scenario, filter, activeFilters, icon, onChangeFilters }: Props) {
    const { t } = useTranslation('search');
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
                    title={t('filters.incidents.title')}
                    option={t('filters.incidents.hasActive')}
                    isSelected={isSelected || false}
                    onSelect={toggleFilter}
                    defaultDisplayFilters
                    count={aggregateCount}
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_PRIMARY && (
                <BooleanSearchFilter
                    icon={icon}
                    title={t('filters.incidents.title')}
                    option={t('filters.incidents.hasActive')}
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                />
            )}
            {scenario === FilterScenarioType.SEARCH_V2_SECONDARY && (
                <BooleanMoreFilter
                    icon={icon}
                    title={t('filters.incidents.title')}
                    option={t('filters.incidents.hasActive')}
                    initialSelected={isSelected || false}
                    onUpdate={toggleFilter}
                    count={aggregateCount}
                />
            )}
        </>
    );
}
