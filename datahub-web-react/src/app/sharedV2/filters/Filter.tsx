import { Button } from 'antd';
import { CaretDown } from 'phosphor-react';
import React from 'react';
import styled, { CSSProperties } from 'styled-components';

import { Pill } from '@src/alchemy-components';
import { IconWrapper } from '@src/app/searchV2/filters/SearchFilterView';
import { FilterPredicate } from '@src/app/searchV2/filters/types';
import useFilterDropdown from '@src/app/searchV2/filters/useSearchFilterDropdown';
import { getFilterDropdownIcon, useFilterDisplayName } from '@src/app/searchV2/filters/utils';
import ValueSelector from '@src/app/searchV2/filters/value/ValueSelector';
import { formatNumber } from '@src/app/shared/formatNumber';
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
}

const FilterLabel = styled(Button)<{ $isActive: boolean }>`
    display: flex;
    align-items: center;
    padding: 8px;
    height: 36px;
    border-radius: 8px;
    background-color: white;
    color: #6b7280;
    font-size: 14px;
    font-weight: 500;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    cursor: pointer;
    transition: all 0.2s ease;
    gap: 8px;
    margin: 0 4px;
    border: 1px solid #ebecf0;

    &:hover,
    &:focus {
        color: inherit;
        border-color: #ebecf0;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.3);
    }

    ${(props) =>
        props.$isActive &&
        `
    color: #374066;
    font-weight: 600;
  `}
`;

export default function Filter({
    filter,
    filterPredicates,
    activeFilters,
    onChangeFilters,
    labelStyle,
    customFilterLabels,
    aggregationsEntityTypes,
}: Props) {
    const { finalAggregations, updateFilters, numActiveFilters } = useFilterDropdown({
        filter,
        activeFilters,
        onChangeFilters,
        aggregationsEntityTypes,
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
                $isActive={!!numActiveFilters}
                style={labelStyle}
                data-testid={`filter-dropdown-${displayName?.replace(/\s/g, '-')}`}
            >
                {filterIcon && <IconWrapper>{filterIcon}</IconWrapper>}
                {displayName} {!!numActiveFilters && <Pill size="xs" label={formatNumber(numActiveFilters)} />}
                <CaretDown style={{ fontSize: '14px', height: '14px' }} />
            </FilterLabel>
        </ValueSelector>
    );
}
