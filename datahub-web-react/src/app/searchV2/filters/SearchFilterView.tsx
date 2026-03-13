import { CaretDown } from '@phosphor-icons/react';
import React from 'react';
import styled, { CSSProperties } from 'styled-components';

import { SearchFilterLabel } from '@app/searchV2/filters/styledComponents';
import { FilterPredicate, FilterValue } from '@app/searchV2/filters/types';
import ValueSelector from '@app/searchV2/filters/value/ValueSelector';
import { AggregationMetadata, FacetFilterInput } from '@src/types.generated';

export const IconWrapper = styled.div`
    margin-right: 8px;
    display: flex;
    svg {
        height: 14px;
        width: 14px;
    }
`;

const CaretIcon = styled(CaretDown)<{ $isOpen?: boolean }>`
    transition: transform 0.2s ease;
    ${(props) => props.$isOpen && 'transform: rotate(180deg);'}
`;

interface Props {
    numActiveFilters: number;
    displayName: string;
    filterPredicate: FilterPredicate;
    onChangeValues: (newValues: FilterValue[]) => void;
    labelStyle?: CSSProperties;
    filterOptions: AggregationMetadata[];
    manuallyUpdateFilters: (newValues: FacetFilterInput[]) => void;
}

export default function SearchFilterView({
    numActiveFilters,
    displayName,
    filterPredicate,
    onChangeValues,
    labelStyle,
    filterOptions,
    manuallyUpdateFilters,
}: Props) {
    return (
        <ValueSelector
            field={filterPredicate?.field}
            values={filterPredicate?.values}
            defaultOptions={filterOptions}
            onChangeValues={onChangeValues}
            manuallyUpdateFilters={manuallyUpdateFilters}
        >
            <SearchFilterLabel
                $isActive={!!numActiveFilters}
                style={labelStyle}
                data-testid={`filter-dropdown-${displayName?.replace(/\s/g, '-')}`}
            >
                {displayName} {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                <CaretIcon size={12} />
            </SearchFilterLabel>
        </ValueSelector>
    );
}
