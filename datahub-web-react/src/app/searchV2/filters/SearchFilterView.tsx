import { FacetFilterInput, AggregationMetadata } from '@src/types.generated';
import { CaretDownFilled } from '@ant-design/icons';
import React from 'react';
import styled, { CSSProperties } from 'styled-components';
import { SearchFilterLabel } from './styledComponents';
import { FilterPredicate, FilterValue } from './types';
import ValueSelector from './value/ValueSelector';

export const IconWrapper = styled.div`
    margin-right: 8px;
    display: flex;
    svg {
        height: 14px;
        width: 14px;
    }
`;

interface Props {
    numActiveFilters: number;
    filterIcon?: JSX.Element;
    displayName: string;
    filterPredicate: FilterPredicate;
    onChangeValues: (newValues: FilterValue[]) => void;
    labelStyle?: CSSProperties;
    filterOptions: AggregationMetadata[];
    manuallyUpdateFilters: (newValues: FacetFilterInput[]) => void;
}

export default function SearchFilterView({
    numActiveFilters,
    filterIcon,
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
                {filterIcon && <IconWrapper>{filterIcon}</IconWrapper>}
                {displayName} {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                <CaretDownFilled style={{ fontSize: '12px', height: '12px' }} />
            </SearchFilterLabel>
        </ValueSelector>
    );
}
