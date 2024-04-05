import { CaretDownFilled } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
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
    filterIcon: JSX.Element | null;
    displayName: string;
    filterPredicate: FilterPredicate,
    onChangeValues: (newValues: FilterValue[]) => void;
}

export default function SearchFilterView({
    numActiveFilters,
    filterIcon,
    displayName,
    filterPredicate,
    onChangeValues,
}: Props) {
    return <ValueSelector
        field={filterPredicate?.field}
        values={filterPredicate?.values}
        defaultOptions={filterPredicate?.defaultValueOptions}
        onChangeValues={onChangeValues}
    >
        <SearchFilterLabel
                $isActive={!!numActiveFilters}
                data-testid={`filter-dropdown-${capitalizeFirstLetterOnly(displayName)}`}
            >
                {filterIcon && <IconWrapper>{filterIcon}</IconWrapper>}
                {capitalizeFirstLetterOnly(displayName)} {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                <CaretDownFilled style={{ fontSize: '12px', height: '12px' }} />
            </SearchFilterLabel>
    </ValueSelector> 

}
