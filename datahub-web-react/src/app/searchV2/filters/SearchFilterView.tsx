import { Pill } from '@components';
import React, { useState } from 'react';
import styled, { CSSProperties } from 'styled-components';

import { SearchFilterBase } from '@app/searchV2/filters/SearchFilterBase';
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
    const [isMenuOpen, setIsMenuOpen] = useState(false);

    const onClear = () => onChangeValues([]);

    return (
        <ValueSelector
            field={filterPredicate?.field}
            values={filterPredicate?.values}
            defaultOptions={filterOptions}
            onChangeValues={onChangeValues}
            manuallyUpdateFilters={manuallyUpdateFilters}
            isOpen={isMenuOpen}
            setIsOpen={setIsMenuOpen}
        >
            <SearchFilterBase
                isActive={!!numActiveFilters}
                isOpen={isMenuOpen}
                style={labelStyle}
                data-testid={`filter-dropdown-${displayName?.replace(/\s/g, '-')}`}
                onClear={onClear}
                showClear={!!numActiveFilters}
            >
                {displayName}{' '}
                {numActiveFilters ? <Pill label={`${numActiveFilters}`} size="sm" variant="filled" /> : null}
            </SearchFilterBase>
        </ValueSelector>
    );
}
