import { Icon, Pill } from '@components';
import { Button } from 'antd';
import { CaretDown } from 'phosphor-react';
import React from 'react';
import styled, { CSSProperties } from 'styled-components';

import { IconWrapper } from '@app/searchV2/filters/SearchFilterView';
import { formatNumber } from '@app/shared/formatNumber';

type Props = {
    numActiveFilters?: number;
    labelStyle?: CSSProperties;
    displayName: string;
    filterIcon?: JSX.Element;
    onClear?: () => void;
};

export const FilterLabelContainer = styled(Button)<{ $isActive: boolean }>`
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

const FilterLabel = ({ numActiveFilters, labelStyle, displayName, filterIcon, onClear, ...otherProps }: Props) => {
    return (
        <FilterLabelContainer
            onClick={() => console.log('captured on filter label')}
            $isActive={!!numActiveFilters}
            style={labelStyle}
            data-testid={`filter-dropdown-${displayName?.replace(/\s/g, '-')}`}
            {...otherProps}
        >
            {filterIcon && <IconWrapper>{filterIcon}</IconWrapper>}
            {displayName} {!!numActiveFilters && <Pill size="xs" label={formatNumber(numActiveFilters)} />}
            {!!numActiveFilters && onClear && (
                <Icon
                    source="phosphor"
                    icon="X"
                    size="sm"
                    onClick={(e) => {
                        e.stopPropagation();
                        onClear?.();
                    }}
                />
            )}
            <CaretDown style={{ fontSize: '14px', height: '14px' }} />
        </FilterLabelContainer>
    );
};

export default FilterLabel;
