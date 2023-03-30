import { CaretDownFilled } from '@ant-design/icons';
import { Button, Dropdown } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import OptionsDropdownMenu from './OptionsDropdownMenu';
import useSearchFilterDropdown from './useSearchFilterDropdown';

export const DropdownLabel = styled(Button)<{ isActive: boolean }>`
    font-size: 14px;
    font-weight: 700;
    margin-right: 12px;
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 8px;
    display: flex;
    align-items: center;
    box-shadow: none;

    ${(props) =>
        props.isActive &&
        `
        background-color: ${props.theme.styles['primary-color']};
        border: 1px solid ${props.theme.styles['primary-color']};
        color: white;
    `}
`;

export const DropdownMenu = styled.div<{ padding?: string }>`
    background-color: white;
    border-radius: 5px;
    box-shadow: 0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 6px 16px 0 rgba(0, 0, 0, 0.08), 0 9px 28px 8px rgba(0, 0, 0, 0.05);
    overflow: hidden;
    min-width: 200px;

    ${(props) => props.padding !== undefined && `padding: ${props.padding};`}
`;

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function SearchFilter({ filter, activeFilters, onChangeFilters }: Props) {
    const { isMenuOpen, handleMenuOpen, updateFilters, filterOptions, numActiveFilters } = useSearchFilterDropdown({
        filter,
        activeFilters,
        onChangeFilters,
    });

    return (
        <Dropdown
            trigger={['click']}
            menu={{ items: filterOptions }}
            open={isMenuOpen}
            onOpenChange={(open) => handleMenuOpen(open)}
            dropdownRender={(menu) => <OptionsDropdownMenu menu={menu} updateFilters={updateFilters} />}
        >
            <DropdownLabel onClick={() => handleMenuOpen(!isMenuOpen)} isActive={!!numActiveFilters}>
                {capitalizeFirstLetterOnly(filter.displayName)} {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                <CaretDownFilled style={{ fontSize: '12px', height: '12px' }} />
            </DropdownLabel>
        </Dropdown>
    );
}
