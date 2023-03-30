import { RightOutlined } from '@ant-design/icons';
import { Dropdown } from 'antd';
import styled from 'styled-components';
import React from 'react';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import OptionsDropdownMenu from './OptionsDropdownMenu';
import { ANTD_GRAY } from '../../entity/shared/constants';
import useSearchFilterDropdown from './useSearchFilterDropdown';

const OptionWrapper = styled.div<{ isActive: boolean }>`
    padding: 5px 12px;
    font-size: 14px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    cursor: pointer;

    &:hover {
        background-color: ${ANTD_GRAY[3]};
    }

    ${(props) => props.isActive && `color: ${props.theme.styles['primary-color']}`}
`;

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function MoreFilterOption({ filter, activeFilters, onChangeFilters }: Props) {
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
            dropdownRender={(menu) => <OptionsDropdownMenu menu={menu} updateFilters={updateFilters} alignRight />}
        >
            <OptionWrapper onClick={() => handleMenuOpen(!isMenuOpen)} isActive={!!numActiveFilters}>
                {capitalizeFirstLetterOnly(filter.displayName)} {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                <RightOutlined style={{ fontSize: '12px', height: '12px' }} />
            </OptionWrapper>
        </Dropdown>
    );
}
