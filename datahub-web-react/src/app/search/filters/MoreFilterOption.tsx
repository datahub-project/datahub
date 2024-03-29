import { RightOutlined } from '@ant-design/icons';
import { Dropdown } from 'antd';
import styled from 'styled-components';
import React, { useRef } from 'react';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';
import OptionsDropdownMenu from './OptionsDropdownMenu';
import useSearchFilterDropdown from './useSearchFilterDropdown';
import { IconWrapper } from './SearchFilterView';
import { getFilterDropdownIcon, useElementDimensions } from './utils';
import { MoreFilterOptionLabel } from './styledComponents';

const IconNameWrapper = styled.span`
    display: flex;
    align-items: center;
`;

interface Props {
    filter: FacetMetadata;
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function MoreFilterOption({ filter, activeFilters, onChangeFilters }: Props) {
    const labelRef = useRef<HTMLDivElement>(null);
    const { width, height } = useElementDimensions(labelRef);

    const {
        isMenuOpen,
        updateIsMenuOpen,
        updateFilters,
        filterOptions,
        numActiveFilters,
        areFiltersLoading,
        searchQuery,
        updateSearchQuery,
    } = useSearchFilterDropdown({
        filter,
        activeFilters,
        onChangeFilters,
    });
    const filterIcon = getFilterDropdownIcon(filter.field);

    return (
        <Dropdown
            trigger={['click']}
            menu={{ items: filterOptions }}
            open={isMenuOpen}
            onOpenChange={(open) => updateIsMenuOpen(open)}
            dropdownRender={(menu) => (
                <OptionsDropdownMenu
                    style={{ left: width, position: 'absolute', top: -height }}
                    menu={menu}
                    updateFilters={updateFilters}
                    searchQuery={searchQuery}
                    updateSearchQuery={updateSearchQuery}
                    isLoading={areFiltersLoading}
                    searchPlaceholder={filter.displayName || ''}
                />
            )}
        >
            <MoreFilterOptionLabel
                ref={labelRef}
                onClick={() => updateIsMenuOpen(!isMenuOpen)}
                isActive={!!numActiveFilters}
                isOpen={isMenuOpen}
                data-testid={`more-filter-${capitalizeFirstLetterOnly(filter.displayName)}`}
            >
                <IconNameWrapper>
                    {filterIcon && <IconWrapper>{filterIcon}</IconWrapper>}
                    {capitalizeFirstLetterOnly(filter.displayName)} {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                </IconNameWrapper>
                <RightOutlined style={{ fontSize: '12px', height: '12px' }} />
            </MoreFilterOptionLabel>
        </Dropdown>
    );
}
