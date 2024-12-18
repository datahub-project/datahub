import { RightOutlined } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React, { useRef } from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import OptionsDropdownMenu from './OptionsDropdownMenu';
import { IconWrapper } from './SearchFilterView';
import { MoreFilterOptionLabel } from './styledComponents';
import useSearchFilterDropdown from './useSearchFilterDropdown';
import { getFilterDropdownIcon, useElementDimensions, useFilterDisplayName } from './utils';

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
        manuallyUpdateFilters,
    } = useSearchFilterDropdown({
        filter,
        activeFilters,
        onChangeFilters,
    });
    const filterIcon = getFilterDropdownIcon(filter.field);
    const displayName = useFilterDisplayName(filter);

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
                    searchPlaceholder={displayName || ''}
                    filter={filter}
                    manuallyUpdateFilters={manuallyUpdateFilters}
                />
            )}
        >
            <MoreFilterOptionLabel
                ref={labelRef}
                onClick={() => updateIsMenuOpen(!isMenuOpen)}
                isActive={!!numActiveFilters}
                isOpen={isMenuOpen}
                data-testid={`more-filter-${displayName?.replace(/\s/g, '-')}`}
            >
                <IconNameWrapper>
                    {filterIcon && <IconWrapper>{filterIcon}</IconWrapper>}
                    {displayName} {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                </IconNameWrapper>
                <RightOutlined style={{ fontSize: '12px', height: '12px' }} />
            </MoreFilterOptionLabel>
        </Dropdown>
    );
}
