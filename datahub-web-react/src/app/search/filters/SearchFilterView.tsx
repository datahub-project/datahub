import { CaretDownFilled } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '@src/types.generated';
import OptionsDropdownMenu from './OptionsDropdownMenu';
import { DisplayedFilterOption } from './mapFilterOption';
import { SearchFilterLabel } from './styledComponents';

export const IconWrapper = styled.div`
    margin-right: 8px;
    display: flex;
    svg {
        height: 14px;
        width: 14px;
    }
`;

interface Props {
    filterOptions: DisplayedFilterOption[];
    isMenuOpen: boolean;
    numActiveFilters: number;
    filterIcon: JSX.Element | null;
    displayName: string;
    searchQuery: string;
    loading: boolean;
    updateIsMenuOpen: (isOpen: boolean) => void;
    setSearchQuery: (query: string) => void;
    updateFilters: () => void;
    filter?: FacetMetadata;
    manuallyUpdateFilters?: (newValues: FacetFilterInput[]) => void;
}

export default function SearchFilterView({
    filterOptions,
    isMenuOpen,
    numActiveFilters,
    filterIcon,
    displayName,
    searchQuery,
    loading,
    updateIsMenuOpen,
    setSearchQuery,
    updateFilters,
    filter,
    manuallyUpdateFilters,
}: Props) {
    return (
        <Dropdown
            trigger={['click']}
            menu={{ items: filterOptions }}
            open={isMenuOpen}
            onOpenChange={(open) => updateIsMenuOpen(open)}
            dropdownRender={(menu) => (
                <OptionsDropdownMenu
                    menu={menu}
                    updateFilters={updateFilters}
                    searchQuery={searchQuery}
                    updateSearchQuery={setSearchQuery}
                    isLoading={loading}
                    searchPlaceholder={displayName}
                    filter={filter}
                    manuallyUpdateFilters={manuallyUpdateFilters}
                />
            )}
        >
            <SearchFilterLabel
                onClick={() => updateIsMenuOpen(!isMenuOpen)}
                isActive={!!numActiveFilters}
                data-testid={`filter-dropdown-${displayName?.replace(/\s/g, '-')}`}
            >
                {filterIcon && <IconWrapper>{filterIcon}</IconWrapper>}
                {displayName} {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                <CaretDownFilled style={{ fontSize: '12px', height: '12px' }} />
            </SearchFilterLabel>
        </Dropdown>
    );
}
