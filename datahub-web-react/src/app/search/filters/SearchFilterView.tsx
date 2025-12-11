/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CaretDownFilled } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React from 'react';
import styled from 'styled-components';

import OptionsDropdownMenu from '@app/search/filters/OptionsDropdownMenu';
import { DisplayedFilterOption } from '@app/search/filters/mapFilterOption';
import { SearchFilterLabel } from '@app/search/filters/styledComponents';
import { FacetFilterInput, FacetMetadata } from '@src/types.generated';

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
