/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { RightOutlined } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React, { useRef } from 'react';
import styled from 'styled-components';

import OptionsDropdownMenu from '@app/search/filters/OptionsDropdownMenu';
import { IconWrapper } from '@app/search/filters/SearchFilterView';
import { MoreFilterOptionLabel } from '@app/search/filters/styledComponents';
import useSearchFilterDropdown from '@app/search/filters/useSearchFilterDropdown';
import { getFilterDropdownIcon, useElementDimensions, useFilterDisplayName } from '@app/search/filters/utils';

import { FacetFilterInput, FacetMetadata } from '@types';

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
