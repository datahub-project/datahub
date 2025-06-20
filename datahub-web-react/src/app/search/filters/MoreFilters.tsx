import { PlusOutlined } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import MoreFilterOption from '@app/search/filters/MoreFilterOption';
import { FilterScenarioType } from '@app/search/filters/render/types';
import { useFilterRendererRegistry } from '@app/search/filters/render/useFilterRenderer';
import { SearchFilterLabel } from '@app/search/filters/styledComponents';
import useSearchFilterAnalytics from '@app/search/filters/useSearchFilterAnalytics';
import { getNumActiveFiltersForGroupOfFilters } from '@app/search/filters/utils';
import { useAppConfig } from '@src/app/useAppConfig';

import { FacetFilterInput, FacetMetadata } from '@types';

const StyledPlus = styled(PlusOutlined)`
    svg {
        height: 12px;
        width: 12px;
    }
`;

const DropdownMenu = styled.div<{ padding?: string }>`
    background-color: white;
    border-radius: 5px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    overflow: hidden;
    min-width: 200px;
    max-width: 240px;

    ${(props) => props.padding !== undefined && `padding: ${props.padding};`}
`;

interface Props {
    filters: FacetMetadata[];
    activeFilters: FacetFilterInput[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function MoreFilters({ filters, activeFilters, onChangeFilters }: Props) {
    const { trackShowMoreEvent } = useSearchFilterAnalytics();
    const [isMenuOpen, setIsMenuOpen] = useState(false);
    const numActiveFilters = getNumActiveFiltersForGroupOfFilters(activeFilters, filters);
    const filterRendererRegistry = useFilterRendererRegistry();
    const { config } = useAppConfig();

    function updateFiltersAndClose(newFilters: FacetFilterInput[]) {
        onChangeFilters(newFilters);
        setIsMenuOpen(false);
    }

    function onOpenChange(isOpen: boolean) {
        if (isOpen) trackShowMoreEvent(activeFilters.length, filters.length);
        setIsMenuOpen(isOpen);
    }

    return (
        <Dropdown
            trigger={['click']}
            dropdownRender={() => (
                <DropdownMenu padding="4px 0px">
                    {filters.map((filter) => {
                        return filterRendererRegistry.hasRenderer(filter.field) ? (
                            filterRendererRegistry.render(filter.field, {
                                scenario: FilterScenarioType.SEARCH_V2_SECONDARY,
                                filter,
                                activeFilters,
                                onChangeFilters: updateFiltersAndClose,
                                config,
                            })
                        ) : (
                            <MoreFilterOption
                                key={filter.field}
                                filter={filter}
                                activeFilters={activeFilters}
                                onChangeFilters={updateFiltersAndClose}
                            />
                        );
                    })}
                </DropdownMenu>
            )}
            open={isMenuOpen}
            onOpenChange={onOpenChange}
        >
            <SearchFilterLabel data-testid="more-filters-dropdown" isActive={!!numActiveFilters}>
                <StyledPlus />
                More Filters {numActiveFilters ? `(${numActiveFilters}) ` : ''}
            </SearchFilterLabel>
        </Dropdown>
    );
}
