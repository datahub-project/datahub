import { CaretDownFilled } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import MoreFilterOption from '@app/searchV2/filters/MoreFilterOption';
import { FilterScenarioType } from '@app/searchV2/filters/render/types';
import { useFilterRendererRegistry } from '@app/searchV2/filters/render/useFilterRenderer';
import { SearchFilterLabel } from '@app/searchV2/filters/styledComponents';
import { FilterPredicate } from '@app/searchV2/filters/types';
import useSearchFilterAnalytics from '@app/searchV2/filters/useSearchFilterAnalytics';
import { getNumActiveFiltersForGroupOfFilters } from '@app/searchV2/filters/utils';
import { useAppConfig } from '@src/app/useAppConfig';

import { FacetFilterInput, FacetMetadata } from '@types';

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
    filterPredicates: FilterPredicate[];
    onChangeFilters: (newFilters: FacetFilterInput[]) => void;
}

export default function MoreFilters({ filters, filterPredicates, activeFilters, onChangeFilters }: Props) {
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
                                filterPredicates={filterPredicates}
                                onChangeFilters={updateFiltersAndClose}
                            />
                        );
                    })}
                </DropdownMenu>
            )}
            open={isMenuOpen}
            onOpenChange={onOpenChange}
        >
            <SearchFilterLabel data-testid="more-filters-dropdown" $isActive={!!numActiveFilters}>
                More {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                <CaretDownFilled style={{ fontSize: '12px', height: '12px' }} />
            </SearchFilterLabel>
        </Dropdown>
    );
}
