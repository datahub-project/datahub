import { CaretDownFilled } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FacetFilterInput, FacetMetadata } from '../../../types.generated';
import MoreFilterOption from './MoreFilterOption';
import { getNumActiveFiltersForGroupOfFilters } from './utils';
import { SearchFilterLabel } from './styledComponents';
import useSearchFilterAnalytics from './useSearchFilterAnalytics';
import { useFilterRendererRegistry } from './render/useFilterRenderer';
import { FilterScenarioType } from './render/types';
import { FilterPredicate } from './types';

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
