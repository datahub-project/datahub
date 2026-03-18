import { CaretDown } from '@phosphor-icons/react';
import React, { useCallback, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import useClickOutside from '@components/components/Utils/ClickOutside/useClickOutside';

import MoreFilterOption from '@app/searchV2/filters/MoreFilterOption';
import { FilterScenarioType } from '@app/searchV2/filters/render/types';
import { useFilterRendererRegistry } from '@app/searchV2/filters/render/useFilterRenderer';
import { SearchFilterLabel } from '@app/searchV2/filters/styledComponents';
import { FilterPredicate } from '@app/searchV2/filters/types';
import useSearchFilterAnalytics from '@app/searchV2/filters/useSearchFilterAnalytics';
import { getNumActiveFiltersForGroupOfFilters } from '@app/searchV2/filters/utils';
import { useAppConfig } from '@src/app/useAppConfig';

import { FacetFilterInput, FacetMetadata } from '@types';

const DropdownWrapper = styled.div`
    position: relative;
`;

const DropdownPanel = styled.div`
    position: absolute;
    top: calc(100% + 4px);
    left: 0;
    z-index: 1050;
    background-color: ${(props) => props.theme.colors.bg};
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 12px;
    box-shadow: ${(props) => props.theme.colors.shadowMd};
    min-width: 200px;
    max-width: 400px;
    padding: 4px;
`;

const CaretIcon = styled(CaretDown)<{ $isOpen?: boolean }>`
    transition: transform 0.2s ease;
    ${(props) => props.$isOpen && 'transform: rotate(180deg);'}
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
    const wrapperRef = useRef<HTMLDivElement>(null);

    function updateFiltersAndClose(newFilters: FacetFilterInput[]) {
        onChangeFilters(newFilters);
        setIsMenuOpen(false);
    }

    function toggleMenu() {
        if (!isMenuOpen) trackShowMoreEvent(activeFilters.length, filters.length);
        setIsMenuOpen((prev) => !prev);
    }

    const handleClickOutside = useCallback(() => setIsMenuOpen(false), []);
    const clickOutsideOptions = useMemo(() => ({ wrappers: [wrapperRef] }), []);
    useClickOutside(handleClickOutside, clickOutsideOptions);

    return (
        <DropdownWrapper ref={wrapperRef}>
            <SearchFilterLabel data-testid="more-filters-dropdown" $isActive={!!numActiveFilters} onClick={toggleMenu}>
                More {numActiveFilters ? `(${numActiveFilters}) ` : ''}
                <CaretIcon size={12} $isOpen={isMenuOpen} />
            </SearchFilterLabel>
            {isMenuOpen && (
                <DropdownPanel>
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
                </DropdownPanel>
            )}
        </DropdownWrapper>
    );
}
