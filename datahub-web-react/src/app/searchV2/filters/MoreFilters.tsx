import { Menu } from '@components';
import React, { useCallback, useMemo, useRef, useState } from 'react';

import { ItemType } from '@components/components/Menu/types';
import useClickOutside from '@components/components/Utils/ClickOutside/useClickOutside';

import MoreFilterOption from '@app/searchV2/filters/MoreFilterOption';
import { SearchFilterBase } from '@app/searchV2/filters/SearchFilterBase';
import { FilterScenarioType } from '@app/searchV2/filters/render/types';
import { useFilterRendererRegistry } from '@app/searchV2/filters/render/useFilterRenderer';
import { FilterPredicate } from '@app/searchV2/filters/types';
import useSearchFilterAnalytics from '@app/searchV2/filters/useSearchFilterAnalytics';
import { getNumActiveFiltersForGroupOfFilters, useGetFilterDisplayName } from '@app/searchV2/filters/utils';
import { useAppConfig } from '@src/app/useAppConfig';

import { FacetFilterInput, FacetMetadata } from '@types';

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

    const updateFiltersAndClose = useCallback(
        (newFilters: FacetFilterInput[]) => {
            onChangeFilters(newFilters);
            setIsMenuOpen(false);
        },
        [onChangeFilters],
    );

    function toggleMenu() {
        if (!isMenuOpen) trackShowMoreEvent(activeFilters.length, filters.length);
        setIsMenuOpen((prev) => !prev);
    }

    const handleClickOutside = useCallback(() => setIsMenuOpen(false), []);
    const clickOutsideOptions = useMemo(() => ({ wrappers: [wrapperRef] }), []);
    useClickOutside(handleClickOutside, clickOutsideOptions);

    const getFilterDisplayName = useGetFilterDisplayName();

    const filtersToRender = useMemo(
        () =>
            filters.filter((filter) => {
                // All filters without specific renderers could be rendered
                if (!filterRendererRegistry.hasRenderer(filter.field)) return true;

                return filterRendererRegistry.canBeRendered(filter.field, config);
            }),
        [filters, config, filterRendererRegistry],
    );

    const menuItems: ItemType[] = useMemo(
        () =>
            filtersToRender.map((filter) => ({
                key: filter.field,
                title: filterRendererRegistry.hasRenderer(filter.field)
                    ? filterRendererRegistry.getName(filter.field)
                    : getFilterDisplayName(filter),
                type: 'item',
                children: [
                    {
                        key: `${filter.field}-children`,
                        type: 'custom',
                        render: () => {
                            return filterRendererRegistry.hasRenderer(filter.field) ? (
                                filterRendererRegistry.render(filter.field, {
                                    scenario: FilterScenarioType.SEARCH_V2_DROPDOWN_ONLY,
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
                        },
                    },
                ],
            })),
        [
            filtersToRender,
            filterRendererRegistry,
            getFilterDisplayName,
            activeFilters,
            filterPredicates,
            updateFiltersAndClose,
            config,
        ],
    );

    return (
        <Menu items={menuItems} onOpenChange={(isOpen) => setIsMenuOpen(isOpen)}>
            <SearchFilterBase
                data-testid="more-filters-dropdown"
                isActive={!!numActiveFilters}
                isOpen={isMenuOpen}
                onClick={toggleMenu}
            >
                More {numActiveFilters ? `(${numActiveFilters}) ` : ''}
            </SearchFilterBase>
        </Menu>
    );
}
