import { Menu } from '@components';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { ItemType } from '@components/components/Menu/types';

import { renderFilterMenuItem } from '@app/searchV2/filters/FilterMenuItem';
import MoreFilterOption from '@app/searchV2/filters/MoreFilterOption';
import { SearchFilterBase } from '@app/searchV2/filters/SearchFilterBase';
import { FilterScenarioType } from '@app/searchV2/filters/render/types';
import { useFilterRendererRegistry } from '@app/searchV2/filters/render/useFilterRenderer';
import { FilterPredicate } from '@app/searchV2/filters/types';
import useSearchFilterAnalytics from '@app/searchV2/filters/useSearchFilterAnalytics';
import { CloseFilterMenuProvider } from '@app/searchV2/filters/value/CloseFilterMenuContext';
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
    const { t } = useTranslation('search');
    const { trackShowMoreEvent } = useSearchFilterAnalytics();
    const [isMenuOpen, setIsMenuOpen] = useState(false);
    const numActiveFilters = getNumActiveFiltersForGroupOfFilters(activeFilters, filters);
    const filterRendererRegistry = useFilterRendererRegistry();
    const { config } = useAppConfig();

    const closeMenu = useCallback(() => setIsMenuOpen(false), []);

    // Boolean renderers apply on click, so they close the menu with it. Value submenus apply when
    // they hide, which includes moving to another item, so they close only on explicit commits.
    const updateFiltersAndClose = useCallback(
        (newFilters: FacetFilterInput[]) => {
            onChangeFilters(newFilters);
            closeMenu();
        },
        [onChangeFilters, closeMenu],
    );

    // The menu is controlled so that applying a filter can close it; antd handles outside clicks,
    // including clicks inside the portaled submenus.
    const onOpenChange = (open: boolean) => {
        if (open && !isMenuOpen) trackShowMoreEvent(activeFilters.length, filters.length);
        setIsMenuOpen(open);
    };

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
            filtersToRender.map((filter) => {
                const title =
                    (filterRendererRegistry.hasRenderer(filter.field) &&
                        filterRendererRegistry.getName(filter.field)) ||
                    getFilterDisplayName(filter);
                return {
                    key: filter.field,
                    title,
                    dataTestId: `more-filter-${title.replace(/\s/g, '-')}`,
                    type: 'item',
                    render: renderFilterMenuItem,
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
                                        onChangeFilters={onChangeFilters}
                                    />
                                );
                            },
                        },
                    ],
                };
            }),
        [
            filtersToRender,
            filterRendererRegistry,
            getFilterDisplayName,
            activeFilters,
            filterPredicates,
            onChangeFilters,
            updateFiltersAndClose,
            config,
        ],
    );

    return (
        <CloseFilterMenuProvider value={closeMenu}>
            <Menu items={menuItems} open={isMenuOpen} onOpenChange={onOpenChange}>
                <SearchFilterBase
                    data-testid="more-filters-dropdown"
                    isActive={!!numActiveFilters}
                    isOpen={isMenuOpen}
                >
                    {t('filters.moreCount', { count: numActiveFilters })}
                </SearchFilterBase>
            </Menu>
        </CloseFilterMenuProvider>
    );
}
