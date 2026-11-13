import { Menu } from '@components';
import React, { useCallback, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { ItemType } from '@components/components/Menu/types';

import { renderFilterMenuItem } from '@app/searchV2/filters/FilterMenuItem';
import MoreFilterOption from '@app/searchV2/filters/MoreFilterOption';
import { SearchFilterBase } from '@app/searchV2/filters/SearchFilterBase';
import { FilterScenarioType } from '@app/searchV2/filters/render/types';
import { useFilterRendererRegistry } from '@app/searchV2/filters/render/useFilterRenderer';
import { mergeFilterChanges } from '@app/searchV2/filters/mergeFilterChanges';
import { FilterPredicate } from '@app/searchV2/filters/types';
import { useFilterMenuOverlay } from '@app/searchV2/filters/useFilterMenuOverlay';
import useSearchFilterAnalytics from '@app/searchV2/filters/useSearchFilterAnalytics';
import { FilterMenuProvider, useFilterMenuCommits } from '@app/searchV2/filters/value/FilterMenuContext';
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
    const { triggerRef, overlayClassName, overlayStyle } = useFilterMenuOverlay<HTMLDivElement>(isMenuOpen);
    const numActiveFilters = getNumActiveFiltersForGroupOfFilters(activeFilters, filters);
    const filterRendererRegistry = useFilterRendererRegistry();
    const { config } = useAppConfig();

    const { registerCommit, commitAll } = useFilterMenuCommits();
    // While the submenus commit together, their changes are merged here and applied once.
    const pendingFiltersRef = useRef<FacetFilterInput[] | null>(null);

    const applyFilters = useCallback(
        (newFilters: FacetFilterInput[]) => {
            if (pendingFiltersRef.current) {
                pendingFiltersRef.current = mergeFilterChanges(activeFilters, pendingFiltersRef.current, newFilters);
            } else {
                onChangeFilters(newFilters);
            }
        },
        [activeFilters, onChangeFilters],
    );

    const commitAndClose = useCallback(
        (applyNow?: () => void) => {
            pendingFiltersRef.current = activeFilters;
            applyNow?.();
            commitAll();
            const pendingFilters = pendingFiltersRef.current;
            pendingFiltersRef.current = null;
            if (pendingFilters !== activeFilters) onChangeFilters(pendingFilters);
            setIsMenuOpen(false);
        },
        [activeFilters, commitAll, onChangeFilters],
    );

    const filterMenu = useMemo(() => ({ registerCommit, commitAndClose }), [registerCommit, commitAndClose]);

    // Boolean renderers apply on click, so they close the menu with it.
    const updateFiltersAndClose = useCallback(
        (newFilters: FacetFilterInput[]) => commitAndClose(() => applyFilters(newFilters)),
        [commitAndClose, applyFilters],
    );

    // Value submenus stage their selection until the menu closes, by any route: an outside click,
    // Escape, or the trigger. antd reports all of them here.
    const onOpenChange = (open: boolean) => {
        if (open) {
            if (!isMenuOpen) trackShowMoreEvent(activeFilters.length, filters.length);
            setIsMenuOpen(true);
        } else {
            commitAndClose();
        }
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
                                        onChangeFilters={applyFilters}
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
            applyFilters,
            updateFiltersAndClose,
            config,
        ],
    );

    return (
        <FilterMenuProvider value={filterMenu}>
            <Menu
                items={menuItems}
                open={isMenuOpen}
                onOpenChange={onOpenChange}
                overlayClassName={overlayClassName}
                overlayStyle={overlayStyle}
            >
                <SearchFilterBase
                    ref={triggerRef}
                    data-testid="more-filters-dropdown"
                    isActive={!!numActiveFilters}
                    isOpen={isMenuOpen}
                >
                    {t('filters.moreCount', { count: numActiveFilters })}
                </SearchFilterBase>
            </Menu>
        </FilterMenuProvider>
    );
}
